"""The Kafka side of ingest: keying, idempotence, and honest delivery counters.

## What `enable.idempotence` does and does not buy

It is the single most over-claimed setting in Kafka. What it actually does is
narrow: the producer stamps each batch with a producer id and a monotonic
sequence number per partition, and the broker rejects a batch whose sequence it
has already appended. So an *internal retry* — the producer sent a batch, the
broker wrote it, the acknowledgement was lost, the producer sent it again — no
longer results in two copies. Within one producer session, for records this
producer sent, retries do not duplicate.

What it does not do, and what this pipeline therefore has to handle downstream:

* **It does not survive the process.** A restarted producer gets a new producer
  id. Records the old session sent but did not see acknowledged will be sent
  again by the new session and appended again, because the broker has no reason
  to connect the two.
* **It does not know about the upstream.** Wikimedia resumes an SSE connection at
  or before the supplied `Last-Event-ID`, so a reconnect replays events this
  producer has already read. Those are new records as far as Kafka is concerned:
  different sequence numbers, same event. Idempotence cannot see the duplication
  because the duplication happened before the producer.
* **It does not make the pipeline exactly-once.** It removes one source of
  duplicates out of three. The other two are handled by making bronze
  append-only — duplicates there are data, not corruption — and by having silver
  MERGE on `meta.id`, which is idempotent under replay.

That chain is the reason this repository exists, so it is written down here rather
than assumed.

## Ordering

The key is `meta.domain` — the wiki an edit belongs to. Same key, same partition,
so edits to `en.wikipedia.org` stay in order relative to each other. That is the
only ordering guarantee on offer and the only one anything downstream needs;
global ordering across all wikis would require one partition and would cap
throughput at one consumer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast

from confluent_kafka import KafkaException, Producer

from wikistream.events import partition_key
from wikistream.logging import get_logger

if TYPE_CHECKING:
    from wikistream.config import Settings
    from wikistream.sources.base import SourceEvent

log = get_logger(__name__)


class _ProducerLike(Protocol):
    """The four `confluent_kafka.Producer` methods this module uses.

    Declared so tests can substitute a recording double without a broker. Naming
    the surface also keeps the dependency visible: if this grows past four
    methods, the sink has started doing something other than sinking.
    """

    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...
    def __len__(self) -> int: ...


@dataclass
class SinkStats:
    """Counters for what was handed to Kafka and what Kafka confirmed.

    `produced` and `delivered` are deliberately separate. `produced` is what this
    process enqueued; `delivered` is what a broker acknowledged. A gap between
    them that does not close is the difference between a producer that looks busy
    and a producer that is achieving nothing, and it is invisible if you count
    only one of the two.
    """

    #: Frames accepted into the local queue.
    produced: int = 0
    #: Frames a broker acknowledged, counted in the delivery callback.
    delivered: int = 0
    #: Frames a broker rejected permanently, counted in the delivery callback.
    failed: int = 0
    #: Frames enqueued with no key because the payload had no usable domain.
    #: These round-robin across partitions and so lose per-wiki ordering; the
    #: count is here because that is a real loss, not a detail to bury.
    unkeyed: int = 0
    #: Times the local queue was full and `send` had to wait for it to drain.
    #: Sustained backpressure means the broker, not the source, is the bottleneck.
    backpressure_waits: int = 0
    bytes_produced: int = 0

    @property
    def in_flight(self) -> int:
        """Enqueued but not yet acknowledged or failed."""
        return self.produced - self.delivered - self.failed


class KafkaSink:
    """Writes raw event frames to one Kafka topic, keyed for per-wiki ordering."""

    def __init__(
        self,
        settings: Settings,
        *,
        producer: _ProducerLike | None = None,
    ) -> None:
        """Build the sink. Connects lazily, on the first `send`.

        Args:
            settings: Topic, broker list and the buffering bounds below.
            producer: Injected Kafka producer. Production passes None and gets a
                real `confluent_kafka.Producer`; tests pass a double so the
                keying and counter behaviour can be asserted with no broker.
        """
        self._settings = settings
        self._topic = settings.kafka_topic
        self._stats = SinkStats()
        if producer is None:
            # `confluent_kafka` ships no type information, so the constructor's
            # result is Any; the cast is what makes the rest of this file
            # type-checked against `_ProducerLike` rather than against nothing.
            producer = cast("_ProducerLike", Producer(self._config(settings)))
        self._producer: _ProducerLike = producer

    @staticmethod
    def _config(settings: Settings) -> dict[str, Any]:
        """Client configuration, with the reasoning for each non-default."""
        return {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # Identifies this process in the broker's logs and in
            # `kafka-consumer-groups.sh` output. Worth setting: "rdkafka#producer-1"
            # tells an operator nothing about which of several clients misbehaved.
            "client.id": "wikistream-producer",
            # See the module docstring for the precise scope of this guarantee.
            # Setting it also pins acks=all, max.in.flight<=5 and retries=INT_MAX;
            # librdkafka rejects the config outright rather than silently
            # downgrading if another setting contradicts it, which is why acks is
            # stated explicitly below instead of left implicit.
            "enable.idempotence": True,
            "acks": "all",
            # 100 ms of batching. Below the 30-second micro-batch trigger by two
            # orders of magnitude, so it costs no observable freshness and cuts
            # the request rate by roughly the batch size.
            "linger.ms": settings.kafka_linger_ms,
            "batch.size": settings.kafka_batch_size_bytes,
            "compression.type": settings.kafka_compression_type,
            # A bounded buffer. The default is 1 GB, which turns a broker outage
            # into a slow OOM instead of an error message.
            "queue.buffering.max.kbytes": settings.kafka_buffer_max_kbytes,
            # How long a record may take to reach an acknowledgement before it is
            # failed to the delivery callback. Five minutes: long enough to ride
            # out a broker restart, short enough that a permanently broken cluster
            # surfaces as failures rather than as unbounded in-flight records.
            "delivery.timeout.ms": 300_000,
        }

    @property
    def stats(self) -> SinkStats:
        """Live delivery counters."""
        return self._stats

    @property
    def queue_depth(self) -> int:
        """Records enqueued locally and not yet handed to a broker."""
        return len(self._producer)

    def send(self, event: SourceEvent) -> None:
        """Enqueue one frame. Returns as soon as it is queued, not delivered.

        The value is `event.raw` encoded as UTF-8 — the frame exactly as it
        arrived, including frames that are not valid JSON. Ingest is not the place
        to decide what is well-formed; that decision belongs to the silver layer,
        which has a table to put the rejects in.
        """
        key = partition_key(event.payload) if event.payload is not None else None
        if key is None:
            self._stats.unkeyed += 1

        value = event.raw.encode("utf-8")
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    key=key.encode("utf-8") if key is not None else None,
                    value=value,
                    on_delivery=self._on_delivery,
                )
            except BufferError:
                # The local queue is full: the broker is slower than the source.
                # Serving the queue is the only useful thing to do here, and
                # dropping the record instead would be a silent data loss that no
                # amount of downstream deduplication can repair.
                self._stats.backpressure_waits += 1
                self._producer.poll(0.5)
                continue
            break

        self._stats.produced += 1
        self._stats.bytes_produced += len(value)
        # Non-blocking: serves any delivery callbacks that are already due. Without
        # a poll on the hot path the callbacks queue up unboundedly and the
        # delivered counter only moves at flush time.
        self._producer.poll(0)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        """Delivery callback. Runs on the librdkafka poll thread, so it stays cheap."""
        # The message is not inspected: on success there is nothing to learn from
        # it, and on failure the error carries the reason. Accepted only because
        # librdkafka calls this positionally with both.
        del msg
        if err is not None:
            self._stats.failed += 1
            log.error(
                "kafka delivery failed",
                extra={
                    "error": str(err),
                    "topic": self._topic,
                    "failed_total": self._stats.failed,
                },
            )
            return
        self._stats.delivered += 1

    def poll(self, timeout: float = 0.0) -> int:
        """Serve delivery callbacks. Returns the number of events served."""
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until the queue drains or `timeout` expires.

        Returns the number of records still queued — non-zero means records were
        abandoned, which the caller should treat as an error rather than log at
        info level and move on.
        """
        return self._producer.flush(timeout)

    def close(self, timeout: float = 30.0) -> None:
        """Flush and report. Safe to call from a shutdown path.

        Raises:
            KafkaException: if the queue did not drain within `timeout`. Exiting
                zero after abandoning records would tell the orchestrator the run
                succeeded when part of its output does not exist.
        """
        started = time.monotonic()
        remaining = self.flush(timeout)
        log.info(
            "kafka sink flushed",
            extra={
                "seconds": round(time.monotonic() - started, 2),
                "remaining": remaining,
                **self.summary(),
            },
        )
        if remaining:
            msg = f"{remaining} records were not delivered before shutdown"
            raise KafkaException(msg)

    def summary(self) -> dict[str, int]:
        """Counters in a shape suitable for a structured log record."""
        return {
            "produced": self._stats.produced,
            "delivered": self._stats.delivered,
            "failed": self._stats.failed,
            "unkeyed": self._stats.unkeyed,
            "backpressure_waits": self._stats.backpressure_waits,
            "bytes_produced": self._stats.bytes_produced,
        }
