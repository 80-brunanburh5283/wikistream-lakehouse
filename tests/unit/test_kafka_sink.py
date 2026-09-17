"""Tests for the Kafka sink, with a recording double in place of a broker.

`KafkaSink` takes an injected producer for exactly this reason. What is worth
asserting here is not that librdkafka works — it does — but the decisions layered
on top of it: which key each frame gets, what happens when the local queue is
full, and whether an incomplete flush is allowed to look like success. All three
are behaviours that a broker-backed test would make slow and flaky, and that no
test at all would leave to hope.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from confluent_kafka import KafkaException
from doubles import RecordingProducer

from wikistream.config import Settings
from wikistream.producer.kafka_sink import KafkaSink
from wikistream.sources.base import SourceEvent

pytestmark = pytest.mark.unit


def _settings(**overrides: Any) -> Settings:
    # _env_file=None so a developer's local .env cannot change what these assert.
    return Settings(_env_file=None, **overrides)


def _event(domain: str | None = "en.wikipedia.org", *, event_id: str = "abc") -> SourceEvent:
    payload: dict[str, Any] = {"meta": {"id": event_id, "dt": "2026-09-17T08:00:00Z"}}
    if domain is not None:
        payload["meta"]["domain"] = domain
    raw = json.dumps(payload)
    return SourceEvent(raw=raw, payload=payload, cursor=None)


def test_frames_are_keyed_by_wiki_domain():
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event("de.wikipedia.org"))

    topic, key, _ = producer.messages[0]
    assert topic == "wiki.recentchange"
    assert key == b"de.wikipedia.org"


def test_the_value_is_the_frame_verbatim_not_a_re_serialisation():
    # The distinction bronze depends on. This raw text has spacing and key order
    # that `json.dumps(json.loads(raw))` would not reproduce, so if the sink ever
    # round-trips through a dict this fails.
    raw = '{"meta":  {"domain": "en.wikipedia.org", "id": "x", "dt": "2026-09-17T08:00:00Z"}}'
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(SourceEvent(raw=raw, payload=json.loads(raw)))

    assert producer.messages[0][2] == raw.encode("utf-8")


def test_a_malformed_frame_still_reaches_kafka_unkeyed():
    # Ingest is not a validation gate: the frame goes through, with no key, and the
    # loss of per-wiki ordering for that record is counted rather than hidden.
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(SourceEvent(raw="{not json", payload=None))

    assert producer.messages[0][1] is None
    assert producer.messages[0][2] == b"{not json"
    assert sink.stats.unkeyed == 1
    assert sink.stats.produced == 1


def test_a_decoded_frame_with_no_domain_is_also_unkeyed():
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event(domain=None))

    assert producer.messages[0][1] is None
    assert sink.stats.unkeyed == 1


def test_produced_and_delivered_are_counted_separately():
    # The gap between the two is the only way to see a producer that is enqueuing
    # happily into a broker that is acknowledging nothing.
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event())
    sink.send(_event())

    assert sink.stats.produced == 2
    # The hot-path poll(0) serves whatever is already due, so by the second send
    # the first record has been acknowledged and one is still outstanding.
    assert sink.stats.delivered == 1
    assert sink.stats.in_flight == 1

    sink.flush(1.0)

    assert sink.stats.delivered == 2
    assert sink.stats.in_flight == 0


def test_a_delivery_error_increments_failed_not_delivered():
    producer = RecordingProducer()
    producer.delivery_error = "Broker: Message too large"
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event())
    sink.flush(1.0)

    assert sink.stats.failed == 1
    assert sink.stats.delivered == 0


def test_a_full_queue_makes_send_wait_rather_than_drop():
    # The one behaviour in this file that is a data-loss bug if it regresses. With
    # capacity 1, the second send must hit BufferError, poll the queue, and retry —
    # never return having discarded the record.
    producer = RecordingProducer(capacity=1)
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event(event_id="first"))
    sink.send(_event(event_id="second"))

    assert len(producer.messages) == 2
    assert sink.stats.produced == 2
    assert sink.stats.backpressure_waits == 1
    # It waited on the queue rather than spinning: the retry poll has a real
    # timeout, unlike the hot-path poll(0).
    assert max(producer.poll_calls) > 0


def test_bytes_produced_counts_encoded_payload_bytes():
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)
    # Two bytes in UTF-8, one character: the counter must measure what goes on the
    # wire, because it is compared against the broker's on-disk size in
    # scripts/measure_throughput.sh.
    raw = '{"meta": {"domain": "de.wikipedia.org", "id": "x", "title": "ü"}}'

    sink.send(SourceEvent(raw=raw, payload=json.loads(raw)))

    assert sink.stats.bytes_produced == len(raw.encode("utf-8"))
    assert sink.stats.bytes_produced == len(raw) + 1


def test_close_raises_when_records_were_abandoned():
    # Exiting 0 here would tell Dagster the run succeeded while part of its output
    # does not exist, which is the failure mode that makes a pipeline untrustworthy.
    producer = RecordingProducer()
    producer.undrainable = 3
    sink = KafkaSink(_settings(), producer=producer)

    sink.send(_event())

    with pytest.raises(KafkaException, match="3 records were not delivered"):
        sink.close(1.0)


def test_close_flushes_with_the_supplied_timeout():
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    sink.close(7.5)

    assert producer.flush_calls == [7.5]


def test_queue_depth_reports_the_local_backlog():
    producer = RecordingProducer()
    sink = KafkaSink(_settings(), producer=producer)

    assert sink.queue_depth == 0
    sink.send(_event())
    assert sink.queue_depth == 1


def test_idempotence_and_acks_are_both_set_explicitly():
    # librdkafka rejects a config where another setting contradicts idempotence
    # rather than downgrading silently, so acks=all is stated rather than implied.
    config = KafkaSink._config(_settings())

    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"


def test_the_send_buffer_is_bounded():
    # The default is 1 GB, which on a laptop turns a broker outage into an OOM kill
    # instead of an error message.
    config = KafkaSink._config(_settings())

    assert config["queue.buffering.max.kbytes"] == 65_536
    assert config["delivery.timeout.ms"] == 300_000


def test_compression_and_batching_come_from_settings():
    config = KafkaSink._config(_settings(kafka_compression_type="lz4", kafka_linger_ms=42))

    assert config["compression.type"] == "lz4"
    assert config["linger.ms"] == 42


def test_summary_exposes_every_counter_the_scripts_read():
    # scripts/measure_throughput.sh and scripts/compare_compression.sh parse these
    # keys out of the flush log line. Renaming one silently breaks the docs.
    sink = KafkaSink(_settings(), producer=RecordingProducer())

    assert set(sink.summary()) == {
        "produced",
        "delivered",
        "failed",
        "unkeyed",
        "backpressure_waits",
        "bytes_produced",
    }
