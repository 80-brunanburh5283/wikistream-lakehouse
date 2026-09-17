"""How a Dagster run reaches the things it orchestrates.

Three resources, and what is *not* here is the interesting part: there is no Spark
resource. The bronze and silver jobs are continuous Structured Streaming queries
started by Docker Compose and supervised by it. Dagster observes them and checks
their output; it does not launch them. ADR-0030 has the reasoning.

Configuration comes from `wikistream.config.Settings` rather than from Dagster's
`EnvVar`. Both would work, and having both would mean two config systems reading
the same `WS_*` variables with two sets of defaults — the kind of divergence that
shows up as a job that works in the container and not on the host.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import trino
from confluent_kafka import Consumer, TopicPartition
from dagster import ConfigurableResource

from wikistream.config import Settings, get_settings


#  Parameterised with itself: a `ConfigurableResource` declares the type it hands
#  to an asset, and these two hand over the resource instance. Left bare — which
#  is how most examples write it — the class is an unparameterised generic and
#  strict mypy is right to object.
class TrinoResource(ConfigurableResource["TrinoResource"]):
    """A Trino connection, opened per query and closed after it.

    Deliberately not pooled. Trino's client protocol is HTTP and a connection
    holds no server-side state worth reusing, so the only thing a pool would add
    here is a socket left open across a fifteen-minute schedule interval — long
    enough for the container network to reset it, and the failure surfaces on the
    next query as a broken pipe rather than as a connection error.
    """

    host: str
    port: int
    user: str
    catalog: str
    #: Statement timeout. An asset check that hangs is worse than one that fails:
    #: it holds a Dagster run open and the next tick queues behind it.
    query_timeout_seconds: int = 120

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> TrinoResource:
        """Build the resource from the same `WS_*` environment the Spark jobs read.

        Takes an optional `Settings` so a test can construct one without touching the
        process environment.
        """
        cfg = settings or get_settings()
        return cls(
            host=cfg.trino_host,
            port=cfg.trino_port,
            user=cfg.trino_user,
            catalog=cfg.iceberg_catalog_name,
        )

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        # `trino.dbapi.connect` carries no annotations, so a strict-typed caller has
        # to say so rather than have every call through it silently become Any.
        connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            http_scheme="http",
            request_timeout=self.query_timeout_seconds,
            # Every timestamp in this project is UTC, in both engines and in the
            # tables. A session in the container's local zone would shift the
            # freshness arithmetic in the asset checks by that offset and nothing
            # would raise.
            timezone="UTC",
        )
        try:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()
        finally:
            connection.close()

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        """Run a query and return every row.

        No streaming variant, deliberately: every query in `sql.py` is an aggregate
        that returns a handful of rows, and offering a cursor would invite one that
        does not.
        """
        with self._cursor() as cursor:
            cursor.execute(sql)
            rows: list[tuple[Any, ...]] = cursor.fetchall()
        return rows

    def fetch_one(self, sql: str) -> tuple[Any, ...] | None:
        """The first row, or `None` for an empty result.

        `None` rather than raising, because an aggregate over an empty table is a
        state the callers report rather than an error.
        """
        rows = self.fetch_all(sql)
        return rows[0] if rows else None

    def execute(self, sql: str) -> None:
        """Run a statement whose result is not read — `ALTER TABLE ... EXECUTE`.

        The `fetchall` is not redundant. Trino's protocol delivers a statement's
        work across successive `nextUri` fetches, so a DDL or maintenance
        statement that is executed but never fetched can be reported as
        submitted while the server is still doing it, or cancelled outright.
        """
        with self._cursor() as cursor:
            cursor.execute(sql)
            cursor.fetchall()


class KafkaResource(ConfigurableResource["KafkaResource"]):
    """Reads topic watermarks. Never consumes, never commits an offset.

    A `Consumer` is the only client in `confluent_kafka` that exposes watermark
    offsets, so one is created — but it is never subscribed and never assigned,
    which means no consumer group state is written and this resource cannot
    interfere with the offsets the Spark jobs track in their own checkpoints.
    """

    bootstrap_servers: str
    topic: str
    #: A group id is mandatory for a `Consumer` even when it never joins a group.
    #: Named for what it is so it is obvious in `kafka-consumer-groups --list`
    #: that this is not a real consumer.
    group_id: str = "wikistream-dagster-observer"
    timeout_seconds: float = 10.0

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> KafkaResource:
        """Point at the same broker and topic the producer writes to."""
        cfg = settings or get_settings()
        return cls(bootstrap_servers=cfg.kafka_bootstrap_servers, topic=cfg.kafka_topic)

    def _consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                # Belt and braces: nothing here subscribes, and if a future
                # change does, it must not silently start committing offsets
                # under a group id the Spark jobs might one day share.
                "enable.auto.commit": False,
            }
        )

    def partition_watermarks(self) -> dict[int, tuple[int, int]]:
        """Return `{partition: (low, high)}` for the configured topic.

        `high` is the offset the next produced record will get, so
        `high - low` is how many records the broker currently holds — not how
        many have ever been produced, because retention deletes from the low end.
        """
        consumer = self._consumer()
        try:
            metadata = consumer.list_topics(topic=self.topic, timeout=self.timeout_seconds)
            topic_metadata = metadata.topics.get(self.topic)
            if topic_metadata is None or topic_metadata.error is not None:
                msg = f"topic {self.topic!r} is not present on {self.bootstrap_servers}"
                raise RuntimeError(msg)
            watermarks: dict[int, tuple[int, int]] = {}
            for partition in sorted(topic_metadata.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(self.topic, partition),
                    timeout=self.timeout_seconds,
                    cached=False,
                )
                watermarks[partition] = (low, high)
        finally:
            consumer.close()
        return watermarks
