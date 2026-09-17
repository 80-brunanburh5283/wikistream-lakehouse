"""What `to_bronze_rows` does to a frame, asserted against a real Spark.

These need a JVM and nothing else — no Kafka, no catalog, no Docker — so they run
in the routine loop (`make test-spark`) rather than only when the stack is up.
`tests/integration/test_bronze.py` proves the same mapping end to end through the
broker; this file exists because the interesting cases are the malformed ones, and
producing a malformed frame through Kafka to assert one column is a slow way to ask
a fast question.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pyspark.sql import functions as F

from wikistream.streaming.bronze import BRONZE_COLUMNS, to_bronze_rows

pytestmark = pytest.mark.spark


def _kafka_like(spark, frames: list[str]):
    """A DataFrame shaped like the Kafka source's output.

    `value` is binary because that is what the Kafka source gives: getting the type
    wrong here would let a test pass against a cast that does not happen in
    production.
    """
    rows = [
        (frame.encode("utf-8"), "wiki.recentchange", index % 3, index, None)
        for index, frame in enumerate(frames)
    ]
    return spark.createDataFrame(
        rows,
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    ).withColumn("timestamp", F.lit("2026-09-17T04:00:00Z").cast("timestamp"))


def test_ansi_mode_is_on_so_this_file_is_testing_something(spark) -> None:
    """The premise of the test below.

    Every assertion here about malformed input is only interesting while ANSI mode
    is enabled, because with ANSI off a bad cast returns null and there is nothing
    to defend against. Spark 4.0 turned it on by default; if a future version or a
    stray config turns it off, this test says so rather than letting the others
    quietly stop proving anything.
    """
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


def test_a_malformed_event_time_yields_null_instead_of_killing_the_batch(spark) -> None:
    """The poison-pill case: one bad `meta.dt` must not fail the micro-batch.

    Under ANSI mode `to_timestamp('nonsense')` raises `CAST_INVALID_INPUT` from
    inside the generated iterator. In a streaming job that exception fails the
    batch, the checkpoint does not advance, and the retry meets the same record —
    so a single malformed frame stops ingestion for the whole topic until someone
    intervenes. `try_to_timestamp` turns it into a null that travels downstream and
    gets quarantined by silver.
    """
    frames = [
        json.dumps({"meta": {"id": "good", "dt": "2026-09-17T04:00:00Z"}}),
        json.dumps({"meta": {"id": "bad", "dt": "nonsense"}}),
    ]

    landed = {
        row["event_id"]: row["event_time"]
        for row in to_bronze_rows(_kafka_like(spark, frames)).collect()
    }

    assert landed["bad"] is None
    assert landed["good"] is not None


def test_an_unparseable_frame_still_lands_with_its_payload(spark) -> None:
    """Bronze appends garbage rather than dropping it, because bronze is the audit log.

    The three interpreted columns come out null and `raw_payload` holds exactly what
    arrived, which is what makes a fix replayable instead of merely reported.
    """
    frames = ["{this is not json", json.dumps({"meta": {"id": "ok", "dt": "2026-09-17T04:00:00Z"}})]

    rows = to_bronze_rows(_kafka_like(spark, frames)).collect()
    garbage = next(row for row in rows if row["raw_payload"] == "{this is not json")

    assert garbage["event_id"] is None
    assert garbage["event_time"] is None
    assert garbage["schema_uri"] is None
    # The Kafka coordinates are still there, so the bad record can be found on the
    # topic and replayed after a fix.
    assert garbage["kafka_offset"] == 0
    assert garbage["kafka_topic"] == "wiki.recentchange"


def test_the_projection_is_exactly_the_bronze_contract(spark) -> None:
    """No extra columns, no missing ones, in the declared order.

    Iceberg matches by name, so an extra column is a write failure at runtime rather
    than a silently ignored one. Catching it here costs milliseconds.
    """
    frame = json.dumps({"meta": {"id": "x", "dt": "2026-09-17T04:00:00Z"}})

    assert tuple(to_bronze_rows(_kafka_like(spark, [frame])).columns) == BRONZE_COLUMNS


def test_every_row_of_a_batch_shares_one_ingested_at(
    spark, sample_events: list[dict[str, Any]]
) -> None:
    """`current_timestamp()` is batch-deterministic, which is what makes `ingest_date` sane.

    If it were evaluated per row, a batch running across midnight would split across
    two partitions and `ingest_date` would stop being a property of the commit.
    """
    frames = [json.dumps(event, ensure_ascii=False) for event in sample_events[:50]]

    rows = to_bronze_rows(_kafka_like(spark, frames))

    assert rows.select("ingested_at").distinct().count() == 1
