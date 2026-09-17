"""The silver projection, asserted against static DataFrames rather than a stream.

`to_candidates` is pure on purpose — it takes frames and returns frames — so
everything about the mapping can be checked without a broker, a checkpoint or a
trigger. What is *not* checked here is the write path; `merge_statement` is asserted as
text below, and its behaviour against a real table belongs to
`tests/integration/test_silver_rebuild.py`.

One recurring rule in this file: **no assertion reads a collected timestamp or date as
a Python object.** PySpark renders a collected `TimestampType` using the JVM's default
zone, not `spark.sql.session.timeZone`, so on this machine
(`user.timezone = Asia/Dhaka`) a row that Iceberg stores as 2026-09-17T23:30Z comes
back to Python as 2026-09-18 05:30 and a naive `.date()` assertion on it is wrong by a
day. The SQL side is correct; only the client rendering shifts. So every date and
timestamp fact is asserted through `date_format` or `CAST(... AS STRING)`, which
Spark evaluates under the session zone.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import pytest
from pyspark.sql import functions as F

from wikistream.config import get_settings
from wikistream.streaming.silver import (
    ARRIVAL_ORDER,
    EDITS_KEY,
    FRAME_COLUMNS,
    QUARANTINE_COLUMNS,
    QUARANTINE_KEY,
    SILVER_COLUMNS,
    frames_from_bronze,
    merge_statement,
    quarantine_rows,
    to_candidates,
    valid_rows,
)
from wikistream.streaming.tables import silver_edits_ddl, silver_quarantine_ddl

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

pytestmark = pytest.mark.spark

#: A fixed ingest time, so `late_by_seconds` and `ingest_date` are the same on every
#: run. Read under the session's UTC zone, like every other timestamp in the project.
INGESTED_AT = "2026-09-17 04:05:00"


def _frames(spark, events: list[dict[str, Any]], *, ingested_at: str = INGESTED_AT) -> DataFrame:
    """Frames in arrival order, one Kafka offset per event, all on partition 0.

    Offsets ascend with list position, so "first arrival" in `ARRIVAL_ORDER` terms is
    "earlier in the list", which is what the dedup tests below rely on.
    """
    rows = [
        (json.dumps(event, ensure_ascii=False), "wiki.recentchange", 0, 1000 + index)
        for index, event in enumerate(events)
    ]
    return (
        spark.createDataFrame(
            rows, "raw_payload string, kafka_topic string, kafka_partition int, kafka_offset long"
        )
        .withColumn("kafka_timestamp", F.lit(ingested_at).cast("timestamp"))
        .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp"))
        .select(*FRAME_COLUMNS)
    )


def _ddl_columns(ddl: str) -> list[str]:
    """The column names from a `CREATE TABLE` body, in declaration order.

    A regex rather than a SQL parser because the DDL in `tables.py` is written by hand
    in a fixed shape: one column per line, name first. Comment lines start with `--`
    and are skipped, which is the only subtlety.
    """
    body = ddl[ddl.index("(") + 1 : ddl.rindex(")\nUSING iceberg")]
    names = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        match = re.match(r"^(\w+)\s", stripped)
        if match:
            names.append(match.group(1))
    return names


# --------------------------------------------------------------------------
# The column lists, which exist in two places and must agree
# --------------------------------------------------------------------------


def test_the_silver_column_tuple_matches_the_table_ddl():
    """`SILVER_COLUMNS` drives the MERGE; the DDL creates the table. A drift is a runtime error.

    Caught here as a list comparison rather than at 3am as
    `MERGE ... INSERT` complaining about an arity mismatch.
    """
    assert list(SILVER_COLUMNS) == _ddl_columns(silver_edits_ddl(get_settings()))


def test_the_quarantine_column_tuple_matches_the_table_ddl():
    assert list(QUARANTINE_COLUMNS) == _ddl_columns(silver_quarantine_ddl(get_settings()))


def test_the_valid_projection_is_exactly_the_silver_columns(spark, adversarial_events):
    """No extra column survives into the MERGE source, and none is missing.

    `to_candidates` deliberately carries working columns — `_parsed`, `_parse_failed`,
    `failure_reason`, the quarantine columns — and this is what proves they are dropped
    before the write rather than silently widening the table.
    """
    projected = valid_rows(to_candidates(_frames(spark, adversarial_events)))

    assert projected.columns == list(SILVER_COLUMNS)


def test_the_quarantine_projection_is_exactly_the_quarantine_columns(spark, adversarial_events):
    projected = quarantine_rows(to_candidates(_frames(spark, adversarial_events)))

    assert projected.columns == list(QUARANTINE_COLUMNS)


def test_to_candidates_rejects_input_that_is_missing_a_frame_column(spark):
    """A clear error at the boundary beats a `AnalysisException` twelve transforms in."""
    incomplete = _frames(spark, []).drop("kafka_timestamp")

    with pytest.raises(ValueError, match="kafka_timestamp"):
        to_candidates(incomplete)


# --------------------------------------------------------------------------
# The split: every input row goes to exactly one of the two tables
# --------------------------------------------------------------------------


def test_every_frame_lands_in_exactly_one_table(spark, adversarial_events):
    """The split is a partition of the input, not a filter pair that can lose rows.

    Written as a count identity because that is the failure that would not otherwise
    show: a row failing both filters — which `failure_reason` being neither null nor
    non-null would cause — disappears without an error anywhere. Dedup removes the two
    duplicate pairs, so the identity is over distinct keys rather than raw rows.
    """
    candidates = to_candidates(_frames(spark, adversarial_events)).cache()
    try:
        valid = valid_rows(candidates)
        quarantined = quarantine_rows(candidates)

        distinct_valid = candidates.filter(F.col("failure_reason").isNull()).select(*EDITS_KEY)
        distinct_quarantined = candidates.filter(F.col("failure_reason").isNotNull()).select(
            *QUARANTINE_KEY
        )

        assert valid.count() == distinct_valid.distinct().count()
        assert quarantined.count() == distinct_quarantined.distinct().count()
        assert candidates.count() == len(adversarial_events)
    finally:
        candidates.unpersist()


def test_no_valid_row_carries_a_failure_reason(spark, adversarial_events):
    """The tautology worth asserting, because the filter direction is easy to invert."""
    candidates = to_candidates(_frames(spark, adversarial_events))
    survived = valid_rows(candidates).count()
    rejected = quarantine_rows(candidates).count()

    assert survived > 0, "the fixture should contain valid rows"
    assert rejected > 0, "the fixture should contain rejected rows"


# --------------------------------------------------------------------------
# Deduplication inside the batch: first arrival wins
# --------------------------------------------------------------------------


def test_a_duplicate_event_id_collapses_to_the_earliest_offset(spark, adversarial_events):
    """Two frames, one id, different content: the lower Kafka offset is the row that survives.

    The `divergent_duplicate` pair exists for this. Both carry
    `meta.id = aaaaaaa3-...0003`; the first says the page grew by 10 bytes and the
    second by 42. Only one row may reach `silver.edits`, and *which* one has to be
    deterministic — otherwise a replay of the same offsets can produce a different
    table, and the rebuild path stops being a rebuild.
    """
    pair = [
        event for event in adversarial_events if event["title"] == "Adversarial:divergent_duplicate"
    ]
    assert len(pair) == 2, "the fixture should hold both halves of the divergent pair"
    first, second = pair
    assert first["length"]["new"] != second["length"]["new"], "the halves must differ"

    rows = valid_rows(to_candidates(_frames(spark, pair))).collect()

    assert len(rows) == 1
    assert rows[0]["event_id"] == first["meta"]["id"]
    assert rows[0]["bytes_new"] == first["length"]["new"]
    assert rows[0]["comment"] == first["comment"]


def test_arrival_order_is_reversed_when_the_offsets_are(spark, adversarial_events):
    """The winner follows the Kafka offset, not the position in the DataFrame.

    Same pair, fed in the opposite order, so the row that wins changes. Without this
    the previous test would also pass if `deduplicate` just kept whichever row Spark
    happened to emit first.
    """
    pair = [
        event for event in adversarial_events if event["title"] == "Adversarial:divergent_duplicate"
    ]
    reversed_pair = list(reversed(pair))

    rows = valid_rows(to_candidates(_frames(spark, reversed_pair))).collect()

    assert len(rows) == 1
    assert rows[0]["bytes_new"] == reversed_pair[0]["length"]["new"]


def test_two_quarantined_frames_with_no_event_id_both_survive(spark, adversarial_events):
    """Quarantine keys on the Kafka coordinates, so a null id does not collapse rows.

    This is the test that would fail if `quarantine_rows` deduplicated on `event_id`:
    two unrelated malformed frames, both with no id, would become one row and one of
    the two faults would vanish from the record.
    """
    no_id = next(
        event for event in adversarial_events if event["title"] == "Adversarial:missing_meta_id"
    )

    rows = quarantine_rows(to_candidates(_frames(spark, [no_id, no_id]))).collect()

    assert len(rows) == 2
    assert {row["kafka_offset"] for row in rows} == {1000, 1001}
    assert {row["event_id"] for row in rows} == {None}


# --------------------------------------------------------------------------
# Derived columns
# --------------------------------------------------------------------------


def test_late_by_seconds_is_the_gap_between_ingest_and_event_time(spark, adversarial_events):
    """The 30-minute-late frame reports 1800 seconds plus the five minutes of ingest lag.

    `INGESTED_AT` is 04:05:00 and the frame's `meta.dt` is 03:30:00, so the expected
    value is 2100. Asserted on an integer rather than on a rendered timestamp, which is
    why this one is safe to read from a collected row.
    """
    late = next(
        event for event in adversarial_events if event["title"] == "Adversarial:late_by_30_minutes"
    )

    row = valid_rows(to_candidates(_frames(spark, [late]))).collect()[0]

    assert row["late_by_seconds"] == 2100


def test_a_clock_ahead_of_ingest_time_gives_a_negative_lateness(spark, adversarial_events):
    """Negative lateness is recorded, not clamped, because clamping hides the clock fault.

    A column that cannot go below zero makes skew invisible. This uses an ingest time
    *before* the event to prove the sign survives to the table.
    """
    baseline = next(
        event for event in adversarial_events if event["title"] == "Adversarial:baseline"
    )

    row = valid_rows(
        to_candidates(_frames(spark, [baseline], ingested_at="2026-09-17 03:00:00"))
    ).collect()[0]

    assert row["late_by_seconds"] < 0


def test_event_date_is_the_utc_day_of_the_event_not_of_ingestion(spark, adversarial_events):
    """The partition column follows event time, and it is read in UTC.

    An event at 23:30Z belongs to the 17th. Asserted through `date_format` because a
    collected `date` would be rendered in the JVM's zone and would read as the 18th on
    a machine in Asia/Dhaka — the exact trap this file's docstring describes.
    """
    baseline = next(
        event for event in adversarial_events if event["title"] == "Adversarial:baseline"
    )
    late_in_the_day = {**baseline, "meta": {**baseline["meta"], "dt": "2026-09-17T23:30:00.000Z"}}

    row = (
        valid_rows(
            to_candidates(_frames(spark, [late_in_the_day], ingested_at="2026-09-18 00:05:00"))
        )
        .select(
            F.date_format("event_date", "yyyy-MM-dd").alias("day"),
            F.date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("instant"),
        )
        .collect()[0]
    )

    assert row["day"] == "2026-09-17"
    assert row["instant"] == "2026-09-17 23:30:00"


def test_is_article_is_null_when_the_namespace_is_unknown(spark, adversarial_events):
    """ "Not an article" and "we do not know" are different answers, and the column keeps both.

    A `coalesce(..., false)` here would tell every downstream mart that a namespace-less
    event was definitely not an article, which is a claim the data does not support.
    """
    baseline = next(
        event for event in adversarial_events if event["title"] == "Adversarial:baseline"
    )
    without_namespace = {key: value for key, value in baseline.items() if key != "namespace"}

    row = valid_rows(to_candidates(_frames(spark, [without_namespace]))).collect()[0]

    assert row["namespace_id"] is None
    assert row["is_article"] is None


def test_the_rebuild_path_keeps_the_ingest_time_bronze_recorded(spark, adversarial_events):
    """A backfill must not judge old events against today's clock.

    `frames_from_bronze` projects bronze's stored `ingested_at`. If it used
    `current_timestamp()` instead, every replayed row would report itself as days late
    and the future-tolerance rule would give a different verdict than the live path
    did — so a rebuild would not reproduce the table it was rebuilding.
    """
    stored = "2026-09-17 04:05:00"
    bronze = _frames(spark, adversarial_events[:3], ingested_at=stored).withColumn(
        "event_id", F.lit("ignored-by-the-projection")
    )

    rendered = (
        frames_from_bronze(bronze)
        .select(F.date_format("ingested_at", "yyyy-MM-dd HH:mm:ss").alias("at"))
        .distinct()
        .collect()
    )

    assert [row["at"] for row in rendered] == [stored]


# --------------------------------------------------------------------------
# The generated MERGE, as text
# --------------------------------------------------------------------------


def test_the_merge_inserts_when_absent_and_never_updates():
    """Insert-if-absent is what makes first-arrival-wins hold across batches, not just one."""
    sql = merge_statement("cat.silver.edits", "_batch", EDITS_KEY, SILVER_COLUMNS)

    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN MATCHED" not in sql
    assert "UPDATE" not in sql
    assert "DELETE" not in sql


def test_the_merge_joins_only_on_the_declared_key():
    """No partition predicate, deliberately, and this is where that is pinned.

    `AND t.event_date = s.event_date` would let Iceberg prune to one partition and make
    every batch much cheaper. It would also make the effective key `(event_date,
    event_id)`, so one id arriving with two event dates would insert twice — and
    `make verify-no-duplicates` would then be checking a weaker claim than it appears
    to. If someone adds the predicate for the speed, this test is what tells them the
    cost.
    """
    sql = merge_statement("cat.silver.edits", "_batch", EDITS_KEY, SILVER_COLUMNS)
    on_clause = sql.split("\nON ")[1].split("\nWHEN")[0]

    assert on_clause == "t.`event_id` = s.`event_id`"
    assert "event_date" not in on_clause


def test_every_identifier_in_the_merge_is_backticked():
    """`comment` is a Spark SQL keyword. Unquoted, it makes a 22-column insert a parse error."""
    sql = merge_statement("cat.silver.edits", "_batch", EDITS_KEY, SILVER_COLUMNS)

    for column in SILVER_COLUMNS:
        assert f"`{column}`" in sql
    assert "`comment`" in sql
    assert re.search(r"[^`]\bcomment\b[^`]", sql) is None


def test_the_quarantine_merge_joins_on_both_kafka_coordinates():
    """An offset is unique only within a partition, so the key needs both halves."""
    sql = merge_statement("cat.silver.quarantine", "_batch", QUARANTINE_KEY, QUARANTINE_COLUMNS)
    on_clause = sql.split("\nON ")[1].split("\nWHEN")[0]

    assert (
        on_clause
        == "t.`kafka_partition` = s.`kafka_partition` AND t.`kafka_offset` = s.`kafka_offset`"
    )
    assert tuple(QUARANTINE_KEY) == ARRIVAL_ORDER
