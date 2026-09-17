"""Kafka to `silver.edits` and `silver.quarantine`: one row per event, or a reason why not.

    make stream-silver        # continuous, 30-second micro-batches
    make stream-silver-once   # one pass over everything available, then exit
    make rebuild-silver FROM=2026-09-17 TO=2026-09-17   # the same logic, over bronze

This is where the pipeline stops recording and starts deciding. Three decisions, each
with a test named after it.

## Why silver reads Kafka and not bronze

Reading bronze would be simpler and it would be wrong for the live path. Bronze is
itself a streaming write, so silver reading bronze would put two commit latencies in
series and make silver's freshness a function of bronze's health — a bronze outage
would stall silver even though every event silver needs is sitting in Kafka. Reading
the topic directly keeps the two paths independent: either table can be rebuilt, or
fall over, without the other noticing.

The cost is that the topic is read twice, and at real volume that is the first thing
I would change. The bronze path is not wasted, though: it is what makes
`make rebuild-silver` possible, and that matters more than the duplicate read,
because it means a bug in the logic below is a replay rather than a data loss. The
rebuild runs this same code over bronze rows — see `frames_from_bronze` — so the two
paths cannot drift.

## Why there is no watermark

`BUILD_PHASES.md` specifies `withWatermark("event_time", "10 minutes")`, and this job
does not declare one. A watermark in Structured Streaming is not a filter; it is state
for a stateful operator, and it does nothing at all unless one is present — so
declaring it here and stopping would satisfy the specification while changing no
output, which is measured rather than asserted.

Making it load-bearing means adding `dropDuplicatesWithinWatermark`, and that operator
admits only rows at or above the watermark: a unique event arriving below it is dropped
with no error, no metric and no dead-letter row.
`tests/spark/test_watermark_would_drop_data.py` reproduces that deletion on the
`late_by_30_minutes` fixture frame, and measures the boundary as well — five minutes
late survives, thirty does not. The objection is therefore not that watermarks discard
data indiscriminately; it is that ten minutes is a hard cliff, and a wiki reconnecting
after a fifteen-minute network partition falls off it.

So dedup is unbounded and exact instead: `row_number()` inside the batch, `MERGE INTO
... WHEN NOT MATCHED` across batches. The ten-minute figure survives as an SLO rather
than as a mechanism — `late_by_seconds` records every row's lateness, and the Dagster
asset check in Phase 7 is what alerts on it. Iceberg absorbs a late write into an old
partition without complaint, which is the property that makes this affordable and is
most of the reason the lakehouse format is here at all. ADR-0019.

## Why the writes are idempotent and the batch is not

`foreachBatch` gives at-least-once, not exactly-once: Spark may re-run a micro-batch
whose commit it could not confirm. Iceberg's own streaming sink defends against that
by stamping `spark.sql.streaming.epochId` into the snapshot and refusing a known
epoch, but a batch write performed *inside* `foreachBatch` carries no such tag — so
`writeTo(...).append()` here would duplicate on retry. `MERGE INTO ... WHEN NOT
MATCHED THEN INSERT` needs no tag: applying it twice is applying it once. Both writes
in this job are MERGEs for exactly that reason, including the quarantine write, which
is why `silver.quarantine` carries the Kafka coordinates.

First arrival wins. Two frames with the same `meta.id` and different content — the
`divergent_duplicate` fixture case — resolve to the earlier Kafka offset, and a row
already in `silver.edits` is never updated. That makes the table append-only in
practice, so a downstream incremental model keyed on `event_id` never has to handle a
changed row. The cost is that a genuine upstream correction under the same id would
be ignored; bronze still holds both, and `make rebuild-silver` against a rewritten
rule is how that would be applied.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.quality.expectations import RULE_INPUT_COLUMNS, failure_reason_sql
from wikistream.streaming.kafka_source import read_kafka
from wikistream.streaming.schema import CORRUPT_RECORD_COLUMN, PARSE_OPTIONS, PARSE_SCHEMA
from wikistream.streaming.session import build_session, checkpoint_location

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming.query import StreamingQuery

    from wikistream.config import Settings

log = get_logger(__name__)

#: Query name, and therefore the checkpoint directory name. Distinct from bronze's so
#: the two jobs hold independent positions in the topic.
QUERY_NAME = "silver_edits"

#: What `to_candidates` requires of its input. Both entry points build these, and the
#: rebuild path gets them free because they are bronze's own column names.
FRAME_COLUMNS: tuple[str, ...] = (
    "raw_payload",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
)

#: `silver.edits` in DDL order. Generated into the MERGE, so this tuple and
#: `tables.silver_edits_ddl` are the only two places the column list appears, and
#: `tests/spark/test_silver_mapping.py` asserts they agree.
SILVER_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "wiki",
    "domain",
    "change_type",
    "namespace_id",
    "is_article",
    "page_title",
    "page_url",
    "editor",
    "is_bot",
    "is_minor",
    "is_anonymous",
    "bytes_old",
    "bytes_new",
    "bytes_delta",
    "rev_old",
    "rev_new",
    "comment",
    "late_by_seconds",
    "ingested_at",
    "event_date",
)

#: `silver.quarantine` in DDL order.
QUARANTINE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "raw_payload",
    "failure_reason",
    "kafka_partition",
    "kafka_offset",
    "failed_at",
    "ingest_date",
)

#: The MERGE key of each table. `silver.edits` keys on the event id; quarantine
#: cannot, because a missing id is one of the reasons a row lands there.
EDITS_KEY: tuple[str, ...] = ("event_id",)
QUARANTINE_KEY: tuple[str, ...] = ("kafka_partition", "kafka_offset")

#: Arrival order within a batch, and therefore which duplicate wins. Kafka offsets
#: are monotonic per partition, so this is "first seen" and it is stable across a
#: replay of the same offsets — which is what makes the choice deterministic rather
#: than merely arbitrary.
ARRIVAL_ORDER: tuple[str, ...] = ("kafka_partition", "kafka_offset")

_VALID_VIEW = "_silver_valid_batch"
_QUARANTINE_VIEW = "_silver_quarantine_batch"

# ---------------------------------------------------------------------------
# SQL that re-expresses a rule from `wikistream.events`
#
# Each of these has a Python twin, and `tests/spark/test_expectation_parity.py`
# runs both over 519 fixture frames and fails on any disagreement. That test is the
# reason the duplication is acceptable; see the docstring of `wikistream.events`.
# ---------------------------------------------------------------------------

#: An IPv4 address, with the octet range enforced rather than approximated, so this
#: agrees with Python's `ipaddress.ip_address` instead of merely resembling it.
_OCTET = r"(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
_IPV4 = rf"^({_OCTET}\.){{3}}{_OCTET}$"

#: A MediaWiki temporary account: `~2026-12345`. Not an IP, not a registered account.
_TEMP_ACCOUNT = r"^~[0-9]{4}-[0-9]+$"

#: `editor` is an IP or a temporary account. The IPv6 test is `contains a colon`
#: rather than a full IPv6 grammar, and that is sound here rather than lazy:
#: MediaWiki reserves `:` as the namespace separator and forbids it in usernames, so
#: on this source a colon in the user field cannot be anything but an address. A
#: regex for the full IPv6 grammar — compressed runs, embedded IPv4 — would be a
#: liability to maintain for no gain in accuracy on this data.
IS_ANONYMOUS_SQL = f"""
CASE
  WHEN editor IS NULL OR length(editor) = 0 THEN false
  WHEN editor RLIKE '{_TEMP_ACCOUNT}' THEN true
  WHEN editor RLIKE '{_IPV4}' THEN true
  WHEN editor LIKE '%:%' THEN true
  ELSE false
END
"""

#: The three cases of `events.byte_delta`, and the middle one is why this is a CASE
#: and not `bytes_new - bytes_old`. A page creation has a null `old` and a real
#: `new`; subtracting would yield null and erase every page creation from a
#: "bytes added" total, silently.
BYTES_DELTA_SQL = """
CASE
  WHEN bytes_new IS NULL THEN NULL
  WHEN bytes_old IS NULL THEN bytes_new
  ELSE bytes_new - bytes_old
END
"""


def frames_from_kafka(kafka_df: DataFrame) -> DataFrame:
    """Kafka's columns, renamed to the shape `to_candidates` expects.

    `ingested_at` is `current_timestamp()`, which Structured Streaming binds once per
    micro-batch and records in the offset log — so every row of a batch shares it,
    and a replayed batch gets the same value it got the first time. That is what
    makes `late_by_seconds` stable under replay instead of growing every time a batch
    is retried.
    """
    return (
        kafka_df.withColumn("raw_payload", F.col("value").cast("string"))
        .withColumn("kafka_topic", F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(*FRAME_COLUMNS)
    )


def frames_from_bronze(bronze_df: DataFrame) -> DataFrame:
    """Bronze rows, for the rebuild path. A projection and nothing else.

    `ingested_at` is bronze's stored value, not the rebuild's clock. This is the
    detail that makes a backfill honest: judging a month-old event against today's
    time would report every replayed row as a month late and would fail the
    future-tolerance rule differently than the live path did. The reference time
    belongs to the arrival, so it is read from the table.
    """
    return bronze_df.select(*FRAME_COLUMNS)


def to_candidates(frames: DataFrame) -> DataFrame:
    """Parse, flatten, and attach a `failure_reason`. No filtering, no writing.

    Returns every input row with both the silver columns and the validation verdict,
    so the caller splits one DataFrame two ways rather than parsing twice. Kept pure
    so `tests/spark/test_silver_mapping.py` can assert the mapping against a static
    DataFrame and never start a stream.
    """
    missing = [name for name in FRAME_COLUMNS if name not in frames.columns]
    if missing:
        raise ValueError(f"input is missing {missing}; expected {list(FRAME_COLUMNS)}")

    flattened = (
        frames.withColumn("_parsed", F.from_json(F.col("raw_payload"), PARSE_SCHEMA, PARSE_OPTIONS))
        # Both halves are needed. A malformed payload yields a struct of nulls with
        # the corrupt-record column set; only a blank payload yields a null struct.
        # Measured, with the full disagreement table, in quality/expectations.py.
        .withColumn(
            "_parse_failed",
            F.col("_parsed").isNull() | F.col(f"_parsed.{CORRUPT_RECORD_COLUMN}").isNotNull(),
        )
        .withColumn("event_id", F.col("_parsed.meta.id"))
        .withColumn("_raw_event_time", F.col("_parsed.meta.dt"))
        # try_to_timestamp, not to_timestamp: under ANSI mode the latter raises on a
        # bad value, which in a streaming job halts ingestion permanently. ADR-0018.
        .withColumn("event_time", F.try_to_timestamp(F.col("_raw_event_time")))
        .withColumn("domain", F.col("_parsed.meta.domain"))
        .withColumn("wiki", F.col("_parsed.wiki"))
        .withColumn("change_type", F.col("_parsed.type"))
        .withColumn("namespace_id", F.col("_parsed.namespace"))
        # Null-safe by accident of SQL rather than by design, and worth stating: a
        # null namespace gives a null is_article, not false. "We do not know" and
        # "not an article" are different answers and the mart decides which it wants.
        .withColumn("is_article", F.col("namespace_id") == F.lit(0))
        .withColumn("page_title", F.col("_parsed.title"))
        .withColumn("page_url", F.col("_parsed.title_url"))
        .withColumn("editor", F.col("_parsed.user"))
        .withColumn("is_bot", F.col("_parsed.bot"))
        .withColumn("is_minor", F.col("_parsed.minor"))
        .withColumn("is_anonymous", F.expr(IS_ANONYMOUS_SQL))
        .withColumn("bytes_old", F.col("_parsed.length.old"))
        .withColumn("bytes_new", F.col("_parsed.length.new"))
        .withColumn("bytes_delta", F.expr(BYTES_DELTA_SQL))
        .withColumn("rev_old", F.col("_parsed.revision.old"))
        .withColumn("rev_new", F.col("_parsed.revision.new"))
        .withColumn("comment", F.col("_parsed.comment"))
        .withColumn(
            "late_by_seconds",
            (F.unix_timestamp("ingested_at") - F.unix_timestamp("event_time")).cast("int"),
        )
        .withColumn("event_date", F.to_date(F.col("event_time")))
        .withColumn("failed_at", F.col("ingested_at"))
        .withColumn("ingest_date", F.to_date(F.col("ingested_at")))
    )

    absent = [name for name in RULE_INPUT_COLUMNS if name not in flattened.columns]
    if absent:  # pragma: no cover - a guard against editing one list and not the other
        raise ValueError(f"the rules read {absent}, which this projection does not produce")

    return flattened.withColumn("failure_reason", F.expr(failure_reason_sql("ingested_at")))


def deduplicate(candidates: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    """Keep the first-arriving row per key within this batch.

    Needed before the MERGE rather than instead of it. `MERGE INTO` raises when the
    source contains two rows matching one target row, so a batch holding a duplicated
    `event_id` — which a producer restart produces routinely — would fail the write
    outright. This collapses them by Kafka arrival order first; the MERGE then
    handles duplicates that arrive in *different* batches, which this cannot see.
    """
    window = Window.partitionBy(*keys).orderBy(*[F.col(name).asc() for name in ARRIVAL_ORDER])
    return (
        candidates.withColumn("_arrival", F.row_number().over(window))
        .filter(F.col("_arrival") == 1)
        .drop("_arrival")
    )


def valid_rows(candidates: DataFrame) -> DataFrame:
    """The rows fit for `silver.edits`, deduplicated, in DDL column order."""
    return deduplicate(candidates.filter(F.col("failure_reason").isNull()), EDITS_KEY).select(
        *SILVER_COLUMNS
    )


def quarantine_rows(candidates: DataFrame) -> DataFrame:
    """The rows that failed a rule, deduplicated on the Kafka coordinates.

    Deduplicated for the same reason as the valid rows: a bronze table that was
    re-read after a checkpoint loss can hold one Kafka record twice, and a MERGE
    source with two rows per key is an error rather than a no-op.
    """
    return deduplicate(
        candidates.filter(F.col("failure_reason").isNotNull()), QUARANTINE_KEY
    ).select(*QUARANTINE_COLUMNS)


def merge_statement(
    table: str, source: str, keys: tuple[str, ...], columns: tuple[str, ...]
) -> str:
    """An insert-if-absent MERGE, generated from the column tuple.

    Generated rather than written out so the column list exists once in this module.
    Every identifier is backticked because `comment` is a Spark SQL keyword and an
    unquoted one turns a 22-column insert into a parse error.

    There is deliberately no partition predicate on the join. Adding
    `AND t.event_date = s.event_date` would let Iceberg prune to one partition and
    would make each batch much cheaper — and it would also change the effective key
    to `(event_date, event_id)`, so the same event id arriving with two different
    event dates would insert twice and the zero-duplicates gate would be a weaker
    claim than it reads as. The full-table scan is the price of the unconditional
    claim, and it is named in the README's limitations.
    """
    on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    names = ", ".join(f"`{name}`" for name in columns)
    values = ", ".join(f"s.`{name}`" for name in columns)
    return (
        f"MERGE INTO {table} AS t\n"
        f"USING {source} AS s\n"
        f"ON {on}\n"
        f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    )


def merge_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    """Write one micro-batch into both silver tables. The `foreachBatch` body.

    Persisted because the batch is read three times — the split counts, the edits
    MERGE and the quarantine MERGE — and the input is a Kafka scan plus a JSON parse
    that there is no reason to repeat. Unpersisted in a `finally` so a failed MERGE
    does not leak a cached block on every retry.
    """
    spark = batch.sparkSession
    batch.persist()
    try:
        # One pass for both counts, rather than count() twice. These are the only
        # visibility into the valid/quarantined split, so they are worth a scan.
        split = {
            bool(row["failed"]): int(row["rows"])
            for row in batch.groupBy(F.col("failure_reason").isNotNull().alias("failed"))
            .agg(F.count("*").alias("rows"))
            .collect()
        }

        valid_rows(batch).createOrReplaceTempView(_VALID_VIEW)
        spark.sql(
            merge_statement(settings.silver_edits_table, _VALID_VIEW, EDITS_KEY, SILVER_COLUMNS)
        )

        quarantine_rows(batch).createOrReplaceTempView(_QUARANTINE_VIEW)
        spark.sql(
            merge_statement(
                settings.silver_quarantine_table,
                _QUARANTINE_VIEW,
                QUARANTINE_KEY,
                QUARANTINE_COLUMNS,
            )
        )

        log.info(
            "silver batch merged",
            extra={
                "batch_id": batch_id,
                "valid_rows": split.get(False, 0),
                "quarantined_rows": split.get(True, 0),
            },
        )
    finally:
        batch.unpersist()


def write_silver(
    candidates: DataFrame,
    settings: Settings,
    *,
    once: bool = False,
    checkpoint: str | None = None,
) -> StreamingQuery:
    """Start the `foreachBatch` write and return the running query.

    `outputMode("update")` rather than `append`: with `foreachBatch` the mode is not
    what decides what is written — `merge_batch` decides that — but `append` on a
    query with no stateful operator is the mode that would have to change if one were
    ever added, and `update` says plainly that rows are being upserted.
    """
    writer = (
        candidates.writeStream.queryName(QUERY_NAME)
        .outputMode("update")
        .option("checkpointLocation", checkpoint or checkpoint_location(QUERY_NAME, settings))
        .foreachBatch(lambda batch, batch_id: merge_batch(batch, batch_id, settings))
    )
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_interval_seconds} seconds")
    return writer.start()


def rebuild_from_bronze(
    spark: SparkSession,
    settings: Settings,
    date_from: str,
    date_to: str,
) -> dict[str, int]:
    """Replay a date range of bronze through the same logic, as one batch.

    Idempotent because the MERGEs are: running it twice leaves the row counts
    unchanged, which `tests/integration/test_silver_rebuild.py` asserts by doing
    exactly that.

    The range filters on `ingest_date`, bronze's partition column, so the scan prunes
    to whole partitions. Filtering on `event_time` instead would read every partition
    to find rows whose event time falls in the range — and would also miss the point,
    because a rebuild is about "reprocess what we ingested then", not "reprocess what
    happened then".
    """
    frames = frames_from_bronze(
        spark.table(settings.bronze_raw_table).where(
            F.col("ingest_date").between(F.lit(date_from).cast("date"), F.lit(date_to).cast("date"))
        )
    )
    candidates = to_candidates(frames)
    merge_batch(candidates, batch_id=-1, settings=settings)
    return {
        "edits": spark.table(settings.silver_edits_table).count(),
        "quarantine": spark.table(settings.silver_quarantine_table).count(),
    }


def run(once: bool = False, topic: str | None = None) -> int:
    """Start the stream and block until it stops."""
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("silver", settings)

    candidates = to_candidates(frames_from_kafka(read_kafka(spark, settings, topic)))
    query = write_silver(candidates, settings, once=once)

    log.info(
        "silver stream started",
        extra={
            "topic": topic or settings.kafka_topic,
            "edits_table": settings.silver_edits_table,
            "quarantine_table": settings.silver_quarantine_table,
            "trigger": "availableNow" if once else f"{settings.trigger_interval_seconds}s",
            "checkpoint": checkpoint_location(QUERY_NAME, settings),
            "query_id": query.id,
        },
    )

    query.awaitTermination()
    log.info("silver stream stopped", extra={"query_id": query.id})
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Stream Kafka into the silver tables.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process everything available and exit, instead of running continuously.",
    )
    parser.add_argument("--topic", default=None, help="Override the source topic.")
    args = parser.parse_args(argv)
    return run(once=args.once, topic=args.topic)


if __name__ == "__main__":
    sys.exit(main())
