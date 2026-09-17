"""Kafka to `bronze.recentchange_raw`: append everything, interpret nothing.

    make stream-bronze        # continuous, 30-second micro-batches
    make stream-bronze-once   # one pass over everything available, then exit

Bronze is the audit log. It is append-only, it is never deduplicated, and it keeps
`raw_payload` exactly as the frame arrived. Everything that could be wrong about
the data is still wrong in here on purpose: duplicates from a producer restart, a
frame that is not valid JSON, an event whose `meta.id` is missing. Silver is where
judgement is applied, and silver can be dropped and rebuilt from this table
precisely because this table applied none.

## Why the three columns that *are* interpreted

`event_id`, `event_time` and `schema_uri` are extracted here even though bronze is
supposed to be uninterpreted. Each one earns it:

* `event_id` makes "did we already land this?" answerable in bronze with a
  `GROUP BY`, which is how the restart-idempotency proof measures duplicate
  suppression in the first place.
* `event_time` makes a time-range replay into silver possible without parsing
  every payload in the range.
* `schema_uri` makes upstream version drift a `SELECT DISTINCT` rather than an
  archaeology exercise.

All three are nullable, and a frame that yields null for all three is still
appended. `raw_payload` remains the source of truth; these are an index over it.

## ANSI mode makes a bad timestamp a poison pill

Spark 4.0 enables `spark.sql.ansi.enabled` by default, which changes what a
malformed cast does. Measured in this container on 2026-09-17:

    SELECT to_timestamp(s)      FROM VALUES ('2026-09-17T10:00:00Z'), ('nonsense')
    -> [CAST_INVALID_INPUT] SparkDateTimeException, raised inside the generated
       iterator, so it is a per-row failure and not just constant folding

    SELECT try_to_timestamp(s)  FROM the same rows
    -> one timestamp, one null

An exception here is worse than a wrong value. The micro-batch fails, the
checkpoint has not advanced, the restart reads the same offsets, and the job dies
on the same record indefinitely — ingestion stops for the whole topic because one
frame had a bad `meta.dt`. So bronze uses `try_to_timestamp` and lets the null
travel: the row still lands with its payload intact, and silver's quarantine step
is what reports it. `tests/spark/test_bronze_mapping.py` holds this down.

## Offsets, and the one flag that looks reckless

`startingOffsets=earliest` applies only on the very first run of a checkpoint.
Afterwards the checkpoint's offset log wins and this option is ignored — which is
the whole mechanism behind restart safety, and also why deleting a checkpoint
directory silently re-reads the topic from the beginning.

`failOnDataLoss=false` deserves its own paragraph, because it is the kind of flag
that a reviewer is right to be suspicious of. It tells Spark to continue when the
offsets it recorded no longer exist on the broker. Here the topic's retention is
24 hours, so a laptop that is shut for two days *will* come back to a checkpoint
pointing at expired offsets, and the alternative behaviour — refusing to start —
turns "I closed my laptop" into a manual checkpoint deletion. The cost is real:
data that expired while the job was down is skipped rather than reported, and
nothing in the pipeline can distinguish that from a quiet period. That is
acceptable for bronze because bronze's guarantee is "everything Kafka still had",
not "everything Wikimedia ever sent" — and it would not be acceptable in a system
where Kafka were the system of record.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.schema import PARSE_OPTIONS, PARSE_SCHEMA
from wikistream.streaming.session import build_session, checkpoint_location

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming.query import StreamingQuery

    from wikistream.config import Settings

log = get_logger(__name__)

#: Query name. Also the checkpoint directory name, so renaming this starts the
#: stream over from `startingOffsets`.
QUERY_NAME = "bronze_recentchange_raw"

#: The column order bronze rows are written in. Iceberg matches by name on
#: `writeTo(...).append()`, so this is for readability, not correctness — but a
#: reader comparing this file against docs/data-contracts.md should not have to
#: reorder anything in their head.
BRONZE_COLUMNS = (
    "event_id",
    "event_time",
    "schema_uri",
    "raw_payload",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
)


def read_kafka(
    spark: SparkSession,
    settings: Settings,
    topic: str | None = None,
    *,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Open the Kafka source. Returns the raw Kafka columns, untransformed."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic or settings.kafka_topic)
        .option("startingOffsets", starting_offsets)
        # See the module docstring: this is a deliberate trade, not an oversight.
        .option("failOnDataLoss", "false")
        # Bound the work per micro-batch. Without this, the first batch after a
        # long outage tries to read every retained record at once, and a 12 GB box
        # meets an executor OOM instead of catching up steadily. 20,000 is about
        # six minutes of stream at the measured 51 events/s.
        .option("maxOffsetsPerTrigger", "20000")
        # Kafka consumer group management is Spark's, not ours: it commits offsets
        # to the checkpoint, not to Kafka. Naming the group prefix anyway makes
        # `kafka-consumer-groups.sh --describe` show something recognisable.
        .option("groupIdPrefix", settings.kafka_consumer_group)
        .load()
    )


def to_bronze_rows(kafka_df: DataFrame) -> DataFrame:
    """Map Kafka's columns onto the bronze contract.

    Kept as a pure DataFrame-to-DataFrame function with no session, no Kafka and no
    table, so `tests/integration/test_bronze.py` can assert the mapping against a
    static DataFrame rather than only through a running stream.
    """
    # CAST(value AS STRING) is a UTF-8 decode. The producer sent the frame's own
    # bytes, so this round-trips to the original text; a frame that was not valid
    # UTF-8 arrives with replacement characters, which is visible in the payload
    # rather than silently dropped.
    raw = F.col("value").cast("string")
    parsed = F.from_json(raw, PARSE_SCHEMA, PARSE_OPTIONS)

    return (
        kafka_df.withColumn("raw_payload", raw)
        .withColumn("_parsed", parsed)
        # A row whose JSON did not parse has a null struct, so every extraction
        # below yields null rather than raising. That is the intended behaviour:
        # bronze appends it, and silver's quarantine step is what notices.
        .withColumn("event_id", F.col("_parsed.meta.id"))
        # `try_to_timestamp`, not `to_timestamp`. Spark 4.0 enables ANSI mode by
        # default, and under ANSI a malformed timestamp string is an exception, not
        # a null — see the module docstring for the measurement. In a streaming job
        # that exception is a poison pill: the batch fails, the checkpoint replays
        # the same offsets, and the job dies again on the same record forever.
        .withColumn("event_time", F.try_to_timestamp(F.col("_parsed.meta.dt")))
        # Backticks because the field is literally named `$schema` and `$` starts
        # nothing in Spark SQL's identifier grammar.
        .withColumn("schema_uri", F.col("_parsed.`$schema`"))
        .withColumn("kafka_topic", F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        # Processing time, evaluated once per micro-batch rather than per row:
        # current_timestamp() is a batch-deterministic expression in Structured
        # Streaming, which is what makes every row of one commit share an
        # ingest_date instead of straddling midnight.
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("ingested_at")))
        .select(*BRONZE_COLUMNS)
    )


def write_bronze(
    rows: DataFrame,
    settings: Settings,
    table: str | None = None,
    *,
    once: bool = False,
    checkpoint: str | None = None,
) -> StreamingQuery:
    """Start the append-only write into Iceberg and return the running query.

    The trigger is the visible decision here. 30 seconds is a choice about file size
    rather than about latency: at the measured 51 events/s a 30-second batch is about
    1,500 rows, which Iceberg writes as a handful of Parquet files per commit. A
    1-second trigger would give a two-second-fresher dashboard and 2,880 commits a
    day, each with its own metadata.json and its own tiny files — the small-files
    problem bought with money the pipeline does not need to spend. Trino's query
    latency against a table of thousands of 30 KB files is far worse than 30 seconds
    of staleness.

    `--once` swaps it for `availableNow`, which drains everything on the topic in
    bounded micro-batches and then terminates. That is what CI and the integration
    tests use, and it is also the right shape for a catch-up run: same code, same
    checkpoint, same exactly-once path, just a stopping condition.
    """
    target = table or settings.bronze_raw_table
    writer = (
        rows.writeStream.queryName(QUERY_NAME)
        .format("iceberg")
        # Append, because bronze never updates a row. Iceberg's streaming writer
        # commits one snapshot per micro-batch, so a failed batch leaves no
        # partial data — the commit is the atomic unit, not the file write.
        .outputMode("append")
        .option("checkpointLocation", checkpoint or checkpoint_location(QUERY_NAME, settings))
        # `fanout-enabled` lets one task write to several partitions without the
        # data being sorted by partition first. With one day per partition almost
        # every batch touches exactly one, so this only matters across midnight and
        # on the first catch-up batch after an outage — where the alternative is a
        # sort of the whole batch.
        .option("fanout-enabled", "true")
    )
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_interval_seconds} seconds")
    return writer.toTable(target)


def last_batch_summary(query: StreamingQuery) -> dict[str, object]:
    """What the most recent micro-batch did, from the sink's metrics rather than the source's.

    The row count comes from `sink.numOutputRows` on purpose. The obvious field,
    `numInputRows`, is wrong when the sink is Iceberg: it reports exactly twice the
    number of records the batch actually read. Measured on 2026-09-17 against this
    pipeline, one batch at a time:

    | sink                                | Kafka offset delta | numInputRows | sink.numOutputRows |
    |-------------------------------------|-------------------:|-------------:|-------------------:|
    | `noop`                              |              5,392 |        5,392 |              5,392 |
    | `iceberg`                           |              3,138 |        6,276 |              3,138 |
    | `iceberg`, `distribution-mode=none` |              7,727 |        7,727 |              7,727 |

    So the doubling is not the Kafka source and not the plan — the same plan
    measures correctly into a `noop` sink — it follows the exchange that Iceberg's
    default distribution mode inserts before the write. `sink.numOutputRows` agrees
    with both the Kafka offset delta and the `added-records` in the resulting
    Iceberg snapshot, which is why it is the number reported here.

    The write keeps the default distribution mode regardless. Changing a write
    option to correct a metric would be tuning the instrument.
    """
    progress = query.lastProgress
    if progress is None:
        return {"batch_id": None, "rows": None}

    sources = progress.get("sources") or [{}]
    return {
        "batch_id": progress.get("batchId"),
        "rows": (progress.get("sink") or {}).get("numOutputRows"),
        "batch_ms": progress.get("batchDuration"),
        "end_offsets": sources[0].get("endOffset"),
        # Zero means the batch drained everything Kafka had at planning time. A
        # number that keeps climbing across batches is the signal that this laptop
        # cannot keep up with the source, which is the one operational failure this
        # pipeline can have while looking perfectly healthy.
        "max_offsets_behind": (sources[0].get("metrics") or {}).get("maxOffsetsBehindLatest"),
    }


def run(once: bool = False, topic: str | None = None, table: str | None = None) -> int:
    """Start the stream and block until it stops."""
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("bronze", settings)

    rows = to_bronze_rows(read_kafka(spark, settings, topic))
    query = write_bronze(rows, settings, table, once=once)

    log.info(
        "bronze stream started",
        extra={
            "topic": topic or settings.kafka_topic,
            "table": table or settings.bronze_raw_table,
            "trigger": "availableNow" if once else f"{settings.trigger_interval_seconds}s",
            "checkpoint": checkpoint_location(QUERY_NAME, settings),
            "query_id": query.id,
        },
    )

    query.awaitTermination()
    log.info("bronze stream stopped", extra=last_batch_summary(query))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. `--topic` and `--table` exist so a test can run in isolation."""
    parser = argparse.ArgumentParser(description="Stream Kafka into bronze.recentchange_raw.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process everything available and exit, instead of running continuously.",
    )
    parser.add_argument("--topic", default=None, help="Override the source topic.")
    parser.add_argument("--table", default=None, help="Override the target Iceberg table.")
    args = parser.parse_args(argv)
    return run(once=args.once, topic=args.topic, table=args.table)


if __name__ == "__main__":
    sys.exit(main())
