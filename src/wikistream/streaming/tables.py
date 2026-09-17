"""The DDL for every Iceberg table, in one place, as idempotent SQL.

Kept as SQL strings rather than as PySpark `DataFrameWriter` calls because the
schema is the contract: a reviewer should be able to read the column list and the
partitioning without reconstructing them from method chaining, and
`docs/data-contracts.md` should be checkable against this file by eye.

Every statement is `CREATE TABLE IF NOT EXISTS`, so `make init-tables` is safe to
run against a populated lakehouse — which it has to be, because it runs as part of
`make up`.

## Types worth explaining

`timestamp` in Spark maps to Iceberg's `timestamptz` (instant semantics) when
`spark.sql.timestampType` is left at its default. That is what is wanted: every
timestamp here is an instant in UTC, and a zone-less local timestamp would make
`days(event_date)` ambiguous for an hour twice a year in any zone that observes
DST.

`raw_payload` is a string, not binary. The frames are UTF-8 JSON and keeping them
readable means `SELECT raw_payload` in Trino is useful without a decode step. The
cost is that a frame which is not valid UTF-8 cannot be stored verbatim; the
source decodes with `errors="replace"`, so such a frame is stored with
replacement characters rather than rejected, and the quarantine path catches it
downstream when the JSON fails to parse.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from wikistream.config import get_settings

if TYPE_CHECKING:
    from wikistream.config import Settings

#: Iceberg table format version. v2 is what enables row-level deletes, which is
#: what `MERGE INTO` needs; v1 would silently fall back to rewriting whole files.
_FORMAT_VERSION = "2"


def _table_properties() -> str:
    """Table properties applied to every table, as a SQL TBLPROPERTIES clause."""
    props = {
        "format-version": _FORMAT_VERSION,
        # Parquet, explicitly. Iceberg's default is Parquet today; saying so means
        # a future default change does not silently rewrite this project's files
        # in another format.
        "write.format.default": "parquet",
        # zstd rather than Parquet's default snappy. Same reasoning as the Kafka
        # topic (ADR-0014) and the same caveat: measured on bytes, not on CPU.
        "write.parquet.compression-codec": "zstd",
        # 128 MB target files. The laptop-scale default of 512 MB would mean this
        # pipeline never compacts into a full file and every commit's files stay
        # small; 128 MB is reachable within a day at the measured rate.
        "write.target-file-size-bytes": str(128 * 1024 * 1024),
        # Keep 7 days of snapshots. Long enough for time travel to be
        # demonstrable, short enough that metadata does not accumulate unbounded
        # on a laptop. `make maintain` expires the rest.
        "history.expire.max-snapshot-age-ms": str(7 * 24 * 60 * 60 * 1000),
        # Iceberg keeps every metadata.json by default, one per commit. At a
        # 30-second trigger that is 2,880 files a day of pure bookkeeping.
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "20",
    }
    rendered = ",\n  ".join(f"'{key}' = '{value}'" for key, value in props.items())
    return f"TBLPROPERTIES (\n  {rendered}\n)"


def namespace_statements(settings: Settings | None = None) -> list[str]:
    """`CREATE NAMESPACE` for bronze, silver and gold."""
    cfg = settings or get_settings()
    catalog = cfg.iceberg_catalog_name
    return [
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}"
        for namespace in (cfg.bronze_namespace, cfg.silver_namespace, cfg.gold_namespace)
    ]


def bronze_raw_ddl(settings: Settings | None = None) -> str:
    """Append-only landing table: one row per Kafka record, no deduplication.

    This is the replay log. If the silver transformation is wrong, silver is
    rebuilt from here, which is only possible because nothing here is ever updated
    and `raw_payload` is byte-exact.

    Partitioned by `days(ingest_date)` rather than by event date, because ingest
    date is monotonic under replay: a backfill of old events writes into today's
    partition and rewrites nothing. Partitioning the audit log by event time would
    mean a late event mutates a partition that was already compacted.
    """
    cfg = settings or get_settings()
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.bronze_raw_table} (
  event_id        string  COMMENT 'meta.id from the source; not unique here by design',
  event_time      timestamp COMMENT 'meta.dt, the time the wiki recorded the change',
  schema_uri      string  COMMENT '$schema, kept so version drift is visible in SQL',
  raw_payload     string  COMMENT 'the frame exactly as it arrived, never reserialised',
  kafka_topic     string,
  kafka_partition int,
  kafka_offset    bigint  COMMENT 'with partition, the natural key of a Kafka record',
  kafka_timestamp timestamp COMMENT 'broker append time, not event time',
  ingested_at     timestamp COMMENT 'Spark processing time for this micro-batch',
  ingest_date     date    COMMENT 'partition column, derived from ingested_at'
)
USING iceberg
PARTITIONED BY (days(ingest_date))
{_table_properties()}
""".strip()


def silver_edits_ddl(settings: Settings | None = None) -> str:
    """One row per source event, keyed on `event_id`.

    `event_id` is a primary key by convention only. **Iceberg has no primary key
    or uniqueness constraint** — nothing in the table prevents a duplicate, and
    the only thing that makes the claim true is that every write goes through a
    `MERGE INTO ... WHEN MATCHED` on this column. That is a real limitation, and
    `make verify-no-duplicates` exists because an invariant with no test is a
    wish.

    Partitioned by `days(event_date)`, derived from event time rather than ingest
    time, because every analytical question here is about when an edit happened.
    A late event therefore does write into an older partition; that is the cost
    and it is why `late_by_seconds` is a stored column rather than a derived one.
    """
    cfg = settings or get_settings()
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.silver_edits_table} (
  event_id         string    NOT NULL COMMENT 'meta.id; unique by MERGE, not by constraint',
  event_time       timestamp NOT NULL COMMENT 'watermark column',
  wiki             string    COMMENT 'wiki database name, e.g. enwiki',
  domain           string    COMMENT 'host, e.g. en.wikipedia.org; the Kafka partition key',
  change_type      string    COMMENT 'edit | new | log | categorize',
  namespace_id     int       COMMENT 'MediaWiki namespace; 0 is article space',
  is_article       boolean   COMMENT 'namespace_id = 0',
  page_title       string,
  page_url         string,
  editor           string    COMMENT 'username, IP, or temporary-account name',
  is_bot           boolean,
  is_minor         boolean,
  is_anonymous     boolean   COMMENT 'editor is an IP or a MediaWiki temporary account',
  bytes_old        int,
  bytes_new        int,
  bytes_delta      int       COMMENT 'bytes_new - bytes_old, null when either side is null',
  rev_old          bigint,
  rev_new          bigint,
  comment          string,
  late_by_seconds  int       COMMENT 'ingested_at - event_time, so lateness is queryable',
  ingested_at      timestamp,
  event_date       date      COMMENT 'partition column, derived from event_time'
)
USING iceberg
PARTITIONED BY (days(event_date))
{_table_properties()}
""".strip()


def silver_quarantine_ddl(settings: Settings | None = None) -> str:
    """Rows that could not be made into a silver row, with the reason.

    A pipeline that drops bad rows silently is broken; a pipeline that dies on one
    bad row is also broken. This is the third option, and it stores `raw_payload`
    so a rejected record can be replayed after a fix rather than merely counted.

    Partitioned by ingest date because the question asked of this table is always
    "what went wrong recently", never "what went wrong about events from March".
    """
    cfg = settings or get_settings()
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.silver_quarantine_table} (
  event_id        string    COMMENT 'nullable: the reason may be that there is no id',
  raw_payload     string    COMMENT 'the frame, so a fix can be replayed not just counted',
  failure_reason  string    COMMENT 'which rule rejected it, from wikistream.events',
  failed_at       timestamp,
  ingest_date     date      COMMENT 'partition column'
)
USING iceberg
PARTITIONED BY (days(ingest_date))
{_table_properties()}
""".strip()


def sort_order_statements(settings: Settings | None = None) -> list[str]:
    """`ALTER TABLE ... WRITE ORDERED BY`, applied after creation.

    Set as a table property rather than at write time so that compaction
    (`rewrite_data_files`) produces files sorted the same way the streaming writes
    were. Sorting silver by `event_time` is what makes a time-range query skip
    files on min/max statistics instead of reading the partition.

    Separate from the DDL because `CREATE TABLE ... ORDERED BY` is not valid
    Spark SQL, and because these are idempotent in their own right: applying the
    same sort order twice is a no-op rather than an error.
    """
    cfg = settings or get_settings()
    return [
        f"ALTER TABLE {cfg.silver_edits_table} WRITE ORDERED BY event_time",
        # Bronze is read by offset ranges during replay far more often than by
        # time, and (partition, offset) is the order the data already arrives in,
        # so this sort order costs nothing at write time.
        f"ALTER TABLE {cfg.bronze_raw_table} WRITE ORDERED BY kafka_partition, kafka_offset",
    ]


def all_statements(settings: Settings | None = None) -> list[str]:
    """Every statement needed to bring an empty catalog to the full schema."""
    cfg = settings or get_settings()
    return [
        *namespace_statements(cfg),
        bronze_raw_ddl(cfg),
        silver_edits_ddl(cfg),
        silver_quarantine_ddl(cfg),
        *sort_order_statements(cfg),
    ]
