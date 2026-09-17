"""The SQL the observations and the asset checks send to Trino.

Kept in one module, away from the Dagster decorators, for two reasons. Every
count in this package has to be spelled the same way or two of them are not
comparable — and a query in a plain function can be read, diffed and unit-tested
without constructing an asset context first. `tests/unit/test_dagster_sql.py`
asserts the shapes these produce.

The identifiers interpolated here come from `wikistream.config.Settings`, never
from a Dagster run config or an external caller: `str.format` on a table name
with a user-supplied value would be an injection, and there is no parameter
binding for an identifier in any SQL dialect.
"""

from __future__ import annotations

from collections.abc import Sequence


def metadata_table(table: str, suffix: str) -> str:
    """Name an Iceberg metadata table for Trino: `cat.ns."tbl$files"`.

    Trino spells these with a `$` inside the table name, which makes the whole
    identifier need quoting — `lakehouse.silver."edits$files"`. Getting that
    wrong produces `Table 'lakehouse.silver.edits$files' does not exist`, which
    reads as a missing table rather than a quoting mistake.
    """
    catalog, namespace, name = table.split(".")
    return f'{catalog}.{namespace}."{name}${suffix}"'


def quote_list(values: Sequence[str]) -> str:
    """Render a sequence of strings as a SQL `in` list of literals."""
    escaped = ", ".join("'" + value.replace("'", "''") + "'" for value in values)
    return f"({escaped})"


def row_count(table: str) -> str:
    return f"select count(*) as rows from {table}"


def file_statistics(table: str) -> str:
    """Data file count, total bytes and record count from the current snapshot.

    This is the small-file problem made visible: the ratio of `record_count` to
    `data_files` is what `make maintain` changes, and having it on the asset in
    the Dagster UI means the number is seen without anyone running a maintenance
    job to find out.
    """
    return (
        "select count(*) as data_files, "
        "coalesce(sum(file_size_in_bytes), 0) as bytes, "
        "coalesce(sum(record_count), 0) as records "
        f"from {metadata_table(table, 'files')}"
    )


def bronze_observation(table: str) -> str:
    """Row count and ingest recency for the append-only bronze table."""
    return (
        "select count(*) as rows, "
        "max(ingested_at) as last_ingested_at, "
        "min(kafka_offset) as min_offset, "
        "max(kafka_offset) as max_offset "
        f"from {table}"
    )


def bronze_offset_contiguity(table: str) -> str:
    """Per-partition row count against the offset range it spans.

    Bronze appends one row per Kafka record and never deduplicates, so for a
    consumer that has read a contiguous range the count must equal
    `max - min + 1` exactly. Fewer rows means records were lost between Kafka
    and Iceberg; more means a micro-batch was applied twice.

    This is the cross-table check that "silver rows <= bronze rows" is usually
    written as, done in a form that is actually an invariant. Bronze and silver
    are independent consumers with separate checkpoints, so either can be ahead
    of the other at any instant and a comparison between them proves nothing.
    A partition's own offsets are self-consistent whatever the other job is doing.
    """
    return (
        "select kafka_partition, "
        "count(*) as rows, "
        "min(kafka_offset) as min_offset, "
        "max(kafka_offset) as max_offset "
        f"from {table} "
        "group by kafka_partition "
        "order by kafka_partition"
    )


def silver_edits_observation(table: str) -> str:
    """Volume, recency and lateness for the deduplicated table."""
    return (
        "select count(*) as rows, "
        "count(distinct event_id) as distinct_event_ids, "
        "max(event_time) as last_event_at, "
        "max(ingested_at) as last_ingested_at, "
        "count_if(domain = 'canary') as canary_events, "
        "approx_percentile(late_by_seconds, 0.95) as late_p95_seconds "
        f"from {table}"
    )


def silver_edits_freshness(table: str) -> str:
    """Seconds between the newest event in the table and now.

    Measured against `event_time`, not against the last Dagster observation.
    Dagster ships `build_last_update_freshness_checks`, which measures the age of
    the newest materialisation or observation *event* — for a table written by a
    process Dagster does not run, that measures how recently the observation
    schedule ticked. It would go green on a dead pipeline as long as the observer
    was alive, which is the opposite of what a freshness check is for.
    """
    return (
        "select to_unixtime(current_timestamp) - to_unixtime(max(event_time)) as lag_seconds, "
        "max(event_time) as last_event_at "
        f"from {table}"
    )


def silver_duplicate_event_ids(table: str) -> str:
    """Total rows against distinct `event_id`s.

    Iceberg has no uniqueness constraint. The only thing making `event_id`
    unique is that every write goes through `MERGE INTO ... WHEN NOT MATCHED`,
    so this is the check that the claim in the table's DDL comment is still true.
    """
    return f"select count(*) as rows, count(distinct event_id) as distinct_event_ids from {table}"


def canary_events_in_last_complete_hour(table: str) -> str:
    """Heartbeat count for the most recent hour that has certainly finished.

    Wikimedia emits two synthetic `canary` events per hour into every stream, at
    about fifteen minutes past. They carry no wiki, page or editor and exist to
    prove the connection is alive when the wikis themselves are quiet. Zero of
    them in a complete hour that has other traffic means the SSE connection
    dropped and reconnected past the gap.

    "Complete" is the hour before the current one, and the current hour is
    excluded because it is always partial. `date_trunc` on `current_timestamp`
    rather than on `max(event_time)`: if ingestion stopped two days ago, the
    newest hour in the table looks complete and is full of canaries, and the
    check would pass. Wall-clock time is what makes the window move.
    """
    return (
        "select count_if(domain = 'canary') as canary_events, count(*) as rows "
        f"from {table} "
        "where event_time >= date_trunc('hour', current_timestamp) - interval '1' hour "
        "and event_time < date_trunc('hour', current_timestamp)"
    )


def quarantine_rate(edits_table: str, quarantine_table: str, lookback_hours: int) -> str:
    """Quarantined rows as a share of everything ingested in the lookback window.

    The two sides are bucketed on different clocks — quarantine on `failed_at`
    (ingest time) because an unparseable timestamp is itself a reason to
    quarantine, edits on `ingested_at` — so this is an approximation. Using
    ingest time on both sides is what makes it as close as it can be: an event's
    two possible destinations are then measured on the same clock even though
    neither is measured on event time.
    """
    window = f"interval '{lookback_hours}' hour"
    return (
        "select "
        f"(select count(*) from {edits_table} "
        f" where ingested_at >= current_timestamp - {window}) as edits, "
        f"(select count(*) from {quarantine_table} "
        f" where failed_at >= current_timestamp - {window}) as quarantined"
    )


def unknown_quarantine_reasons(table: str, known: Sequence[str]) -> str:
    """Any `failure_reason` the Python rule set does not define.

    `wikistream.quality.expectations.FAILURE_REASONS` is the closed list, and it
    is imported rather than restated so that adding a rule cannot leave this
    check asserting the old set. A reason outside it means the Spark job is
    writing a value no rule produces, which makes every `group by
    failure_reason` dashboard incomplete.
    """
    return (
        "select failure_reason, count(*) as rows "
        f"from {table} "
        f"where failure_reason is null or failure_reason not in {quote_list(known)} "
        "group by failure_reason"
    )


def quarantine_observation(table: str) -> str:
    return (
        "select count(*) as rows, "
        "max(failed_at) as last_failed_at, "
        "count(distinct failure_reason) as distinct_failure_reasons "
        f"from {table}"
    )


def optimize(table: str) -> str:
    """Compact a table's data files. Trino's equivalent of `rewrite_data_files`."""
    return f"alter table {table} execute optimize"


def expire_snapshots(table: str, retention: str) -> str:
    """Drop snapshots older than `retention`, freeing the files they pin.

    Trino refuses a retention shorter than `iceberg.expire-snapshots.min-retention`
    (7 days by default) rather than silently clamping it, which is why the
    default here is not something demo-friendly like an hour.
    """
    return f"alter table {table} execute expire_snapshots(retention_threshold => '{retention}')"
