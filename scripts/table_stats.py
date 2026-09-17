#!/usr/bin/env python
"""Row counts, file counts and file sizes for every Iceberg table.

    make table-stats

Reads Iceberg's own metadata tables rather than the data, so the file statistics
cost one manifest read instead of a full scan. The row count is a real `COUNT(*)`,
which Iceberg answers from manifest row counts when there are no deletes to apply.

The reason this script exists at all is the small-files problem. A streaming
pipeline that commits every 30 seconds produces a lot of small Parquet files, and
the difference between a healthy table and one that needs `make maintain` is
visible in exactly two numbers: files per partition, and average file size. Both
are printed here, so "compaction helps" is a measurement in this repository rather
than an assertion.
"""

from __future__ import annotations

import sys
from dataclasses import asdict
from typing import TYPE_CHECKING

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.maintenance import TableStats, collect_stats, duplicate_keys
from wikistream.streaming.session import build_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger("wikistream.table_stats")


def _fmt(number: float, suffix: str = "") -> str:
    """Thousands separators, because six-digit row counts are misread without them."""
    return f"{number:,.0f}{suffix}" if number == int(number) else f"{number:,.1f}{suffix}"


def duplicate_event_ids(spark: SparkSession, table: str) -> tuple[int, int]:
    """`(distinct event ids, ids appearing more than once)` for a table.

    Reported for bronze too, where duplicates are expected and correct: bronze is
    the audit log, so a producer restart legitimately lands the same `meta.id`
    twice. Printing it here is what makes the silver number mean something — an
    invariant of "zero duplicates in silver" is only interesting next to evidence
    that duplicates existed upstream.
    """
    distinct = spark.sql(
        f"SELECT count(DISTINCT event_id) AS ids FROM {table} WHERE event_id IS NOT NULL"
    ).first()
    if distinct is None:  # pragma: no cover - an aggregate always returns one row
        raise RuntimeError(f"no aggregate row for {table}")
    return int(distinct["ids"]), duplicate_keys(spark, table, ("event_id",))


def report(stats: TableStats) -> None:
    """Print one table's block, aligned so two runs can be diffed by eye."""
    print(f"\n{stats.table}")
    print(
        f"  rows {_fmt(stats.rows):>12}"
        f"   files {_fmt(stats.files):>6}"
        f"   partitions {_fmt(stats.partitions):>4}"
        f"   snapshots {_fmt(stats.snapshots):>5}"
    )
    print(
        f"  total {_fmt(stats.total_mib, ' MiB'):>12}"
        f"   avg file {_fmt(stats.avg_kib, ' KiB'):>12}"
        f"   min {_fmt(stats.min_kib, ' KiB'):>10}"
        f"   max {_fmt(stats.max_kib, ' KiB'):>10}"
    )
    print(f"  files per partition {stats.files_per_partition:.1f}")


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("table-stats", settings)

    bronze = settings.bronze_raw_table
    quarantine = settings.silver_quarantine_table

    for table in (bronze, settings.silver_edits_table, quarantine):
        stats = collect_stats(spark, table)
        report(stats)
        log.info("table stats", extra=asdict(stats))

        # Quarantine has no uniqueness expectation: the same bad frame replayed
        # twice is two rows there, and that is the point of the table.
        if table != quarantine:
            distinct_ids, repeated_ids = duplicate_event_ids(spark, table)
            note = "expected in an audit log" if table == bronze else "must be zero"
            print(
                f"  distinct event_id {_fmt(distinct_ids):>10}"
                f"   repeated {_fmt(repeated_ids):>8}   ({note})"
            )

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
