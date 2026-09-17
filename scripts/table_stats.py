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
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.session import build_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger("wikistream.table_stats")

_KIB = 1024
_MIB = 1024 * 1024


@dataclass(frozen=True)
class TableStats:
    """What one table looks like on disk right now."""

    table: str
    rows: int
    files: int
    partitions: int
    snapshots: int
    total_mib: float
    avg_kib: float
    min_kib: float
    max_kib: float

    @property
    def files_per_partition(self) -> float:
        """The number that decides whether compaction is overdue.

        A streaming write commits at least one file per partition per micro-batch,
        so this grows linearly with uptime until `make maintain` rewrites them.
        """
        return self.files / self.partitions if self.partitions else 0.0


def _fmt(number: float, suffix: str = "") -> str:
    """Thousands separators, because six-digit row counts are misread without them."""
    return f"{number:,.0f}{suffix}" if number == int(number) else f"{number:,.1f}{suffix}"


def collect_stats(spark: SparkSession, table: str) -> TableStats:
    """One table's row, file, partition and snapshot counts."""
    rows = spark.table(table).count()

    # `.files` is the current snapshot's data files. `file_size_in_bytes` is the
    # on-disk Parquet size after compression, not the in-memory size.
    file_agg = (
        spark.sql(f"SELECT file_size_in_bytes AS bytes FROM {table}.files")
        .selectExpr(
            "count(*) AS file_count",
            "coalesce(sum(bytes), 0) AS total_bytes",
            "coalesce(avg(bytes), 0) AS avg_bytes",
            "coalesce(min(bytes), 0) AS min_bytes",
            "coalesce(max(bytes), 0) AS max_bytes",
        )
        .first()
    )
    if file_agg is None:  # pragma: no cover - an aggregate always returns one row
        raise RuntimeError(f"no aggregate row for {table}.files")

    return TableStats(
        table=table,
        rows=rows,
        files=int(file_agg["file_count"]),
        partitions=spark.sql(f"SELECT * FROM {table}.partitions").count(),
        snapshots=spark.sql(f"SELECT * FROM {table}.snapshots").count(),
        total_mib=float(file_agg["total_bytes"]) / _MIB,
        avg_kib=float(file_agg["avg_bytes"]) / _KIB,
        min_kib=float(file_agg["min_bytes"]) / _KIB,
        max_kib=float(file_agg["max_bytes"]) / _KIB,
    )


def duplicate_event_ids(spark: SparkSession, table: str) -> tuple[int, int]:
    """`(distinct event ids, ids appearing more than once)` for a table.

    Reported for bronze too, where duplicates are expected and correct: bronze is
    the audit log, so a producer restart legitimately lands the same `meta.id`
    twice. Printing it here is what makes the silver number mean something — an
    invariant of "zero duplicates in silver" is only interesting next to evidence
    that duplicates existed upstream.
    """
    result = spark.sql(f"""
        SELECT count(*) AS distinct_ids,
               count_if(occurrences > 1) AS repeated_ids
        FROM (
          SELECT event_id, count(*) AS occurrences
          FROM {table}
          WHERE event_id IS NOT NULL
          GROUP BY event_id
        )
    """).first()
    if result is None:  # pragma: no cover - an aggregate always returns one row
        raise RuntimeError(f"no aggregate row for {table}")
    return int(result["distinct_ids"]), int(result["repeated_ids"])


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
