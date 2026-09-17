#!/usr/bin/env python
"""Compact files, reorganise manifests, expire snapshots, delete orphans.

    make maintain

Prints each table's statistics before and after, because the whole point of this script
is a claim that can be checked: "compaction reduced file count from N to M and raised
average file size from X to Y". Those are the two numbers in `docs/lakehouse.md`, and
they come from here rather than from a blog post.

## Stop the streams first

`remove_orphan_files` deletes files the metadata does not reference, and a file being
written by a commit that has not landed yet fits that description. The age threshold
(`--orphan-age-hours`, three days by default) is what makes it safe, but the honest
instruction is to stop the writers: `make down-streams` or Ctrl-C the stream, then run
this. The default is conservative enough that forgetting is survivable.

## What the numbers will and will not show on a laptop

At the measured ingest rate a day of running produces tens of MiB, not gigabytes, so
`rewrite_data_files` with the table's 128 MiB target consolidates a partition into one
file and stops. The interesting effect is visible anyway — files per partition falls to
one, average file size rises by an order of magnitude — but nobody should read a
laptop's compaction figures as capacity planning. The mechanism is what this
demonstrates; the scale is not claimed.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from typing import TYPE_CHECKING

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.maintenance import (
    ORPHAN_AGE_DEFAULT_HOURS,
    RETAIN_LAST_SNAPSHOTS,
    TableStats,
    collect_stats,
    expire_snapshots_sql,
    hours_ago,
    remove_orphan_files_sql,
    rewrite_data_files_sql,
    rewrite_manifests_sql,
    run_procedure,
)
from wikistream.streaming.session import build_session

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import SparkSession

log = get_logger("wikistream.maintain_tables")


def _summarise(stats: TableStats) -> str:
    """One line, so before and after can be read as a pair."""
    return (
        f"rows {stats.rows:>10,}   files {stats.files:>5,}   "
        f"partitions {stats.partitions:>3,}   snapshots {stats.snapshots:>4,}   "
        f"avg {stats.avg_kib:>9,.1f} KiB   total {stats.total_mib:>8,.1f} MiB"
    )


def maintain(
    spark: SparkSession,
    table: str,
    *,
    orphan_cutoff: datetime,
    snapshot_cutoff: datetime,
    target_bytes: int | None = None,
) -> tuple[TableStats, TableStats]:
    """Run all four procedures against one table; return its stats before and after.

    The order is deliberate and is explained in `wikistream.maintenance`: compaction
    first, then manifests, then expiry — which is what actually frees the disk the
    compaction just doubled — then orphans.

    Both cutoffs are passed in rather than derived here so that every table in one run
    is measured against the same instant. Recomputing "24 hours ago" per table would
    make the boundary drift by however long the previous table took.
    """
    before = collect_stats(spark, table)
    print(f"\n{table}")
    print(f"  before  {_summarise(before)}")

    steps = (
        ("rewrite_data_files", rewrite_data_files_sql(table, target_bytes=target_bytes)),
        ("rewrite_manifests", rewrite_manifests_sql(table)),
        (
            "expire_snapshots",
            expire_snapshots_sql(
                table,
                older_than=snapshot_cutoff,
                retain_last=RETAIN_LAST_SNAPSHOTS,
            ),
        ),
        ("remove_orphan_files", remove_orphan_files_sql(table, older_than=orphan_cutoff)),
    )

    for name, sql in steps:
        result = run_procedure(spark, sql)
        rendered = "  ".join(f"{key}={value}" for key, value in result.items())
        print(f"  {name:<20} {rendered or 'no result columns'}")
        log.info("maintenance step", extra={"table": table, "step": name, **result})

    after = collect_stats(spark, table)
    print(f"  after   {_summarise(after)}")
    return before, after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Iceberg maintenance on every table.")
    parser.add_argument(
        "--orphan-age-hours",
        type=int,
        default=ORPHAN_AGE_DEFAULT_HOURS,
        help=(
            "Only delete unreferenced files older than this. Lowering it while a stream "
            "is running can delete files belonging to a commit in flight."
        ),
    )
    parser.add_argument(
        "--snapshot-age-hours",
        type=int,
        default=24,
        help="Expire snapshots older than this, subject to the retain-last floor.",
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=None,
        help="Override the table's compaction target. Only useful at test volumes.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("maintain-tables", settings)
    # One instant for the whole run, so the two cutoffs mean the same thing for the
    # third table as for the first.
    orphan_cutoff = hours_ago(args.orphan_age_hours)
    snapshot_cutoff = hours_ago(args.snapshot_age_hours)
    try:
        tables = (
            settings.bronze_raw_table,
            settings.silver_edits_table,
            settings.silver_quarantine_table,
        )
        for table in tables:
            before, after = maintain(
                spark,
                table,
                orphan_cutoff=orphan_cutoff,
                snapshot_cutoff=snapshot_cutoff,
                target_bytes=args.target_file_size_bytes,
            )
            log.info(
                "maintenance complete",
                extra={
                    "table": table,
                    "files_before": before.files,
                    "files_after": after.files,
                    "avg_kib_before": round(before.avg_kib, 1),
                    "avg_kib_after": round(after.avg_kib, 1),
                },
            )
            if before.rows != after.rows:
                # Maintenance is not allowed to change what the table says. If it does,
                # something deleted live data and the exit code has to say so.
                log.error(
                    "maintenance changed the row count",
                    extra={"table": table, **asdict(after)},
                )
                print(
                    f"\nFAIL {table}: {before.rows:,} rows before, {after.rows:,} after. "
                    "Maintenance must not change table contents."
                )
                return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
