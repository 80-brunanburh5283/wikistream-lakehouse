#!/usr/bin/env python
"""Replay a date range of bronze through the silver logic.

    make rebuild-silver FROM=2026-09-17 TO=2026-09-17

This is what the bronze table is for. If a transformation rule was wrong — a bad
`is_anonymous` regex, a validation rule that quarantined valid rows — the fix is to
correct the rule and replay, because bronze kept every payload byte-exact and nothing
in silver is authoritative. Without bronze, a logic bug would be permanent data loss.

It runs the same functions as the stream, not a copy of them: `frames_from_bronze`
projects bronze into the identical shape `frames_from_kafka` produces, and both feed
`to_candidates` and `merge_batch`. So the rebuild cannot drift from the live path, and
the pair of tests in `tests/integration/test_silver_rebuild.py` asserts the property
that follows from the MERGE being insert-if-absent: running this twice changes nothing.

## Why the range is on ingest date, not event date

`--from`/`--to` filter bronze's `ingest_date` partition column. "Reprocess what we
ingested on the 17th" prunes to whole partitions and is answerable; "reprocess events
that happened on the 17th" would scan every partition to find them, and is also the
wrong question — the unit of replay is an ingestion window, because that is what a
deployment of a broken rule affects.

## What a rebuild does not undo

The MERGE only inserts. A row already in `silver.edits` is left exactly as it was, so a
replay repairs *missing* rows and rows that were wrongly quarantined; it does not
rewrite rows that were written with a bad value. Correcting those means deleting the
affected range first — `DELETE FROM silver.edits WHERE event_date = ...` — and that is
a deliberate manual step rather than a flag on this script, because it is destructive
and Iceberg's snapshot history is the only way back.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.session import build_session
from wikistream.streaming.silver import rebuild_from_bronze

log = get_logger("wikistream.rebuild_silver")


def _iso_date(value: str) -> str:
    """Validate `YYYY-MM-DD` before Spark sees it.

    A malformed date cast in Spark yields null rather than an error, so
    `--from notadate` would silently filter everything out and report a successful
    rebuild of zero rows. Failing in `argparse` instead keeps that from looking like
    success.
    """
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value!r} is not a YYYY-MM-DD date") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild silver from bronze for a date range.")
    parser.add_argument(
        "--from",
        dest="date_from",
        required=True,
        type=_iso_date,
        help="First ingest date to replay, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        required=True,
        type=_iso_date,
        help="Last ingest date to replay, inclusive (YYYY-MM-DD).",
    )
    args = parser.parse_args(argv)

    if args.date_from > args.date_to:
        parser.error(f"--from {args.date_from} is after --to {args.date_to}")

    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("rebuild-silver", settings)
    try:
        source_rows = (
            spark.table(settings.bronze_raw_table)
            .where(f"ingest_date BETWEEN date '{args.date_from}' AND date '{args.date_to}'")
            .count()
        )
        print(
            f"replaying {source_rows:,} bronze rows "
            f"from {args.date_from} to {args.date_to} inclusive"
        )
        if source_rows == 0:
            # Not an error: an empty window is a legitimate answer, and failing here
            # would make the phase gate depend on the machine having run overnight.
            print("nothing to replay in that window")

        counts = rebuild_from_bronze(spark, settings, args.date_from, args.date_to)

        print(f"silver.edits now holds      {counts['edits']:>12,} rows")
        print(f"silver.quarantine now holds {counts['quarantine']:>12,} rows")
        log.info(
            "rebuild complete",
            extra={"date_from": args.date_from, "date_to": args.date_to, **counts},
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
