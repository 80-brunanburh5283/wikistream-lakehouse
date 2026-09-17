#!/usr/bin/env python
"""The duplicate-suppression gate. Exits non-zero when the invariant is broken.

    make verify-no-duplicates

`silver.edits` claims one row per source event, and Iceberg has no primary key to
enforce it — the only thing making the claim true is that every write goes through
`MERGE INTO ... WHEN NOT MATCHED`. An invariant with no test is a wish, so this is the
test, and it is a script rather than a `.sql` file for one reason: a query prints a
number, and a gate has to fail. `make up`, the restart-idempotency proof in
`docs/correctness.md` and CI all call this and all depend on the exit code.

## The vacuous pass

Zero duplicates in an empty table is not evidence of anything, and a gate that passes
on an empty lakehouse is worse than no gate: it goes green on the exact failure it
exists to catch, which is a silver stream that died before writing. So an empty
`silver.edits` is a failure here unless `--allow-empty` is given, which only the
fresh-stack path uses.

## What it deliberately does not check

Completeness against bronze. Silver reads Kafka directly rather than reading bronze
(see `wikistream.streaming.silver`), so at any instant the two tables are at different
offsets and a set difference between them is a race, not a defect. Bronze's own
duplicate count is printed instead, as context: "zero duplicates in silver" only means
something next to evidence that duplicates existed upstream.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.maintenance import duplicate_keys
from wikistream.streaming.session import build_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from wikistream.config import Settings

log = get_logger("wikistream.verify_no_duplicates")


@dataclass(frozen=True)
class Check:
    """One assertion about the lakehouse, and what the data said."""

    name: str
    detail: str
    observed: int
    expected: int

    @property
    def passed(self) -> bool:
        """Whether the observed count matched. Every check here expects an exact number."""
        return self.observed == self.expected

    def line(self) -> str:
        """One aligned row of output, so a run can be diffed against the previous one."""
        verdict = "pass" if self.passed else "FAIL"
        return (
            f"  [{verdict}] {self.name:<38} "
            f"observed {self.observed:>10,}  expected {self.expected:,}"
        )


def _scalar(spark: SparkSession, sql: str) -> int:
    """The first column of the first row, as an int."""
    row = spark.sql(sql).first()
    if row is None:  # pragma: no cover - every aggregate returns one row
        raise RuntimeError(f"no row returned by: {sql}")
    return int(row[0])


def checks(spark: SparkSession, settings: Settings, *, allow_empty: bool) -> list[Check]:
    """Every assertion this gate makes, evaluated."""
    edits = settings.silver_edits_table
    quarantine = settings.silver_quarantine_table

    rows = _scalar(spark, f"SELECT count(*) FROM {edits}")
    results = [
        Check(
            name="silver.edits duplicate event_id",
            detail="MERGE INTO ... WHEN NOT MATCHED is the only thing enforcing this",
            observed=duplicate_keys(spark, edits, ("event_id",)),
            expected=0,
        ),
        Check(
            name="silver.edits null event_id",
            detail="the MERGE key; a null would match nothing and insert every time",
            observed=_scalar(spark, f"SELECT count(*) FROM {edits} WHERE event_id IS NULL"),
            expected=0,
        ),
        Check(
            name="silver.edits null event_time",
            detail="the partition source; a null would land rows outside every partition",
            observed=_scalar(spark, f"SELECT count(*) FROM {edits} WHERE event_time IS NULL"),
            expected=0,
        ),
        Check(
            name="silver.quarantine duplicate kafka offset",
            detail="quarantine keys on (partition, offset) because event_id may be absent",
            observed=duplicate_keys(spark, quarantine, ("kafka_partition", "kafka_offset")),
            expected=0,
        ),
    ]

    if not allow_empty:
        # Phrased as "is it empty" so a failure reads as `observed 1, expected 0`
        # rather than as an inequality the reader has to interpret.
        results.append(
            Check(
                name="silver.edits is empty",
                detail="a zero-duplicate claim about an empty table is vacuous",
                observed=int(rows == 0),
                expected=0,
            )
        )
    return results


def report_context(spark: SparkSession, settings: Settings) -> None:
    """Print what the tables hold, and how many duplicates bronze absorbed.

    Bronze duplicates are expected and correct — it is the audit log, so a producer
    restart legitimately lands the same `meta.id` twice. Printing the number next to
    silver's zero is what turns the gate from a tautology into evidence.
    """
    edits = _scalar(spark, f"SELECT count(*) FROM {settings.silver_edits_table}")
    quarantined = _scalar(spark, f"SELECT count(*) FROM {settings.silver_quarantine_table}")
    bronze_rows = _scalar(spark, f"SELECT count(*) FROM {settings.bronze_raw_table}")
    bronze_dupes = duplicate_keys(spark, settings.bronze_raw_table, ("event_id",))

    print("\nlakehouse contents")
    print(f"  bronze.raw rows           {bronze_rows:>12,}")
    print(f"  bronze.raw repeated ids   {bronze_dupes:>12,}   (expected: it is an audit log)")
    print(f"  silver.edits rows         {edits:>12,}")
    print(f"  silver.quarantine rows    {quarantined:>12,}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail if the silver tables contain duplicates.")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Do not fail when silver.edits is empty (a stack that has not ingested yet).",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("verify-no-duplicates", settings)
    try:
        report_context(spark, settings)
        results = checks(spark, settings, allow_empty=args.allow_empty)

        print("\nchecks")
        for check in results:
            print(check.line())

        failed = [check for check in results if not check.passed]
        for check in failed:
            log.error(
                "duplicate check failed",
                extra={"check": check.name, "observed": check.observed, "detail": check.detail},
            )

        if failed:
            print(f"\n{len(failed)} of {len(results)} checks FAILED")
            for check in failed:
                print(f"  {check.name}: {check.detail}")
            return 1

        print(f"\nall {len(results)} checks passed")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
