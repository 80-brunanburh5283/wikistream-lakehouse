#!/usr/bin/env python
"""Run every schema change this pipeline could need, and check what it cost.

    make prove-schema-evolution

Iceberg's headline claim is that adding, renaming, reordering, widening and dropping a
column are metadata operations: no data file is rewritten, and old files read correctly
through the new schema. That is the property the silver layer depends on — the source can
add a field at any time — so it is worth demonstrating rather than citing.

Every step below prints what it observed and is checked, so the exit code means
something. The output of one real run is in `docs/correctness.md`.

## Why a scratch namespace

The table is created from `silver_edits_ddl` — the real DDL, not a toy — in a namespace
named for this script, and dropped at the end. Running the demonstration against
`silver.edits` would leave it carrying a permanently null `edit_source` column and a
`namespace_id` widened for no reason, which is exactly the kind of debris that makes a
portfolio repository untrustworthy. `--keep` skips the cleanup for a human who wants to
poke at the result.

## What this cannot show

That a *rename* is safe for downstream readers. It is safe for the data — Iceberg tracks
columns by id, so the bytes are untouched and every old file still resolves — but any dbt
model or dashboard naming the old column breaks immediately. Iceberg protects the table,
not the queries, and no test can rescue a query that names a column that no longer
exists.
"""

from __future__ import annotations

import argparse
import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyspark.errors import AnalysisException

from wikistream.config import Settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.session import build_session
from wikistream.streaming.tables import silver_edits_ddl

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructField

log = get_logger("wikistream.prove_schema_evolution")

#: Two rows written before the schema changes, and one after. Small on purpose: the
#: claim is about metadata, and a million rows would make the run slow without making
#: the file count more meaningful.
#:
#: Column to SQL literal, not Python value: `_insert` has to name every column of the
#: real DDL, and writing the literal here keeps the type of the two that need a prefix
#: with the value rather than in a lookup table somewhere else.
BEFORE_ROWS = (
    {
        "event_id": "'evo-1'",
        "event_time": "TIMESTAMP '2026-09-17 10:00:00'",
        "event_date": "DATE '2026-09-17'",
        "wiki": "'enwiki'",
        "domain": "'en.wikipedia.org'",
        "namespace_id": "0",
        "bytes_new": "100",
    },
    {
        "event_id": "'evo-2'",
        "event_time": "TIMESTAMP '2026-09-17 10:00:01'",
        "event_date": "DATE '2026-09-17'",
        "wiki": "'dewiki'",
        "domain": "'de.wikipedia.org'",
        "namespace_id": "4",
        "bytes_new": "200",
    },
)
AFTER_ROW = {
    "event_id": "'evo-3'",
    "event_time": "TIMESTAMP '2026-09-17 11:00:00'",
    "event_date": "DATE '2026-09-17'",
    "wiki": "'frwiki'",
    "domain": "'fr.wikipedia.org'",
    "namespace_id": "0",
    "bytes_new": "300",
}

#: The `event_id` of the row written after the schema changed, unquoted, for the
#: predicates that go looking for it.
AFTER_ID = AFTER_ROW["event_id"].strip("'")

#: The column added, renamed and dropped over the course of the run. Named as a field
#: the source could plausibly grow, so the demonstration reads like the change it stands
#: in for.
ADDED_COLUMN = "edit_source"
RENAMED_COLUMN = "edit_channel"


@dataclass
class Check:
    """One claim about a schema change, and what the table said."""

    name: str
    observed: Any
    expected: Any

    @property
    def passed(self) -> bool:
        """Exact equality: every claim here is about a count, a value or a column name."""
        return self.observed == self.expected

    def line(self) -> str:
        """One aligned row, so two runs can be diffed."""
        verdict = "pass" if self.passed else "FAIL"
        return f"  [{verdict}] {self.name:<52} {self.observed!r:>30}  expected {self.expected!r}"


@contextmanager
def _expected_to_fail(logger_name: str) -> Iterator[None]:
    """Quieten one logger around a statement whose failure is the result being measured.

    `PySparkException._log_exception` logs every `AnalysisException` at ERROR, with the
    whole Java stack trace, before raising it. This script's output is quoted verbatim in
    `docs/correctness.md`, and eighty lines of trace printed under a check that passed
    reads as a broken run.

    It is a Python logger, not log4j — `PySparkLogger.getLogger("SQLQueryContextLogger")`
    in `pyspark/errors/exceptions/base.py`, with `propagate = False` and its own JSON
    handler, so `configure_logging` never sees the record and the only lever is that
    logger's own level. Scoped to one name and one statement, so a failure anywhere else
    in the script is still loud, and the level is put back afterwards.
    """
    logger = logging.getLogger(logger_name)
    previous = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        logger.setLevel(previous)


def _scalar(spark: SparkSession, sql: str) -> Any:
    """The first column of the first row, or None when there is no row."""
    row = spark.sql(sql).first()
    return None if row is None else row[0]


def _files(spark: SparkSession, table: str) -> int:
    """Data files in the current snapshot. The number a metadata-only change must not move."""
    return spark.sql(f"SELECT * FROM {table}.files").count()


def _columns(spark: SparkSession, table: str) -> list[str]:
    return [field.name for field in spark.table(table).schema.fields]


def _cell(row: dict[str, str], field: StructField) -> str:
    """The literal for one column of one fixture row, or a typed NULL if it sets none."""
    literal = row.get(field.name)
    if literal is None:
        return f"CAST(NULL AS {field.dataType.simpleString()})"
    return literal


def _insert(spark: SparkSession, table: str, rows: tuple[dict[str, str], ...]) -> None:
    """Insert `rows`, naming every column the table currently has.

    Spark 4 will not null-fill a column left out of `INSERT INTO t (a, b) VALUES ...` here.
    It fails the write with `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA: Cannot find data
    for the output column`, because the column list is a promise to supply values and
    Iceberg declares no defaults to fall back on. So the statement is built from the
    table's live schema and casts an explicit NULL into every column the fixture leaves
    unset — which also keeps it correct after the ALTERs below change what the schema is.

    Literal SQL rather than a DataFrame write: this script is a demonstration and its
    statements are meant to be copied out of `docs/correctness.md` into a `make sql`
    call by a reader who wants to repeat it.
    """
    fields = spark.table(table).schema.fields
    selects = [
        "SELECT " + ", ".join(f"{_cell(row, field)} AS {field.name}" for field in fields)
        for row in rows
    ]
    spark.sql(f"INSERT INTO {table} " + " UNION ALL ".join(selects))


def _current_snapshot(spark: SparkSession, table: str) -> int:
    """The newest snapshot id, captured before a change so time travel has a target."""
    return int(
        _scalar(
            spark, f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        )
    )


def add_a_column(spark: SparkSession, table: str) -> list[Check]:
    """The case the source will actually cause: a new field appears upstream.

    Two claims, and the second is the one that matters: no data file is rewritten, and
    the rows written before the column existed read as null rather than failing or
    disappearing. Null is the honest answer — the old files genuinely do not contain the
    value — and it is why a backfill is a separate decision from an `ALTER TABLE`.
    """
    files_before = _files(spark, table)
    spark.sql(f"ALTER TABLE {table} ADD COLUMN {ADDED_COLUMN} string COMMENT 'added by the demo'")

    checks = [
        Check("ADD COLUMN rewrote no data file", _files(spark, table), files_before),
        Check(
            "rows written before the column read null",
            _scalar(spark, f"SELECT count(*) FROM {table} WHERE {ADDED_COLUMN} IS NULL"),
            len(BEFORE_ROWS),
        ),
    ]

    _insert(spark, table, ({**AFTER_ROW, ADDED_COLUMN: "'demo'"},))
    checks += [
        Check(
            "a row written after it carries the value",
            _scalar(spark, f"SELECT {ADDED_COLUMN} FROM {table} WHERE event_id = '{AFTER_ID}'"),
            "demo",
        ),
        Check(
            "old and new files coexist in one query",
            _scalar(spark, f"SELECT count(*) FROM {table}"),
            len(BEFORE_ROWS) + 1,
        ),
    ]
    return checks


def travel_back(spark: SparkSession, table: str, snapshot: int) -> list[Check]:
    """Read the table as it was before any of this happened.

    Both forms are exercised because they fail differently: a snapshot id is exact and
    outlives nothing, while a timestamp resolves to whichever snapshot was current at
    that instant and silently moves if an earlier one is expired. `expire_snapshots`
    keeps a floor of recent snapshots for exactly this reason — see
    `wikistream.maintenance.RETAIN_LAST_SNAPSHOTS`.
    """
    committed_at = _scalar(
        spark, f"SELECT committed_at FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    )
    return [
        Check(
            "FOR VERSION AS OF sees the pre-change table",
            _scalar(spark, f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot}"),
            len(BEFORE_ROWS),
        ),
        Check(
            "FOR TIMESTAMP AS OF resolves to the same snapshot",
            _scalar(
                spark,
                f"SELECT count(*) FROM {table} FOR TIMESTAMP AS OF TIMESTAMP '{committed_at}'",
            ),
            len(BEFORE_ROWS),
        ),
        Check(
            "the live table is unchanged by reading history",
            _scalar(spark, f"SELECT count(*) FROM {table}"),
            len(BEFORE_ROWS) + 1,
        ),
    ]


def rename_a_column(spark: SparkSession, table: str) -> list[Check]:
    """Prove that Iceberg tracks columns by id and not by name.

    A rename in a format that resolves columns by name — Hive, or Parquet read
    positionally — means the old files no longer match and the column reads null. Here
    the value survives, which is the whole reason schema evolution in Iceberg is called
    safe. The bytes on disk still say `edit_source`; only the metadata moved.
    """
    files_before = _files(spark, table)
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN {ADDED_COLUMN} TO {RENAMED_COLUMN}")
    return [
        Check("RENAME COLUMN rewrote no data file", _files(spark, table), files_before),
        Check(
            "the value survived the rename",
            _scalar(spark, f"SELECT {RENAMED_COLUMN} FROM {table} WHERE event_id = '{AFTER_ID}'"),
            "demo",
        ),
        Check(
            f"{ADDED_COLUMN} is gone from the schema",
            ADDED_COLUMN in _columns(spark, table),
            False,
        ),
    ]


def widen_and_refuse_to_narrow(spark: SparkSession, table: str) -> list[Check]:
    """Widening int to bigint is allowed; narrowing back is not, and that is the point.

    Widening is safe because every value in an existing file still fits the new type, so
    Iceberg permits it as metadata. Narrowing is not: the check is against the *schema*,
    not against the data, so it is refused even here where all three values would fit in
    an int. That refusal is the guard rail behind making every source-derived integer
    bigint — see the comment in `streaming.tables` — and a demonstration that showed only
    the widening working would be telling half the story.

    Worth being precise about who refuses. The error is `NOT_SUPPORTED_CHANGE_COLUMN`
    raised by Spark's own analyzer, before the request reaches the catalog, so this check
    would pass against a table format that would happily have narrowed the column. Iceberg
    refuses it too — `SchemaUpdate.updateColumn` allows only widening promotions — but
    this script is not what proves that.
    """
    files_before = _files(spark, table)
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN namespace_id TYPE bigint")
    checks = [
        Check(
            "namespace_id widened to bigint",
            dict(spark.table(table).dtypes)["namespace_id"],
            "bigint",
        ),
        Check("widening rewrote no data file", _files(spark, table), files_before),
        Check(
            "the widened values are unchanged",
            _scalar(spark, f"SELECT sum(namespace_id) FROM {table}"),
            sum(int(row["namespace_id"]) for row in (*BEFORE_ROWS, AFTER_ROW)),
        ),
    ]

    # The refusal is the assertion. `AnalysisException` and not `Exception`, because a
    # broad catch would swallow a lost connection to the catalog and print it as proof
    # that the schema is protected.
    with _expected_to_fail("SQLQueryContextLogger"):
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN namespace_id TYPE int")
            refused = "the ALTER succeeded"
        except AnalysisException as exc:
            refused = exc.getCondition() or type(exc).__name__
            print(f"  narrowing bigint to int was refused: {str(exc).splitlines()[0]}")

    checks.append(
        Check("narrowing bigint to int is refused", refused, "NOT_SUPPORTED_CHANGE_COLUMN")
    )
    return checks


def drop_a_column(spark: SparkSession, table: str) -> list[Check]:
    """The destructive one, and the reason time travel is part of this script.

    `DROP COLUMN` is metadata too: the values stay in the data files, unreachable
    through the current schema. The rows keep reading, and an older snapshot still
    exposes the column — until `expire_snapshots` removes that snapshot, at which point
    the data becomes genuinely unreachable. Dropping a column is therefore reversible
    for as long as the history you kept, and no longer.
    """
    files_before = _files(spark, table)
    spark.sql(f"ALTER TABLE {table} DROP COLUMN {RENAMED_COLUMN}")
    return [
        Check("DROP COLUMN rewrote no data file", _files(spark, table), files_before),
        Check(
            "every row still reads",
            _scalar(spark, f"SELECT count(*) FROM {table}"),
            len(BEFORE_ROWS) + 1,
        ),
        Check(
            f"{RENAMED_COLUMN} is gone from the schema",
            RENAMED_COLUMN in _columns(spark, table),
            False,
        ),
    ]


def prove(spark: SparkSession, table: str) -> list[Check]:
    """Run the whole sequence against `table`, in the order a real migration would."""
    print(f"\n{table}")
    _insert(spark, table, BEFORE_ROWS)
    baseline = _current_snapshot(spark, table)
    print(
        f"  baseline snapshot {baseline}, {len(BEFORE_ROWS)} rows, {_files(spark, table)} file(s)"
    )
    print(f"  columns: {len(_columns(spark, table))}")

    checks = add_a_column(spark, table)
    checks += travel_back(spark, table, baseline)
    checks += rename_a_column(spark, table)
    checks += widen_and_refuse_to_narrow(spark, table)
    checks += drop_a_column(spark, table)
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--namespace",
        default="schema_evolution_demo",
        help="Scratch namespace to create the table in. Never the real silver namespace.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Leave the table behind for inspection instead of dropping it.",
    )
    args = parser.parse_args(argv)

    # A fresh Settings rather than get_settings(), so the DDL is generated for the
    # scratch namespace. Everything else — catalog, endpoint, credentials — still comes
    # from the environment.
    settings = Settings(silver_namespace=args.namespace)
    if args.namespace == Settings().silver_namespace:
        parser.error("refusing to run against the real silver namespace; pass --namespace")

    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("prove-schema-evolution", settings)
    table = settings.silver_edits_table
    try:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {settings.iceberg_catalog_name}.{args.namespace}"
        )
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.sql(silver_edits_ddl(settings))

        checks = prove(spark, table)

        print("\nchecks")
        for check in checks:
            print(check.line())

        failed = [check for check in checks if not check.passed]
        for check in failed:
            log.error(
                "schema evolution check failed",
                extra={"check": check.name, "observed": check.observed, "expected": check.expected},
            )
        if failed:
            print(f"\n{len(failed)} of {len(checks)} checks FAILED")
            return 1
        print(f"\nall {len(checks)} checks passed")
        return 0
    finally:
        if not args.keep:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
            spark.sql(f"DROP NAMESPACE IF EXISTS {settings.iceberg_catalog_name}.{args.namespace}")
            print(f"dropped {table} and its namespace")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
