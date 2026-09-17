"""Iceberg table housekeeping: what a table looks like, and the four procedures that fix it.

A streaming write commits on every micro-batch, and every commit writes at least one
Parquet file per partition it touched plus a new manifest and a new `metadata.json`. At
a 30-second trigger that is 2,880 commits a day. Nothing is wrong with any individual
commit; the accumulation is the problem, and it shows up in two numbers — files per
partition, and average file size — long before it shows up as a slow query.

This module holds the measurements and the remedies so that `scripts/table_stats.py`,
`scripts/maintain_tables.py` and `scripts/verify_no_duplicates.py` share one
implementation rather than three. The scripts print; this decides.

## The four procedures, and what each one actually reclaims

| Procedure | Rewrites | Reclaims |
|---|---|---|
| `rewrite_data_files` | data files | query time: small Parquet files become large ones |
| `rewrite_manifests` | manifest lists | planning time: a manifest per commit becomes one per day |
| `expire_snapshots` | nothing | disk: data files no live snapshot references |
| `remove_orphan_files` | nothing | disk: files no *metadata at all* references |

The order matters. `rewrite_data_files` leaves the pre-compaction files referenced by
older snapshots, so it *increases* disk usage until `expire_snapshots` runs; running
expiry first would just mean doing it twice.

## remove_orphan_files is the dangerous one

It lists the table's directory and deletes anything the metadata does not reference.
A file being written right now by a commit that has not landed yet is, by that
definition, an orphan. Iceberg guards this with an age threshold, and
`ORPHAN_AGE_DEFAULT` keeps the conservative three days rather than trimming it to make
a demo look tidy: on this project the streams are stopped before maintenance runs, but
the default has to be safe for the case where somebody forgets. Iceberg itself refuses
an interval under 24 hours, for the same reason.

It is also the only procedure that has to be told *how* to look at storage, because it
is the only one that reads the object store directly rather than through the table's
metadata. See `remove_orphan_files_sql`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_KIB = 1024
_MIB = 1024 * 1024

#: How old a file must be before `remove_orphan_files` will delete it. Iceberg's own
#: default, kept deliberately: see the module docstring.
ORPHAN_AGE_DEFAULT_HOURS = 72

#: Snapshots younger than this are always kept, whatever the age threshold says, so
#: time travel in `docs/correctness.md` keeps working right after a maintenance run.
RETAIN_LAST_SNAPSHOTS = 5

#: Below this, `rewrite_data_files` does nothing at all — compacting a single file into
#: a single file is pure write amplification. Two is Iceberg's own floor and the point
#: at which compaction starts being worth the rewrite.
MIN_INPUT_FILES = 2


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

        A streaming write commits at least one file per partition per micro-batch, so
        this grows linearly with uptime until `make maintain` rewrites them.
        """
        return self.files / self.partitions if self.partitions else 0.0


def split_catalog(table: str) -> tuple[str, str]:
    """`lakehouse.silver.edits` becomes `("lakehouse", "silver.edits")`.

    Iceberg's stored procedures live on the catalog and take the table as a string
    argument relative to it — `CALL lakehouse.system.rewrite_data_files(table =>
    'silver.edits')`. Passing the fully qualified name in the argument fails with a
    "table not found" naming the catalog twice, which is a confusing five minutes.
    """
    catalog, _, relative = table.partition(".")
    if not relative:
        raise ValueError(f"{table!r} has no catalog prefix; expected catalog.namespace.table")
    return catalog, relative


def collect_stats(spark: SparkSession, table: str) -> TableStats:
    """One table's row, file, partition and snapshot counts.

    Reads Iceberg's metadata tables rather than the data, so the file statistics cost
    one manifest read instead of a full scan. The row count is a real `COUNT(*)`, which
    Iceberg answers from manifest row counts when there are no deletes to apply.
    """
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


def duplicate_keys(spark: SparkSession, table: str, keys: tuple[str, ...]) -> int:
    """How many key values appear more than once in `table`.

    Counted as "groups with more than one row" rather than "rows minus distinct rows",
    because the group count is the number a reader can act on: it says how many events
    to go and look at, not how many surplus rows exist.
    """
    grouped = ", ".join(f"`{key}`" for key in keys)
    row = spark.sql(f"""
        SELECT count(*) AS repeated FROM (
          SELECT {grouped}
          FROM {table}
          GROUP BY {grouped}
          HAVING count(*) > 1
        )
    """).first()
    if row is None:  # pragma: no cover - an aggregate always returns one row
        raise RuntimeError(f"no aggregate row for {table}")
    return int(row["repeated"])


# ---------------------------------------------------------------------------
# The procedure calls, generated as SQL
#
# Generated as text, and unit-tested as text, because a typo in a named argument
# is an error Iceberg reports as "procedure not found" — which sends you looking
# for a missing extension rather than a misspelled keyword.
# ---------------------------------------------------------------------------


def rewrite_data_files_sql(table: str, *, target_bytes: int | None = None) -> str:
    """Compact small files into files of the table's target size.

    `min-input-files` is set rather than left at Iceberg's default so a partition with
    one file is skipped instead of rewritten into an identical file. `target-file-size-
    bytes` is normally left to the table property set in `streaming.tables`, and is
    only overridden here for tests that need compaction to happen at laptop volumes.
    """
    catalog, relative = split_catalog(table)
    options = {"min-input-files": str(MIN_INPUT_FILES)}
    if target_bytes is not None:
        options["target-file-size-bytes"] = str(target_bytes)
    rendered = ", ".join(f"'{key}', '{value}'" for key, value in options.items())
    return (
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{relative}', options => map({rendered}))"
    )


def rewrite_manifests_sql(table: str) -> str:
    """Reorganise manifests so one covers a partition rather than a commit.

    Query planning reads manifests to decide which data files to open. After a day of
    30-second commits there are thousands of them, each describing a handful of files,
    and planning starts to cost more than scanning.
    """
    catalog, relative = split_catalog(table)
    return f"CALL {catalog}.system.rewrite_manifests(table => '{relative}')"


def hours_ago(hours: int, *, now: datetime | None = None) -> datetime:
    """A cutoff `hours` before now, in UTC.

    Computed in Python rather than in SQL because the procedures below need a literal:
    see `_timestamp_literal`. Taking `now` as an argument makes the generated statement
    assertable in a unit test instead of only observable at runtime.
    """
    return (now or datetime.now(timezone.utc)) - timedelta(hours=hours)


def _timestamp_literal(moment: datetime) -> str:
    """A Spark `TIMESTAMP '...'` literal for `moment`, as UTC.

    Iceberg's `CALL` binds *literal* arguments only. Passing an expression —
    `older_than => TIMESTAMPADD(HOUR, -24, current_timestamp())`, which is the obvious
    way to write "a day ago" — fails with `IllegalArgumentException: requirement
    failed: number of args and params must match after binding`, a message that says
    nothing about the actual problem and sends you counting arguments. Measured against
    Iceberg 1.10.0; the working form is this one.

    Rendered as UTC because the literal is interpreted in the session's zone, which
    `streaming.session` pins to UTC for every job in this project.
    """
    return f"TIMESTAMP '{moment.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S}'"


def expire_snapshots_sql(table: str, *, older_than: datetime, retain_last: int) -> str:
    """Drop snapshots past the retention window, and the data files only they referenced.

    This is the procedure that actually frees disk after a compaction, and it is also
    the one that ends time travel: a snapshot expired is a `FOR VERSION AS OF` that no
    longer resolves. `retain_last` is the floor that keeps the demo in
    `docs/correctness.md` working regardless of the age threshold.
    """
    catalog, relative = split_catalog(table)
    return (
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{relative}', "
        f"older_than => {_timestamp_literal(older_than)}, "
        f"retain_last => {retain_last})"
    )


def remove_orphan_files_sql(table: str, *, older_than: datetime) -> str:
    """Delete files in the table's directory that no metadata references.

    Orphans come from failed commits and from writes interrupted mid-flight — which
    this pipeline produces on purpose in the restart-idempotency proof. Read the
    warning in the module docstring before shortening the age.

    `prefix_listing => true` is load-bearing on this stack, and it is the one procedure
    where the choice of FileIO leaks into the SQL. Without it Iceberg lists the table's
    directory through Hadoop's `FileSystem`, and the table's location is `s3://…` — a
    scheme Hadoop has no implementation for, because `streaming.session` deliberately
    routes storage through Iceberg's own `S3FileIO` instead of `s3a`. Measured, against
    Iceberg 1.10.1 on the live catalog:

        UnsupportedFileSystemException: No FileSystem for scheme "s3"
          at FileSystemWalker.listDirRecursivelyWithHadoop(FileSystemWalker.java:122)
          at DeleteOrphanFilesSparkAction.listedFileDS(...:329)

    The flag switches that call to `listDirRecursivelyWithFileIO`, which uses the
    table's own FileIO. Set unconditionally rather than probed, because the catalog is
    configured in exactly one place and `S3FileIO` supports prefix listing; on a
    `HadoopFileIO` warehouse this flag would be the thing that broke instead. ADR-0024.
    """
    catalog, relative = split_catalog(table)
    return (
        f"CALL {catalog}.system.remove_orphan_files("
        f"table => '{relative}', "
        f"older_than => {_timestamp_literal(older_than)}, "
        f"prefix_listing => true)"
    )


def run_procedure(spark: SparkSession, sql: str) -> dict[str, Any]:
    """Execute one `CALL` and return its result row as a dict.

    Read generically rather than by column name because Iceberg's procedures have
    gained result columns between minor versions — `expire_snapshots` grew
    `deleted_statistics_files_count`, for one — and a script that names them breaks on
    upgrade for no reason. `remove_orphan_files` returns one row per deleted file
    instead of a summary, so it reports a count under `orphan_files_removed`.
    """
    result = spark.sql(sql)
    if result.columns == ["orphan_file_location"]:
        return {"orphan_files_removed": result.count()}
    row = result.first()
    return dict(row.asDict()) if row is not None else {}
