"""Iceberg table maintenance for the tables this process is allowed to touch.

Streaming into Iceberg produces small files. A 30-second micro-batch writes at
least one data file per partition per batch, so a day of ingestion leaves
thousands of files holding what would fit in tens, and every query pays for it in
manifest reads and open calls. Compaction is not optional for a streaming
lakehouse; it is the running cost of one.

**Only the gold tables are maintained here.** Bronze and silver are compacted by
`scripts/maintain_tables.py` inside the Spark container, on `make maintain`. The
split is not tidiness — it is that this container has no Spark, and the
alternatives to accepting that were both worse than a seam:

- Mounting the Docker socket so Dagster could `docker exec` into the Spark
  container. That hands the orchestrator root on the host daemon, to compact a
  table. It also cannot be validated in CI, where there is no Spark container.
- Running `ALTER TABLE ... EXECUTE optimize` against bronze and silver through
  Trino anyway. Trino can do it, and it would rewrite files underneath a running
  Spark streaming query. Iceberg's optimistic concurrency means the loser of that
  race retries rather than corrupts, but a compaction that competes with the
  writer it is meant to help is not maintenance.

So each engine maintains what it writes: Spark owns bronze and silver, Trino owns
gold. ADR-0031.
"""

#  No `from __future__ import annotations` here — see the comment at the top of
#  lakehouse.py.

from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from wikistream.config import get_settings
from wikistream_dagster import sql
from wikistream_dagster.dbt_project import materialized_gold_models
from wikistream_dagster.keys import GOLD_GROUP, gold_key
from wikistream_dagster.resources import TrinoResource

_SETTINGS = get_settings()

#: Snapshots younger than this are kept, so time travel and rollback still work
#: over the recent past. Trino rejects a retention below
#: `iceberg.expire-snapshots.min-retention` — 7 days by default — rather than
#: clamping it, which is why this is not set to something demo-sized like an hour.
SNAPSHOT_RETENTION = "7d"

#: The tables to compact, read out of the dbt manifest rather than listed, so this
#: asset covers a new mart the day it is added. Views are excluded because they have
#: no data files — `optimize` on one is an error, not a no-op. Same set the
#: `marts_are_not_empty` check covers: a table worth checking is a table worth
#: compacting.
_GOLD_TABLES = materialized_gold_models()


@asset(
    name="gold_table_maintenance",
    group_name=GOLD_GROUP,
    kinds={"trino", "iceberg"},
    deps=[gold_key(table) for table in _GOLD_TABLES],
    description=(
        "Compacts the gold tables' data files and expires snapshots older than "
        f"{SNAPSHOT_RETENTION}. Runs after the marts, on its own schedule — see "
        "`schedules.py` for why it is not on the same one."
    ),
)
def gold_table_maintenance(
    context: AssetExecutionContext, trino: TrinoResource
) -> Output[dict[str, Any]]:
    """Compact and expire, reporting the file counts either side of the change.

    Reporting both sides is the point of doing this as an asset rather than a cron
    job: the output metadata is a measurement of what compaction bought, which is
    the number the README's claim about small files has to come from.

    A failure on one table does not stop the others. Compaction is per-table and
    independent, and the useful behaviour when one table's rewrite conflicts with a
    concurrent read is to finish the rest and report the one that did not.
    """
    results: dict[str, Any] = {}
    failures: list[str] = []

    for table_name in _GOLD_TABLES:
        table = _SETTINGS.table(_SETTINGS.gold_namespace, table_name)
        try:
            before = _file_stats(trino, table)
            trino.execute(sql.optimize(table))
            trino.execute(sql.expire_snapshots(table, SNAPSHOT_RETENTION))
            after = _file_stats(trino, table)
            results[table_name] = {
                "files_before": before["files"],
                "files_after": after["files"],
                "bytes_before": before["bytes"],
                "bytes_after": after["bytes"],
                "records": after["records"],
            }
            context.log.info(
                "%s: %d files -> %d files", table_name, before["files"], after["files"]
            )
        # Broad on purpose: one table's failure must not hide the other four, and
        # what comes back from Trino for a conflicting rewrite is not a type this
        # code can usefully enumerate.
        except Exception as exc:
            failures.append(table_name)
            results[table_name] = {"error": str(exc)}
            context.log.exception("maintenance failed for %s", table_name)

    if failures:
        raise RuntimeError(
            f"maintenance failed for {', '.join(failures)}; see the log for each cause"
        )

    files_before = sum(r["files_before"] for r in results.values())
    files_after = sum(r["files_after"] for r in results.values())
    return Output(
        results,
        metadata={
            "tables": len(results),
            "data_files_before": files_before,
            "data_files_after": files_after,
            # Negative means compaction found nothing to do, which is the expected
            # result on a freshly built mart and not worth hiding.
            "data_files_removed": files_before - files_after,
            "snapshot_retention": SNAPSHOT_RETENTION,
            "detail": MetadataValue.json(results),
        },
    )


def _file_stats(trino: TrinoResource, table: str) -> dict[str, int]:
    row = trino.fetch_one(sql.file_statistics(table))
    files, total_bytes, records = row if row else (0, 0, 0)
    return {"files": files, "bytes": total_bytes, "records": records}
