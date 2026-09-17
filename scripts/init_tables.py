#!/usr/bin/env python
"""Create the namespaces and Iceberg tables. Idempotent; safe to rerun.

    make init-tables

Runs inside the Spark container because it needs the Iceberg jars and the catalog
configuration. Every statement is `IF NOT EXISTS` or an idempotent `ALTER`, so
`make up` can call this unconditionally on a lakehouse that already has data.

Prints each statement's target before running it, because the first time this fails
it will fail on exactly one statement and the useful information is which.
"""

from __future__ import annotations

import sys

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.session import build_session
from wikistream.streaming.tables import all_statements

log = get_logger("wikistream.init_tables")


def first_line(statement: str) -> str:
    """The statement's first line, for logging without dumping 25 columns."""
    return statement.strip().splitlines()[0].strip()


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    spark = build_session("init-tables", settings)

    statements = all_statements(settings)
    for statement in statements:
        log.info("applying", extra={"statement": first_line(statement)})
        spark.sql(statement)

    # Reading the schema back is not ceremony: `CREATE TABLE IF NOT EXISTS`
    # succeeds without complaint against a table whose schema differs from the DDL
    # above, so a column added to this file after the table exists would appear to
    # have been applied. Printing what the catalog actually holds is the only way
    # the operator sees that.
    for table in (
        settings.bronze_raw_table,
        settings.silver_edits_table,
        settings.silver_quarantine_table,
    ):
        columns = [field.name for field in spark.table(table).schema.fields]
        log.info("table ready", extra={"table": table, "columns": len(columns)})
        print(f"\n{table}")
        print("  " + ", ".join(columns))

    print(f"\n{len(statements)} statements applied.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
