#!/usr/bin/env python
"""Run one SQL statement against the Iceberg catalog and print the result.

    make sql SQL="SELECT count(*) FROM bronze.recentchange_raw"
    make sql SQL="SELECT * FROM bronze.recentchange_raw.snapshots" JSON=--json

This is the escape hatch. Trino and dbt are the intended query path, but they are
in the `full` profile, and plenty of questions come up while only the core tier is
running — did the table get the DDL I think it did, how many snapshots are there,
what is in `.files`. Spark can answer those without another container.

`--json` prints one JSON object per row, which is what
`tests/integration/test_bronze.py` parses to assert against the real catalog
instead of against a mock of it. The parsing contract is "every line that starts
with `{` is a row", and it has to be, because `spark-submit` merges a Python
application's stderr into its stdout — verified by printing to both and discarding
one:

    spark-submit app.py 2>/dev/null    # prints both lines
    spark-submit app.py 2>&1 >/dev/null  # prints neither

So a caller cannot separate diagnostics from data by stream. `--json` therefore
raises this script's own log level to WARNING, leaving stdout with Spark's
non-JSON log4j lines and the rows.

Deliberately runs a single statement with no shell-style splitting on `;`. A tool
that quietly runs half of what you typed is worse than one that refuses.
"""

from __future__ import annotations

import argparse
import json
import sys

from wikistream.config import get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.streaming.session import build_session

log = get_logger("wikistream.spark_sql")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sql", help="The statement to run. One statement, no trailing semicolon.")
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print rows as JSON lines on stdout, with logs on stderr.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Rows to print for the human-readable form. Ignored with --json.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(
        "WARNING" if args.as_json else settings.log_level,
        as_json=settings.log_json,
    )
    spark = build_session("spark-sql", settings)

    statement = args.sql.strip().rstrip(";")
    log.info("running statement", extra={"sql": statement})
    frame = spark.sql(statement)

    if args.as_json:
        # `collect()` rather than a streaming write: this is for assertions and
        # small answers, and a query that returns more than fits in the driver is
        # a query that wanted `make query` instead.
        for row in frame.collect():
            # default=str so dates, timestamps and Row objects from Iceberg's
            # metadata tables serialise instead of raising.
            print(json.dumps(row.asDict(recursive=True), default=str))
    else:
        frame.show(args.limit, truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
