#!/usr/bin/env python
"""Read the same Iceberg tables with DuckDB — no JVM, no Spark, no Trino.

    make query-duckdb          # needs `make up-core` first

Why a third engine. "Iceberg means the storage is open" is a claim, and Spark
writing tables that Trino reads only half demonstrates it: both are JVM query
engines configured against the same catalog by the same author, so a sceptical
reader can reasonably suspect the two are agreeing because they were told to.

DuckDB is the control. It is a single process with no cluster, no JVM and no
configuration file in this repository. It is given three things — the catalog's HTTP
address, the object store's address, and a table name — and it reads the Parquet and
the manifests that a Spark Structured Streaming job committed. Nothing exports,
converts or synchronises anything.

What it cannot do here, stated so nobody goes looking: it does not write. The
Iceberg extension's write support is young, and a second writer to the silver table
is exactly what the deduplication guarantee is not designed to survive. This is a
read path.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any

import duckdb

from wikistream.config import get_settings

CATALOG_ALIAS = "ice"
SECRET_NAME = "wikistream_object_store"


def heading(text: str) -> None:
    """Section title, in the same shape `scripts/query_trino.sh` prints."""
    print(f"\n── {text}")


def explain(*lines: str) -> None:
    """Commentary above a result, one argument per line."""
    for line in lines:
        print(f"   {line}")
    print()


def show(cursor: Any) -> None:
    """Print a result set as an aligned table.

    Written out rather than handed to `cursor.show()` because DuckDB's own renderer
    truncates to the terminal width and prints a row-count footer, and this output
    is read next to `make query`'s.
    """
    show_rows(
        [description[0] for description in cursor.description],
        cursor.fetchall(),
    )


def show_rows(columns: list[str], values: list[Any]) -> None:
    """The same aligned table, for rows assembled in Python rather than by one query."""
    rows = [["NULL" if value is None else str(value) for value in row] for row in values]
    widths = [
        max(len(column), *(len(row[i]) for row in rows)) if rows else len(column)
        for i, column in enumerate(columns)
    ]

    print("   " + "  ".join(column.ljust(widths[i]) for i, column in enumerate(columns)))
    print("   " + "  ".join("-" * width for width in widths))
    for row in rows:
        print("   " + "  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
    if not rows:
        print("   (no rows)")


def connect() -> Any:
    """An in-memory DuckDB with the object store and the Iceberg catalog attached.

    The S3 credentials go into a DuckDB secret rather than into `SET s3_access_key_id`
    settings: a secret is scoped and is not readable back out of the session, and the
    setting form is deprecated in DuckDB 1.x anyway.
    """
    settings = get_settings()
    connection = duckdb.connect()

    for extension in ("httpfs", "iceberg"):
        # INSTALL is a no-op once the extension is in DuckDB's local cache. The first
        # run fetches it from extensions.duckdb.org, which is the one network call
        # this script makes beyond the two local services.
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")

    # MinIO speaks S3 over plain HTTP at a host:port, so the endpoint is given without
    # a scheme and TLS is off. `URL_STYLE 'path'` because MinIO serves the bucket in
    # the path; the virtual-host default resolves `lakehouse.localhost` and fails with
    # a DNS error that reads like a network fault rather than a configuration one.
    endpoint = settings.s3_endpoint.removeprefix("http://").removeprefix("https://")
    connection.execute(
        f"""
        CREATE OR REPLACE SECRET {SECRET_NAME} (
            TYPE s3,
            KEY_ID '{settings.s3_access_key}',
            SECRET '{settings.s3_secret_key}',
            ENDPOINT '{endpoint}',
            REGION '{settings.s3_region}',
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )

    # `AUTHORIZATION_TYPE 'none'`: the REST catalog in docker-compose.yml is
    # unauthenticated, and DuckDB's default is OAuth2 — without this it fails asking
    # for a client secret rather than for a reachable endpoint.
    connection.execute(
        f"""
        ATTACH '{settings.warehouse_uri}' AS {CATALOG_ALIAS} (
            TYPE iceberg,
            ENDPOINT '{settings.iceberg_rest_uri}',
            AUTHORIZATION_TYPE 'none'
        )
        """
    )
    return connection


def table_listing(connection: Any) -> Iterator[tuple[str, str, int]]:
    """Every table in the attached catalog, with how many columns it has.

    The column count needs a `DESCRIBE` per table rather than `len(column_names)` from
    `SHOW ALL TABLES`. DuckDB attaches an Iceberg catalog lazily, so until a table is
    touched `SHOW ALL TABLES` reports it with the placeholder `['__']` and type
    `UNKNOWN` — which renders as a plausible-looking `columns = 1` for every table in
    the lakehouse. `DESCRIBE` loads the metadata, which is the point of the section:
    the schema comes from the same Iceberg metadata Spark wrote, not from a local
    definition.
    """
    tables = connection.execute(
        f"""
        SELECT schema, name
        FROM (SHOW ALL TABLES)
        WHERE database = '{CATALOG_ALIAS}'
        ORDER BY schema, name
        """
    ).fetchall()
    for schema, name in tables:
        described = connection.execute(f'DESCRIBE {CATALOG_ALIAS}."{schema}"."{name}"').fetchall()
        yield schema, name, len(described)


def report(connection: Any) -> None:
    """The three questions worth asking from outside the JVM."""
    settings = get_settings()
    bronze = f"{CATALOG_ALIAS}.{settings.bronze_namespace}.recentchange_raw"
    edits = f"{CATALOG_ALIAS}.{settings.silver_namespace}.edits"

    heading("1. DuckDB lists the tables Spark created")
    explain("This process has no Spark, no Trino and no JVM. It asked the catalog.")
    show_rows(["schema", "name", "columns"], list(table_listing(connection)))

    heading("2. The same row counts, read from the same Parquet")
    explain(
        "Compare these with `make query`. Two engines, one copy of the data,",
        "and the deduplication invariant holds regardless of who is asking.",
    )
    show(
        connection.execute(
            f"""
            SELECT
                (SELECT count(*) FROM {bronze})                AS bronze_rows,
                (SELECT count(*) FROM {edits})                 AS silver_rows,
                (SELECT count(DISTINCT event_id) FROM {edits}) AS silver_distinct_ids
            """
        )
    )

    heading("3. A real aggregate, to prove it is reading data and not metadata")
    explain("Row counts can come from manifest summaries. A GROUP BY cannot.")
    show(
        connection.execute(
            f"""
            SELECT wiki, count(*) AS edits, max(event_time) AS newest_event
            FROM {edits}
            WHERE domain <> 'canary'
            GROUP BY wiki
            ORDER BY edits DESC
            LIMIT 10
            """
        )
    )
    print()


def main() -> int:
    """Attach and report, or explain what could not be reached."""
    settings = get_settings()
    try:
        connection = connect()
    except duckdb.Error as error:
        print(f"error: could not attach the Iceberg catalog: {error}", file=sys.stderr)
        print(
            f"       catalog {settings.iceberg_rest_uri}, object store {settings.s3_endpoint}\n"
            "       both come from .env; start them with 'make up-core'.",
            file=sys.stderr,
        )
        return 1

    with connection:
        try:
            report(connection)
        except duckdb.Error as error:
            # A missing table here means the streaming jobs have not run, which is a
            # different problem from an unreachable catalog and deserves its own
            # message rather than the same one.
            print(f"error: query failed: {error}", file=sys.stderr)
            print(
                "       if a table is missing, run 'make init-tables' and then"
                " 'make stream-bronze' / 'make stream-silver'.",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
