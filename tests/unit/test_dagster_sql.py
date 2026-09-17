"""Tests for the SQL the Dagster package sends to Trino.

These run without Trino, so they cannot prove a query returns the right answer —
that is what the asset checks themselves do against a live warehouse. What they can
prove is the set of mistakes that are invisible until a query runs at 03:00: a
metadata table quoted wrongly, a column projected in the wrong order, a window
anchored to the data instead of the clock.
"""

from __future__ import annotations

import pytest

from wikistream_dagster import sql

pytestmark = pytest.mark.unit


def test_metadata_table_puts_the_dollar_inside_the_quotes():
    """`lakehouse.silver."edits$files"`, not `lakehouse.silver.edits$files`.

    Trino needs the `$` quoted as part of the table name. Getting it wrong produces
    `Table 'lakehouse.silver.edits$files' does not exist`, which reads as a missing
    table rather than a quoting bug and sends the reader looking in the catalog.
    """
    assert sql.metadata_table("lakehouse.silver.edits", "files") == 'lakehouse.silver."edits$files"'
    assert (
        sql.metadata_table("warehouse.bronze.recentchange_raw", "snapshots")
        == 'warehouse.bronze."recentchange_raw$snapshots"'
    )


def test_metadata_table_rejects_an_unqualified_name():
    """A bare table name would silently produce a query against the wrong catalog."""
    with pytest.raises(ValueError):
        sql.metadata_table("edits", "files")


def test_quote_list_escapes_embedded_quotes():
    """The only injection surface in this module, and it is closed.

    `FAILURE_REASONS` is a tuple of Python identifiers today, so nothing in it needs
    escaping. This asserts the escaping anyway, because the day someone adds a
    human-readable reason with an apostrophe in it, the failure should be a passing
    test rather than a syntax error in a scheduled check.
    """
    assert sql.quote_list(["a", "b"]) == "('a', 'b')"
    assert sql.quote_list(["it's"]) == "('it''s')"


class TestObservationQueries:
    """The projections the observation functions unpack positionally."""

    def test_bronze_observation_projects_four_columns_in_order(self):
        """`lakehouse._observe_bronze` unpacks this tuple by position.

        The order of the SELECT list is therefore a contract between two modules, and
        swapping two columns of the same type — `min_offset` and `max_offset` — is a
        bug no type checker can see and no query error reveals.
        """
        query = sql.bronze_observation("cat.bronze.raw")
        columns = ["count(*)", "max(ingested_at)", "min(kafka_offset)", "max(kafka_offset)"]

        for column in columns:
            assert column in query
        positions = [query.index(column) for column in columns]
        assert positions == sorted(positions)

    def test_silver_observation_projects_six_columns_in_order(self):
        query = sql.silver_edits_observation("cat.silver.edits")
        columns = [
            "count(*)",
            "count(distinct event_id)",
            "max(event_time)",
            "max(ingested_at)",
            "count_if(domain = 'canary')",
            "approx_percentile(late_by_seconds, 0.95)",
        ]
        positions = [query.index(column) for column in columns]
        assert positions == sorted(positions)

    def test_quarantine_observation_projects_three_columns_in_order(self):
        query = sql.quarantine_observation("cat.silver.quarantine")
        positions = [
            query.index(column)
            for column in ["count(*)", "max(failed_at)", "count(distinct failure_reason)"]
        ]
        assert positions == sorted(positions)

    def test_file_statistics_reads_the_files_metadata_table(self):
        query = sql.file_statistics("cat.silver.edits")

        assert 'cat.silver."edits$files"' in query
        # `coalesce` on both sums: a table with no data files returns one row of
        # NULLs, and `_file_metadata` divides by the file count.
        assert query.count("coalesce(") == 2


class TestInvariantQueries:
    def test_offset_contiguity_groups_by_partition(self):
        """Per-partition, because offsets are only monotonic within a partition.

        A global `min`/`max` across four partitions would compare offsets from
        different sequences and fail on a healthy table.
        """
        query = sql.bronze_offset_contiguity("cat.bronze.raw")

        assert "group by kafka_partition" in query
        assert "min(kafka_offset)" in query
        assert "max(kafka_offset)" in query

    def test_freshness_measures_event_time_against_the_wall_clock(self):
        """Not `ingested_at`, and not Dagster's own event log.

        `ingested_at` is written by the job being checked, so a job that is running
        but reading a dead connection keeps it current. `event_time` comes from
        Wikimedia.
        """
        query = sql.silver_edits_freshness("cat.silver.edits")

        assert "current_timestamp" in query
        assert "max(event_time)" in query
        assert "ingested_at" not in query

    def test_canary_window_is_anchored_to_the_clock_not_to_the_data(self):
        """The regression this guards is specific and it looks harmless.

        `date_trunc('hour', max(event_time))` is the obvious way to write "the last
        complete hour" and it makes the check useless: if ingestion stopped two days
        ago, the newest hour in the table is complete and full of canaries, so the
        check passes for as long as the pipeline stays dead. Only wall-clock time
        makes the window move.
        """
        query = sql.canary_events_in_last_complete_hour("cat.silver.edits")

        assert "date_trunc('hour', current_timestamp)" in query
        assert "max(event_time)" not in query
        # Both bounds: the lower one alone would include the current, partial hour,
        # in which a missing canary means "it is 12 past" rather than "a gap".
        assert "interval '1' hour" in query
        assert query.count("date_trunc('hour', current_timestamp)") == 2

    def test_duplicate_check_compares_rows_to_distinct_ids(self):
        query = sql.silver_duplicate_event_ids("cat.silver.edits")

        assert "count(*)" in query
        assert "count(distinct event_id)" in query

    def test_quarantine_rate_measures_both_sides_on_ingest_time(self):
        """One clock for both numerator and denominator.

        An unparseable `event_time` is itself a reason to quarantine, so the
        quarantine table cannot be windowed on event time. Windowing the accepted
        side on event time and the rejected side on ingest time would make the ratio
        drift with the pipeline's lag rather than with its error rate.
        """
        query = sql.quarantine_rate("cat.silver.edits", "cat.silver.quarantine", 24)

        assert "ingested_at >=" in query
        assert "failed_at >=" in query
        assert query.count("interval '24' hour") == 2
        assert "event_time" not in query

    def test_unknown_reasons_query_catches_null_as_well_as_unlisted(self):
        """`not in (...)` is NULL for a NULL input, so NULL needs its own predicate.

        Without `is null`, a quarantined row with no reason at all — the exact
        symptom of a Spark job writing the column wrongly — passes the check.
        """
        query = sql.unknown_quarantine_reasons("cat.silver.quarantine", ["a", "b"])

        assert "failure_reason is null" in query
        assert "not in ('a', 'b')" in query


class TestMaintenanceStatements:
    def test_optimize_targets_the_named_table(self):
        assert (
            sql.optimize("cat.gold.dim_wikis") == "alter table cat.gold.dim_wikis execute optimize"
        )

    def test_expire_snapshots_passes_retention_as_a_named_argument(self):
        """Trino's procedure signature is `expire_snapshots(retention_threshold => ...)`.

        Positionally it does not parse. The retention has to be a string literal
        because Trino parses the duration itself.
        """
        statement = sql.expire_snapshots("cat.gold.dim_wikis", "7d")

        assert statement == (
            "alter table cat.gold.dim_wikis execute expire_snapshots(retention_threshold => '7d')"
        )


def test_every_query_names_a_fully_qualified_table():
    """No query may rely on Trino's session catalog or schema.

    The Dagster resource connects with a catalog but no schema, and a schedule that
    works interactively and fails at 03:00 because a query assumed `use silver` is a
    bad half hour. Every builder in this module takes the table as an argument, so
    this is a check that none of them stopped using it.
    """
    table = "cat.ns.tbl"
    builders = [
        sql.row_count(table),
        sql.file_statistics(table),
        sql.bronze_observation(table),
        sql.bronze_offset_contiguity(table),
        sql.silver_edits_observation(table),
        sql.silver_edits_freshness(table),
        sql.silver_duplicate_event_ids(table),
        sql.canary_events_in_last_complete_hour(table),
        sql.quarantine_rate(table, table, 1),
        sql.unknown_quarantine_reasons(table, ["x"]),
        sql.quarantine_observation(table),
        sql.optimize(table),
        sql.expire_snapshots(table, "7d"),
    ]

    for query in builders:
        assert "cat.ns." in query, query
