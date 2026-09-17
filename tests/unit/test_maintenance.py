"""The Iceberg maintenance calls, asserted as text.

A `CALL` with a misspelled named argument does not fail with "unknown argument"; Iceberg
reports it as the procedure not existing, which sends you looking for a missing runtime
extension instead of a typo. Generating the SQL in one module and asserting it here is
cheaper than finding that out against a running catalog.
"""

from __future__ import annotations

import pytest

from wikistream.maintenance import (
    MIN_INPUT_FILES,
    ORPHAN_AGE_DEFAULT_HOURS,
    RETAIN_LAST_SNAPSHOTS,
    TableStats,
    expire_snapshots_sql,
    remove_orphan_files_sql,
    rewrite_data_files_sql,
    rewrite_manifests_sql,
    split_catalog,
)

pytestmark = pytest.mark.unit

TABLE = "lakehouse.silver.edits"

ALL_PROCEDURES = (
    rewrite_data_files_sql(TABLE),
    rewrite_manifests_sql(TABLE),
    expire_snapshots_sql(TABLE, older_than_hours=24, retain_last=RETAIN_LAST_SNAPSHOTS),
    remove_orphan_files_sql(TABLE, older_than_hours=ORPHAN_AGE_DEFAULT_HOURS),
)


def test_the_catalog_is_split_off_the_table_name():
    """The procedure lives on the catalog; its `table` argument is relative to it.

    Passing the fully qualified name in the argument gives a "table not found" that
    names the catalog twice, which is a confusing five minutes.
    """
    assert split_catalog(TABLE) == ("lakehouse", "silver.edits")


def test_a_table_name_with_no_catalog_is_rejected():
    with pytest.raises(ValueError, match="no catalog prefix"):
        split_catalog("edits")


@pytest.mark.parametrize("sql", ALL_PROCEDURES)
def test_every_procedure_is_called_on_the_catalog_not_the_table(sql):
    assert sql.startswith("CALL lakehouse.system.")
    assert "table => 'silver.edits'" in sql
    assert "lakehouse.silver.edits" not in sql


def test_compaction_skips_partitions_with_a_single_file():
    """Rewriting one file into one file is write amplification for no gain."""
    sql = rewrite_data_files_sql(TABLE)

    assert f"'min-input-files', '{MIN_INPUT_FILES}'" in sql
    assert "target-file-size-bytes" not in sql, "the table property should decide the target"


def test_the_compaction_target_can_be_overridden_for_test_volumes():
    """Only tests need this: at laptop volumes a 128 MiB target never triggers a rewrite."""
    sql = rewrite_data_files_sql(TABLE, target_bytes=4096)

    assert "'target-file-size-bytes', '4096'" in sql


def test_expiry_keeps_a_floor_of_recent_snapshots():
    """Otherwise a maintenance run breaks the time-travel demo it shares a page with."""
    sql = expire_snapshots_sql(TABLE, older_than_hours=24, retain_last=RETAIN_LAST_SNAPSHOTS)

    assert f"retain_last => {RETAIN_LAST_SNAPSHOTS}" in sql
    assert "TIMESTAMPADD(HOUR, -24, current_timestamp())" in sql


def test_the_orphan_default_stays_conservative():
    """Three days, which is Iceberg's own default and the reason a forgotten stream survives.

    A file being written by a commit that has not landed yet is an orphan by
    definition. Shortening this to make a demo tidy is how a live write gets deleted,
    so the default is asserted rather than left to a reviewer's memory.
    """
    assert ORPHAN_AGE_DEFAULT_HOURS == 72
    assert "TIMESTAMPADD(HOUR, -72, current_timestamp())" in remove_orphan_files_sql(
        TABLE, older_than_hours=ORPHAN_AGE_DEFAULT_HOURS
    )


def test_files_per_partition_is_the_compaction_signal():
    """The number `make maintain` is judged on, and it must not divide by zero."""
    populated = TableStats(
        table=TABLE,
        rows=1000,
        files=56,
        partitions=2,
        snapshots=10,
        total_mib=14.7,
        avg_kib=269.0,
        min_kib=1.0,
        max_kib=900.0,
    )
    empty = TableStats(
        table=TABLE,
        rows=0,
        files=0,
        partitions=0,
        snapshots=1,
        total_mib=0.0,
        avg_kib=0.0,
        min_kib=0.0,
        max_kib=0.0,
    )

    assert populated.files_per_partition == 28.0
    assert empty.files_per_partition == 0.0
