"""The three jobs, and why the selections are spelled out rather than left to default.

Dagster's `AssetSelection.all()` selects every *materializable* asset, which means
it silently excludes the five external assets in `lakehouse.py` — you cannot
materialise a table another process writes. A single "do everything" job would
therefore quietly not observe anything, and the observations are where the
streaming half's state comes from. So the work splits three ways:

| Job | What it does | Cadence |
|---|---|---|
| `observe_lakehouse` | Kafka watermarks, Iceberg metadata, 5 checks | every 2 minutes |
| `build_marts` | `dbt build` — models, their tests, the source tests | every 15 minutes |
| `maintain_gold` | compaction and snapshot expiry | daily at 03:00 UTC |

The split is by cost as much as by kind, and getting it wrong is easy in a way
worth recording. Selecting the silver assets pulls in *their* checks, and four of
those are dbt tests on the source — including `unique(event_id)`, which is a
`count(distinct)` over the whole table. Left at the default, the two-minute
observation job would launch a dbt process and scan all of silver every two
minutes, and the `cost: cheap` tag on it would be a lie. The selections below
therefore say explicitly which checks run where: the metadata checks run often, and
the checks that need dbt run when dbt is already running.

`tests/unit/test_dagster_jobs.py` asserts the partition is exact — every check in
the code location belongs to exactly one of these jobs. A check in none of them is
a check nobody runs, which is the failure mode this arrangement is prone to.
"""

from __future__ import annotations

from dagster import AssetSelection, define_asset_job

from wikistream_dagster.checks import (
    bronze_offsets_are_contiguous,
    canary_heartbeat_was_received,
    marts_are_not_empty,
    quarantine_rate_is_low,
    quarantine_reasons_are_known,
    silver_edits_are_fresh,
)
from wikistream_dagster.keys import BRONZE_KEY, SILVER_EDITS_KEY, SILVER_QUARANTINE_KEY
from wikistream_dagster.maintenance import SNAPSHOT_RETENTION, gold_table_maintenance

#: The three observable Iceberg tables. The Kafka topic node comes along with them:
#: they are all one `@multi_observable_source_asset`, so selecting any of them runs
#: the same op, and naming the topic here would not change what executes.
_OBSERVED = AssetSelection.assets(BRONZE_KEY, SILVER_EDITS_KEY, SILVER_QUARANTINE_KEY)

#: The checks that are pure Trino metadata queries, listed by definition rather than
#: derived from the assets, because "the checks on these assets" is exactly the set
#: that includes the expensive dbt ones.
_STREAMING_CHECKS = AssetSelection.checks(
    bronze_offsets_are_contiguous,
    silver_edits_are_fresh,
    canary_heartbeat_was_received,
    quarantine_rate_is_low,
    quarantine_reasons_are_known,
)

observe_lakehouse = define_asset_job(
    name="observe_lakehouse",
    selection=_OBSERVED.without_checks() | _STREAMING_CHECKS,
    description=(
        "Record what is in Kafka and the three Iceberg tables, and run the five checks "
        "that are metadata queries: offset contiguity, freshness, canary heartbeat, "
        "quarantine rate and quarantine vocabulary."
    ),
    tags={"layer": "lakehouse", "cost": "cheap"},
)

#: The dbt project, selected by group so a new model in `dbt/models/` is built
#: without being named anywhere in Python — plus the dbt tests on the two silver
#: sources, which are not in the `gold` group and would otherwise be defined and
#: never run. Minus `gold_table_maintenance`, which shares the group but must not
#: compact five tables every fifteen minutes. Minus the streaming checks, which the
#: observation job owns.
build_marts = define_asset_job(
    name="build_marts",
    selection=(
        (AssetSelection.groups("gold") - AssetSelection.assets(gold_table_maintenance))
        | (
            AssetSelection.checks_for_assets(SILVER_EDITS_KEY, SILVER_QUARANTINE_KEY)
            - _STREAMING_CHECKS
        )
    ),
    description=(
        "Run `dbt build`: the staging views, the five gold tables, their tests, and the "
        "uniqueness and not-null tests on the silver sources they read."
    ),
    tags={"layer": "gold", "cost": "moderate"},
)

#: Compaction, on its own. The marts' checks are deliberately not re-run here: this
#: job changes how many files a table is stored in, not what it contains, so
#: re-asserting its contents would be sixty queries proving nothing changed.
maintain_gold = define_asset_job(
    name="maintain_gold",
    selection=AssetSelection.assets(gold_table_maintenance),
    description=f"Compact the gold tables and expire snapshots older than {SNAPSHOT_RETENTION}.",
    tags={"layer": "gold", "cost": "expensive"},
)

#: `marts_are_not_empty` runs inside `build_marts` — it is a check on the mart
#: assets, which that job selects — so it is not listed separately. Imported above
#: only so this module fails to import if the check is renamed.
_ = marts_are_not_empty

JOBS = [observe_lakehouse, build_marts, maintain_gold]
