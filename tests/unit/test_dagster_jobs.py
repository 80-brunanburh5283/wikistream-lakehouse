"""Tests that the three jobs cover the work exactly once between them.

Selections are the part of a Dagster project that fails quietly. Dagster has no
opinion about an asset in no job or a check in two, so a mis-scoped selection is not
an error — it is a check nobody runs, or a table compacted every fifteen minutes,
and neither shows up until someone reads the run history and wonders.
"""

from __future__ import annotations

import pytest

from wikistream_dagster import keys
from wikistream_dagster.dbt_project import materialized_gold_models
from wikistream_dagster.maintenance import gold_table_maintenance

pytestmark = pytest.mark.unit

JOB_NAMES = ("observe_lakehouse", "build_marts", "maintain_gold")


@pytest.fixture(scope="session")
def definitions():
    from wikistream_dagster.definitions import defs

    return defs


@pytest.fixture(scope="session")
def resolved(definitions) -> dict:
    return {name: definitions.resolve_job_def(name) for name in JOB_NAMES}


def _checks(job) -> set:
    return set(job.asset_layer.asset_graph.asset_check_keys)


def test_every_check_runs_in_exactly_one_job(definitions, resolved):
    """The partition property, asserted as a partition rather than job by job.

    Seventy-one checks, three jobs, and the two ways to get this wrong are opposite:
    a check in no job is an assertion that never runs, and a check in two jobs is the
    same query billed twice on two different cadences.
    """
    all_checks = set(definitions.resolve_asset_graph().asset_check_keys)
    assert all_checks, "no checks in the code location at all"

    counts: dict = {}
    for job in resolved.values():
        for key in _checks(job):
            counts[key] = counts.get(key, 0) + 1

    in_no_job = {(k.asset_key.to_user_string(), k.name) for k in all_checks - set(counts)}
    in_two_jobs = {(k.asset_key.to_user_string(), k.name) for k, n in counts.items() if n > 1}

    assert not in_no_job, f"checks no job runs: {sorted(in_no_job)}"
    assert not in_two_jobs, f"checks more than one job runs: {sorted(in_two_jobs)}"


def test_the_observation_job_never_invokes_dbt(resolved):
    """The cost regression this arrangement exists to prevent.

    `observe_lakehouse` runs every two minutes. The tempting selection —
    `AssetSelection.assets(bronze, edits, quarantine)`, checks included by default —
    pulls in the dbt tests declared on the silver *sources*, one of which is
    `unique(event_id)`: a `count(distinct)` over the whole table. That selection makes
    the two-minute job launch a dbt process and scan all of silver, and nothing about
    it looks wrong in the UI.
    """
    nodes = set(resolved["observe_lakehouse"].graph.node_dict)

    assert "wikistream_dbt_assets" not in nodes, (
        "the observation job would run dbt; its selection has pulled in a source test"
    )


def test_the_observation_job_runs_the_five_metadata_checks(resolved):
    job = resolved["observe_lakehouse"]

    assert {key.name for key in _checks(job)} == {
        "bronze_offsets_are_contiguous",
        "silver_edits_are_fresh",
        "canary_heartbeat_was_received",
        "quarantine_rate_is_low",
        "quarantine_reasons_are_known",
    }
    assert job.asset_layer.selected_asset_keys == {
        keys.BRONZE_KEY,
        keys.SILVER_EDITS_KEY,
        keys.SILVER_QUARANTINE_KEY,
    }


def test_the_marts_job_builds_every_dbt_model(resolved):
    """Selected by group, so a new model needs no change in Python — asserted here.

    The staging views are included as well as the marts: they are ephemeral in the
    sense that nothing reads them but the marts, but they are dbt views in the
    warehouse and a `dbt build` that skipped them would build marts on a stale view
    definition.
    """
    selected = resolved["build_marts"].asset_layer.selected_asset_keys
    expected = {
        keys.gold_key("stg_edits"),
        keys.gold_key("stg_quarantine"),
        *(keys.gold_key(model) for model in materialized_gold_models()),
    }

    assert selected == expected


def test_maintenance_is_not_in_the_marts_job(resolved):
    """It shares the `gold` group, so it has to be subtracted explicitly.

    `AssetSelection.groups("gold")` would otherwise compact five Iceberg tables every
    fifteen minutes, which is more write amplification than the marts themselves
    produce.
    """
    key = gold_table_maintenance.key

    assert key not in resolved["build_marts"].asset_layer.selected_asset_keys
    assert resolved["maintain_gold"].asset_layer.selected_asset_keys == {key}


def test_maintenance_depends_on_the_marts_it_compacts(definitions):
    """So the UI shows why it runs after them, even though it is on its own schedule."""
    graph = definitions.resolve_asset_graph()
    parents = graph.get(gold_table_maintenance.key).parent_keys

    assert parents == {keys.gold_key(model) for model in materialized_gold_models()}


def test_every_materializable_asset_is_in_a_job(definitions, resolved):
    """Nothing buildable is left without a way to build it on a schedule."""
    graph = definitions.resolve_asset_graph()
    materializable = {key for key in graph.get_all_asset_keys() if graph.get(key).is_materializable}
    covered: set = set()
    for job in resolved.values():
        covered |= job.asset_layer.selected_asset_keys

    assert materializable <= covered, f"no job builds: {sorted(materializable - covered)}"


class TestSchedules:
    def test_each_job_has_exactly_one_schedule(self, definitions):
        scheduled = [schedule.job_name for schedule in definitions.schedules]

        assert sorted(scheduled) == sorted(JOB_NAMES)

    def test_no_schedule_starts_itself(self, definitions):
        """A fresh clone must not begin rewriting tables while the README is being read.

        The failure sensor is the deliberate exception — it watches rather than works.
        """
        for schedule in definitions.schedules:
            assert schedule.default_status.value == "STOPPED", schedule.name

    def test_every_schedule_runs_in_utc(self, definitions):
        """Every timestamp in the warehouse is UTC; a local-time cron would move twice a year."""
        for schedule in definitions.schedules:
            assert schedule.execution_timezone == "UTC", schedule.name

    def test_observation_is_at_least_as_frequent_as_the_marts(self, definitions):
        """Ordering the cadences, rather than asserting the literal cron strings.

        The numbers are tuning and may change; the relationship is the design. State
        arriving in the UI less often than the marts are rebuilt would mean a mart
        showing as up to date against a row count taken after it was built.
        """
        by_job = {schedule.job_name: schedule.cron_schedule for schedule in definitions.schedules}

        assert by_job["observe_lakehouse"] == "*/2 * * * *"
        assert by_job["build_marts"] == "*/15 * * * *"
        assert by_job["maintain_gold"].startswith("0 3 ")


class TestSensors:
    def test_the_failure_sensor_is_on_by_default(self, definitions):
        """The one thing that should be watching on a fresh clone."""
        [sensor] = list(definitions.sensors)

        assert sensor.name == "log_run_failures"
        assert sensor.default_status.value == "RUNNING"
