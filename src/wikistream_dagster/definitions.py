"""The code location: one `Definitions` object, assembled from the modules beside it.

Everything here is a reference to something defined elsewhere. Keeping the module
that Dagster loads free of definitions means `dagster definitions validate` fails
on the module with the mistake in it rather than on this one, and it means the
graph can be read in one screen.

Loaded by name — `dagster dev -m wikistream_dagster.definitions` — by the
webserver, the daemon and every `make dagster-*` target, so there is one code
location and no `workspace.yaml` to keep in step with it.
"""

from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from wikistream_dagster import dbt_project, lakehouse, maintenance
from wikistream_dagster.checks import CHECKS
from wikistream_dagster.jobs import JOBS
from wikistream_dagster.resources import KafkaResource, TrinoResource
from wikistream_dagster.schedules import SCHEDULES
from wikistream_dagster.sensors import SENSORS

defs = Definitions(
    assets=[
        # The streaming half, observed rather than run.
        lakehouse.source_stream,
        lakehouse.lakehouse_state,
        # The dbt half: seven models and sixty-one tests, from the manifest.
        dbt_project.wikistream_dbt_assets,
        maintenance.gold_table_maintenance,
    ],
    asset_checks=CHECKS,
    jobs=JOBS,
    schedules=SCHEDULES,
    sensors=SENSORS,
    resources={
        # `from_settings()` rather than a literal: the same `WS_*` environment that
        # configures the producer and the Spark jobs configures these, so there is
        # one answer to "where does Trino live" for the whole repository.
        "trino": TrinoResource.from_settings(),
        "kafka": KafkaResource.from_settings(),
        # dbt is given the `DbtProject` object, not just a path, so `dagster dev`
        # can regenerate the manifest on a change and the deployed image does not.
        "dbt": DbtCliResource(project_dir=dbt_project.dbt_project),
    },
)
