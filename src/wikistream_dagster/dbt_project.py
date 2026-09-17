"""The dbt project as Dagster assets — seven models and sixty-one tests.

This is where the graph becomes continuous. The dbt source `silver.edits` carries
`meta.dagster.asset_key: [lakehouse, silver, edits]` in
`dbt/models/staging/_silver__sources.yml`, and `wikistream_dagster.keys` declares
the same key for the external asset the Spark stream writes. dbt's source and
Dagster's observed table are therefore the *same node*, and the lineage runs
Wikimedia → Kafka → silver → staging views → marts in one graph with no gap in it.
`tests/unit/test_dagster_lineage.py` asserts that, because a typo in either place
produces two disconnected halves rather than an error.

`dbt build`, not `dbt run`: the models and their tests are one operation. Every dbt
test becomes a Dagster asset check on the model it belongs to, so a failing
`unique_combination_of_columns` shows up on the mart in the UI rather than in a log
line. That includes the tests on the *source*, which is why
`enable_source_tests_as_checks` is on — it puts dbt's uniqueness assertion about
`event_id` on the silver asset, which is the asset it is about, and means this
package does not need a second implementation of the same query.

No `from __future__ import annotations` here, for the reason given at the top of
`lakehouse.py`.
"""

import json
import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from wikistream_dagster.keys import GOLD_GROUP, GOLD_PREFIX

#: Repo-relative, so `dagster dev -m wikistream_dagster.definitions` works from a
#: clone with no environment set. Read from `os.environ` rather than from
#: `wikistream.config.Settings` — unlike every other tunable in this project —
#: because the useful default is a path relative to *this file*, and a
#: pydantic-settings default cannot know where the package was installed.
_REPO_DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
PROJECT_DIR = Path(os.environ.get("WS_DBT_PROJECT_DIR", str(_REPO_DBT_DIR)))

#: Where dbt writes `manifest.json`, `run_results.json` and compiled SQL. Split out
#: as a variable because the container mounts the project **read-only** and points
#: this at `/tmp`: dagster-dbt writes a fresh target directory per invocation, so a
#: read-only project directory with the default `target` path fails partway into
#: the first run rather than at start-up.
TARGET_PATH = Path(os.environ.get("WS_DBT_TARGET_PATH", "target"))

dbt_project = DbtProject(
    project_dir=PROJECT_DIR,
    target_path=TARGET_PATH,
    # profiles.yml lives in the project directory, in version control, next to the
    # models it configures. dbt's default of `~/.dbt` would put it outside the repo.
    profiles_dir=PROJECT_DIR,
)

# Regenerates the manifest when running under `dagster dev`, and does nothing
# otherwise. Deployed images build the manifest once, at image build time: parsing
# a project on every container start is several seconds of start-up for an artefact
# that cannot have changed.
dbt_project.prepare_if_dev()


class WikistreamDbtTranslator(DagsterDbtTranslator):
    """Names dbt's nodes the way the rest of this project names tables.

    Two overrides, both about consistency rather than function. dbt-trino would
    give a model the asset key `mart_edits_per_minute`; the external assets are
    keyed `lakehouse/silver/edits` after the table they are. Prefixing the models
    with the catalog and schema they are written to makes every node in the graph
    read as `catalog/namespace/table`, which is also how `scripts/query_trino.sh`
    and the README spell them.

    Sources are left alone: their keys come from `meta.dagster.asset_key` in the
    dbt YAML, and overriding them here would silently win over the file a reader
    would check.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Prefix models with catalog and namespace; leave sources to the dbt YAML."""
        key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props["resource_type"] == "model":
            # `keys.gold_key` builds the same key for the asset checks in
            # `checks.py`. Sharing the prefix constant is what keeps them equal.
            return key.with_prefix(GOLD_PREFIX)
        return key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Put every model in one `gold` group, whatever subdirectory it lives in."""
        if dbt_resource_props["resource_type"] == "model":
            # One group, not one per subdirectory: staging views and marts share a
            # schema (ADR-0027), and splitting them in the UI would suggest a
            # boundary that does not exist in the warehouse.
            return GOLD_GROUP
        return super().get_group_name(dbt_resource_props)


translator = WikistreamDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        # dbt tests become asset checks. On by default; stated because it is the
        # reason this project has sixty-one checks without writing sixty-one.
        enable_asset_checks=True,
        # Tests declared on the *source* attach to the external asset the Spark job
        # writes, which is where an assertion about `silver.edits` belongs.
        enable_source_tests_as_checks=True,
        # Links each asset back to the .sql file that defines it. Costs nothing and
        # turns the UI into a way to read the project.
        enable_code_references=True,
    )
)


def materialized_gold_models() -> tuple[str, ...]:
    """The dbt models that exist in the warehouse as tables, read from the manifest.

    Read rather than listed, because two other modules need this set and a
    hand-maintained copy of it would drift from the dbt project the first time a
    mart is added: `checks.py` asserts each of these holds rows, and
    `maintenance.py` compacts each of them.

    Views are excluded, and the same exclusion happens to be right for both
    callers. `stg_edits` is a view over `silver.edits`, so it is empty exactly when
    silver is empty — which the freshness check already reports, and reporting it
    twice would turn one fault into two red checks. And a view has no data files,
    so `ALTER TABLE ... EXECUTE optimize` on one is an error rather than a no-op.
    """
    manifest = json.loads(dbt_project.manifest_path.read_text())
    return tuple(
        sorted(
            node["name"]
            for node in manifest["nodes"].values()
            if node["resource_type"] == "model" and node["config"]["materialized"] != "view"
        )
    )


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=translator,
    project=dbt_project,
)
def wikistream_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    """Build the staging views and the gold marts, and run their tests.

    `--target-path` is not passed: `DbtCliResource` sets it per invocation so two
    concurrent runs cannot overwrite each other's `run_results.json`, which is the
    artefact a failed run is diagnosed from.
    """
    yield from dbt.cli(["build"], context=context).stream()
