"""Tests that the lineage graph is one graph.

The seam these guard is the only place in this repository where two files have to
agree on a string and neither tool complains when they do not. `keys.py` declares
`AssetKey(["lakehouse", "silver", "edits"])` for the table the Spark job writes;
`dbt/models/staging/_silver__sources.yml` declares the same key in
`meta.dagster.asset_key` for the source `stg_edits` reads. If the two spellings
drift, Dagster does not raise — it draws two disconnected halves, one ending at
silver and one starting at it, and the lineage that is the whole point of adding an
orchestrator quietly stops being lineage.

Loading the code location takes a few seconds, so it is done once per session.
"""

from __future__ import annotations

import json

import pytest
import yaml

from wikistream_dagster import keys
from wikistream_dagster.dbt_project import PROJECT_DIR, dbt_project, materialized_gold_models

pytestmark = pytest.mark.unit


@pytest.fixture(scope="session")
def asset_graph():
    from wikistream_dagster.definitions import defs

    return defs.resolve_asset_graph()


@pytest.fixture(scope="session")
def dbt_sources() -> dict:
    """The source YAML as written, not as dbt parsed it.

    Read from the file rather than from the manifest on purpose. The manifest is
    generated, so a manifest that has not been regenerated since the YAML changed
    would let this test pass on stale evidence.
    """
    path = PROJECT_DIR / "models" / "staging" / "_silver__sources.yml"
    return yaml.safe_load(path.read_text())


@pytest.fixture(scope="session")
def manifest() -> dict:
    return json.loads(dbt_project.manifest_path.read_text())


def _table(sources: dict, name: str) -> dict:
    [silver] = [source for source in sources["sources"] if source["name"] == "silver"]
    [table] = [table for table in silver["tables"] if table["name"] == name]
    return table


@pytest.mark.parametrize(
    ("table_name", "key"),
    [
        ("edits", keys.SILVER_EDITS_KEY),
        ("quarantine", keys.SILVER_QUARANTINE_KEY),
    ],
)
def test_dbt_source_declares_the_same_key_as_the_python(dbt_sources, table_name, key):
    """The one string two files must agree on, asserted in both directions."""
    declared = _table(dbt_sources, table_name)["meta"]["dagster"]["asset_key"]

    assert declared == list(key.path)


def test_silver_and_the_staging_views_are_one_connected_graph(asset_graph):
    """`stg_edits` sits directly on the observed silver asset, with nothing between.

    This is the assertion that the seam holds. If the keys drifted, `stg_edits` would
    have a parent named after the dbt source's default key — `silver/edits` with no
    catalog prefix — and this would fail naming the key it actually found.
    """
    parents = asset_graph.get(keys.gold_key("stg_edits")).parent_keys

    assert parents == {keys.SILVER_EDITS_KEY}, f"stg_edits reads from {parents}"


def test_the_whole_pipeline_is_reachable_from_the_source(asset_graph):
    """Walk from the Wikimedia endpoint and expect to arrive at every mart.

    A parametrised per-edge test would pass on a graph in two pieces as long as each
    piece was internally correct. Reachability from the one true root is the property
    that cannot be satisfied by two halves.
    """
    reachable: set = set()
    frontier = [keys.SOURCE_KEY]
    while frontier:
        key = frontier.pop()
        if key in reachable:
            continue
        reachable.add(key)
        frontier.extend(asset_graph.get(key).child_keys)

    expected = {
        *keys.EXTERNAL_KEYS,
        keys.gold_key("stg_edits"),
        keys.gold_key("stg_quarantine"),
        *(keys.gold_key(model) for model in materialized_gold_models()),
    }

    assert expected <= reachable, f"unreachable from the source: {expected - reachable}"


def test_the_source_is_the_only_root(asset_graph):
    """Exactly one node with no parents, and it is the Wikimedia endpoint.

    A second root is the signature of the drift this module exists to catch: a
    mis-keyed dbt source becomes a brand new node that nothing feeds.
    """
    roots = {
        key for key in asset_graph.get_all_asset_keys() if not asset_graph.get(key).parent_keys
    }

    assert roots == {keys.SOURCE_KEY}


def test_gold_key_agrees_with_the_dbt_translator(asset_graph, manifest):
    """`keys.gold_key` and the translator must produce the same key for every model.

    They are two independent spellings of the same convention —
    `WikistreamDbtTranslator.get_asset_key` prefixes what dbt gives it, `gold_key`
    builds the key from scratch for the asset checks — and Dagster accepts a check
    on an asset key that no definition provides. The check then appears in the UI
    attached to nothing at all, which is worse than an error.
    """
    model_names = [
        node["name"] for node in manifest["nodes"].values() if node["resource_type"] == "model"
    ]
    assert model_names, "no models in the manifest — has `dbt parse` run?"

    defined = asset_graph.get_all_asset_keys()
    for name in model_names:
        assert keys.gold_key(name) in defined, name


def test_silver_depends_on_kafka_and_not_on_bronze(asset_graph):
    """The edge that is easy to draw wrong, asserted so it stays as designed.

    Bronze and silver are independent consumers of the same topic with separate
    checkpoints. Drawing silver downstream of bronze would suggest silver's contents
    are a function of bronze's, and the first thing anyone would then write is a
    check comparing their row counts — which fails routinely on a healthy pipeline,
    because either consumer can be ahead of the other.
    """
    assert asset_graph.get(keys.SILVER_EDITS_KEY).parent_keys == {keys.KAFKA_TOPIC_KEY}
    assert asset_graph.get(keys.SILVER_QUARANTINE_KEY).parent_keys == {keys.KAFKA_TOPIC_KEY}
    assert asset_graph.get(keys.BRONZE_KEY).parent_keys == {keys.KAFKA_TOPIC_KEY}


def test_the_streaming_half_is_not_materializable(asset_graph):
    """Every external asset must stay external.

    Turning one of these into a materializable asset would put a "Materialize" button
    in the UI for a table a continuous Spark query owns. Pressing it would either do
    nothing or start a second writer.
    """
    for key in keys.EXTERNAL_KEYS:
        assert not asset_graph.get(key).is_materializable, key


def test_every_gold_model_is_in_the_gold_group(asset_graph, manifest):
    """One group for the dbt half, so the UI's grouping matches the warehouse's."""
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model":
            continue
        assert asset_graph.get(keys.gold_key(node["name"])).group_name == keys.GOLD_GROUP
