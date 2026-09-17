"""The asset keys, in one place, spelled as literals.

An asset key is a *name in Dagster*, not a table identifier. The two look alike
here on purpose — `lakehouse/silver/edits` next to `lakehouse.silver.edits` — but
they are not the same kind of thing, and deriving the key from
`Settings.iceberg_catalog_name` would be a mistake worth explaining:

- `dbt/models/staging/_silver__sources.yml` pins the same key in
  `meta.dagster.asset_key`, and dbt YAML cannot read this module. A key computed
  from the environment would silently stop matching the one dbt declares, and the
  symptom is not an error — it is two disconnected halves of the lineage graph,
  which is exactly the thing the graph exists to show.
- Renaming a catalog should not rewrite history. Dagster stores materialisation
  and observation events under the key, so a key that moves with configuration
  orphans everything recorded before the change.

`tests/unit/test_dagster_lineage.py` asserts that the literals here and the ones
in the dbt YAML are the same strings, so the drift this comment warns about fails
a test rather than being noticed in a screenshot.
"""

from __future__ import annotations

from dagster import AssetKey

#: The Wikimedia EventStreams endpoint. Not observable: finding out whether it is
#: alive means opening a streaming connection to it, and the producer already
#: holds the one connection this project is allowed to have.
SOURCE_KEY = AssetKey(["wikimedia", "recentchange"])

#: The Kafka topic. Underscored rather than `wiki.recentchange`, because a dot in
#: a key component reads as a path separator everywhere Dagster prints it.
KAFKA_TOPIC_KEY = AssetKey(["kafka", "wiki_recentchange"])

BRONZE_KEY = AssetKey(["lakehouse", "bronze", "recentchange_raw"])
SILVER_EDITS_KEY = AssetKey(["lakehouse", "silver", "edits"])
SILVER_QUARANTINE_KEY = AssetKey(["lakehouse", "silver", "quarantine"])

#: The prefix `WikistreamDbtTranslator` puts on every dbt model, so that a dbt
#: node's key reads the same way as an external asset's: `catalog/namespace/table`.
GOLD_PREFIX = ["lakehouse", "gold"]


def gold_key(model_name: str) -> AssetKey:
    """The Dagster key for a dbt model, spelled once.

    Asset checks defined in this package have to name the dbt assets they attach
    to, and dbt-dagster derives those keys from the manifest through the translator
    in `dbt_project.py`. A check that spells the key itself and gets it wrong does
    not raise: Dagster accepts a check on an asset key that no definition provides,
    and it appears in the UI attached to nothing. This function and the translator
    are the two callers, and `tests/unit/test_dagster_lineage.py` asserts they
    agree for every model in the project.
    """
    return AssetKey([*GOLD_PREFIX, model_name])


#: Group names. Dagster renders these as boxes around the graph, so they are the
#: layers of the architecture diagram: what the stream owns, what Spark owns,
#: what dbt owns.
INGEST_GROUP = "ingest"
LAKEHOUSE_GROUP = "lakehouse"
GOLD_GROUP = "gold"

EXTERNAL_KEYS = (
    SOURCE_KEY,
    KAFKA_TOPIC_KEY,
    BRONZE_KEY,
    SILVER_EDITS_KEY,
    SILVER_QUARANTINE_KEY,
)
