"""The half of the pipeline Dagster watches but does not run.

The producer, the bronze stream and the silver stream are long-running processes.
Compose starts them, restarts them and health-checks them; they have no beginning
and no end, so "materialise this asset" is not a thing that can be asked of them.
Modelling them as Dagster assets that a schedule triggers would mean either
stopping a continuous query to run a batch one, or lying about what the button
does.

They are external assets instead: real nodes in the graph, with real upstream and
downstream edges, and — for the three Iceberg tables — an *observation* that
records what is in them right now. That is the honest shape, and it is also the
useful one, because the observation carries a data version, so a mart shows as
stale in the UI when the table under it has grown. ADR-0030.

One edge in here is worth reading twice: silver depends on the **Kafka topic**,
not on bronze. The silver stream is a second independent consumer of the same
topic with its own checkpoint, which is why bronze can be behind silver, why no
check compares their row counts, and why `docs/architecture.md` draws two arrows
out of Kafka rather than a chain.
"""

#  No `from __future__ import annotations` in this module, unlike the rest of the
#  package. Dagster validates the `context` parameter by comparing
#  `inspect.signature(fn).parameters["context"].annotation` against the context
#  classes themselves, and postponed evaluation makes that annotation the *string*
#  `"AssetExecutionContext"`, which matches nothing. The error it raises reads
#  "Cannot annotate `context` parameter with type AssetExecutionContext" — naming
#  the type it just rejected — so it is worth knowing the cause before losing an
#  hour to it. Every annotation here is valid at runtime on Python 3.10.

from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    DataVersion,
    MetadataValue,
    ObserveResult,
    multi_observable_source_asset,
)

from wikistream.config import get_settings
from wikistream_dagster import sql
from wikistream_dagster.keys import (
    BRONZE_KEY,
    INGEST_GROUP,
    KAFKA_TOPIC_KEY,
    LAKEHOUSE_GROUP,
    SILVER_EDITS_KEY,
    SILVER_QUARANTINE_KEY,
    SOURCE_KEY,
)
from wikistream_dagster.resources import KafkaResource, TrinoResource
from wikistream_dagster.timestamps import epoch, timestamp_metadata

_SETTINGS = get_settings()


# --------------------------------------------------------------------- upstream
#
# Two nodes with nothing to observe. The stream is a URL and the topic's contents
# are Kafka's business, so neither has a row count; they are here because a
# lineage graph that starts at bronze does not tell a reader where the data comes
# from, and that is the first question anyone asks of this repository.

source_stream = AssetSpec(
    key=SOURCE_KEY,
    group_name=INGEST_GROUP,
    kinds={"source"},
    description=(
        "Wikimedia EventStreams `recentchange`: a public, unauthenticated SSE feed of "
        "every edit, log action and category change across all Wikimedia wikis. "
        "Consumed by one long-lived connection with exponential backoff on reconnect, "
        "identifying itself with a project User-Agent, which is what Wikimedia asks of "
        "clients."
    ),
    metadata={
        "url": MetadataValue.url(_SETTINGS.wikimedia_stream_url),
        "protocol": "server-sent events",
        "terms": MetadataValue.url(
            "https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams"
        ),
    },
)


# ------------------------------------------------------------------ observations
#
# One decorated function for the four observable nodes rather than four, because
# each one is a Trino query or a Kafka metadata call and doing them together means
# a single Dagster run shows the whole pipeline's state at one instant. Four
# separate assets would each report a different moment, and the arithmetic between
# them — Kafka high watermark against bronze rows — would be comparing two clocks.


#  The decorator is unannotated in dagster, so mypy would otherwise treat the
#  observation function below as untyped and stop checking its body.
@multi_observable_source_asset(  # type: ignore[untyped-decorator]
    name="lakehouse_state",
    specs=[
        AssetSpec(
            key=KAFKA_TOPIC_KEY,
            deps=[SOURCE_KEY],
            group_name=INGEST_GROUP,
            kinds={"kafka"},
            description=(
                "The topic the producer writes to, keyed by wiki domain so all events for "
                "one wiki are ordered. Observation reports each partition's low and high "
                "watermark: `high - low` is what the broker currently holds, not what has "
                "ever been produced, because 24-hour retention deletes from the low end."
            ),
            metadata={
                "topic": _SETTINGS.kafka_topic,
                "partitions": _SETTINGS.kafka_topic_partitions,
            },
        ),
        AssetSpec(
            key=BRONZE_KEY,
            deps=[KAFKA_TOPIC_KEY],
            group_name=LAKEHOUSE_GROUP,
            kinds={"spark", "iceberg"},
            description=(
                "Append-only audit table: one row per Kafka record, the payload stored "
                "exactly as it arrived and never reserialised. No deduplication by design "
                "— this is the table a replay is rebuilt from, so it has to record what "
                "actually happened, including a record delivered twice."
            ),
            metadata={"table": _SETTINGS.bronze_raw_table, "partitioning": "days(ingest_date)"},
        ),
        AssetSpec(
            key=SILVER_EDITS_KEY,
            deps=[KAFKA_TOPIC_KEY],
            group_name=LAKEHOUSE_GROUP,
            kinds={"spark", "iceberg"},
            description=(
                "One row per source event, typed and validated. Written by `MERGE INTO ... "
                "WHEN NOT MATCHED` on `event_id`, which is the only reason the uniqueness "
                "claim holds — Iceberg has no primary key. Depends on the Kafka topic and "
                "not on bronze: the silver stream is a second, independent consumer."
            ),
            metadata={"table": _SETTINGS.silver_edits_table, "partitioning": "days(event_date)"},
        ),
        AssetSpec(
            key=SILVER_QUARANTINE_KEY,
            deps=[KAFKA_TOPIC_KEY],
            group_name=LAKEHOUSE_GROUP,
            kinds={"spark", "iceberg"},
            description=(
                "Events that failed validation, with the rule that rejected them and the "
                "original payload, so a fix can be replayed rather than merely counted. "
                "Expected to be empty; it has never received a row from the live stream."
            ),
            metadata={"table": _SETTINGS.silver_quarantine_table},
        ),
    ],
    can_subset=True,
)
def lakehouse_state(
    context: AssetExecutionContext,
    trino: TrinoResource,
    kafka: KafkaResource,
) -> Any:
    """Report what is in Kafka and in the three Iceberg tables right now."""
    selected = context.selected_asset_keys

    if KAFKA_TOPIC_KEY in selected:
        watermarks = kafka.partition_watermarks()
        retained = sum(high - low for low, high in watermarks.values())
        yield ObserveResult(
            asset_key=KAFKA_TOPIC_KEY,
            metadata={
                "records_retained": retained,
                "partitions": len(watermarks),
                "watermarks": MetadataValue.json(
                    {str(p): {"low": low, "high": high} for p, (low, high) in watermarks.items()}
                ),
            },
            # The sum of high watermarks only ever increases, so it identifies the
            # topic's state without storing the whole watermark map as a version.
            data_version=DataVersion(str(sum(high for _, high in watermarks.values()))),
        )

    if BRONZE_KEY in selected:
        yield _observe_bronze(trino)

    if SILVER_EDITS_KEY in selected:
        yield _observe_silver_edits(trino)

    if SILVER_QUARANTINE_KEY in selected:
        yield _observe_quarantine(trino)


def _file_metadata(trino: TrinoResource, table: str) -> dict[str, Any]:
    """Data-file statistics from the current Iceberg snapshot.

    Surfaced on every table because the ratio of records to files is the small-file
    problem, and having it on the asset means it is seen without anyone running a
    maintenance job to find out. `make maintain` is what moves it.
    """
    row = trino.fetch_one(sql.file_statistics(table))
    if row is None:
        return {}
    files, total_bytes, records = row
    metadata: dict[str, Any] = {
        "data_files": files,
        "bytes": total_bytes,
        "records_in_files": records,
    }
    if files:
        metadata["avg_file_kib"] = round(total_bytes / files / 1024, 1)
    return metadata


def _observe_bronze(trino: TrinoResource) -> ObserveResult:
    row = trino.fetch_one(sql.bronze_observation(_SETTINGS.bronze_raw_table))
    rows, last_ingested_at, min_offset, max_offset = row if row else (0, None, None, None)
    return ObserveResult(
        asset_key=BRONZE_KEY,
        metadata={
            "rows": rows,
            "last_ingested_at": timestamp_metadata(last_ingested_at),
            "min_kafka_offset": min_offset if min_offset is not None else -1,
            "max_kafka_offset": max_offset if max_offset is not None else -1,
            **_file_metadata(trino, _SETTINGS.bronze_raw_table),
        },
        data_version=DataVersion(str(rows)),
    )


def _observe_silver_edits(trino: TrinoResource) -> ObserveResult:
    row = trino.fetch_one(sql.silver_edits_observation(_SETTINGS.silver_edits_table))
    rows, distinct_ids, last_event_at, last_ingested_at, canaries, late_p95 = (
        row if row else (0, 0, None, None, 0, None)
    )
    lag = None
    if (event_epoch := epoch(last_event_at)) and (ingest_epoch := epoch(last_ingested_at)):
        lag = round(ingest_epoch - event_epoch, 1)
    return ObserveResult(
        asset_key=SILVER_EDITS_KEY,
        metadata={
            "rows": rows,
            "distinct_event_ids": distinct_ids,
            "canary_events": canaries,
            "last_event_at": timestamp_metadata(last_event_at),
            "last_ingested_at": timestamp_metadata(last_ingested_at),
            # Two different lags, and the difference matters. `late_p95_seconds` is
            # the source's own lag distribution over the whole table; the other is
            # how far behind the newest row's ingest was. One sizes a watermark,
            # the other says whether the pipeline is keeping up now.
            "late_p95_seconds": late_p95 if late_p95 is not None else -1,
            "newest_row_ingest_lag_seconds": lag if lag is not None else -1,
            **_file_metadata(trino, _SETTINGS.silver_edits_table),
        },
        data_version=DataVersion(str(rows)),
    )


def _observe_quarantine(trino: TrinoResource) -> ObserveResult:
    row = trino.fetch_one(sql.quarantine_observation(_SETTINGS.silver_quarantine_table))
    rows, last_failed_at, distinct_reasons = row if row else (0, None, 0)
    return ObserveResult(
        asset_key=SILVER_QUARANTINE_KEY,
        metadata={
            "rows": rows,
            "distinct_failure_reasons": distinct_reasons,
            "last_failed_at": timestamp_metadata(last_failed_at),
            **_file_metadata(trino, _SETTINGS.silver_quarantine_table),
        },
        data_version=DataVersion(str(rows)),
    )
