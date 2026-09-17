"""Asset checks on the tables dbt does not own.

dbt already contributes sixty-one checks, one per test, and they all sit on the
`gold` models or on the two silver *sources*. What is left uncovered is
everything that is true of the streaming layer but not expressible as a dbt test:
bronze is not in the dbt project at all, and freshness and heartbeat are
properties of wall-clock time rather than of a table's contents.

The six checks here are chosen on one rule — each one fails for a *different*
reason, and each reason is a real failure this pipeline can have:

| Check | Catches |
|---|---|
| `bronze_offsets_are_contiguous` | records lost or double-applied between Kafka and Iceberg |
| `silver_edits_are_fresh` | the silver stream stopped, or fell behind |
| `canary_heartbeat_was_received` | the SSE connection dropped and reconnected past a gap |
| `quarantine_rate_is_low` | a validation rule, or the source's schema, changed |
| `quarantine_reasons_are_known` | Spark writing a `failure_reason` no Python rule defines |
| `marts_are_not_empty` | the whole gold layer passing its tests vacuously |

The last one is the least obvious and the most valuable. Every dbt test in this
project passes on an empty table: `not_null` over no rows is true, `unique` over
no rows is true. A mart that silently stopped being written would show sixty-one
green checks. Row count is the assertion that makes the other sixty-one mean
something.

Severities are set from what a human should do about it, not from how bad it
sounds. ERROR is "something is wrong with the code or the plumbing"; WARN is
"this is a fact about the live stream that you should look at". A laptop closed
for twenty minutes is not a bug in this repository, so freshness warns.
"""

#  No `from __future__ import annotations` here — see the comment at the top of
#  lakehouse.py for what postponed annotations do to a `context` parameter.

from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    MetadataValue,
    asset_check,
    multi_asset_check,
)

from wikistream.config import get_settings
from wikistream.quality.expectations import FAILURE_REASONS
from wikistream_dagster import sql
from wikistream_dagster.dbt_project import materialized_gold_models
from wikistream_dagster.keys import BRONZE_KEY, SILVER_EDITS_KEY, SILVER_QUARANTINE_KEY, gold_key
from wikistream_dagster.resources import TrinoResource
from wikistream_dagster.timestamps import timestamp_metadata

_SETTINGS = get_settings()

#: Freshness threshold. Deliberately thirty times the 30-second micro-batch
#: trigger: this is meant to catch "the stream is not running", not "the stream is
#: running slowly", and the same fifteen minutes is what
#: `dbt/models/staging/_silver__sources.yml` gives `dbt source freshness`, so the
#: two tools do not disagree about what stale means.
FRESHNESS_WARN_SECONDS = 15 * 60

#: Quarantine budget over the lookback window. The live stream has produced zero
#: quarantined rows in every run so far, so any sustained non-zero rate means
#: something changed upstream; 0.5% is loose enough that a handful of malformed
#: events during a Wikimedia deployment does not page anyone.
QUARANTINE_WARN_RATE = 0.005
QUARANTINE_LOOKBACK_HOURS = 24

#: The gold tables that must contain rows, read out of the dbt manifest so that a
#: mart added to the dbt project gets this check without anyone remembering to add
#: it here. `dbt_project.materialized_gold_models` explains why views are excluded.
_GOLD_TABLES = materialized_gold_models()


@asset_check(
    asset=BRONZE_KEY,
    name="bronze_offsets_are_contiguous",
    description=(
        "Every Kafka partition's row count in bronze equals `max(offset) - min(offset) + 1`. "
        "Fewer rows means records were lost; more means a micro-batch was written twice."
    ),
    blocking=False,
)
def bronze_offsets_are_contiguous(
    context: AssetCheckExecutionContext, trino: TrinoResource
) -> AssetCheckResult:
    """Prove no record was dropped or duplicated on the way into bronze.

    This is the exactly-once claim, checked in the one place it is checkable.
    Bronze appends without deduplicating, so its offsets are a complete record of
    what the consumer actually did — an at-least-once write would show up as a
    count above the range, and a lost commit as a count below it. Silver cannot be
    checked this way, because `MERGE INTO` is what makes it correct and it keeps no
    offsets.
    """
    rows = trino.fetch_all(sql.bronze_offset_contiguity(_SETTINGS.bronze_raw_table))

    if not rows:
        # An empty bronze table is not a contiguity failure. It happens on a stack
        # that has just come up, and reporting it as an error here would mean the
        # first check after `make up` is red for a reason that resolves itself.
        return AssetCheckResult(
            passed=True,
            metadata={
                "partitions": 0,
                "note": "bronze is empty — nothing to check yet, not a failure",
            },
        )

    gaps: list[dict[str, Any]] = []
    total = 0
    for partition, row_count, min_offset, max_offset in rows:
        total += row_count
        expected = max_offset - min_offset + 1
        if row_count != expected:
            gaps.append(
                {
                    "partition": partition,
                    "rows": row_count,
                    "expected": expected,
                    "offset_range": f"{min_offset}..{max_offset}",
                    # Signed, so the direction is readable at a glance: negative is
                    # loss, positive is duplication, and they are different bugs.
                    "difference": row_count - expected,
                }
            )

    context.log.info("checked %d partitions, %d rows, %d gaps", len(rows), total, len(gaps))
    return AssetCheckResult(
        passed=not gaps,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "partitions": len(rows),
            "rows": total,
            "partitions_with_gaps": len(gaps),
            "detail": MetadataValue.json(gaps),
        },
    )


@asset_check(
    asset=SILVER_EDITS_KEY,
    name="silver_edits_are_fresh",
    description=(
        f"The newest `event_time` in silver is less than {FRESHNESS_WARN_SECONDS // 60} "
        "minutes old. Measured against the event's own timestamp, not against the last "
        "observation."
    ),
    blocking=False,
)
def silver_edits_are_fresh(
    context: AssetCheckExecutionContext, trino: TrinoResource
) -> AssetCheckResult:
    """Measure lag from the data, not from Dagster's own event log.

    Dagster ships `build_last_update_freshness_checks` for this, and it is the
    wrong tool here: it measures the age of the newest observation, which for a
    table Dagster does not write is a measurement of whether the observation
    schedule is ticking. It would be green throughout a total outage of the
    streaming layer. `max(event_time)` against `current_timestamp` in Trino asks
    the question that was meant.
    """
    row = trino.fetch_one(sql.silver_edits_freshness(_SETTINGS.silver_edits_table))
    lag_seconds, last_event_at = row if row else (None, None)

    if lag_seconds is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"note": "silver.edits is empty — no event has been written yet"},
        )

    lag = float(lag_seconds)
    context.log.info("silver lag is %.1fs", lag)
    return AssetCheckResult(
        passed=lag <= FRESHNESS_WARN_SECONDS,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "lag_seconds": round(lag, 1),
            "threshold_seconds": FRESHNESS_WARN_SECONDS,
            "last_event_at": timestamp_metadata(last_event_at),
        },
    )


@asset_check(
    asset=SILVER_EDITS_KEY,
    name="canary_heartbeat_was_received",
    description=(
        "The last complete clock hour contains at least one Wikimedia `canary` event. "
        "Wikimedia emits two per hour into every stream, so a complete hour with traffic "
        "and no canary means the connection dropped across a gap."
    ),
    blocking=False,
)
def canary_heartbeat_was_received(
    context: AssetCheckExecutionContext, trino: TrinoResource
) -> AssetCheckResult:
    """Use the source's own heartbeat to detect a silent gap in the stream.

    A reconnect that happens promptly loses only the events in the gap, and no
    count over the table can distinguish that from a quiet hour on the wikis. The
    canary events can: Wikimedia injects them on a fixed schedule regardless of
    edit volume, so their absence from an hour with other traffic is positive
    evidence of a gap rather than the absence of evidence of edits.

    The window is the previous wall-clock hour. Anchoring it to `max(event_time)`
    instead would make a pipeline that died two days ago look healthy, because the
    newest hour it holds is complete and full of canaries.
    """
    row = trino.fetch_one(sql.canary_events_in_last_complete_hour(_SETTINGS.silver_edits_table))
    canaries, rows = row if row else (0, 0)

    if not rows:
        # No rows at all in that hour is not evidence of a gap: the stack may have
        # come up ten minutes ago. Passing with the reason recorded is honest;
        # failing would make the check red for the first hour of every fresh clone.
        return AssetCheckResult(
            passed=True,
            metadata={
                "canary_events": 0,
                "rows_in_window": 0,
                "note": (
                    "inconclusive — the last complete hour holds no events at all, so there "
                    "is nothing to compare a missing heartbeat against"
                ),
            },
        )

    context.log.info("%d canary events among %d rows in the last complete hour", canaries, rows)
    return AssetCheckResult(
        passed=canaries > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "canary_events": canaries,
            "rows_in_window": rows,
            "expected_per_hour": 2,
        },
    )


@asset_check(
    asset=SILVER_QUARANTINE_KEY,
    name="quarantine_rate_is_low",
    description=(
        f"Quarantined rows are under {QUARANTINE_WARN_RATE:.1%} of everything ingested in the "
        f"last {QUARANTINE_LOOKBACK_HOURS} hours."
    ),
    blocking=False,
)
def quarantine_rate_is_low(
    context: AssetCheckExecutionContext, trino: TrinoResource
) -> AssetCheckResult:
    """Watch the *share* of rejected events, not the count.

    A count threshold has to be re-tuned every time throughput changes, and this
    stream's throughput varies by a factor of several between a European afternoon
    and 04:00 UTC. A rate does not.

    Both sides are measured on ingest time — see `sql.quarantine_rate` for why
    that makes this an approximation rather than an exact ratio.
    """
    row = trino.fetch_one(
        sql.quarantine_rate(
            _SETTINGS.silver_edits_table,
            _SETTINGS.silver_quarantine_table,
            QUARANTINE_LOOKBACK_HOURS,
        )
    )
    edits, quarantined = row if row else (0, 0)
    total = edits + quarantined
    rate = quarantined / total if total else 0.0

    context.log.info("%d quarantined of %d ingested (%.4f%%)", quarantined, total, rate * 100)
    return AssetCheckResult(
        passed=rate <= QUARANTINE_WARN_RATE,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "quarantined": quarantined,
            "accepted": edits,
            "rate": rate,
            "threshold": QUARANTINE_WARN_RATE,
            "lookback_hours": QUARANTINE_LOOKBACK_HOURS,
        },
    )


@asset_check(
    asset=SILVER_QUARANTINE_KEY,
    name="quarantine_reasons_are_known",
    description=(
        "Every `failure_reason` in quarantine is one of the values "
        "`wikistream.quality.expectations.FAILURE_REASONS` defines."
    ),
    blocking=False,
)
def quarantine_reasons_are_known(
    context: AssetCheckExecutionContext, trino: TrinoResource
) -> AssetCheckResult:
    """Keep the vocabulary shared between the Python rules and the warehouse.

    `FAILURE_REASONS` is imported rather than restated, so a rule added in Python
    cannot leave this check asserting last week's set. The dbt source has an
    `accepted_values` test with the same intent, and it is not redundant with this
    one: dbt's list is written out in YAML and can drift from the Python, which is
    what makes it a useful second opinion — if these two ever disagree, the
    disagreement is the finding.
    """
    rows = trino.fetch_all(
        sql.unknown_quarantine_reasons(_SETTINGS.silver_quarantine_table, FAILURE_REASONS)
    )
    unknown = {str(reason): count for reason, count in rows}

    if unknown:
        context.log.error("unknown failure reasons in quarantine: %s", sorted(unknown))
    return AssetCheckResult(
        passed=not unknown,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "unknown_reasons": len(unknown),
            "detail": MetadataValue.json(unknown),
            "known_reasons": MetadataValue.json(list(FAILURE_REASONS)),
        },
    )


@multi_asset_check(
    specs=[
        AssetCheckSpec(
            name="marts_are_not_empty",
            asset=gold_key(table),
            description="The table holds at least one row, so its other checks are not vacuous.",
            blocking=False,
        )
        for table in _GOLD_TABLES
    ],
    can_subset=True,
)
def marts_are_not_empty(context: AssetCheckExecutionContext, trino: TrinoResource) -> Any:
    """Assert each mart has rows — the check that gives the other sixty-one meaning.

    One decorated function rather than five, because five would open five Trino
    connections to run five `count(*)`s that belong to the same question.

    A `count(*)` on an Iceberg table does not scan data files; Trino answers it
    from the manifest, so this is five metadata reads rather than five table scans.
    """
    for key in context.selected_asset_check_keys:
        # The last component of the asset key is the dbt model name, which is also
        # the table name: `lakehouse/gold/dim_wikis` -> `lakehouse.gold.dim_wikis`.
        # Going through `Settings.table` rather than joining the key's own path
        # means a renamed catalog moves the query and not the check's identity.
        table = _SETTINGS.table(_SETTINGS.gold_namespace, key.asset_key.path[-1])
        row = trino.fetch_one(sql.row_count(table))
        rows = row[0] if row else 0
        yield AssetCheckResult(
            asset_key=key.asset_key,
            check_name=key.name,
            passed=rows > 0,
            severity=AssetCheckSeverity.WARN,
            metadata={"rows": rows, "table": table},
        )


CHECKS = [
    bronze_offsets_are_contiguous,
    silver_edits_are_fresh,
    canary_heartbeat_was_received,
    quarantine_rate_is_low,
    quarantine_reasons_are_known,
    marts_are_not_empty,
]
