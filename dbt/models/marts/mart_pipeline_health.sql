/*
    One row per hour describing the pipeline rather than the wikis: how much
    arrived, how late it was, how much was rejected, and whether the source was
    alive. This is the mart an operator looks at, and the only one that keeps the
    canary events.

    **Why a full rebuild and not incremental.** The grain is an hour, so the table
    has one row per hour since the pipeline started — a few hundred rows after a
    fortnight. Incrementalising a table that small would add the boundary-bucket
    reasoning of the other three marts in exchange for saving a scan that costs
    less than the planning. Materialised as a table rather than a view because the
    percentiles are the expensive part and an operator refreshing a dashboard
    should not pay for them on every look.

    **Two clocks, joined anyway.** `stg_edits` is bucketed on event time — when the
    edit happened. `stg_quarantine` is bucketed on ingest time, because "the
    timestamp could not be parsed" is one of the reasons a row lands in quarantine,
    so there is no event time to bucket on. The join is therefore an approximation:
    an event rejected at 14:05 for having an unparseable timestamp is counted
    against the 14:00 bucket even though nobody knows when it happened. At the
    measured lateness of this source the two clocks agree to within a minute for
    the overwhelming majority of rows, which makes the approximation good enough
    to alert on and not good enough to reconcile against. `quarantine_rate` is
    labelled accordingly.

    **The full outer join is load-bearing.** An hour can have edits and no
    quarantine (the normal case), or — if the source ever emitted an hour of
    nothing but malformed payloads — quarantine and no edits. An inner or left
    join would silently drop the second case, which is precisely the case this
    mart exists to make visible.

    **`canary_events` is the liveness signal.** Wikimedia injects two synthetic
    heartbeat events per hour, at :15. Two is healthy. Zero in a *complete* hour
    means the connection dropped rather than the wikis going quiet — the one
    reading that distinguishes a broken pipeline from a slow news day. No boolean
    flag is derived from it here because the newest bucket is always partial and a
    flag would report a healthy pipeline as dead for up to fifteen minutes an hour.
*/

{{ config(materialized='table') }}

with edit_hours as (
    select
        event_hour,
        count(*) as events,
        count_if(is_canary) as canary_events,
        count_if(not is_canary and is_bot) as bot_events,
        count(distinct wiki) as distinct_wikis,
        count(distinct domain) as distinct_domains,
        count_if(bytes_delta is null) as events_without_bytes,
        min(event_time) as first_event_at,
        max(event_time) as last_event_at,
        max(ingested_at) as last_ingested_at,
        -- approx_percentile, not an exact one: the exact form sorts the whole
        -- group, and the number is used to size a watermark, where being right to
        -- the second matters and being right to the row does not.
        approx_percentile(late_by_seconds, 0.5) as late_p50_seconds,
        approx_percentile(late_by_seconds, 0.95) as late_p95_seconds,
        approx_percentile(late_by_seconds, 0.99) as late_p99_seconds,
        max(late_by_seconds) as late_max_seconds
    from {{ ref('stg_edits') }}
    group by event_hour
),

quarantine_hours as (
    select
        failed_hour,
        count(*) as quarantined_events,
        count(distinct failure_reason) as distinct_failure_reasons
    from {{ ref('stg_quarantine') }}
    group by failed_hour
)

select
    coalesce(edits.event_hour, quarantine.failed_hour) as bucket_hour,
    coalesce(edits.events, 0) as events,
    coalesce(edits.canary_events, 0) as canary_events,
    coalesce(edits.bot_events, 0) as bot_events,
    coalesce(quarantine.quarantined_events, 0) as quarantined_events,
    coalesce(quarantine.distinct_failure_reasons, 0) as distinct_failure_reasons,
    coalesce(edits.distinct_wikis, 0) as distinct_wikis,
    coalesce(edits.distinct_domains, 0) as distinct_domains,
    coalesce(edits.events_without_bytes, 0) as events_without_bytes,
    edits.first_event_at,
    edits.last_event_at,
    edits.last_ingested_at,
    edits.late_p50_seconds,
    edits.late_p95_seconds,
    edits.late_p99_seconds,
    edits.late_max_seconds,
    round(
        cast(coalesce(quarantine.quarantined_events, 0) as double)
        / nullif(coalesce(edits.events, 0) + coalesce(quarantine.quarantined_events, 0), 0),
        6
    ) as quarantine_rate
from edit_hours as edits
full outer join quarantine_hours as quarantine
    on edits.event_hour = quarantine.failed_hour
