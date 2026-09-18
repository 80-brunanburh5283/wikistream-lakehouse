/*
    Bot versus human activity per wiki per hour.

    The question this answers is the one people are usually surprised by: on most
    Wikimedia projects a large share of recorded changes are made by bots, and the
    share varies enormously between wikis. Keeping the two populations in separate
    columns rather than as a `is_bot` dimension row means a consumer gets the ratio
    without a self-join, and the ratio is the point.

    Incremental `merge` on (event_hour, wiki), with the same `>=` boundary
    treatment as `mart_edits_per_minute` — see the long comment there for why the
    boundary bucket is recomputed rather than skipped, and why there are two
    predicates on two different columns.

    `count(distinct ...)` is safe under this pattern only because the boundary
    bucket is recomputed from silver in full. A distinct count cannot be merged
    from a partial recomputation, so if the filter ever changes to `>` these two
    editor columns become wrong before anything else does.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_hour', 'wiki'],
    properties={
        'partitioning': "array['day(event_hour)']",
        'sorted_by': "array['event_hour']",
    }
) }}

with source_events as (
    select
        edits.event_hour,
        edits.wiki,
        edits.editor,
        edits.is_bot,
        edits.bytes_delta
    from {{ ref('stg_edits') }} as edits
    where
        not edits.is_canary
        -- Shared with dim_wikis, and the reason the relationships test between
        -- them holds. See macros/run_cutoff.sql.
        and edits.ingested_at < {{ run_cutoff() }}
        {% if is_incremental() %}
            and edits.event_date >= (
                select cast(coalesce(max(t.event_hour), {{ epoch_utc() }}) as date)
                from {{ this }} as t
            )
            and edits.event_hour >= (
                select coalesce(max(t.event_hour), {{ epoch_utc() }})
                from {{ this }} as t
            )
        {% endif %}
),

split as (
    select
        event_hour,
        wiki,
        count(*) as events,
        count_if(is_bot) as bot_events,
        count_if(not is_bot) as human_events,
        count(distinct case when is_bot then editor end) as bot_editors,
        count(distinct case when not is_bot then editor end) as human_editors,
        count_if(is_bot and bytes_delta is not null) as bot_events_with_bytes,
        count_if(not is_bot and bytes_delta is not null) as human_events_with_bytes,
        sum(case when is_bot then coalesce(bytes_delta, 0) else 0 end) as bot_bytes_net,
        sum(case when not is_bot then coalesce(bytes_delta, 0) else 0 end) as human_bytes_net
    from source_events
    group by event_hour, wiki
)

select
    event_hour,
    wiki,
    events,
    bot_events,
    human_events,
    bot_editors,
    human_editors,
    bot_events_with_bytes,
    human_events_with_bytes,
    bot_bytes_net,
    human_bytes_net,
    round(cast(bot_events as double) / nullif(events, 0), 4) as bot_share
from split
