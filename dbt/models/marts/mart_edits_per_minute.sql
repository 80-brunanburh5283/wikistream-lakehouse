/*
    Events per minute, split by wiki and change type. The finest-grained mart,
    and the one that makes the streaming half of this project visible: a chart of
    `events` against `event_minute` is the pipeline's pulse.

    Incremental, strategy `merge`. Two things about that are worth reading before
    changing anything here.

    **The boundary minute is recomputed, not skipped.** The filter is `>=` the
    mart's current maximum minute, not `>`. The last minute in the mart is almost
    always partial — the run happened part way through it — so a `>` filter would
    freeze that minute at whatever fraction had arrived and never correct it. With
    `>=`, the minute is recomputed from silver on the next run and the merge
    overwrites the partial row. That is the whole reason the strategy is `merge`
    and not `append`: append would add a second row for the same minute and the
    grain would quietly stop being unique.

    **Two predicates, one purpose each.** `event_minute >=` is the correctness
    filter. `event_date >=` is redundant to it and exists only so Iceberg can
    prune partitions: silver.edits is partitioned by `event_date`, and a
    predicate on `event_minute` alone does not let the planner drop files,
    because Trino cannot prove the relationship between a truncated timestamp in
    a view and the partition column underneath it. Stating both turns a full-table
    scan into a one-or-two-partition scan.

    Canary events are excluded, which also keeps the merge keys non-null: canary
    rows have no `wiki`, and a merge on a null key matches nothing and inserts a
    duplicate every run.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_minute', 'wiki', 'change_type'],
    properties={
        'partitioning': "array['day(event_minute)']",
        'sorted_by': "array['event_minute']",
    }
) }}

with source_events as (
    select
        edits.event_minute,
        edits.wiki,
        edits.change_type,
        edits.editor,
        edits.page_title,
        edits.is_bot,
        edits.is_article,
        edits.bytes_delta
    from {{ ref('stg_edits') }} as edits
    where
        not edits.is_canary
        {% if is_incremental() %}
        -- Scalar subqueries, so Trino evaluates each once and pushes a constant
        -- into the scan. Cross-joining the same aggregate would give the same
        -- answer and prune nothing.
            and edits.event_date >= (
                select cast(coalesce(max(t.event_minute), {{ epoch_utc() }}) as date)
                from {{ this }} as t
            )
            and edits.event_minute >= (
                select coalesce(max(t.event_minute), {{ epoch_utc() }})
                from {{ this }} as t
            )
        {% endif %}
)

select
    event_minute,
    wiki,
    change_type,
    count(*) as events,
    count(distinct editor) as distinct_editors,
    count(distinct page_title) as distinct_pages,
    count_if(is_bot) as bot_events,
    count_if(is_article) as article_events,
    count_if(bytes_delta is not null) as events_with_bytes,
    sum(coalesce(greatest(bytes_delta, 0), 0)) as bytes_added,
    sum(coalesce(least(bytes_delta, 0), 0)) as bytes_removed
from source_events
group by event_minute, wiki, change_type
