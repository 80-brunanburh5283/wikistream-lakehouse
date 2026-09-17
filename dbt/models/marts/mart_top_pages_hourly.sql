/*
    The ten most-edited pages per wiki per hour.

    Incremental, strategy `delete+insert`, and that difference from the other two
    marts is the interesting thing in this file.

    **Why not `merge`.** Top-N is not additive and not stable. A page that was
    ranked 8th when the hour was half over can fall out of the top ten by the end
    of it. A `merge` keyed on (event_hour, wiki, page_title) would update the rows
    it finds and insert the ones it does not — and leave the evicted page sitting
    in the table at its stale rank, because nothing in a merge deletes a row that
    the new batch no longer produces. The mart would slowly accumulate an eleventh,
    twelfth, thirteenth "top ten" entry per hour. `delete+insert` keyed on
    `event_hour` removes the whole hour and rebuilds it, which is the only correct
    treatment for a non-additive aggregate.

    dbt-trino implements that as `delete from <target> where (event_hour) in
    (select event_hour from <tmp>)` followed by an `insert`, materialising the
    model into a temporary *table* first rather than a view so that both
    statements see identical input.

    **`unique_key` here is not unique in the target.** It is `event_hour`, and the
    target holds up to ten rows per (event_hour, wiki). For `delete+insert` the
    argument names the delete predicate, not a key — the naming is dbt's, and it
    is worth saying out loud because a reader who assumes otherwise will conclude
    the model is broken. The real grain is asserted by the
    `unique_combination_of_columns` test in `_marts__models.yml`.

    **The ranking is deterministic.** `order by events desc, page_title asc`: the
    tie-break on title is not cosmetic. Without it, two pages with equal counts
    could swap places between runs, and the boundary hour is recomputed on every
    run — so the table's contents would change with no change in the data, and
    "re-running produces the same table" would stop being true.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_hour',
    properties={
        'partitioning': "array['day(event_hour)']",
        'sorted_by': "array['event_hour']",
    }
) }}

{% set top_n = 10 %}

with source_events as (
    select
        edits.event_hour,
        edits.wiki,
        edits.page_title,
        edits.page_url,
        edits.editor,
        edits.is_bot,
        edits.is_article,
        edits.bytes_delta
    from {{ ref('stg_edits') }} as edits
    where
        not edits.is_canary
        -- Canary events have no page at all; this second condition catches the
        -- handful of log events that also arrive without one.
        and edits.page_title is not null
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

page_hours as (
    select
        event_hour,
        wiki,
        page_title,
        max(page_url) as page_url,
        count(*) as events,
        count(distinct editor) as distinct_editors,
        count_if(is_bot) as bot_events,
        bool_or(is_article) as is_article,
        sum(coalesce(bytes_delta, 0)) as bytes_net
    from source_events
    group by event_hour, wiki, page_title
),

ranked as (
    select
        event_hour,
        wiki,
        page_title,
        page_url,
        events,
        distinct_editors,
        bot_events,
        is_article,
        bytes_net,
        row_number() over (
            partition by event_hour, wiki
            order by events desc, page_title asc
        ) as rank_in_wiki
    from page_hours
)

select
    event_hour,
    wiki,
    rank_in_wiki,
    page_title,
    page_url,
    events,
    distinct_editors,
    bot_events,
    is_article,
    bytes_net
from ranked
where rank_in_wiki <= {{ top_n }}
