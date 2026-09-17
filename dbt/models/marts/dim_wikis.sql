/*
    One row per wiki. The dimension the hourly marts join to for a stable
    denominator and a display domain.

    It exists mainly to record two facts that silver does not make obvious:

    1. `wiki` and `domain` are not one to one. On the sample this was written
       against, 84 of 370 wikis emit events under more than one domain, and one
       domain — auth.wikimedia.org, which carries central-authentication log
       events — is shared by 91 wikis. So this model keeps `domain_count` next to
       `primary_domain` instead of pretending the mapping is a key. Any query
       that groups by domain and calls the result "per wiki" is wrong for 23% of
       wikis, and the column is here so that is visible rather than discovered.
    2. `bytes_delta` is null for most rows, because `categorize` and `log` events
       carry no page length at all. `events_with_bytes` is the honest denominator
       for the byte columns. `total_events` is not.

    Aggregated in two steps deliberately. The inner grain is (wiki, domain) so
    that `max_by(domain, events)` can pick the plurality domain; every measure
    carried up from there is additive, which is why the roll-up is a plain sum.
    Doing it in one pass would mean a second scan for the domain ranking.
*/

{{ config(materialized='table') }}

with by_wiki_domain as (
    select
        wiki,
        domain,
        count(*) as events,
        min(event_time) as first_seen_at,
        max(event_time) as last_seen_at,
        count_if(is_bot) as bot_events,
        count_if(is_article) as article_events,
        count_if(is_anonymous) as anonymous_events,
        count_if(bytes_delta is not null) as events_with_bytes,
        -- greatest/least return null on a null input, so the coalesce is what
        -- keeps a `categorize` row from turning the whole sum into null.
        sum(coalesce(greatest(bytes_delta, 0), 0)) as bytes_added,
        sum(coalesce(least(bytes_delta, 0), 0)) as bytes_removed
    from {{ ref('stg_edits') }}
    where not is_canary
    group by wiki, domain
),

rolled_up as (
    select
        wiki,
        max_by(domain, events) as primary_domain,
        count(*) as domain_count,
        min(first_seen_at) as first_seen_at,
        max(last_seen_at) as last_seen_at,
        sum(events) as total_events,
        sum(bot_events) as bot_events,
        sum(article_events) as article_events,
        sum(anonymous_events) as anonymous_events,
        sum(events_with_bytes) as events_with_bytes,
        sum(bytes_added) as bytes_added,
        sum(bytes_removed) as bytes_removed
    from by_wiki_domain
    group by wiki
)

select
    wiki,
    primary_domain,
    domain_count,
    first_seen_at,
    last_seen_at,
    total_events,
    bot_events,
    total_events - bot_events as human_events,
    article_events,
    anonymous_events,
    events_with_bytes,
    bytes_added,
    bytes_removed,
    bytes_added + bytes_removed as bytes_net,
    -- Cast before dividing: bigint / bigint is integer division in Trino, so
    -- every share would come out as 0 or 1.
    round(cast(bot_events as double) / nullif(total_events, 0), 4) as bot_share,
    round(cast(article_events as double) / nullif(total_events, 0), 4) as article_share
from rolled_up
