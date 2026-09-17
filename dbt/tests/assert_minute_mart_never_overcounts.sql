/*
    No bucket in `mart_edits_per_minute` claims more events than silver actually
    holds for it.

    The assertion is deliberately one-sided, and the reason is worth stating
    because a reviewer will otherwise read it as a weak test.

    The equality — mart count = silver count, per bucket — is not an invariant of a
    pipeline reading a live stream. Silver keeps growing while the mart does not:
    between the `dbt run` that built the mart and the `dbt test` that checks it,
    more events arrive, and a late event can land in a minute the mart has already
    written. So `mart_events < silver_events` is the normal, correct state of a
    bucket, and a test asserting equality would fail on a healthy pipeline several
    times an hour.

    `mart_events > silver_events` is a different thing entirely. Silver is
    deduplicated by MERGE and only ever gains rows, so there is no legitimate way
    for an aggregate over it to exceed it. The only ways to reach this state are a
    broken incremental key — the merge inserting a second row for a bucket instead
    of updating the first — or a double-counting join introduced into the mart.
    Both are exactly the bugs that an incremental model with a composite key is
    prone to, and both are invisible in the row counts alone.

    Paired with the `unique_combination_of_columns` test on the same grain: that one
    catches duplicate buckets, this one catches inflated counts within a bucket.
*/

with silver_by_minute as (
    select
        event_minute,
        wiki,
        change_type,
        count(*) as silver_events
    from {{ ref('stg_edits') }}
    where not is_canary
    group by event_minute, wiki, change_type
)

select
    mart.event_minute,
    mart.wiki,
    mart.change_type,
    mart.events as mart_events,
    coalesce(silver.silver_events, 0) as silver_events
from {{ ref('mart_edits_per_minute') }} as mart
left join silver_by_minute as silver
    on
        mart.event_minute = silver.event_minute
        and mart.wiki = silver.wiki
        and mart.change_type = silver.change_type
where mart.events > coalesce(silver.silver_events, 0)
