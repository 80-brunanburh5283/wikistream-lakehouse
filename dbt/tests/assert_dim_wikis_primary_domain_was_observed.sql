/*
    `dim_wikis.primary_domain` is a domain that wiki was actually seen under, and
    `domain_count` matches how many distinct domains that was.

    This guards a specific, easy mistake. The model picks the plurality domain with
    `max_by(domain, events)`, and `max_by` takes the value first and the ordering
    key second — so `max_by(events, domain)` compiles, runs, and returns an event
    count where a hostname belongs. Trino would not complain until something tried
    to use the column, and by then the dimension has been joined to.

    The second condition is a weaker but independent check on the same model: the
    number of domains is recounted straight off the staging view with `count(distinct
    domain)`, rather than re-deriving it from the model's own (wiki, domain) CTE, so
    a bug in that CTE shows up as a disagreement instead of cancelling out.
*/

with observed_pairs as (
    select distinct
        wiki,
        domain
    from {{ ref('stg_edits') }}
    where not is_canary
),

observed_counts as (
    select
        wiki,
        count(distinct domain) as observed_domain_count
    from {{ ref('stg_edits') }}
    where not is_canary
    group by wiki
)

select
    dim.wiki,
    dim.primary_domain,
    dim.domain_count,
    counts.observed_domain_count
from {{ ref('dim_wikis') }} as dim
left join observed_pairs as pairs
    on
        dim.wiki = pairs.wiki
        and dim.primary_domain = pairs.domain
left join observed_counts as counts on dim.wiki = counts.wiki
where
    pairs.domain is null
    or dim.domain_count != counts.observed_domain_count
