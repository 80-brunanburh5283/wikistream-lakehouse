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

{#-
    This test reads two models, so Dagster cannot infer which asset it is an
    assertion about, and without this hint it silently becomes a test that runs
    under `dbt build` but is attached to nothing in the UI. `meta.dagster.ref`
    names the model the assertion is *about* — `stg_edits` is only the independent
    yardstick it is measured against. See DECISIONS.md ADR-0032.
-#}
{{ config(meta={'dagster': {'ref': {'name': 'dim_wikis'}}}) }}

{#-
    Both yardsticks are bounded by the dimension's own `built_through`, not by this
    invocation's `run_cutoff()`. `dbt build` would make the two identical, but
    `dbt test` on its own is a separate invocation with a later cutoff, and silver
    grows between them: a wiki that picks up a second domain after the dimension was
    built would fail the `domain_count` comparison while both the model and the
    stream were behaving correctly. Reading the bound off the row under test is what
    makes this assertion independent of when it runs.
-#}
{% set dim_cutoff %}(select max(d.built_through) from {{ ref('dim_wikis') }} as d){% endset %}

with observed_pairs as (
    select distinct
        wiki,
        domain
    from {{ ref('stg_edits') }}
    where
        not is_canary
        and ingested_at < {{ dim_cutoff }}
),

observed_counts as (
    select
        wiki,
        count(distinct domain) as observed_domain_count
    from {{ ref('stg_edits') }}
    where
        not is_canary
        and ingested_at < {{ dim_cutoff }}
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
