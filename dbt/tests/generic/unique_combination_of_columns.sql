{#
    A composite-key uniqueness test, written out rather than imported.

    dbt core ships `unique`, which takes one column. Every mart in this project has
    a composite grain — (event_minute, wiki, change_type), (event_hour, wiki),
    (event_hour, wiki, rank_in_wiki) — and a grain that is not asserted is a grain
    that drifts: an incremental model whose `unique_key` stops matching its `group
    by` duplicates silently and looks fine until someone sums a column.

    The usual answer is `dbt_utils.unique_combination_of_columns`. This is that
    test, in ten lines, and the reason it is here instead of in a `packages.yml` is
    DECISIONS.md ADR-0026: adding the package would make `dbt run` on a fresh clone
    depend on reaching hub.getdbt.com, and the one network dependency this stack is
    allowed is the Wikimedia stream.

    A grouping key containing a null would not be caught by this test — Trino's
    `group by` treats nulls as equal, so duplicate null keys *do* group together and
    are counted. That is the behaviour wanted here; the marts exclude the only rows
    that carry null keys anyway.
#}

{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set column_list = combination_of_columns | join(', ') -%}

    with validation as (
        select
            {{ column_list }},
            count(*) as occurrences
        from {{ model }}
        group by {{ column_list }}
    )

    select *
    from validation
    where occurrences > 1

{% endtest %}
