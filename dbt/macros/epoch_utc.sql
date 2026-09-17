{#
    The lower bound an incremental model falls back to on its very first run.

    Every incremental mart here filters on `>= (select max(bucket) from {{ this }})`,
    and on the first run that table is empty, so the aggregate is null and the
    predicate would be null — which is not true, so the model would select nothing
    and build empty. `coalesce(..., epoch_utc())` is what makes the first run a full
    backfill instead.

    It is a macro rather than a literal repeated in four models because the type has
    to match the column exactly. `silver.edits.event_time` is
    `timestamp(6) with time zone` in Trino, so the bound needs a zone on it; a bare
    `timestamp '1970-01-01 00:00:00'` is `timestamp(6)` without one, and comparing
    the two is a type error rather than a silently wrong answer. Getting that wrong
    once and fixing it in one place is the reason this file exists.
#}

{% macro epoch_utc() %}timestamp '1970-01-01 00:00:00 utc'{% endmacro %}
