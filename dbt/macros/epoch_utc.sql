{#
    The lower bound an incremental model falls back to on its very first run.

    Every incremental mart here filters on `>= (select max(bucket) from {{ this }})`,
    and on the first run that table is empty, so the aggregate is null and the
    predicate would be null — which is not true, so the model would select nothing
    and build empty. `coalesce(..., epoch_utc())` is what makes the first run a full
    backfill instead.

    It is a macro rather than a literal repeated in four models because the type has
    to carry a zone. `silver.edits.event_time` is `timestamp(6) with time zone` in
    Trino, and a bare `timestamp '1970-01-01 00:00:00'` is `timestamp(6)` without
    one; comparing the two is a type error rather than a silently wrong answer. The
    precision does not have to match — Trino coerces `timestamp(0) with time zone`
    to `timestamp(6) with time zone` — but the zone does.

    `UTC` is uppercase because Trino resolves the zone name case-sensitively, the
    way `java.time.ZoneId` does. Lowercase `utc` parses as far as the analyser and
    then fails with INVALID_LITERAL, and only on a run where a model is actually
    incremental, which is the run after the one that created the table.
#}

{% macro epoch_utc() %}timestamp '1970-01-01 00:00:00 UTC'{% endmacro %}
