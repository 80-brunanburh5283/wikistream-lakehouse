{#
    The upper bound every gold model reads silver to, and the reason all five of
    them see the same data.

    `silver.edits` is written by a Spark stream that commits a new Iceberg snapshot
    every 30 seconds. A `dbt build` of this project takes 72 seconds — longer than
    the trigger — and dbt runs models on four threads, so two models that both read
    `stg_edits` otherwise read two *different* snapshots. Any test that compares
    them then fails for a reason that has nothing to do with either model.

    That is not hypothetical. On 2026-09-18 the relationships test from
    `mart_top_pages_hourly.wiki` to `dim_wikis.wiki` failed with three rows, because
    `bewiktionary` emitted its first ever event at 01:48:15, ingested at 01:48:30 —
    after `dim_wikis` had been built and before `mart_top_pages_hourly` was. Three
    top-ten rows pointing at a wiki the dimension did not have yet. The mart was
    right, the dimension was right, and the reference between them was broken by the
    clock.

    `run_started_at` is one timestamp per dbt invocation, identical in every model
    and every test of that run. So `ingested_at < run_started_at` means "the rows
    that were visible when this run began", which is snapshot isolation expressed as
    a predicate — what a lakehouse offers in place of a transaction that spans five
    tables.

    Two details that are easy to get wrong:

    - **The cutoff is on ingest time, not event time.** A late event carries an old
      `event_time` and a new `ingested_at`, so bounding on event time would still let
      a row that arrived mid-run into a model built after a model that had already
      read past it. Bounding on ingest time is what makes the set closed.
    - **Nothing is dropped.** Every incremental mart recomputes its boundary bucket
      with `>=` rather than `>`, so rows excluded by one run's cutoff are picked up by
      the next one. The cost of the boundary is latency of at most one run, not data.

    Iceberg time travel (`FOR TIMESTAMP AS OF`) would say this more directly and was
    rejected: the staging models are views, so pinning them would freeze what an
    ad-hoc `select * from gold.stg_edits` returns until the next dbt run. A predicate
    keeps the views live and the run consistent. See DECISIONS.md ADR-0046.

    Rendered rather than passed through as a Jinja object because Trino needs a zone
    on the literal to compare it with `timestamp(6) with time zone`, for the same
    reason `epoch_utc()` exists.
#}

{% macro run_cutoff() %}timestamp '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }} UTC'{% endmacro %}
