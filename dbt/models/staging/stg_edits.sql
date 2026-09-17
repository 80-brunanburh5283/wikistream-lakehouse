/*
    The one place the marts read silver from, and the only place that knows how
    an hour or a minute is defined.

    Three jobs, and nothing else:

    1. It picks the columns the analytics layer is allowed to depend on. Adding a
       column to `silver.edits` does not silently become part of the gold
       contract; it becomes part of it here, on purpose.
    2. It derives `event_minute` and `event_hour` once. Four marts truncate
       timestamps and if each did its own `date_trunc` they would eventually
       disagree — someone would truncate `ingested_at` instead of `event_time`,
       and the difference between event time and ingest time is exactly what this
       pipeline exists to be careful about.
    3. It names the canary events. Wikimedia injects synthetic heartbeat events
       into the firehose with `meta.domain = 'canary'` and no wiki, no page and
       no editor, so that a consumer can tell "nothing happened" from "the
       connection died". They pass every validation rule, because they are valid
       events — they are just not edits. Every mart that counts editing activity
       filters them out; `mart_pipeline_health` counts them on purpose.
*/

select
    event_id,
    event_time,
    date_trunc('minute', event_time) as event_minute,
    date_trunc('hour', event_time) as event_hour,
    event_date,
    domain = 'canary' as is_canary,
    wiki,
    domain,
    change_type,
    namespace_id,
    is_article,
    page_title,
    page_url,
    editor,
    is_bot,
    is_minor,
    is_anonymous,
    bytes_old,
    bytes_new,
    bytes_delta,
    late_by_seconds,
    ingested_at
from {{ source('silver', 'edits') }}
