/*
    The dead-letter table, without the payload.

    `raw_payload` is deliberately not exposed. It is the reason the quarantine is
    worth having — a rejected event can be fixed and replayed rather than merely
    counted — but replaying it is Spark's job, and a mart that could select the
    raw frame would eventually have one that does, at which point gold contains
    unvalidated free text.

    Note which clock this table is on. `failed_at` is ingest time: the moment the
    batch that rejected the event ran. There is no reliable event time here,
    because "the timestamp could not be parsed" is one of the reasons an event
    lands in this table. So `failed_hour` and `stg_edits.event_hour` are not the
    same measurement, and joining them is an approximation — see
    `mart_pipeline_health`, which does it anyway and says so.
*/

select
    event_id,
    failure_reason,
    kafka_partition,
    kafka_offset,
    failed_at,
    date_trunc('hour', failed_at) as failed_hour,
    ingest_date
from {{ source('silver', 'quarantine') }}
