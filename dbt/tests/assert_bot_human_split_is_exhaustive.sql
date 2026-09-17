/*
    `bot_events + human_events = events`, per bucket, with nothing left over.

    The mart derives the two columns from `count_if(is_bot)` and
    `count_if(not is_bot)`. Those two are exhaustive only while `is_bot` is never
    null: `count_if` counts true and ignores both false and null, so a null
    `is_bot` would be counted in neither column and the two would quietly stop
    summing to the total. That is not a hypothetical — `is_bot` *is* null on canary
    events, and the mart is correct only because it filters them out.

    So this test guards the filter, not the arithmetic. If someone removes the
    `where not is_canary` from `mart_bot_vs_human_hourly`, this is what fails, and
    it fails with the bucket and the shortfall rather than with a silently wrong
    bot share.
*/

select
    event_hour,
    wiki,
    events,
    bot_events,
    human_events,
    events - (bot_events + human_events) as unaccounted_events
from {{ ref('mart_bot_vs_human_hourly') }}
where bot_events + human_events != events
