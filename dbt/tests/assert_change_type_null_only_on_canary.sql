/*
    `change_type` is null on exactly one kind of row: Wikimedia's synthetic canary
    heartbeats, which carry a domain and a timestamp and nothing else.

    This test exists because the `accepted_values` test on the same column cannot
    say it. `accepted_values` renders as `where value not in ('edit', 'new', ...)`,
    and `null not in (...)` evaluates to null rather than to true, so a null slips
    through unremarked. Adding `not_null` instead would fail on every canary event.
    The assertion actually worth making is the conditional one, and a singular test
    is the only way to make it.

    If this fails, the source has started emitting a change type this project has
    never seen, or the silver transform has stopped populating the column — either
    way the marts that split on `change_type` are now dropping rows.
*/

select
    event_id,
    event_time,
    domain,
    change_type
from {{ ref('stg_edits') }}
where
    change_type is null
    and not is_canary
