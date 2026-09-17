/*
    Within every (event_hour, wiki), the ranks are 1..n with no gaps and no
    repeats, and n is the number of rows present.

    This is the test that catches the failure mode `mart_top_pages_hourly` was
    designed around. If its incremental strategy is ever changed from
    `delete+insert` to `merge`, pages evicted from the top ten stay behind at their
    old rank: the group ends up with eleven rows, two of them ranked 8, and `max
    rank != row count`. Nothing else in the project would notice — the row counts
    still look plausible and the top of the list is still right.

    Three conditions, because they fail differently:
      min_rank != 1              the window function did not start at 1
      distinct_ranks != rows     two rows share a rank (the merge-drift case)
      max_rank != rows           a rank is missing (a filter applied after ranking)
*/

with per_group as (
    select
        event_hour,
        wiki,
        count(*) as rows_in_group,
        min(rank_in_wiki) as min_rank,
        max(rank_in_wiki) as max_rank,
        count(distinct rank_in_wiki) as distinct_ranks
    from {{ ref('mart_top_pages_hourly') }}
    group by event_hour, wiki
)

select
    event_hour,
    wiki,
    rows_in_group,
    min_rank,
    max_rank,
    distinct_ranks
from per_group
where
    min_rank != 1
    or max_rank != rows_in_group
    or distinct_ranks != rows_in_group
