"""When each job runs, and one honest note about what a schedule means here.

The cadences are chosen from what the data underneath actually does:

- **Observation, every 2 minutes.** The silver stream commits a micro-batch every
  30 seconds, so anything slower than a couple of minutes makes the UI's row
  counts look stale for a reason that has nothing to do with the pipeline. Four
  metadata queries is cheap enough to do at that rate.
- **Marts, every 15 minutes.** `mart_edits_per_minute` buckets by minute, so
  building more often than that produces buckets that are still filling and will
  be rewritten on the next run. Fifteen minutes is the point where an incremental
  run is mostly doing new work rather than re-merging the same tail. It also
  matches the freshness threshold in `checks.py`, which means a stale-source
  warning and a late mart show up together rather than an hour apart.
- **Maintenance, daily at 03:00 UTC.** Compaction rewrites files, so it wants a
  quiet moment; 03:00 UTC is the low point in Wikimedia's daily edit curve, which
  is also when this pipeline's throughput is lowest.

All three are `default_status=STOPPED`. That is deliberate and it is the kind of
default that is easy to get wrong in the other direction: a fresh clone that
starts three schedules the moment the daemon comes up will start rewriting tables
while the person who cloned it is still reading the README. `make dagster-start`
turns them on, and the README says so.

Times are UTC because every timestamp in the warehouse is UTC. A schedule in local
time would mean the daily maintenance window moved twice a year.
"""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from wikistream_dagster.jobs import build_marts, maintain_gold, observe_lakehouse

observe_schedule = ScheduleDefinition(
    name="observe_lakehouse_every_2_minutes",
    job=observe_lakehouse,
    cron_schedule="*/2 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Keep the streaming half's row counts and checks current in the UI.",
)

build_marts_schedule = ScheduleDefinition(
    name="build_marts_every_15_minutes",
    job=build_marts,
    cron_schedule="*/15 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Incrementally rebuild the gold layer and run its tests.",
)

maintain_gold_schedule = ScheduleDefinition(
    name="maintain_gold_daily",
    job=maintain_gold,
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Compact the gold tables during the quietest hour of the Wikimedia day.",
)

SCHEDULES = [observe_schedule, build_marts_schedule, maintain_gold_schedule]
