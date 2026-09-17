"""One sensor: log every run failure with enough context to act on it.

This is the smallest useful thing a failure sensor can be, and it is deliberately
not more. A sensor that posted to Slack or paged someone would be a feature this
repository cannot demonstrate — there is no Slack workspace inside a `docker
compose up`, and a webhook URL is a secret this repository will not carry. So it
logs, and the docstring below says what a real deployment would put in its place.

What it adds over reading the Dagster UI is that failures land in the daemon's log,
which is where anyone tailing `docker compose logs` is already looking, and they
land with the failed *step* attached — the difference between "the marts job
failed" and "`mart_top_pages_hourly` failed".
"""

#  No `from __future__ import annotations` here — see the comment at the top of
#  lakehouse.py.

from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor


@run_failure_sensor(
    name="log_run_failures",
    default_status=DefaultSensorStatus.RUNNING,
    description="Write every failed run to the daemon log with its job, cause and failed step.",
)
def log_run_failures(context: RunFailureSensorContext) -> None:
    """Report a failed run once, with the failure's own message rather than a link.

    `default_status=RUNNING`, unlike the schedules in `schedules.py`, and the
    asymmetry is the point: a schedule that starts itself does work nobody asked
    for, while a sensor that starts itself only watches. Something has to be paying
    attention on a fresh clone, or the first failure a new reader causes is
    invisible to them.

    In a deployment this is where an alert would go — `context.dagster_run` and
    `context.failure_event` carry everything a Slack or PagerDuty payload needs.
    """
    run = context.dagster_run
    context.log.error(
        "run failed: job=%s run_id=%s reason=%s",
        run.job_name,
        run.run_id,
        context.failure_event.message or "no message on the failure event",
    )

    step_key = context.failure_event.step_key
    if step_key:
        context.log.error("failed step: %s", step_key)

    # The job tags from jobs.py — layer and cost — so a reader tailing the log can
    # tell a failed two-minute observation from a failed compaction.
    for tag in ("layer", "cost"):
        if tag in run.tags:
            context.log.error("  %s=%s", tag, run.tags[tag])


SENSORS = [log_run_failures]
