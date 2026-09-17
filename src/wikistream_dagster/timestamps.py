"""Turning Trino's timestamps into Dagster metadata, in one place.

Small module for two functions because the trap they avoid is not obvious and
both the observations and the asset checks would otherwise each carry their own
copy of the fix.

Iceberg's `timestamp` type carries no zone and every timestamp this project
writes is UTC, so the Trino client hands back a *naive* `datetime`. Python's
`datetime.timestamp()` on a naive value applies the local zone — UTC inside the
containers, and something else on a developer's laptop. The failure mode is not
an exception: it is every freshness number in the UI shifted by a whole number of
hours, in a direction that depends on who is running it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dagster import MetadataValue


def epoch(value: Any) -> float | None:
    """Seconds since the epoch, treating a naive timestamp as UTC.

    Returns `None` rather than raising for anything that is not a `datetime`,
    because the callers are all reading `max(...)` over a table that can legally
    be empty, and an empty table is a state to report rather than an error.
    """
    if not isinstance(value, datetime):
        return None
    # Bound to its own name rather than reassigning the `Any` parameter, so the
    # narrowing survives the assignment and mypy still checks the arithmetic.
    moment: datetime = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def timestamp_metadata(value: Any) -> MetadataValue:
    """Render a timestamp for the Dagster UI, or the text `none` if there isn't one."""
    seconds = epoch(value)
    return MetadataValue.timestamp(seconds) if seconds is not None else MetadataValue.text("none")
