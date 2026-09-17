"""A file whose modification time answers one question: is the loop still turning?

Docker's `HEALTHCHECK` wants a command that exits 0 when the container is healthy.
The producer has no port to probe, so there is nothing to curl. The options are a
process check — which passes for a deadlocked process, and so is worthless — or
some artefact the loop has to keep refreshing. This is the second one.

What it detects and what it does not, stated plainly because a healthcheck that
is trusted for more than it measures is worse than none:

* **Detected:** the main loop stopped iterating. A deadlock, a wedged socket read
  the timeout somehow did not catch, an exception path that left the process alive
  but idle.
* **Not detected:** the upstream went quiet while the loop kept reconnecting. That
  is by design — the source's reconnect path *is* the loop working — and it shows
  up instead as `reconnects` climbing with `produced` flat in the throughput log,
  and as Kafka end offsets not advancing (`make kafka-offsets`).

The heartbeat is refreshed once per stats interval rather than once per event: at
~40 events/s a per-event write would be 40 syscalls a second to say something that
changes meaningfully once every ten.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

#: Written into the file alongside the counters. Not read by the healthcheck,
#: which uses the file's mtime — the content is for a human running `docker exec
#: cat` on a container that is failing its healthcheck and wants to know why.
_SCHEMA = 1


def write(path: str | Path, payload: dict[str, Any] | None = None) -> None:
    """Refresh the heartbeat, atomically.

    Written to a sibling temp file and renamed, because a healthcheck reading a
    half-written file would see truncated JSON and report a fault that does not
    exist. `os.replace` is atomic within a filesystem.

    Args:
        path: Heartbeat file location. Parent directories are created.
        payload: Counters to record for a human debugging the container.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        {"schema": _SCHEMA, "unix_time": time.time(), "pid": os.getpid(), **(payload or {})},
        default=str,
    )
    temp = target.with_name(f"{target.name}.tmp")
    temp.write_text(body, encoding="utf-8")
    os.replace(temp, target)  # noqa: PTH105 — os.replace is the atomic primitive


def age_seconds(path: str | Path) -> float | None:
    """Seconds since the heartbeat was last refreshed, or None if it is absent.

    None and "very old" are different answers and the caller has to tell them
    apart: absent means the process has not started yet, which during
    `start_period` is fine, while old means it started and stopped.
    """
    target = Path(path)
    try:
        return max(0.0, time.time() - target.stat().st_mtime)
    except FileNotFoundError:
        return None
