"""`python -m wikistream.producer.healthcheck` — the container's HEALTHCHECK command.

Exits 0 if the producer's heartbeat is fresh, 1 if it is stale, and 1 if it is
missing. Missing is treated as unhealthy rather than as "not started yet" because
Docker's `--start-period` already covers start-up: within that window a failing
check does not count against the retry budget, and after it a process that has
never written a heartbeat is genuinely broken.

Writes its reason to stdout, which Docker records in
`docker inspect --format '{{json .State.Health}}'`, so a container that went
unhealthy at three in the morning still says why.
"""

from __future__ import annotations

import sys

from wikistream.config import get_settings
from wikistream.producer.heartbeat import age_seconds


def main() -> int:
    """Return a process exit code: 0 healthy, 1 unhealthy."""
    settings = get_settings()
    limit = settings.producer_heartbeat_timeout_seconds
    age = age_seconds(settings.producer_heartbeat_path)

    if age is None:
        print(f"unhealthy: no heartbeat at {settings.producer_heartbeat_path}")
        return 1
    if age > limit:
        print(f"unhealthy: heartbeat is {age:.0f}s old, limit is {limit:.0f}s")
        return 1
    print(f"healthy: heartbeat {age:.0f}s old")
    return 0


if __name__ == "__main__":
    sys.exit(main())
