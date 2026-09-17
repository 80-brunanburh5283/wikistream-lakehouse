#!/usr/bin/env python
"""Check that the public event stream is reachable and still the shape we expect.

The one command in this repository that touches the internet. It needs no Docker,
no Kafka and no JVM, which is the point: when the pipeline is not producing data,
this answers "is it me or is it upstream?" in fifteen seconds, before anyone starts
reading Spark logs.

    uv run python scripts/smoke_live.py            # 20 frames or 30 seconds
    uv run python scripts/smoke_live.py --frames 200 --seconds 60

Five checks, and each one is a thing that has a different fix:

    reachable        the connection opened and frames arrived
    decodable        the frames are JSON objects
    keyable          every frame carries the field used as the Kafka key
    complete         every frame carries the fields the pipeline requires
    contract         the frames contain no field the declared schema omits

The last one is a drift alarm rather than a liveness check, and it exits non-zero
on purpose. A new upstream field is not an outage, but it does mean the bronze
table would silently stop containing everything the stream sent, and the whole
argument for a declared schema (see wikistream/streaming/schema.py) is that this
gets noticed rather than absorbed.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from wikistream.config import get_settings
from wikistream.events import REQUIRED_FIELDS, event_time, missing_required_fields, partition_key
from wikistream.logging import configure_logging
from wikistream.sources.base import EventSource
from wikistream.sources.jetstream import JetstreamSource
from wikistream.sources.wikimedia import WikimediaSource

#: How many frames to look at before deciding. Twenty is enough to see every
#: common event type at the measured distribution (52% categorize, 42% edit) and
#: small enough that the whole check is over in under a second of stream time.
_DEFAULT_FRAMES = 20


@dataclass
class Report:
    """Accumulates check results and prints them as they are decided.

    Same three verbs as `scripts/bootstrap.sh`, so the two preflight commands read
    the same way.
    """

    failures: int = 0
    warnings: int = 0

    def ok(self, label: str, detail: str = "") -> None:
        """Record a check that passed."""
        print(f"  ok    {label}{f': {detail}' if detail else ''}")

    def warn(self, label: str, detail: str) -> None:
        """Record something worth knowing that does not make the run a failure."""
        self.warnings += 1
        print(f"  warn  {label}: {detail}")

    def bad(self, label: str, detail: str) -> None:
        """Record a check that failed. The script exits non-zero if any did."""
        self.failures += 1
        print(f"  FAIL  {label}: {detail}")


def build_source(name: str) -> EventSource:
    settings = get_settings()
    if name == "jetstream":
        return JetstreamSource(settings)
    return WikimediaSource(settings)


def collect(
    source: EventSource, frames: int, seconds: float
) -> tuple[list[str], list[dict[str, Any]]]:
    """Read up to `frames` frames, or until `seconds` elapse.

    Returns the raw frame bodies and the ones that decoded, separately, because a
    frame that did not decode is the most interesting kind and dropping it here
    would hide it.
    """
    raws: list[str] = []
    decoded: list[dict[str, Any]] = []
    deadline = time.monotonic() + seconds

    for event in source.iter_raw():
        raws.append(event.raw)
        if event.payload is not None:
            decoded.append(event.payload)
        if len(raws) >= frames or time.monotonic() >= deadline:
            break
    source.stop()
    return raws, decoded


def check_contract(decoded: list[dict[str, Any]], report: Report) -> None:
    """Compare the live keys against the declared schema, both directions."""
    # Imported here rather than at module scope: it pulls in pyspark.sql.types,
    # which costs about a second, and the four checks above are worth running even
    # on a machine where that import fails.
    from wikistream.streaming.schema import TOP_LEVEL_FIELDS

    observed = {key for event in decoded for key in event}
    undeclared = sorted(observed - TOP_LEVEL_FIELDS)
    if undeclared:
        report.bad(
            "contract",
            f"the stream sent fields the schema does not declare: {undeclared}. "
            "Add them to RECENTCHANGE_SCHEMA in wikistream/streaming/schema.py",
        )
    else:
        report.ok("contract", f"{len(observed)} live fields, all declared")

    # The other direction is informational: log_* fields appear in under 3% of
    # traffic, so a short sample legitimately misses them.
    unseen = sorted(TOP_LEVEL_FIELDS - observed)
    if unseen:
        print(f"        declared but not seen in this sample: {unseen}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the live event stream.")
    parser.add_argument("--frames", type=int, default=_DEFAULT_FRAMES)
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--source", choices=("wikimedia", "jetstream"), default=None)
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging("WARNING", as_json=False)
    source = build_source(args.source or settings.source_name)
    report = Report()

    print(f"\nsmoke test: {source.name}, up to {args.frames} frames or {args.seconds:.0f}s\n")

    started = time.monotonic()
    try:
        raws, decoded = collect(source, args.frames, args.seconds)
    except Exception as exc:
        report.bad("reachable", f"{type(exc).__name__}: {exc}")
        print("\n1 check failed. The stream could not be read at all.\n")
        return 1
    elapsed = time.monotonic() - started

    if not raws:
        report.bad("reachable", f"no frames in {elapsed:.1f}s")
        print("\n1 check failed.\n")
        return 1
    report.ok("reachable", f"{len(raws)} frames in {elapsed:.1f}s ({len(raws) / elapsed:.1f}/s)")

    malformed = len(raws) - len(decoded)
    if malformed:
        # Not fatal: one bad frame in a firehose is expected occasionally, and the
        # pipeline is built to quarantine rather than reject. Worth surfacing.
        report.warn("decodable", f"{malformed} of {len(raws)} frames were not JSON objects")
    else:
        report.ok("decodable", f"all {len(decoded)} frames are JSON objects")

    unkeyable = [e for e in decoded if partition_key(e) is None]
    if unkeyable:
        report.bad("keyable", f"{len(unkeyable)} frames have no meta.domain to key on")
    else:
        report.ok("keyable", f"{len({partition_key(e) for e in decoded})} distinct wikis")

    incomplete = Counter(field for event in decoded for field in missing_required_fields(event))
    if incomplete:
        report.bad("complete", f"missing required fields: {dict(incomplete)}")
    else:
        report.ok("complete", f"all of {', '.join(REQUIRED_FIELDS)} present")

    check_contract(decoded, report)

    lags = [
        (datetime.now(UTC) - t).total_seconds() for t in map(event_time, decoded) if t is not None
    ]
    if lags:
        # Informational, not a check. One sample of twenty says nothing about the
        # tail, and the watermark is set from scripts/measure_source_lag.py, which
        # takes thousands. Printed because an obviously wrong number here — hours,
        # or negative minutes — means a clock problem worth knowing about early.
        print(f"\n  observed lag: median {statistics.median(lags):.1f}s over {len(lags)} frames")
        print("  (indicative only; the watermark comes from scripts/measure_source_lag.py)")

    print()
    if report.failures:
        print(f"{report.failures} check(s) failed.\n")
        return 1
    if report.warnings:
        print(f"all checks passed with {report.warnings} warning(s).\n")
    else:
        print("all checks passed.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
