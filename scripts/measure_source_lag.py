#!/usr/bin/env python
"""Measure how stale events are when they arrive, to justify the watermark.

The watermark on the silver stream decides how long to wait for late events
before finalising a window and dropping stragglers. Picking it by taste means
either losing real data or holding state forever. This script measures the
distribution the choice should be based on:

    lag = (time this process received the event) - (event's own meta.dt)

That lag is the sum of MediaWiki's internal propagation, Wikimedia's Kafka hop,
the EventStreams HTTP hop and the network path to here. Anything the pipeline
cannot control is already inside it, which is what makes it the right input.

    uv run python scripts/measure_source_lag.py --seconds 120

Writes a markdown table to stdout. The figures in `docs/latency.md`, and the
`watermark_minutes` default, come from a run of this script.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from wikistream.config import get_settings
from wikistream.logging import configure_logging
from wikistream.sources.wikimedia import WikimediaSource

#: Lag buckets, in seconds. Deliberately fine-grained below 10s because that is
#: where almost everything lands, and open-ended at the top because the tail is
#: the only part that matters for the watermark.
_BUCKETS: tuple[tuple[str, float], ...] = (
    ("< 1s", 1.0),
    ("1-2s", 2.0),
    ("2-5s", 5.0),
    ("5-10s", 10.0),
    ("10-30s", 30.0),
    ("30-60s", 60.0),
    ("1-5m", 300.0),
    ("5-10m", 600.0),
    (">= 10m", float("inf")),
)


def _parse_event_time(event: dict[str, Any]) -> datetime | None:
    """Read `meta.dt` as an aware UTC datetime, or None if unusable."""
    raw = event.get("meta", {}).get("dt")
    if not isinstance(raw, str):
        return None
    try:
        # `fromisoformat` handles the trailing Z from Python 3.11 onwards.
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def measure(seconds: float, max_events: int) -> list[float]:
    """Collect lag samples for a fixed wall-clock window."""
    settings = get_settings()
    source = WikimediaSource(settings)
    lags: list[float] = []
    deadline = time.monotonic() + seconds

    for event in source.iter_events():
        received = datetime.now(timezone.utc)
        event_time = _parse_event_time(event)
        if event_time is not None:
            lags.append((received - event_time).total_seconds())
        if time.monotonic() >= deadline or len(lags) >= max_events:
            break
    source.stop()
    return lags


def report(lags: list[float], seconds: float) -> None:
    """Print percentiles and a bucket histogram as markdown."""
    if not lags:
        print("no events with a parseable meta.dt were received")
        return

    ordered = sorted(lags)

    def pct(p: float) -> float:
        # Nearest-rank rather than interpolated: with a few thousand samples the
        # difference is noise, and nearest-rank is a value that actually occurred.
        index = min(len(ordered) - 1, round(p / 100 * len(ordered)))
        return ordered[index]

    print(f"\nsamples: {len(lags)} over {seconds:.0f}s ({len(lags) / seconds:.1f} events/s)\n")
    print("| statistic | lag |")
    print("|---|---|")
    print(f"| min | {ordered[0]:.2f}s |")
    print(f"| median | {statistics.median(ordered):.2f}s |")
    print(f"| mean | {statistics.fmean(ordered):.2f}s |")
    print(f"| p90 | {pct(90):.2f}s |")
    print(f"| p99 | {pct(99):.2f}s |")
    print(f"| p99.9 | {pct(99.9):.2f}s |")
    print(f"| max | {ordered[-1]:.2f}s |")

    counts: Counter[str] = Counter()
    for lag in lags:
        for label, upper in _BUCKETS:
            if lag < upper:
                counts[label] += 1
                break

    print("\n| lag bucket | events | share |")
    print("|---|---|---|")
    for label, _ in _BUCKETS:
        n = counts[label]
        if n:
            print(f"| {label} | {n} | {100 * n / len(lags):.2f}% |")

    negative = sum(1 for lag in lags if lag < 0)
    if negative:
        # A negative lag means the event's own timestamp is in the future
        # relative to this machine's clock — so the local clock is *behind* the
        # source's. Printed rather than clamped to zero because the size of this
        # number is the accuracy limit on every other number in the table, and
        # because a watermark tuned on skewed measurements is tuned on nothing.
        print(
            f"\nnegative lags (local clock behind source): {negative} "
            f"({100 * negative / len(lags):.2f}%), most negative {ordered[0]:.2f}s"
        )
        print(
            "  -> treat the absolute values above as accurate to about "
            f"{abs(statistics.median(ordered)):.1f}s, and the shape as reliable."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=120.0)
    parser.add_argument("--max-events", type=int, default=100_000)
    args = parser.parse_args(argv)

    configure_logging("WARNING", as_json=False)
    lags = measure(args.seconds, args.max_events)
    report(lags, args.seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
