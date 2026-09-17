#!/usr/bin/env python
"""Capture N live events into the test fixture, and describe what came back.

Run this to regenerate `tests/fixtures/recentchange_sample.jsonl`. The whole test
suite depends on captured bytes rather than a hand-written schema, because the
fields the stream actually sends and the fields its documentation lists are not
the same set — the capture is the source of truth.

The profile it prints is not decoration. It is where the watermark, the bot
filtering decision and the nullability of every silver column come from.

    uv run python scripts/capture_fixture.py --count 500
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from wikistream.config import get_settings
from wikistream.logging import configure_logging
from wikistream.sources.wikimedia import WikimediaSource

DEFAULT_OUTPUT = Path("tests/fixtures/recentchange_sample.jsonl")


def capture(count: int, output: Path) -> list[dict[str, Any]]:
    """Stream until `count` events are collected, writing one JSON object per line."""
    settings = get_settings()
    source = WikimediaSource(settings)
    events: list[dict[str, Any]] = []

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for event in source.iter_events():
            # separators without spaces and ensure_ascii=False keep the file
            # compact and keep non-Latin titles readable in a diff.
            handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            events.append(event)
            if len(events) >= count:
                break
    source.stop()
    return events


def _null_rates(events: list[dict[str, Any]], paths: list[str]) -> list[tuple[str, float]]:
    """Fraction of events where a dotted path is absent or null."""
    rates = []
    for path in paths:
        missing = 0
        for event in events:
            node: Any = event
            for part in path.split("."):
                node = node.get(part) if isinstance(node, dict) else None
            if node is None:
                missing += 1
        rates.append((path, missing / len(events)))
    return rates


def profile(events: list[dict[str, Any]]) -> None:
    """Print the distribution facts that downstream design decisions rest on."""
    total = len(events)
    print(f"\ncaptured {total} events\n")

    print("$schema values:")
    for schema, n in Counter(e.get("$schema") for e in events).most_common():
        print(f"  {n:5d}  {schema}")

    print("\ntype distribution:")
    for change_type, n in Counter(e.get("type") for e in events).most_common():
        print(f"  {n:5d}  {100 * n / total:5.1f}%  {change_type}")

    bots = sum(1 for e in events if e.get("bot"))
    print(f"\nbot share: {bots}/{total} = {100 * bots / total:.1f}%")

    ids = [e.get("meta", {}).get("id") for e in events]
    distinct = len(set(ids))
    print(f"meta.id: {distinct} distinct out of {total} -> {total - distinct} repeated in sample")

    print("\ntop 8 domains:")
    for domain, n in Counter(e.get("meta", {}).get("domain") for e in events).most_common(8):
        print(f"  {n:5d}  {domain}")

    print("\nnull / absent rate by field:")
    watched = [
        "meta.id",
        "meta.dt",
        "meta.domain",
        "id",
        "title",
        "comment",
        "user",
        "bot",
        "minor",
        "patrolled",
        "length.old",
        "length.new",
        "revision.old",
        "revision.new",
        "server_name",
        "wiki",
        "namespace",
    ]
    for path, rate in _null_rates(events, watched):
        flag = "  <-- nullable" if rate > 0 else ""
        print(f"  {100 * rate:5.1f}%  {path}{flag}")

    all_keys = Counter(key for event in events for key in event)
    print("\ntop-level keys and how often present:")
    for key, n in all_keys.most_common():
        print(f"  {100 * n / total:5.1f}%  {key}")

    print("\nnull-rate of length/revision within `log` events only:")
    logs = [e for e in events if e.get("type") == "log"]
    if logs:
        for path, rate in _null_rates(logs, ["length.old", "length.new", "revision.new"]):
            print(f"  {100 * rate:5.1f}%  {path}   (n={len(logs)})")
    else:
        print("  no log events in this sample")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500, help="How many events to capture.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    configure_logging("INFO", as_json=False)
    events = capture(args.count, args.output)
    profile(events)
    print(f"\nwrote {args.output} ({args.output.stat().st_size / 1024:.0f} KiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
