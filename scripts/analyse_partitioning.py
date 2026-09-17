#!/usr/bin/env python
"""Explain the Kafka partition skew, and say what more partitions would buy.

The producer keys every record on `meta.domain` so that edits to one wiki stay in
order relative to each other. The cost is balance, and the measured cost turned out
to be larger than the obvious estimate: the busiest single wiki is 31.8% of traffic,
but the busiest *partition* is around 60%.

This script shows why, from the captured fixture and with no network:

    uv run python scripts/analyse_partitioning.py
    uv run python scripts/analyse_partitioning.py --counts 3 6 12

librdkafka's default partitioner is `consistent_random`, which for a keyed record
is `crc32(key) % partition_count`. That is reproducible in four lines of Python,
which is what makes this answerable offline instead of by rerunning the pipeline
with different topic settings.

Note that this is librdkafka's hash, not the Java client's. The Java producer uses
murmur2, so a Java consumer group rebalancing against these partitions is fine but
a Java *producer* writing the same keys to the same topic would place them
differently. Only one producer writes this topic, so that stays theoretical — it is
noted because "the partitioner is the same everywhere" is a common and wrong
assumption.
"""

from __future__ import annotations

import argparse
import json
import sys
import zlib
from collections import Counter
from pathlib import Path

from wikistream.config import get_settings

FIXTURE = (
    Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "recentchange_sample.jsonl"
)


def partition_of(key: str, count: int) -> int:
    """Which partition librdkafka's `consistent` partitioner sends `key` to."""
    return zlib.crc32(key.encode("utf-8")) % count


def domain_counts(path: Path) -> Counter[str]:
    """Frequency of each wiki domain in the capture."""
    counts: Counter[str] = Counter()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        domain = json.loads(line).get("meta", {}).get("domain")
        if isinstance(domain, str):
            counts[domain] += 1
    return counts


def report_domains(counts: Counter[str], count: int, top: int) -> None:
    total = sum(counts.values())
    print(f"\n### Busiest wikis in the capture, and where {count} partitions put them\n")
    print("| Wiki | Events | Share | Partition |")
    print("|---|---|---|---|")
    for domain, n in counts.most_common(top):
        print(f"| `{domain}` | {n} | {100 * n / total:.1f}% | {partition_of(domain, count)} |")


def report_counts(counts: Counter[str], partition_counts: list[int]) -> None:
    total = sum(counts.values())
    print("\n### Predicted skew by partition count\n")
    print("| Partitions | Busiest partition | Idle partitions | Perfect balance would be |")
    print("|---|---|---|---|")
    for count in partition_counts:
        loads: Counter[int] = Counter()
        for domain, n in counts.items():
            loads[partition_of(domain, count)] += n
        busiest = max(loads.values()) if loads else 0
        idle = count - len(loads)
        print(f"| {count} | {100 * busiest / total:.1f}% | {idle} | {100 / count:.1f}% |")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Explain Kafka partition skew from the fixture.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE)
    parser.add_argument("--counts", type=int, nargs="+", default=[1, 3, 6, 12, 24])
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Partition count for the per-wiki table. Defaults to the configured topic's.",
    )
    parser.add_argument("--top", type=int, default=8)
    args = parser.parse_args(argv)

    if not args.fixture.exists():
        print(f"fixture not found: {args.fixture}", file=sys.stderr)
        print("regenerate it with: uv run python scripts/capture_fixture.py", file=sys.stderr)
        return 1

    counts = domain_counts(args.fixture)
    total = sum(counts.values())
    if not total:
        print(f"no keyable events in {args.fixture}", file=sys.stderr)
        return 1

    live = args.partitions or get_settings().kafka_topic_partitions
    busiest_domain, busiest_events = counts.most_common(1)[0]
    floor = 100 * busiest_events / total

    print(f"\n{total} events across {len(counts)} wikis, from {args.fixture.name}")
    report_domains(counts, live, args.top)
    report_counts(counts, args.counts)
    print(
        f"\nThe floor is {floor:.1f}%: `{busiest_domain}` alone is that share of traffic, and one "
        f"key\nnever splits across partitions, so no partition count can do better than that.\n"
        f"Past the point where the busiest keys stop colliding, more partitions buy idle\n"
        f"partitions rather than balance."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
