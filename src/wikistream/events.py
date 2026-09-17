"""Field-extraction rules for a `recentchange` event, as pure functions.

These are the definitions the pipeline runs on, written where they can be tested
against a file instead of against a cluster. The producer imports `event_id` and
`partition_key` directly. The rest — `byte_delta`, `is_anonymous_editor`,
`event_time` — are the reference definitions that silver's SQL re-expresses in
Spark, because a `MERGE INTO` cannot call Python.

That duplication is a real cost and worth naming: two expressions of one rule can
drift. It is accepted here rather than removed, because the alternatives are
worse — a Python UDF in the streaming path would serialise every row out of the
JVM and back, and pushing these rules into SQL only would leave them untestable
without a five-second Spark session per assertion. The mitigation is that the
rules are small, total, and pinned by the adversarial fixture.

**This module must not import pyspark, directly or transitively.** The producer
image contains no Spark — that is the point of it being a 250 MB image rather than
a 700 MB one — and the producer imports `partition_key` from here. An import of
`wikistream.streaming.schema` for the sake of one tuple of strings is what broke
that once already, which is why the field contract below lives here, in the
Spark-free module, and the Spark schema is the thing that reads across.
"""

from __future__ import annotations

import ipaddress
import re
from datetime import datetime, timezone
from typing import Any

#: Fields without which a row cannot be processed at all, checked explicitly
#: because `from_json` will not check it. `meta.id` is the dedup key, `meta.dt`
#: is the event time the watermark reads, and `meta.domain` is the Kafka
#: partition key; a row missing any of them cannot be placed, ordered or
#: deduplicated, so it goes to `silver.quarantine` instead of silently poisoning
#: the merge. All three were present in 100% of the sample — the check exists
#: for the day that stops being true.
REQUIRED_FIELDS: tuple[str, ...] = (
    "meta.id",
    "meta.dt",
    "meta.domain",
)

#: The dedup key, as it is named once it reaches bronze and silver. `meta.id` is
#: renamed on the way in so that no downstream SQL has to quote a nested path,
#: and so the Iceberg tables read as tables rather than as a JSON dump.
EVENT_ID_COLUMN = "event_id"

#: MediaWiki temporary accounts, introduced for unregistered editors. The name
#: looks like `~2026-12345` and is not an IP, but it is not a registered account
#: either, so a "how much editing is anonymous" question that only looks for IPs
#: undercounts on any wiki where temp accounts are enabled.
_TEMP_ACCOUNT_PATTERN = re.compile(r"^~\d{4}-\d+$")


def _dig(event: dict[str, Any], path: str) -> Any:
    """Follow a dotted path, returning None if any step is missing or not a dict."""
    node: Any = event
    for part in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def event_id(event: dict[str, Any]) -> str | None:
    """The deduplication key: `meta.id`, a UUID.

    Not the top-level `id`. That field is the recentchanges row id, and it was
    null in 0.4% of a 500-event sample — both times on a `log` event, which has
    no recentchanges row at all. A key that is sometimes null is not a key.
    """
    value = _dig(event, "meta.id")
    return value if isinstance(value, str) and value else None


def partition_key(event: dict[str, Any]) -> str | None:
    """The Kafka partition key: `meta.domain`.

    Keying by wiki rather than round-robin buys per-wiki ordering, which is what
    makes "the last edit to this page" answerable without a global sort. It costs
    balance: `commons.wikimedia.org` was 31.8% of a 500-event sample on its own,
    so one partition runs hot. That trade is deliberate — see DECISIONS.md.
    """
    value = _dig(event, "meta.domain")
    return value if isinstance(value, str) and value else None


def raw_event_time(event: dict[str, Any]) -> str | None:
    """`meta.dt` exactly as it arrived, unparsed.

    Kept separate from `event_time` so that "the field was not sent" and "the field
    was sent and was unreadable" stay distinguishable. They are different upstream
    faults — a dropped field is a contract change, a bad value is usually one wiki
    misbehaving — and a dead-letter table that conflates them costs whoever reads it
    the first hour of the investigation.
    """
    value = _dig(event, "meta.dt")
    return value if isinstance(value, str) else None


def event_time(event: dict[str, Any]) -> datetime | None:
    """Parse `meta.dt` into an aware UTC datetime, or None if unusable.

    `meta.dt` is preferred over the top-level `timestamp` even though they agree,
    because `timestamp` is whole seconds and `meta.dt` carries milliseconds. At a
    measured 40 events/second, second-granularity event time would collapse
    dozens of distinct events onto one instant.
    """
    raw = _dig(event, "meta.dt")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def missing_required_fields(event: dict[str, Any]) -> tuple[str, ...]:
    """Which of `REQUIRED_FIELDS` are absent or null.

    Returns a tuple rather than a bool so the quarantine row can record *what*
    was wrong. A dead-letter table that says only "invalid" forces whoever reads
    it to re-derive the reason from the payload.
    """
    return tuple(path for path in REQUIRED_FIELDS if _dig(event, path) is None)


def is_anonymous_editor(user: str | None) -> bool:
    """Whether an edit was made without a logged-in named account.

    True for an IPv4 or IPv6 address, and for a MediaWiki temporary account.
    A heuristic, and labelled as one: a registered user is free to choose a
    username that happens to look like an IP address, and this would call them
    anonymous. The measured cost of being wrong is low — a 500-event sample
    contained zero anonymous editors of either kind, which is itself why the
    adversarial fixture has to supply them.
    """
    if not user:
        return False
    if _TEMP_ACCOUNT_PATTERN.match(user):
        return True
    try:
        ipaddress.ip_address(user)
    except ValueError:
        return False
    return True


def byte_delta(event: dict[str, Any]) -> int | None:
    """Change in page size in bytes, or None where the question does not apply.

    Three cases, and the middle one is the reason this function exists rather
    than a `new - old` expression:

    * No `length` struct at all — `categorize` and `log` events, 55% of measured
      traffic. Returns None: the event did not change a page's size.
    * `length.old` null with `length.new` set — a page creation. Returns
      `length.new`, because a page appearing from nothing is a real gain of that
      many bytes, and returning None would erase page creations from every
      "bytes added" total.
    * Both set — returns the difference, which may legitimately be negative.

    A plain `new - old` in SQL yields null for case two, silently. That is the
    null-safety bug this pins down.
    """
    length = _dig(event, "length")
    if not isinstance(length, dict):
        return None
    new = length.get("new")
    old = length.get("old")
    if not isinstance(new, int) or isinstance(new, bool):
        return None
    if old is None:
        return new
    if not isinstance(old, int) or isinstance(old, bool):
        return None
    return new - old
