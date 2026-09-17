"""What makes a parsed event fit to become a silver row, and why each rule exists.

Every rule is written twice: once as a Python predicate over a `Candidate`, and
once as a fragment of Spark SQL. The pipeline runs the SQL — a `MERGE INTO` cannot
call Python and a Python UDF in the streaming path would serialise every row out of
the JVM and back. The predicates exist so the rules can be tested against a file in
milliseconds instead of against a cluster in seconds, and
`tests/spark/test_expectation_parity.py` runs both sides over the same payloads and
fails if they ever disagree. That test is the whole justification for the
duplication; without it this module would be two definitions drifting apart.

This module imports no pyspark. It emits SQL as text.

## Ordering is part of the contract

`failure_reason_sql` builds a single `CASE`, and SQL `CASE` stops at the first
`WHEN` that is true. So the rules are evaluated in the order they are declared and
the first failure is the reported reason — which is why the later rules may assume
the earlier ones passed. `event_time_out_of_range` compares a timestamp without a
null guard because `event_time_unparseable` has already rejected the null.

## Why the parse failure has no Python twin

`payload_not_json` is decided by Spark alone, and deliberately so. Measured against
Spark 4.0.4 on 2026-09-17, `from_json` with `PERMISSIVE` mode disagrees with
`json.loads` on nine of twenty-one payload shapes:

| payload                        | Spark        | `json.loads` |
|--------------------------------|--------------|--------------|
| `''` or `'   '`                | null struct  | invalid      |
| `'{"meta":{"id":"a"}} junk'`   | valid        | invalid      |
| `"{'meta':{'id':'a'}}"`        | valid        | invalid      |
| `'{"namespace":"7"}'`          | corrupt      | valid        |
| `'{"type":123}'`               | valid, `'123'` | valid      |
| `'{"id":1234567890123456789012}'` | corrupt   | valid        |
| `'{"meta":"nope"}'`            | corrupt      | valid        |
| `'{"id":NaN}'`                 | corrupt      | valid        |

Reading down that list: only a *blank* payload yields a null struct, so a
`_corrupt_record IS NOT NULL` test on its own misses the empty frame — which is why
the SQL check below is two conditions. Jackson stops at the end of the first JSON
value, so trailing junk is ignored; it accepts single-quoted keys; a string where the
schema declares an int is a parse failure, while a number where the schema declares a
string is quietly coerced; a value too wide for its declared type is a failure; and
`NaN` is beyond even Jackson's leniency.

Mirroring that in Python would mean reimplementing Jackson's leniency flags and
Spark's coercion table, and the mirror would be wrong on the first version bump. So
the split is: Spark decides whether the bytes were readable, and these rules decide
whether the readable result is usable. Note also from the same measurement that a
corrupt-flagged row keeps whatever sibling fields *did* parse, so a row can be
flagged and still carry a perfectly good `event_id`. It is quarantined anyway — see
`docs/correctness.md` on why that is deliberately loud.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from wikistream import events

#: Nothing on this stream can predate Wikipedia. 15 January 2001 is the project's
#: launch date (https://en.wikipedia.org/wiki/History_of_Wikipedia, read
#: 2026-09-17), so an `event_time` below it is a broken clock or a mis-parsed
#: field, not an old edit. An absolute floor is used rather than "within N days of
#: now" on purpose: a relative floor would make a replay of last month's bronze
#: quarantine rows that were valid when they arrived, which would make
#: `make rebuild-silver` non-idempotent with respect to wall-clock time.
EVENT_TIME_FLOOR = datetime(2001, 1, 15, tzinfo=timezone.utc)

#: How far ahead of the reference time an `event_time` may be before it is treated
#: as a clock fault. An hour is generous for a source whose measured p99 lag is
#: 1.32s (docs/latency.md); the point is not to catch small skew but to reject the
#: event stamped 2099, which would open an Iceberg partition that never compacts,
#: break `max(event_time)` as a freshness signal, and — for a year above 9999 —
#: produce a timestamp that Spark stores happily and Python cannot represent at all.
FUTURE_TOLERANCE = timedelta(hours=1)

#: Columns a caller must supply before applying `failure_reason_sql`. The rules
#: below read these names and nothing else, so a projection that omits one is a
#: build-time error rather than a `CASE` that silently never fires.
RULE_INPUT_COLUMNS: tuple[str, ...] = (
    "_parse_failed",
    "event_id",
    "_raw_event_time",
    "event_time",
    "domain",
)

#: The reason recorded when Spark could not read the payload at all.
PARSE_FAILURE = "payload_not_json"


@dataclass(frozen=True)
class Candidate:
    """The fields a rule can see, as Spark would have parsed them.

    A deliberately narrow view: only what some rule reads. Widening it to the whole
    event would invite rules that depend on fields the SQL side does not project,
    and the parity test would start passing for the wrong reason.
    """

    event_id: str | None = None
    #: `meta.dt` as it arrived, before parsing. Kept so that "the field was absent"
    #: and "the field was there and was garbage" are different reasons in the
    #: quarantine table; they point at different upstream problems.
    raw_event_time: str | None = None
    event_time: datetime | None = None
    domain: str | None = None


@dataclass(frozen=True)
class Expectation:
    """One rule, in both languages.

    `check(candidate, reference)` returns True when the candidate *passes*. Phrasing
    the predicate positively means the rule reads the same way as its SQL, which is
    negated once, in one place, when the `CASE` is built.

    `reference` is when the pipeline saw the row. It is a parameter rather than a
    field of `Candidate` because it is not part of the event: only the
    future-tolerance rule has any business reading it, and every other rule ignores
    it visibly.
    """

    name: str
    description: str
    check: Callable[[Candidate, datetime], bool]
    #: A boolean Spark SQL expression, true when the row passes. May contain
    #: `{reference}`, which `failure_reason_sql` substitutes with the column holding
    #: the time to measure "the future" against.
    sql: str


def _present(value: str | None) -> bool:
    """A string field counts as present only if it holds a non-blank value.

    `''` and `'  '` are treated as absent because a key whose value is an empty
    string is not a usable identifier, and the SQL side cannot tell the difference
    without the same `trim`.
    """
    return value is not None and value.strip() != ""


def _sql_present(column: str) -> str:
    """`column` holds a non-blank string. The SQL twin of `_present`."""
    return f"{column} IS NOT NULL AND length(trim({column})) > 0"


EXPECTATIONS: tuple[Expectation, ...] = (
    Expectation(
        name="event_id_missing",
        description="meta.id is absent or blank, so the row cannot be deduplicated.",
        check=lambda c, _: _present(c.event_id),
        sql=_sql_present("event_id"),
    ),
    Expectation(
        name="event_time_missing",
        description="meta.dt is absent or blank, so the row cannot be placed in time.",
        check=lambda c, _: _present(c.raw_event_time),
        sql=_sql_present("_raw_event_time"),
    ),
    Expectation(
        name="event_time_unparseable",
        description="meta.dt was sent but is not a timestamp this pipeline can read.",
        # Distinct from the rule above on purpose: absent means the source stopped
        # sending a field, garbage means the source changed its format. The first is
        # a contract change, the second is usually a bug at one wiki.
        check=lambda c, _: c.event_time is not None,
        sql="event_time IS NOT NULL",
    ),
    Expectation(
        name="domain_missing",
        description="meta.domain is absent or blank; it is the Kafka partition key.",
        check=lambda c, _: _present(c.domain),
        sql=_sql_present("domain"),
    ),
    Expectation(
        name="event_time_before_wikipedia",
        description=f"event_time predates {EVENT_TIME_FLOOR:%Y-%m-%d}, so the clock is wrong.",
        check=lambda c, _: c.event_time is not None and c.event_time >= EVENT_TIME_FLOOR,
        sql=f"event_time >= TIMESTAMP '{EVENT_TIME_FLOOR:%Y-%m-%d %H:%M:%S}'",
    ),
    Expectation(
        name="event_time_in_future",
        description="event_time is far enough ahead of ingest time to be a clock fault.",
        check=lambda c, reference: (
            c.event_time is not None and c.event_time <= reference + FUTURE_TOLERANCE
        ),
        sql=(
            "event_time <= {reference} + "
            f"INTERVAL {int(FUTURE_TOLERANCE.total_seconds())} SECONDS"
        ),
    ),
)

#: Every rule name, in evaluation order, with the parse failure first. This is the
#: closed set of values `silver.quarantine.failure_reason` can hold, which is what
#: makes a `GROUP BY failure_reason` dashboard finite.
FAILURE_REASONS: tuple[str, ...] = (PARSE_FAILURE, *(rule.name for rule in EXPECTATIONS))


def candidate_from_event(event: dict[str, Any]) -> Candidate:
    """Build a `Candidate` from a decoded event, using the shared field rules.

    Goes through `wikistream.events` rather than digging the dict here, so that
    "which field is the event id" is answered in exactly one place for the producer,
    the rules and the docs.
    """
    return Candidate(
        event_id=events.event_id(event),
        raw_event_time=events.raw_event_time(event),
        event_time=events.event_time(event),
        domain=events.partition_key(event),
    )


def first_failure(candidate: Candidate, *, reference: datetime) -> str | None:
    """The name of the first rule `candidate` fails, or None if it passes them all.

    First failure rather than every failure: the quarantine table records one reason
    per row, because a row that fails four rules is almost always failing one root
    cause four times over, and the earliest rule is the closest to it.
    """
    for rule in EXPECTATIONS:
        if not rule.check(candidate, reference):
            return rule.name
    return None


def failure_reason_sql(reference: str) -> str:
    """A Spark SQL expression yielding the failure reason, or NULL when the row is fit.

    `reference` is the column to measure "the future" against — the row's ingest
    time, not `current_timestamp()`. Passing the stored ingest time is what lets
    `make rebuild-silver` replay a month-old bronze partition and reach the same
    verdict the live path reached at the time, rather than re-judging old events
    against today's clock.
    """
    branches = [f"WHEN _parse_failed THEN '{PARSE_FAILURE}'"]
    branches += [
        f"WHEN NOT ({rule.sql.format(reference=reference)}) THEN '{rule.name}'"
        for rule in EXPECTATIONS
    ]
    body = "\n  ".join(branches)
    return f"CASE\n  {body}\n  ELSE NULL\nEND"
