"""The Python rules and the Spark SQL rules must agree. This is what checks it.

Three rules in this pipeline exist twice — once in `wikistream.quality.expectations`
or `wikistream.events` as a Python function, once in `wikistream.streaming.silver` as
Spark SQL. The docstring of `wikistream.events` names the cost of that duplication
and says the mitigation is a test. This is that test, and it is the reason the
duplication is defensible rather than sloppy: a rule changed on one side and not the
other fails here, over 519 real and hand-built payloads, in about a second.

What is deliberately *not* checked for parity is whether a payload is readable JSON.
Spark decides that alone, because `from_json` disagrees with `json.loads` on nine of
twenty-one measured payload shapes — see the table in
`wikistream.quality.expectations`. The corrupt cases below therefore assert only
Spark's verdict.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pyspark.sql import functions as F

from wikistream.events import byte_delta, is_anonymous_editor
from wikistream.quality.expectations import (
    PARSE_FAILURE,
    candidate_from_event,
    first_failure,
)
from wikistream.streaming.silver import FRAME_COLUMNS, to_candidates

pytestmark = pytest.mark.spark

#: The ingest time both sides judge "the future" against, as a SQL literal and as a
#: Python datetime. A literal rather than `current_timestamp()` so the future-tolerance
#: rule is deterministic; written without a zone suffix and read under the session's
#: UTC timezone, which is how every timestamp in this project is interpreted.
REFERENCE_SQL = "2026-09-17 04:05:00"
REFERENCE = datetime(2026, 9, 17, 4, 5, 0, tzinfo=UTC)


def _frames(spark, payloads: list[str]):
    """A DataFrame in the shape `to_candidates` requires, from raw payload strings.

    Built directly rather than through `frames_from_kafka` so `ingested_at` can be a
    fixed literal. The transformation under test starts at `to_candidates`; the two
    adapters in front of it only rename columns.
    """
    rows = [
        (payload, "wiki.recentchange", index % 3, index) for index, payload in enumerate(payloads)
    ]
    return (
        spark.createDataFrame(
            rows, "raw_payload string, kafka_topic string, kafka_partition int, kafka_offset long"
        )
        .withColumn("kafka_timestamp", F.lit(REFERENCE_SQL).cast("timestamp"))
        .withColumn("ingested_at", F.lit(REFERENCE_SQL).cast("timestamp"))
        .select(*FRAME_COLUMNS)
    )


def _spark_verdicts(spark, payloads: list[str]) -> dict[int, dict[str, Any]]:
    """What the SQL says about each payload, keyed by its position in the input."""
    rows = (
        to_candidates(_frames(spark, payloads))
        .select("kafka_offset", "failure_reason", "is_anonymous", "bytes_delta")
        .collect()
    )
    return {
        int(row["kafka_offset"]): {
            "failure_reason": row["failure_reason"],
            "is_anonymous": row["is_anonymous"],
            "bytes_delta": row["bytes_delta"],
        }
        for row in rows
    }


def _python_verdicts(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """What the Python functions say about each event, keyed the same way."""
    return {
        index: {
            "failure_reason": first_failure(candidate_from_event(event), reference=REFERENCE),
            "is_anonymous": is_anonymous_editor(event.get("user")),
            "bytes_delta": byte_delta(event),
        }
        for index, event in enumerate(events)
    }


def _assert_agree(spark, events: list[dict[str, Any]]) -> None:
    """Compare both sides over the same events and report every disagreement at once.

    Reporting all of them rather than failing on the first is deliberate: a rule
    changed on one side usually breaks a class of rows, and seeing the class is what
    tells you which rule moved.
    """
    payloads = [json.dumps(event, ensure_ascii=False) for event in events]
    from_spark = _spark_verdicts(spark, payloads)
    from_python = _python_verdicts(events)

    disagreements = {
        index: {"spark": from_spark[index], "python": from_python[index]}
        for index in from_python
        if from_spark[index] != from_python[index]
    }

    assert disagreements == {}, (
        f"{len(disagreements)} of {len(events)} events disagree between the "
        f"Python rules and the Spark SQL: {list(disagreements.items())[:5]}"
    )


def test_the_rules_agree_over_500_captured_events(spark, sample_events):
    """Real traffic. Proves the SQL is not merely self-consistent on hand-built cases."""
    _assert_agree(spark, sample_events)


def test_the_rules_agree_over_every_adversarial_case(spark, adversarial_events):
    """Hand-built traffic: the cases 500 real events did not contain.

    This is where anonymous editors, missing required fields, an unparseable
    `meta.dt`, a far-future `meta.dt` and a page creation with a null old length all
    live, so it is the half of the corpus that actually exercises the branches.
    """
    _assert_agree(spark, adversarial_events)


def test_a_page_creation_agrees_on_bytes_delta(spark, adversarial_events):
    """The null-safety case, called out because a plain subtraction passes every other test.

    `length.old` is null and `length.new` is 4200 on a page creation. Both sides must
    return 4200, not null; returning null would erase page creations from every
    "bytes added" total without any test noticing.
    """
    creation = next(
        event for event in adversarial_events if event["title"].endswith("new_page_null_old_length")
    )
    payload = json.dumps(creation, ensure_ascii=False)

    from_spark = _spark_verdicts(spark, [payload])[0]

    assert from_spark["bytes_delta"] == byte_delta(creation)
    assert from_spark["bytes_delta"] == creation["length"]["new"]
    assert creation["length"]["old"] is None


@pytest.mark.parametrize(
    ("editor", "expected"),
    [
        ("ExampleEditor", False),
        ("192.0.2.146", True),
        ("2001:db8:3333:4444:5555:6666:7777:8888", True),
        ("~2026-19", True),
        # 999 is not an octet. A regex of `\d{1,3}` would call this an address, and
        # Python's ipaddress would not, so this is the case that pins the SQL to the
        # real octet range rather than to something that merely looks like one.
        ("999.999.999.999", False),
        # A username that is all digits and dots but too short to be an address.
        ("1.2.3", False),
        (None, False),
        ("", False),
    ],
)
def test_the_anonymity_rules_agree_on_editor_shapes(spark, editor, expected):
    """One row per editor shape, both sides asserted against the same expectation.

    Parameterised rather than folded into the corpus test so a failure names the shape
    that broke instead of a count.
    """
    payload = json.dumps(
        {
            "meta": {
                "id": "aaaaaaa1-0000-4000-8000-000000000099",
                "dt": "2026-09-17T04:00:00.000Z",
                "domain": "en.wikipedia.org",
            },
            "user": editor,
        }
    )

    from_spark = _spark_verdicts(spark, [payload])[0]

    assert from_spark["is_anonymous"] == expected
    assert is_anonymous_editor(editor) == expected


@pytest.mark.parametrize(
    "payload",
    [
        "{this is not json",
        "",
        "   ",
        "[1, 2, 3]",
        '{"meta": "a string where the schema says struct"}',
        '{"namespace": "seven"}',
    ],
)
def test_spark_alone_decides_what_counts_as_unreadable(spark, payload):
    """The parse verdict has no Python twin, and these are the shapes that prove why.

    Two of these — the blank payload and the whitespace payload — are the reason the
    check is `_parsed IS NULL OR _corrupt_record IS NOT NULL` rather than just the
    second half: a blank frame produces a null struct and never sets the corrupt
    column, so a `_corrupt_record` test alone would let it through as a row of nulls.
    """
    assert _spark_verdicts(spark, [payload])[0]["failure_reason"] == PARSE_FAILURE
