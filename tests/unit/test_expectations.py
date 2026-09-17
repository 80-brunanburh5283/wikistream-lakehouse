"""The validation rules, as pure functions. No Spark, no Kafka, no Docker.

These assert the Python side of each rule. The SQL side is asserted separately, and
the two are compared against each other in
`tests/spark/test_expectation_parity.py` — the only test that can catch the failure
mode this design has, which is one language being fixed and the other forgotten.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from wikistream.quality.expectations import (
    EVENT_TIME_FLOOR,
    EXPECTATIONS,
    FAILURE_REASONS,
    FUTURE_TOLERANCE,
    PARSE_FAILURE,
    RULE_INPUT_COLUMNS,
    Candidate,
    candidate_from_event,
    failure_reason_sql,
    first_failure,
)

pytestmark = pytest.mark.unit

#: A fixed reference time, so "the future" means the same thing on every run. Using
#: `datetime.now()` here would make the future-tolerance test pass or fail depending
#: on the fixture's age, which is the sort of test that goes red six months later
#: for no reason anyone can reconstruct.
NOW = datetime(2026, 9, 17, 4, 5, 0, tzinfo=timezone.utc)

GOOD = Candidate(
    event_id="aaaaaaa1-0000-4000-8000-000000000001",
    raw_event_time="2026-09-17T04:00:00.000Z",
    event_time=datetime(2026, 9, 17, 4, 0, 0, tzinfo=timezone.utc),
    domain="en.wikipedia.org",
)


@pytest.fixture
def by_title(adversarial_events):
    """The adversarial events keyed by the case they encode."""
    return {event["title"]: event for event in adversarial_events}


def reason(candidate: Candidate) -> str | None:
    """`first_failure` at the fixed reference time."""
    return first_failure(candidate, reference=NOW)


# --------------------------------------------------------------------------
# The happy path, and the rule set as a whole
# --------------------------------------------------------------------------


def test_a_well_formed_candidate_passes_every_rule():
    assert reason(GOOD) is None


def test_every_real_sampled_event_passes(sample_events):
    """500 captured events, none quarantined.

    A rule set that rejects real traffic is worse than no rule set: the quarantine
    table fills with valid data and the marts go empty. This is the test that stops
    a tightened rule from silently becoming an outage.
    """
    failures = {
        event["meta"]["id"]: reason(candidate_from_event(event))
        for event in sample_events
        if reason(candidate_from_event(event)) is not None
    }
    assert failures == {}


def test_rule_names_are_unique_and_all_reachable():
    """Two rules sharing a name would make `failure_reason` ambiguous to group by."""
    names = [rule.name for rule in EXPECTATIONS]
    assert len(names) == len(set(names))
    assert (PARSE_FAILURE, *names) == FAILURE_REASONS


def test_every_rule_reads_only_declared_columns():
    """A rule referencing an unprojected column is a `CASE` branch that never fires.

    Checked by substring rather than by parsing SQL: crude, but it catches the real
    mistake, which is adding a rule about a field nobody projected.
    """
    sql = failure_reason_sql("ingested_at")
    for rule in EXPECTATIONS:
        columns = [name for name in RULE_INPUT_COLUMNS if name in rule.sql]
        assert columns, f"{rule.name} reads no declared column: {rule.sql}"
    assert "{reference}" not in sql, "the reference placeholder must be substituted"


def test_every_rule_has_a_description_that_says_what_is_wrong():
    """The description is what a quarantine reader sees. An empty one wastes their time."""
    for rule in EXPECTATIONS:
        assert len(rule.description) > 20
        assert rule.description.endswith(".")


# --------------------------------------------------------------------------
# One test per rule, each naming the upstream fault it stands for
# --------------------------------------------------------------------------


def test_a_missing_event_id_is_rejected_because_dedup_needs_a_key():
    assert reason(replace(GOOD, event_id=None)) == "event_id_missing"


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_a_blank_event_id_is_as_useless_as_a_missing_one(blank):
    """`length(trim(...)) > 0` on the SQL side; the same judgement here.

    An empty-string id would pass a plain null check, then collapse every such row
    onto one silver row under the MERGE.
    """
    assert reason(replace(GOOD, event_id=blank)) == "event_id_missing"


def test_a_missing_event_time_is_distinguished_from_an_unreadable_one():
    """Two rules, not one, because they point at different upstream problems."""
    absent = replace(GOOD, raw_event_time=None, event_time=None)
    garbage = replace(GOOD, raw_event_time="17/09/2026 04:00", event_time=None)

    assert reason(absent) == "event_time_missing"
    assert reason(garbage) == "event_time_unparseable"


def test_a_missing_domain_is_rejected_because_it_is_the_partition_key():
    assert reason(replace(GOOD, domain=None)) == "domain_missing"


def test_an_event_time_before_wikipedia_existed_is_a_broken_clock():
    ancient = replace(GOOD, event_time=EVENT_TIME_FLOOR - timedelta(seconds=1))

    assert reason(ancient) == "event_time_before_wikipedia"


def test_the_floor_itself_is_accepted():
    """An inclusive bound, asserted so a later `>` for `>=` swap is caught."""
    assert reason(replace(GOOD, event_time=EVENT_TIME_FLOOR)) is None


def test_an_event_time_far_ahead_of_ingest_time_is_a_clock_fault():
    assert reason(replace(GOOD, event_time=NOW + FUTURE_TOLERANCE + timedelta(seconds=1))) == (
        "event_time_in_future"
    )


def test_small_clock_skew_ahead_of_ingest_time_is_tolerated():
    """The rule is for the event stamped 2099, not for a second of skew.

    Rejecting mild skew would quarantine real edits whenever a wiki's clock ran
    slightly fast, which is a self-inflicted outage.
    """
    assert reason(replace(GOOD, event_time=NOW + timedelta(seconds=30))) is None


# --------------------------------------------------------------------------
# Ordering, which the SQL CASE depends on
# --------------------------------------------------------------------------


def test_the_first_failure_wins_when_several_rules_would_fail():
    """A row with no id and no time reports the id, because the id rule is declared first.

    The SQL side gets this from `CASE` short-circuiting. Asserting it here pins the
    behaviour the two sides have to agree on.
    """
    broken = Candidate(event_id=None, raw_event_time=None, event_time=None, domain=None)

    assert reason(broken) == "event_id_missing"


def test_the_range_rules_are_declared_after_the_parseable_rule():
    """Otherwise a null `event_time` would reach a comparison that cannot judge it.

    In SQL, `NOT (NULL >= ts)` is NULL, so the branch would not fire and a row with
    no event time would be declared valid. Ordering is what prevents that, so the
    ordering is a test.
    """
    names = [rule.name for rule in EXPECTATIONS]

    assert names.index("event_time_unparseable") < names.index("event_time_before_wikipedia")
    assert names.index("event_time_unparseable") < names.index("event_time_in_future")


# --------------------------------------------------------------------------
# The adversarial fixture, which is where these cases came from
# --------------------------------------------------------------------------


def test_the_adversarial_fixture_cases_land_on_their_intended_reasons(by_title):
    """Each hand-built frame exists to trip exactly one rule. This says which.

    Keyed by title so appending a case to the fixture cannot break this test, and so
    a reader can go from a rule to the frame that exercises it.
    """
    expected = {
        "Adversarial:baseline": None,
        "Adversarial:missing_meta_id": "event_id_missing",
        "Adversarial:missing_meta_dt": "event_time_missing",
        "Adversarial:unparseable_meta_dt": "event_time_unparseable",
        "Adversarial:missing_meta_domain": "domain_missing",
        "Adversarial:far_future_meta_dt": "event_time_in_future",
        # Late data is valid data. This is the case that must NOT be quarantined,
        # and the reason the pipeline has no watermark-based dropping.
        "Adversarial:late_by_30_minutes": None,
    }

    actual = {title: reason(candidate_from_event(by_title[title])) for title in expected}

    assert actual == expected


# --------------------------------------------------------------------------
# The generated SQL, as text
# --------------------------------------------------------------------------


def test_the_generated_case_lists_every_reason_in_order():
    sql = failure_reason_sql("ingested_at")
    positions = [sql.index(f"'{name}'") for name in FAILURE_REASONS]

    assert positions == sorted(positions)
    assert sql.startswith("CASE")
    assert sql.rstrip().endswith("END")


def test_the_reference_column_is_substituted_into_the_future_rule():
    assert "ingested_at + INTERVAL 3600 SECONDS" in failure_reason_sql("ingested_at")
