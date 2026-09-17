"""Tests for the field-extraction rules, against captured and hand-built events.

Two fixtures, two jobs. `sample_events` is 500 real events and proves the rules
hold over real traffic. `adversarial_events` is hand-written and covers the cases
real traffic did not supply in 500 events — no duplicate ids, no anonymous
editors, no newline or quote in a comment — which is precisely why they had to be
written by hand rather than captured.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from wikistream.events import (
    byte_delta,
    event_id,
    event_time,
    is_anonymous_editor,
    missing_required_fields,
    partition_key,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def by_title(adversarial_events):
    """The adversarial events keyed by the case they encode, which is their title."""
    return {event["title"]: event for event in adversarial_events}


# --------------------------------------------------------------------------
# event_id
# --------------------------------------------------------------------------


def test_every_sampled_event_has_an_event_id(sample_events):
    assert all(event_id(event) is not None for event in sample_events)


def test_event_ids_are_uuid_shaped(sample_events):
    ids = {event_id(event) for event in sample_events}
    assert all(len(value) == 36 and value.count("-") == 4 for value in ids)


def test_sampled_event_ids_are_distinct_within_one_connection(sample_events):
    # 500 distinct ids out of 500. Worth asserting because it locates where
    # duplicates actually come from: not from the steady stream, but from
    # reconnects replaying at Last-Event-ID. A test suite that only ever saw this
    # file would wrongly conclude deduplication is unnecessary.
    ids = [event_id(event) for event in sample_events]
    assert len(set(ids)) == len(ids)


def test_event_id_ignores_the_top_level_id_field(by_title):
    # This log event has `"id": null` and a valid meta.id. Keying on the
    # top-level id would drop it.
    event = by_title["Adversarial:log_event_empty_list_params"]
    assert event["id"] is None
    assert event_id(event) == "aaaaaaa6-0000-4000-8000-000000000006"


def test_event_id_is_none_when_meta_id_is_absent(by_title):
    assert event_id(by_title["Adversarial:missing_meta_id"]) is None


@pytest.mark.parametrize("event", [{}, {"meta": {}}, {"meta": {"id": ""}}, {"meta": None}])
def test_event_id_is_none_for_degenerate_shapes(event):
    assert event_id(event) is None


def test_duplicate_case_really_does_repeat_an_id(adversarial_events):
    ids = [event_id(event) for event in adversarial_events]
    repeated = {value for value in ids if ids.count(value) > 1 and value is not None}
    assert repeated == {
        "aaaaaaa2-0000-4000-8000-000000000002",  # byte-identical redelivery
        "aaaaaaa3-0000-4000-8000-000000000003",  # same id, divergent payload
    }


def test_exact_duplicate_pair_is_byte_identical(adversarial_events):
    pair = [
        event
        for event in adversarial_events
        if event_id(event) == "aaaaaaa2-0000-4000-8000-000000000002"
    ]
    assert len(pair) == 2
    assert pair[0] == pair[1]


def test_divergent_duplicate_has_the_same_id_but_different_content(adversarial_events):
    # The nastier of the two duplicate cases: one event id, two different
    # payloads, as happens when a redelivery crosses an upstream edit. A MERGE
    # matching on event_id has to pick one deterministically — if it picks by
    # arrival order, the table's contents depend on micro-batch boundaries.
    pair = [
        event
        for event in adversarial_events
        if event_id(event) == "aaaaaaa3-0000-4000-8000-000000000003"
    ]
    assert len(pair) == 2
    assert pair[0] != pair[1]
    assert {event["length"]["new"] for event in pair} == {5010, 5042}
    # Same event time, so event time alone cannot break the tie. The tiebreak has
    # to come from ingestion order, which is what silver records for the purpose.
    assert event_time(pair[0]) == event_time(pair[1])


# --------------------------------------------------------------------------
# partition_key
# --------------------------------------------------------------------------


def test_every_sampled_event_has_a_partition_key(sample_events):
    assert all(partition_key(event) is not None for event in sample_events)


def test_partition_key_is_the_wiki_domain(by_title):
    assert partition_key(by_title["Adversarial:baseline"]) == "en.wikipedia.org"


def test_partition_key_is_none_when_domain_is_missing():
    assert partition_key({"meta": {"id": "x"}}) is None


def test_partition_key_skew_is_real_and_measured(sample_events):
    # Documents the cost of keying by domain rather than round-robin. If this
    # ratio ever drops a lot, the DECISIONS.md entry describing the skew is stale.
    keys = [partition_key(event) for event in sample_events]
    busiest = max(set(keys), key=keys.count)
    assert keys.count(busiest) / len(keys) > 0.2


# --------------------------------------------------------------------------
# event_time
# --------------------------------------------------------------------------


def test_event_time_is_parsed_as_aware_utc(by_title):
    parsed = event_time(by_title["Adversarial:baseline"])
    assert parsed == datetime(2026, 9, 17, 4, 0, 0, tzinfo=UTC)
    assert parsed.tzinfo is not None


def test_event_time_keeps_millisecond_precision(by_title):
    parsed = event_time(by_title["Adversarial:duplicate_event_id"])
    assert parsed.microsecond == 250_000


def test_every_sampled_event_time_parses(sample_events):
    assert all(event_time(event) is not None for event in sample_events)


def test_all_sampled_event_times_are_utc_aware(sample_events):
    assert all(event_time(event).utcoffset().total_seconds() == 0 for event in sample_events)


def test_late_event_is_thirty_minutes_behind_its_neighbours(by_title):
    late = event_time(by_title["Adversarial:late_by_30_minutes"])
    baseline = event_time(by_title["Adversarial:baseline"])
    assert (baseline - late).total_seconds() == pytest.approx(1800, abs=1)


def test_event_time_is_none_when_meta_dt_is_absent(by_title):
    assert event_time(by_title["Adversarial:missing_meta_dt"]) is None


@pytest.mark.parametrize("raw", ["not-a-date", "", "2026-13-45T99:99:99Z", None, 12345])
def test_event_time_is_none_for_unparseable_values(raw):
    assert event_time({"meta": {"dt": raw}}) is None


# --------------------------------------------------------------------------
# missing_required_fields
# --------------------------------------------------------------------------


def test_no_sampled_event_is_missing_a_required_field(sample_events):
    assert all(missing_required_fields(event) == () for event in sample_events)


def test_missing_meta_id_is_reported_by_name(by_title):
    assert missing_required_fields(by_title["Adversarial:missing_meta_id"]) == ("meta.id",)


def test_missing_meta_dt_is_reported_by_name(by_title):
    assert missing_required_fields(by_title["Adversarial:missing_meta_dt"]) == ("meta.dt",)


def test_all_required_fields_are_reported_together():
    assert missing_required_fields({}) == ("meta.id", "meta.dt", "meta.domain")


def test_unknown_extra_fields_do_not_make_an_event_invalid(by_title):
    # Schema evolution must not quarantine. An event carrying a field the schema
    # has never seen is still a valid event.
    event = by_title["Adversarial:unknown_future_field"]
    assert "wikistream_unknown_top_level_field" in event
    assert missing_required_fields(event) == ()
    assert event_id(event) is not None


# --------------------------------------------------------------------------
# is_anonymous_editor
# --------------------------------------------------------------------------


def test_no_sampled_editor_is_anonymous(sample_events):
    # Measured: 0 of 500. Recorded as a test so the claim in the docstring is
    # checked rather than remembered.
    assert not any(is_anonymous_editor(event.get("user")) for event in sample_events)


def test_ipv4_editor_is_anonymous(by_title):
    assert is_anonymous_editor(by_title["Adversarial:anonymous_ipv4_editor"]["user"])


def test_ipv6_editor_is_anonymous(by_title):
    assert is_anonymous_editor(by_title["Adversarial:anonymous_ipv6_editor"]["user"])


@pytest.mark.parametrize("user", ["192.0.2.51", "2001:db8:85a3::8a2e:370:7334", "::1"])
def test_ip_addresses_are_anonymous(user):
    assert is_anonymous_editor(user)


@pytest.mark.parametrize("user", ["~2026-12345", "~2024-1"])
def test_temporary_accounts_are_anonymous(user):
    # MediaWiki temp accounts are neither an IP nor a registered account. A
    # heuristic that only checks for IPs undercounts anonymous editing on any
    # wiki where they are enabled.
    assert is_anonymous_editor(user)


@pytest.mark.parametrize(
    "user",
    ["ExampleEditor", "テスト利用者", "UploadBot", "", None, "192.0.2", "~notayear-1", "1.2.3.4.5"],
)
def test_named_accounts_and_near_misses_are_not_anonymous(user):
    assert not is_anonymous_editor(user)


# --------------------------------------------------------------------------
# byte_delta
# --------------------------------------------------------------------------


def test_byte_delta_on_a_normal_edit(by_title):
    assert byte_delta(by_title["Adversarial:baseline"]) == 120


def test_byte_delta_can_be_negative(by_title):
    assert byte_delta(by_title["Adversarial:anonymous_ipv4_editor"]) == -2


def test_byte_delta_can_be_zero(by_title):
    assert byte_delta(by_title["Adversarial:anonymous_ipv6_editor"]) == 0


def test_page_creation_counts_the_whole_new_size(by_title):
    # length.old is null here. A plain `new - old` yields null and silently
    # erases every page creation from any "bytes added" total.
    event = by_title["Adversarial:new_page_null_old_length"]
    assert event["length"]["old"] is None
    assert byte_delta(event) == 1520


def test_byte_delta_is_none_when_there_is_no_length_struct(by_title):
    # categorize and log events, 55% of measured traffic. None, not zero: the
    # event did not change a page's size, and calling that "changed by 0 bytes"
    # would drag every average toward zero.
    log_event = by_title["File:Adversarial_log_event.jpg"]
    assert "length" not in log_event
    assert "revision" not in log_event
    assert byte_delta(log_event) is None
    assert byte_delta({}) is None


def test_categorize_events_have_no_byte_delta(by_title):
    categorize = by_title["Category:Adversarial_categorize"]
    assert categorize["type"] == "categorize"
    assert "length" not in categorize
    assert byte_delta(categorize) is None


def test_byte_delta_matches_the_sample_where_length_is_present(sample_events):
    with_length = [event for event in sample_events if isinstance(event.get("length"), dict)]
    assert with_length, "expected some sampled events to carry a length struct"
    for event in with_length:
        old = event["length"].get("old")
        new = event["length"].get("new")
        expected = None if new is None else (new if old is None else new - old)
        assert byte_delta(event) == expected


def test_byte_delta_is_none_for_non_integer_values():
    assert byte_delta({"length": {"old": "1", "new": 2}}) is None
    assert byte_delta({"length": {"old": 1, "new": "2"}}) is None
    assert byte_delta({"length": None}) is None
    assert byte_delta({"length": {"new": None}}) is None


def test_byte_delta_rejects_booleans_masquerading_as_integers():
    # bool is a subclass of int in Python, so a naive isinstance check would
    # accept True and compute a delta of 1.
    assert byte_delta({"length": {"old": True, "new": 5}}) is None
    assert byte_delta({"length": {"old": 5, "new": True}}) is None


# --------------------------------------------------------------------------
# comment content
# --------------------------------------------------------------------------


def test_comment_with_newline_quote_tab_and_emoji_survives_a_round_trip(by_title):
    # These characters are the ones that break a CSV-shaped mental model. The
    # pipeline never writes CSV, and this test is what makes that safe to say.
    comment = by_title["Adversarial:comment_with_newline_quote_and_emoji"]["comment"]
    assert "\n" in comment
    assert '"' in comment
    assert "\t" in comment
    assert "\\" in comment
    assert "🚀" in comment
    assert any(ord(char) > 0xFFFF for char in comment)


def test_sampled_comments_include_non_ascii_but_no_newlines(sample_events):
    comments = [event.get("comment") or "" for event in sample_events]
    assert any(any(ord(c) > 127 for c in comment) for comment in comments)
    assert not any("\n" in comment for comment in comments)
