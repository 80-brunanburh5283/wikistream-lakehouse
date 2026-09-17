"""Tests for the SSE frame parser.

Every case here is a rule from WHATWG HTML section 9.2.6 that has a plausible
wrong implementation, plus the two Wikimedia-specific behaviours the parser has
to survive: `:ok` on connect and periodic bare comments as keep-alives.
"""

from __future__ import annotations

import pytest

from wikistream.sources.sse import ServerSentEvent, parse_sse_lines

pytestmark = pytest.mark.unit


def test_single_frame_is_dispatched_on_blank_line():
    events = list(parse_sse_lines(["data: {}", ""]))
    assert events == [ServerSentEvent(data="{}")]


def test_frame_without_trailing_blank_line_is_still_flushed():
    # A clean server-side close ends the byte stream without a blank line. An
    # implementation that only dispatches on a blank line loses one event per
    # disconnect, which at a measured ~1 reconnect per 3 minutes is a slow leak
    # that no error log would ever mention.
    events = list(parse_sse_lines(["data: last"]))
    assert [e.data for e in events] == ["last"]


def test_multi_line_data_is_joined_with_newlines():
    events = list(parse_sse_lines(["data: {", 'data:   "a": 1', "data: }", ""]))
    assert events[0].data == '{\n  "a": 1\n}'


def test_exactly_one_space_after_colon_is_stripped():
    events = list(parse_sse_lines(["data:  two spaces", ""]))
    assert events[0].data == " two spaces"


def test_no_space_after_colon_is_preserved():
    events = list(parse_sse_lines(["data:no-space", ""]))
    assert events[0].data == "no-space"


def test_comment_lines_are_ignored():
    # Wikimedia sends `:ok` immediately on connect.
    events = list(parse_sse_lines([":ok", "data: real", ""]))
    assert [e.data for e in events] == ["real"]


def test_keepalive_frame_with_no_data_is_not_dispatched():
    # A bare comment followed by a blank line must not become an empty row in
    # bronze.
    events = list(parse_sse_lines([":", "", ":", "", "data: real", ""]))
    assert [e.data for e in events] == ["real"]


def test_event_type_is_read_and_resets_between_frames():
    events = list(
        parse_sse_lines(["event: message", "data: a", "", "data: b", ""]),
    )
    assert [(e.event, e.data) for e in events] == [("message", "a"), ("message", "b")]


def test_custom_event_type_does_not_leak_into_the_next_frame():
    events = list(parse_sse_lines(["event: custom", "data: a", "", "data: b", ""]))
    assert [(e.event, e.data) for e in events] == [("custom", "a"), ("message", "b")]


def test_last_event_id_persists_across_frames_without_one():
    # The reconnection point is the last id *seen*, not the last id in the frame
    # being dispatched. Resetting it per frame would make the cursor go
    # backwards whenever the server omitted an id, replaying more than needed.
    events = list(parse_sse_lines(["id: cursor-1", "data: a", "", "data: b", ""]))
    assert [e.last_event_id for e in events] == ["cursor-1", "cursor-1"]


def test_later_id_overrides_an_earlier_one():
    events = list(
        parse_sse_lines(["id: one", "data: a", "", "id: two", "data: b", ""]),
    )
    assert [e.last_event_id for e in events] == ["one", "two"]


def test_id_containing_nul_is_ignored_entirely():
    # The spec says ignore, not truncate. A truncated cursor is worse than no
    # cursor: it resumes at a position that is wrong rather than at the start.
    events = list(
        parse_sse_lines(["id: good", "data: a", "", "id: ba\x00d", "data: b", ""]),
    )
    assert [e.last_event_id for e in events] == ["good", "good"]


def test_retry_is_parsed_as_an_integer():
    events = list(parse_sse_lines(["retry: 5000", "data: a", ""]))
    assert events[0].retry_ms == 5000


def test_non_numeric_retry_is_ignored_rather_than_fatal():
    events = list(parse_sse_lines(["retry: soon", "data: a", ""]))
    assert events[0].retry_ms is None
    assert events[0].data == "a"


def test_field_with_no_colon_is_a_field_with_an_empty_value():
    events = list(parse_sse_lines(["data", ""]))
    assert events[0].data == ""


def test_unknown_fields_are_ignored():
    events = list(parse_sse_lines(["nonsense: 1", "data: a", ""]))
    assert [e.data for e in events] == ["a"]


def test_carriage_returns_are_stripped():
    events = list(parse_sse_lines(["data: a\r", "\r"]))
    assert [e.data for e in events] == ["a"]


def test_empty_input_yields_nothing():
    assert list(parse_sse_lines([])) == []


def test_realistic_wikimedia_frame_sequence():
    lines = [
        ":ok",
        "",
        "event: message",
        'id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":6524211441}]',
        'data: {"meta":{"id":"abc"}}',
        "",
        ":",
        "",
    ]
    events = list(parse_sse_lines(lines))
    assert len(events) == 1
    assert events[0].data == '{"meta":{"id":"abc"}}'
    assert events[0].last_event_id is not None
    assert "6524211441" in events[0].last_event_id
