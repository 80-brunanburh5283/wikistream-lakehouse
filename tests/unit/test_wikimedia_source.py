"""Tests for the Wikimedia source's connection lifecycle, with no network.

An `httpx.MockTransport` stands in for the firehose, which makes the interesting
behaviour cheap to assert: that a 5xx is retried and a 404 is not, that
`Last-Event-ID` is replayed on reconnect, and — the one that justifies the whole
downstream design — that a reconnect can hand back events the client has already
seen.
"""

from __future__ import annotations

import json

import httpx
import pytest

from wikistream.config import USER_AGENT, Settings
from wikistream.sources.wikimedia import WikimediaSource

pytestmark = pytest.mark.unit


def _settings(**overrides) -> Settings:
    base = {
        # Sub-millisecond backoff: these tests exercise the retry path, and there
        # is no reason to spend real seconds proving it.
        "source_backoff_initial_seconds": 0.001,
        "source_backoff_max_seconds": 0.002,
    }
    return Settings(**{**base, **overrides})


def _sse_body(events: list[dict], *, cursor: str | None = None) -> str:
    frames = []
    for event in events:
        frame = []
        if cursor is not None:
            frame.append(f"id: {cursor}")
        frame.append(f"data: {json.dumps(event)}")
        frames.append("\n".join(frame))
    return ":ok\n\n" + "\n\n".join(frames) + "\n\n"


def _event(event_id: str, domain: str = "en.wikipedia.org") -> dict:
    return {
        "meta": {"id": event_id, "dt": "2026-09-17T04:00:00.000Z", "domain": domain},
        "type": "edit",
        "title": event_id,
    }


class _Responder:
    """Serves a scripted sequence of responses, recording the requests it saw."""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self._responses) - 1)
        outcome = self._responses[index]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _take(source: WikimediaSource, count: int) -> list[dict]:
    """Pull exactly `count` events, then stop the source."""
    collected = []
    for event in source.iter_events():
        collected.append(event)
        if len(collected) >= count:
            source.stop()
            break
    return collected


def test_events_are_decoded_from_a_single_connection():
    responder = _Responder(httpx.Response(200, text=_sse_body([_event("a"), _event("b")])))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    events = _take(source, 2)

    assert [event["meta"]["id"] for event in events] == ["a", "b"]
    assert source.stats.events == 2
    assert source.stats.malformed == 0


def test_required_headers_are_sent():
    responder = _Responder(httpx.Response(200, text=_sse_body([_event("a")])))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 1)

    headers = responder.requests[0].headers
    # Wikimedia asks clients to identify themselves; the project and its URL are
    # both in the string so an operator can find whoever is responsible.
    assert headers["user-agent"] == USER_AGENT
    assert "wikistream-lakehouse" in headers["user-agent"]
    assert "https://github.com/" in headers["user-agent"]
    assert headers["accept"] == "text/event-stream"
    assert headers["cache-control"] == "no-cache"


def test_no_last_event_id_is_sent_on_the_first_connection():
    responder = _Responder(httpx.Response(200, text=_sse_body([_event("a")])))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 1)

    assert "last-event-id" not in responder.requests[0].headers


def test_cursor_is_replayed_as_last_event_id_after_a_reconnect():
    first = httpx.Response(200, text=_sse_body([_event("a")], cursor="cursor-1"))
    second = httpx.Response(200, text=_sse_body([_event("b")], cursor="cursor-2"))
    responder = _Responder(first, second)
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 2)

    assert len(responder.requests) == 2
    assert responder.requests[1].headers["last-event-id"] == "cursor-1"
    assert source.cursor == "cursor-2"
    assert source.stats.last_cursor == "cursor-2"


def test_reconnect_can_replay_events_already_seen():
    # This is the whole reason bronze is append-only and silver upserts. The
    # server resumes at or before the cursor, so the client legitimately receives
    # event "b" twice. Nothing here is a bug; the deduplication downstream exists
    # because this is correct behaviour.
    first = httpx.Response(200, text=_sse_body([_event("a"), _event("b")], cursor="c1"))
    second = httpx.Response(200, text=_sse_body([_event("b"), _event("c")], cursor="c2"))
    responder = _Responder(first, second)
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    events = _take(source, 4)

    ids = [event["meta"]["id"] for event in events]
    assert ids == ["a", "b", "b", "c"]
    assert len(set(ids)) == 3


def test_a_clean_stream_close_is_treated_as_abnormal_and_retried():
    # The firehose is unbounded, so a clean end of body is not a normal outcome.
    # Reconnecting instantly on it would be a tight loop against a server that is
    # closing connections as fast as it accepts them.
    responder = _Responder(
        httpx.Response(200, text=_sse_body([_event("a")])),
        httpx.Response(200, text=_sse_body([_event("b")])),
    )
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 2)

    assert source.stats.reconnects >= 1


def test_server_error_is_retried():
    responder = _Responder(
        httpx.Response(503, text="unavailable"),
        httpx.Response(200, text=_sse_body([_event("a")])),
    )
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    events = _take(source, 1)

    assert [event["meta"]["id"] for event in events] == ["a"]
    assert source.stats.reconnects == 1


def test_transport_error_is_retried():
    responder = _Responder(
        httpx.ConnectError("connection refused"),
        httpx.Response(200, text=_sse_body([_event("a")])),
    )
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    events = _take(source, 1)

    assert [event["meta"]["id"] for event in events] == ["a"]
    assert source.stats.reconnects == 1


def test_read_timeout_is_retried():
    responder = _Responder(
        httpx.ReadTimeout("read timed out"),
        httpx.Response(200, text=_sse_body([_event("a")])),
    )
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    assert len(_take(source, 1)) == 1


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
def test_fatal_statuses_propagate_instead_of_looping(status):
    # Retrying these cannot help. A producer that retries a 404 forever looks
    # healthy in every dashboard while delivering nothing, which is strictly
    # worse than a process that exits and gets noticed.
    responder = _Responder(httpx.Response(status, text="nope"))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    with pytest.raises(httpx.HTTPStatusError):
        _take(source, 1)


def test_malformed_frame_is_counted_and_skipped():
    body = ":ok\n\ndata: {not json}\n\ndata: " + json.dumps(_event("a")) + "\n\n"
    responder = _Responder(httpx.Response(200, text=body))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    events = _take(source, 1)

    assert [event["meta"]["id"] for event in events] == ["a"]
    assert source.stats.malformed == 1


def test_keepalive_comments_do_not_become_events():
    body = ":ok\n\n:\n\n:\n\ndata: " + json.dumps(_event("a")) + "\n\n"
    responder = _Responder(httpx.Response(200, text=body))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    assert len(_take(source, 1)) == 1
    assert source.stats.events == 1


def test_stop_before_iteration_yields_nothing():
    responder = _Responder(httpx.Response(200, text=_sse_body([_event("a")])))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))
    source.stop()

    assert list(source.iter_events()) == []
    assert responder.requests == []


def test_backoff_does_not_reset_on_a_connection_that_delivers_nothing():
    # A server that accepts a connection and immediately closes it would reset
    # the schedule on every attempt if the reset were tied to connecting rather
    # than to receiving. That turns the backoff into a tight loop precisely when
    # the upstream is least able to cope.
    responder = _Responder(
        httpx.Response(200, text=":ok\n\n"),
        httpx.Response(200, text=":ok\n\n"),
        httpx.Response(200, text=":ok\n\n"),
        httpx.Response(200, text=_sse_body([_event("a")])),
    )
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 1)

    assert source.stats.reconnects == 3


def test_bytes_received_is_tracked():
    responder = _Responder(httpx.Response(200, text=_sse_body([_event("a")])))
    source = WikimediaSource(_settings(), transport=httpx.MockTransport(responder))

    _take(source, 1)

    assert source.stats.bytes_received == len(json.dumps(_event("a")))


def test_source_satisfies_the_event_source_protocol():
    from wikistream.sources.base import EventSource

    assert isinstance(WikimediaSource(_settings()), EventSource)
