"""Wikimedia EventStreams `recentchange` source.

One long-lived HTTP connection to a public firehose. The endpoint is documented as
open for exactly this use; Wikimedia asks in return that clients identify
themselves with a real User-Agent and that they reconnect politely rather than
reopening in a loop. Both are implemented here.

The reconnect path is the origin of the whole correctness story downstream. On
reconnect the client sends `Last-Event-ID`, and the server resumes from *at or
before* that position — so a redelivery of events already seen is normal, expected
behaviour, not a bug. That is why bronze is append-only and why silver upserts
rather than inserts.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from typing import Any

import httpx

from wikistream.config import USER_AGENT, Settings
from wikistream.logging import get_logger
from wikistream.sources.backoff import ExponentialBackoff
from wikistream.sources.base import SourceEvent, SourceStats
from wikistream.sources.sse import parse_sse_lines

log = get_logger(__name__)

#: HTTP statuses where retrying the same request cannot help. 404 means the
#: stream was renamed, 401/403 mean the endpoint now wants credentials — both are
#: for a human to look at, so the process exits instead of retrying forever and
#: looking healthy.
_FATAL_STATUSES = frozenset({400, 401, 403, 404, 410})


class WikimediaSource:
    """Yields `recentchange` events, reconnecting with backoff indefinitely."""

    name = "wikimedia"

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Build the source. No network activity until `iter_events` is called.

        Args:
            settings: Timeouts, backoff bounds and the stream URL.
            transport: Injected HTTP transport. Production passes None and gets
                httpx's default. Tests pass an `httpx.MockTransport` so the
                reconnect-and-replay behaviour — the reason the pipeline
                deduplicates at all — can be asserted without a network.
        """
        self._settings = settings
        self._transport = transport
        self._url = settings.wikimedia_stream_url
        self._cursor: str | None = None
        self._stats = SourceStats()
        self._stop_event = threading.Event()
        self._backoff = ExponentialBackoff(
            settings.source_backoff_initial_seconds,
            settings.source_backoff_max_seconds,
        )

    @property
    def cursor(self) -> str | None:
        """Last `id:` seen, echoed as `Last-Event-ID` on reconnect."""
        return self._cursor

    @property
    def stats(self) -> SourceStats:
        """Live counters for the producer's throughput log."""
        return self._stats

    def stop(self) -> None:
        """Signal the iterator to finish. Safe to call from a signal handler."""
        self._stop_event.set()

    def iter_events(self) -> Iterator[dict[str, Any]]:
        """Yield decoded events until `stop` is called, skipping unreadable frames."""
        for item in self.iter_raw():
            if item.payload is not None:
                yield item.payload

    def iter_raw(self) -> Iterator[SourceEvent]:
        """Yield every frame until `stop` is called, decoded where possible.

        Transient faults are absorbed: a dropped connection, a read timeout or a
        5xx becomes a logged reconnect with a backoff delay. Only a status in
        `_FATAL_STATUSES` propagates, because retrying those is pointless and a
        silently retrying producer is worse than a crashed one.
        """
        timeout = httpx.Timeout(
            connect=self._settings.source_connect_timeout_seconds,
            read=self._settings.source_read_timeout_seconds,
            write=self._settings.source_connect_timeout_seconds,
            pool=self._settings.source_connect_timeout_seconds,
        )
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            transport=self._transport,
        ) as client:
            while not self._stop_event.is_set():
                try:
                    yield from self._stream_once(client)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in _FATAL_STATUSES:
                        raise
                    self._sleep_before_retry(f"HTTP {exc.response.status_code}")
                except (httpx.HTTPError, json.JSONDecodeError, OSError) as exc:
                    self._sleep_before_retry(f"{type(exc).__name__}: {exc}")
                else:
                    # A clean end of stream is not an error, but it is not normal
                    # either — the firehose is unbounded. Back off rather than
                    # reconnecting instantly.
                    if not self._stop_event.is_set():
                        self._sleep_before_retry("stream closed by server")

    def _stream_once(self, client: httpx.Client) -> Iterator[SourceEvent]:
        """Hold one connection open, yielding frames until it ends."""
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/event-stream",
            # Any intermediary buffering this would defeat the point of streaming.
            "Cache-Control": "no-cache",
        }
        if self._cursor is not None:
            headers["Last-Event-ID"] = self._cursor

        log.info(
            "connecting to event stream",
            extra={"url": self._url, "resuming": self._cursor is not None},
        )
        with client.stream("GET", self._url, headers=headers) as response:
            response.raise_for_status()
            first_event = True
            for frame in parse_sse_lines(response.iter_lines()):
                if self._stop_event.is_set():
                    return
                if frame.last_event_id is not None:
                    self._cursor = frame.last_event_id
                    self._stats.last_cursor = frame.last_event_id
                self._stats.bytes_received += len(frame.data)

                if first_event:
                    # Reset on the first frame of any kind: a connection that is
                    # accepted and closed without delivering anything must not
                    # clear the backoff, but one delivering even a bad frame is
                    # demonstrably alive.
                    self._backoff.reset()
                    first_event = False

                payload: dict[str, Any] | None
                try:
                    payload = json.loads(frame.data)
                except json.JSONDecodeError:
                    # Counted and forwarded, not dropped. One unparseable frame
                    # in a firehose must not stop the process, and it must not
                    # vanish either: the quarantine table exists to hold exactly
                    # this, and a frame discarded here would never reach it.
                    payload = None
                    self._stats.malformed += 1
                    log.warning(
                        "frame did not decode, forwarding raw",
                        extra={"malformed_total": self._stats.malformed},
                    )
                else:
                    if not isinstance(payload, dict):
                        # Valid JSON that is not an object — a bare number or an
                        # array. Nothing downstream can key it, so it travels the
                        # same path as undecodable input.
                        payload = None
                        self._stats.malformed += 1
                    else:
                        self._stats.events += 1

                yield SourceEvent(raw=frame.data, payload=payload, cursor=self._cursor)

    def _sleep_before_retry(self, reason: str) -> None:
        """Log the fault and wait, waking early if shutdown is requested."""
        delay = self._backoff.next_delay()
        self._stats.reconnects += 1
        log.warning(
            "stream interrupted, reconnecting",
            extra={
                "reason": reason,
                "delay_seconds": round(delay, 2),
                "attempt": self._backoff.attempt,
                "reconnects_total": self._stats.reconnects,
                "events_so_far": self._stats.events,
            },
        )
        # wait() rather than sleep() so SIGTERM during a 60-second backoff does
        # not leave the container hanging until Docker's kill timeout.
        self._stop_event.wait(delay)
