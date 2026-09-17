"""Bluesky Jetstream fallback source.

Exists so that a single upstream is not a single point of failure for the whole
demonstration. If Wikimedia's EventStreams is unreachable, rate-limits this
client, or changes shape, `WS_SOURCE_NAME=jetstream` switches the producer over
without touching the producer.

Jetstream is a public, unauthenticated firehose of Bluesky repository commits.
Like EventStreams it is resumable, and like EventStreams the resumption is
inclusive rather than exclusive — reconnecting at a cursor replays the event at
that cursor. So both sources produce duplicates on reconnect, and the
deduplication downstream is not specific to either.

Two honest notes:

* This source is implemented, connection-tested, and not used by the shipped
  pipeline. The Iceberg schema, the dbt marts and the asset checks are all
  written for `recentchange`, so switching sources gets bytes into Kafka but does
  not populate the wiki marts. It is a documented escape hatch, not a second
  supported mode.
* Jetstream's event identity is weaker than Wikimedia's. There is no UUID; the
  key used here is the pair `(did, time_us)`, hashed. See `event_key` below.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

from websockets.exceptions import WebSocketException
from websockets.sync.client import connect

from wikistream.config import USER_AGENT, Settings
from wikistream.logging import get_logger
from wikistream.sources.backoff import ExponentialBackoff
from wikistream.sources.base import SourceStats

log = get_logger(__name__)


class JetstreamSource:
    """Yields Bluesky commit events, reconnecting with backoff indefinitely."""

    name = "jetstream"

    def __init__(self, settings: Settings) -> None:
        """Build the source. No network activity until `iter_events` is called."""
        self._settings = settings
        self._url = settings.jetstream_url
        self._cursor: str | None = None
        self._stats = SourceStats()
        self._stop_event = threading.Event()
        self._backoff = ExponentialBackoff(
            settings.source_backoff_initial_seconds,
            settings.source_backoff_max_seconds,
        )

    @property
    def cursor(self) -> str | None:
        """Last `time_us` seen, as a string, replayed as `?cursor=` on reconnect."""
        return self._cursor

    @property
    def stats(self) -> SourceStats:
        """Live counters for the producer's throughput log."""
        return self._stats

    def stop(self) -> None:
        """Signal the iterator to finish. Safe to call from a signal handler."""
        self._stop_event.set()

    def _resume_url(self) -> str:
        """Add or replace the `cursor` query parameter with the current position.

        Rebuilt through `urlparse` rather than string-concatenated because the
        configured URL already carries a `wantedCollections` parameter, so naive
        appending produces a second `?` and a silently unfiltered subscription.
        """
        if self._cursor is None:
            return self._url
        parts = urlparse(self._url)
        params = [
            (key, value)
            for key, value in (
                pair.split("=", 1) if "=" in pair else (pair, "")
                for pair in parts.query.split("&")
                if pair
            )
            if key != "cursor"
        ]
        params.append(("cursor", self._cursor))
        return urlunparse(parts._replace(query=urlencode(params)))

    def iter_events(self) -> Iterator[dict[str, Any]]:
        """Yield decoded events until `stop` is called, absorbing transient faults."""
        while not self._stop_event.is_set():
            try:
                yield from self._stream_once()
            except (WebSocketException, OSError, json.JSONDecodeError) as exc:
                self._sleep_before_retry(f"{type(exc).__name__}: {exc}")
            else:
                if not self._stop_event.is_set():
                    self._sleep_before_retry("stream closed by server")

    def _stream_once(self) -> Iterator[dict[str, Any]]:
        """Hold one WebSocket open, yielding events until it closes."""
        url = self._resume_url()
        log.info(
            "connecting to jetstream",
            extra={"resuming": self._cursor is not None},
        )
        with connect(
            url,
            user_agent_header=USER_AGENT,
            open_timeout=self._settings.source_connect_timeout_seconds,
            close_timeout=self._settings.source_connect_timeout_seconds,
            # Jetstream is quiet between commits on a filtered subscription, so a
            # read timeout would fire on a healthy connection. The library's ping
            # keepalive detects a genuinely dead peer instead.
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as socket:
            first_event = True
            for message in socket:
                if self._stop_event.is_set():
                    return
                payload = message.decode() if isinstance(message, bytes) else message
                self._stats.bytes_received += len(payload)

                try:
                    event: dict[str, Any] = json.loads(payload)
                except json.JSONDecodeError:
                    self._stats.malformed += 1
                    log.warning(
                        "discarding malformed message",
                        extra={"malformed_total": self._stats.malformed},
                    )
                    continue

                time_us = event.get("time_us")
                if isinstance(time_us, int):
                    self._cursor = str(time_us)
                    self._stats.last_cursor = self._cursor

                if first_event:
                    self._backoff.reset()
                    first_event = False
                self._stats.events += 1
                yield event

    def _sleep_before_retry(self, reason: str) -> None:
        """Log the fault and wait, waking early if shutdown is requested."""
        delay = self._backoff.next_delay()
        self._stats.reconnects += 1
        log.warning(
            "jetstream interrupted, reconnecting",
            extra={
                "reason": reason,
                "delay_seconds": round(delay, 2),
                "attempt": self._backoff.attempt,
                "reconnects_total": self._stats.reconnects,
                "events_so_far": self._stats.events,
            },
        )
        self._stop_event.wait(delay)


def event_key(event: dict[str, Any]) -> str | None:
    """Best available identity for a Jetstream event.

    Bluesky gives no event UUID. The commit CID identifies the *record* written,
    which is close but not the same thing: a record can be written, deleted and
    rewritten. `(did, time_us)` identifies the *emission* — which repository, at
    which microsecond — and Jetstream's cursor is that same `time_us`, so replay
    after reconnect produces byte-identical keys. That is the property
    deduplication needs.

    Returns None when either component is missing, which routes the event to
    quarantine rather than giving it a key that collides with another event's.
    """
    did = event.get("did")
    time_us = event.get("time_us")
    if not isinstance(did, str) or not did or not isinstance(time_us, int):
        return None
    return f"{did}:{time_us}"
