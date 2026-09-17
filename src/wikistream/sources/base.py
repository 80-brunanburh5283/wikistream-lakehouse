"""The interface every event source implements.

Two sources exist: Wikimedia's `recentchange` SSE firehose (primary) and
Bluesky's Jetstream WebSocket firehose (fallback, if Wikimedia becomes
unreachable or changes shape). Putting them behind one protocol means switching
is a config change rather than a rewrite of the producer.

The protocol is deliberately narrow. A source yields decoded JSON objects and
tracks a resumption cursor; everything else — keying, batching, delivery
guarantees — belongs to the sink, which is the only component that knows what
Kafka wants.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class SourceStats:
    """Counters a source keeps about its own health.

    Exists because "the producer is running" is not the same as "the producer is
    receiving data", and the difference is only visible in these numbers. A
    climbing `reconnects` with a flat `events` is a stalled upstream; a climbing
    `malformed` is a schema change.
    """

    events: int = 0
    #: Frames that could not be JSON-decoded. Counted and skipped, never fatal:
    #: one bad frame in a firehose must not take the process down.
    malformed: int = 0
    #: Every reconnect is a place duplicates can enter the pipeline, which is why
    #: this number is worth surfacing rather than hiding.
    reconnects: int = 0
    bytes_received: int = 0
    last_cursor: str | None = field(default=None)


@runtime_checkable
class EventSource(Protocol):
    """A resumable, indefinitely long stream of JSON events."""

    @property
    def name(self) -> str:
        """Short identifier used in logs and metrics, e.g. ``"wikimedia"``."""
        ...

    @property
    def cursor(self) -> str | None:
        """Opaque position token to resume from, or None before the first event.

        Opaque is the operative word. Wikimedia's is a JSON array of upstream
        Kafka coordinates and Jetstream's is a microsecond integer; neither
        should be parsed by anything but the source that produced it.
        """
        ...

    @property
    def stats(self) -> SourceStats:
        """Live counters. Read by the producer for its throughput log."""
        ...

    def iter_events(self) -> Iterator[dict[str, Any]]:
        """Yield decoded events forever, reconnecting as needed.

        Implementations must not raise on a transient network fault or a single
        malformed frame — they reconnect with backoff and count the failure. The
        iterator ends only on a shutdown request.
        """
        ...

    def stop(self) -> None:
        """Ask the iterator to finish at the next opportunity.

        Called from a signal handler, so it must be safe to call from a different
        thread than `iter_events` and must not block.
        """
        ...
