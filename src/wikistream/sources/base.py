"""The interface every event source implements.

Two sources exist: Wikimedia's `recentchange` SSE firehose (primary) and
Bluesky's Jetstream WebSocket firehose (fallback, if Wikimedia becomes
unreachable or changes shape). Putting them behind one protocol means switching
is a config change rather than a rewrite of the producer.

The protocol is deliberately narrow. A source yields frames and tracks a
resumption cursor; everything else — keying, batching, delivery guarantees —
belongs to the sink, which is the only component that knows what Kafka wants.

There are two ways to read a source and the difference matters. `iter_raw` gives
every frame that arrived, verbatim, including ones that are not valid JSON.
`iter_events` gives only the frames that decoded, as dicts. The producer uses the
first, because ingest must not silently discard what it cannot understand; the
capture and measurement scripts use the second, because they are doing analysis
and a frame they cannot read is of no use to them.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class SourceEvent:
    """One frame as it arrived, with the decode attempt attached."""

    #: The frame body verbatim, *not* re-serialised from `payload`. The producer
    #: forwards these bytes to Kafka unchanged and bronze keeps them as the audit
    #: record, so a round trip through a dict — which would rewrite key order and
    #: number formatting — would mean bronze no longer holds what the wire held.
    raw: str
    #: None when the frame would not decode. Such frames are still yielded here.
    #: Dropping them would make ingest a validation gate wearing an audit trail's
    #: clothes, and would make the quarantine path downstream unreachable in
    #: production — a correctness feature that can never fire is not one.
    payload: dict[str, Any] | None
    #: The source's resumption position *as of this frame*, so a consumer can
    #: record how far it has durably got without reaching back into the source.
    cursor: str | None = None


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

    def iter_raw(self) -> Iterator[SourceEvent]:
        """Yield every frame forever, reconnecting as needed.

        Implementations must not raise on a transient network fault or a single
        malformed frame — they reconnect with backoff and count the failure. The
        iterator ends only on a shutdown request.
        """
        ...

    def iter_events(self) -> Iterator[dict[str, Any]]:
        """Yield only the frames that decoded, as dicts.

        A convenience view over `iter_raw` for callers doing analysis in Python.
        Not what the producer uses; see this module's docstring.
        """
        ...

    def stop(self) -> None:
        """Ask the iterator to finish at the next opportunity.

        Called from a signal handler, so it must be safe to call from a different
        thread than `iter_events` and must not block.
        """
        ...
