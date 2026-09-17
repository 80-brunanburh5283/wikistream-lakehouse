"""Test doubles shared between test modules.

Not in `conftest.py` because these are classes, not fixtures, and a fixture that
exists only to hand back a class is indirection with nothing in it. `tests` is on
`sys.path` via the `pythonpath` setting in pyproject.toml, so `from doubles import
...` works from any test module.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from wikistream.sources.base import SourceEvent, SourceStats


class RecordingProducer:
    """A `_ProducerLike` double that queues locally and delivers on demand.

    Deliberately not a Mock: the interesting cases are about *sequences* — enqueue,
    then fill, then drain — and asserting on a call list is a weaker statement than
    asserting on the state a real sequence would leave behind.

    The two-stage queue models the one librdkafka behaviour the sink's counters
    depend on: a record is not acknowledgeable the instant it is produced. With
    `linger.ms=100` it sits in a batch first, which is why `in_flight` is normally
    non-zero in a healthy run and why a double that delivered immediately would
    make the `produced`/`delivered` distinction untestable.
    """

    def __init__(self, *, capacity: int | None = None) -> None:
        #: (topic, key, value) for every accepted produce call.
        self.messages: list[tuple[str, bytes | None, bytes]] = []
        #: How many records fit before `produce` raises BufferError, as librdkafka
        #: does when `queue.buffering.max.kbytes` is reached.
        self.capacity = capacity
        self.poll_calls: list[float] = []
        self.flush_calls: list[float] = []
        #: Just enqueued; not acknowledgeable until a poll has gone by.
        self._queued: list[tuple[Any, Any]] = []
        #: Acknowledgeable on the next poll.
        self._ready: list[tuple[Any, Any]] = []
        #: Set to an error object to make every subsequent delivery fail.
        self.delivery_error: Any = None
        #: Records that flush() will refuse to drain, simulating a timeout.
        self.undrainable = 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self.capacity is not None and len(self) >= self.capacity:
            raise BufferError("Local: Queue full")
        self.messages.append((topic, kwargs["key"], kwargs["value"]))
        self._queued.append((kwargs["on_delivery"], self.delivery_error))

    def poll(self, timeout: float) -> int:
        self.poll_calls.append(timeout)
        served, self._ready = self._ready, []
        for callback, err in served:
            callback(err, object())
        self._ready, self._queued = self._queued, []
        return len(served)

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        # Twice, because one poll only drains what was already acknowledgeable and
        # flush is supposed to drain everything.
        self.poll(0.0)
        self.poll(0.0)
        return self.undrainable

    def __len__(self) -> int:
        return len(self._queued) + len(self._ready)


class FakeSource:
    """An `EventSource` that yields a fixed frame until asked to stop.

    Infinite by construction, because that is what a firehose is and because a
    finite fake would let a broken bound pass: a loop that never checks
    `max_events` still terminates against a list of ten events.
    """

    name = "fake"

    def __init__(self, frames: list[SourceEvent] | None = None) -> None:
        payload: dict[str, Any] = {
            "meta": {"id": "id-0", "dt": "2026-09-17T08:00:00Z", "domain": "en.wikipedia.org"}
        }
        self._frames = frames or [SourceEvent(raw=json.dumps(payload), payload=payload)]
        self._stats = SourceStats()
        self._stopped = False
        self.stop_calls = 0
        self.frames_yielded = 0

    @property
    def cursor(self) -> str | None:
        return self._stats.last_cursor

    @property
    def stats(self) -> SourceStats:
        return self._stats

    def iter_raw(self) -> Iterator[SourceEvent]:
        while not self._stopped:
            frame = self._frames[self.frames_yielded % len(self._frames)]
            self.frames_yielded += 1
            self._stats.events += 1
            self._stats.bytes_received += len(frame.raw)
            yield frame

    def iter_events(self) -> Iterator[dict[str, Any]]:
        for event in self.iter_raw():
            if event.payload is not None:
                yield event.payload

    def stop(self) -> None:
        self.stop_calls += 1
        self._stopped = True


class FakeClock:
    """A monotonic clock that advances by a fixed step on every read.

    Substituted for the `time` module inside a module under test. Reporting
    intervals are the one part of the loop whose behaviour depends on elapsed time,
    and a test that waits for a real clock is a test that is either slow or flaky.
    """

    def __init__(self, step: float = 5.0, start: float = 0.0) -> None:
        self.step = step
        self.now = start

    def monotonic(self) -> float:
        self.now += self.step
        return self.now
