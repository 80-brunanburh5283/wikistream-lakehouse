"""A Server-Sent Events line parser, written as a pure function.

Pulled out of the HTTP client so the parsing rules can be tested against a list
of strings with no socket involved. The rules are small but every one of them has
a way of biting: a `data:` field spread over several lines, the single optional
space after the colon, comment frames used as keep-alives, and a field with no
colon at all.

Reference: WHATWG HTML, "Server-sent events", section 9.2.6.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ServerSentEvent:
    """One dispatched SSE frame."""

    data: str
    event: str = "message"
    #: Wikimedia puts a JSON array of upstream Kafka coordinates here. It is
    #: echoed back verbatim in `Last-Event-ID` and never parsed.
    last_event_id: str | None = None
    #: Server's requested reconnect delay in milliseconds, if it sent one.
    retry_ms: int | None = None


def parse_sse_lines(lines: Iterable[str]) -> Iterator[ServerSentEvent]:
    """Turn a stream of decoded lines into dispatched events.

    Args:
        lines: Lines with their trailing newline already removed, as
            `httpx.Response.iter_lines` yields them.

    Yields:
        One `ServerSentEvent` per blank-line-terminated frame that carried data.

    Frames with no `data` field are not dispatched. Wikimedia sends `:ok` on
    connect and a bare comment periodically as a keep-alive; treating those as
    events would put empty rows into bronze.
    """
    data_lines: list[str] = []
    event_type = "message"
    last_event_id: str | None = None
    retry_ms: int | None = None

    for raw in lines:
        line = raw.rstrip("\r")

        if not line:
            if data_lines:
                yield ServerSentEvent(
                    data="\n".join(data_lines),
                    event=event_type,
                    last_event_id=last_event_id,
                    retry_ms=retry_ms,
                )
            data_lines = []
            event_type = "message"
            retry_ms = None
            # last_event_id deliberately persists across frames: the spec keeps
            # the last id seen as the reconnection point even for frames that
            # do not carry one.
            continue

        if line.startswith(":"):
            continue

        field, _, value = line.partition(":")
        # Exactly one leading space after the colon is part of the syntax, not
        # part of the value. Two spaces means the value starts with a space.
        value = value[1:] if value.startswith(" ") else value

        if field == "data":
            data_lines.append(value)
        elif field == "event":
            event_type = value
        elif field == "id":
            # The spec says to ignore an id containing a NUL rather than
            # truncating it, because a truncated cursor resumes in the wrong place.
            if "\x00" not in value:
                last_event_id = value
        elif field == "retry":
            # A non-numeric retry is ignored rather than fatal.
            with contextlib.suppress(ValueError):
                retry_ms = int(value)

    # A stream that ends without a trailing blank line leaves a frame buffered.
    # Dropping it silently loses an event on every clean disconnect.
    if data_lines:
        yield ServerSentEvent(
            data="\n".join(data_lines),
            event=event_type,
            last_event_id=last_event_id,
            retry_ms=retry_ms,
        )
