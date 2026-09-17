"""Structured logging, configured in exactly one function.

Written by hand rather than pulled from a library because the requirement is
small and specific: one JSON object per line on stdout so `docker compose logs`
stays greppable and `jq`-able, with arbitrary key/value context attached to a
record without string formatting.

The alternative — plain text — makes the producer's per-interval throughput
summary unusable: you cannot compute a rate from a sentence.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

#: Attributes `logging.LogRecord` always sets. Anything outside this set was put
#: there by a caller via `extra=` and belongs in the JSON output.
_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Render a log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialise the record, promoting `extra=` keys to top-level fields."""
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO", *, as_json: bool = True) -> None:
    """Install a single stdout handler on the root logger.

    Idempotent: calling it twice does not double every line, which matters
    because Spark's `foreachBatch` closure may run in a worker that has already
    been configured.

    Args:
        level: Minimum level to emit, e.g. ``"INFO"``.
        as_json: JSON lines when true, human-readable text when false. Text is
            easier to read while debugging interactively; JSON is what the
            container logs should carry.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter()
        if as_json
        else logging.Formatter("%(asctime)s %(levelname)-7s %(name)s  %(message)s")
    )
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())

    # py4j logs one line per JVM call at INFO, which drowns everything else.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a module logger. A thin alias, kept so imports read consistently."""
    return logging.getLogger(name)
