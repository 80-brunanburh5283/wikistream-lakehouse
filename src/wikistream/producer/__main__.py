"""`python -m wikistream.producer` — read the firehose, write Kafka, forever.

Or not forever: `--duration` and `--max-events` bound the run, which is what makes
this usable from a test and from CI. An unbounded ingest process is the right
default for a running stack and the wrong one for a pipeline step that has to
finish and report.

The loop is deliberately dull. Everything interesting — reconnects, backoff,
delivery retries, keying — lives in the source and the sink, and this module's
whole job is to connect them, bound them, log them, and shut them down without
losing the tail of the queue.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from types import FrameType

from wikistream.config import Settings, get_settings
from wikistream.logging import configure_logging, get_logger
from wikistream.producer import heartbeat
from wikistream.producer.kafka_sink import KafkaSink
from wikistream.sources.base import EventSource
from wikistream.sources.jetstream import JetstreamSource
from wikistream.sources.wikimedia import WikimediaSource

log = get_logger("wikistream.producer")


def build_source(settings: Settings) -> EventSource:
    """Return the configured source.

    Two implementations, one switch. The fallback exists so an outage upstream is
    a one-variable change rather than a dead demonstration; see
    `wikistream.sources.jetstream` for what it does and does not cover.
    """
    if settings.source_name == "jetstream":
        return JetstreamSource(settings)
    return WikimediaSource(settings)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        prog="python -m wikistream.producer",
        description="Stream public wiki edits into Kafka.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Stop after this many seconds. Unbounded when omitted.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        metavar="N",
        help="Stop after enqueuing this many frames. Unbounded when omitted.",
    )
    parser.add_argument(
        "--flush-timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="How long to wait for the send queue to drain on shutdown.",
    )
    return parser.parse_args(argv)


def run(
    source: EventSource,
    sink: KafkaSink,
    settings: Settings,
    *,
    duration: float | None = None,
    max_events: int | None = None,
) -> int:
    """Pump frames from `source` into `sink` until a bound or a signal stops it.

    Returns the number of frames enqueued. Does not flush — the caller owns
    shutdown, because whether an incomplete flush is fatal depends on why the run
    ended and only the caller knows that.
    """
    deadline = None if duration is None else time.monotonic() + duration
    interval = settings.kafka_stats_interval_seconds
    next_report = time.monotonic() + interval
    started = time.monotonic()
    count = 0
    last_reported_at = started
    last_reported_count = 0

    for event in source.iter_raw():
        sink.send(event)
        count += 1

        now = time.monotonic()
        if now >= next_report:
            _report(source, sink, now - last_reported_at, count - last_reported_count)
            heartbeat.write(settings.producer_heartbeat_path, sink.summary())
            last_reported_at, last_reported_count = now, count
            next_report = now + interval

        if max_events is not None and count >= max_events:
            log.info("reached --max-events", extra={"max_events": max_events})
            source.stop()
            break
        if deadline is not None and now >= deadline:
            log.info("reached --duration", extra={"duration_seconds": duration})
            source.stop()
            break

    # A final heartbeat, so a bounded run leaves a readable record of what it did
    # rather than a file that stops mid-interval.
    heartbeat.write(settings.producer_heartbeat_path, sink.summary())
    log.info(
        "ingest loop finished",
        extra={
            "seconds": round(time.monotonic() - started, 2),
            "frames": count,
            **sink.summary(),
        },
    )
    return count


def _report(source: EventSource, sink: KafkaSink, seconds: float, frames: int) -> None:
    """Log one throughput line.

    Rates are computed over the interval rather than since start-up. A
    since-start-up average hides a stall: it decays slowly and still looks
    plausible ten minutes after the stream went quiet.
    """
    stats = source.stats
    log.info(
        "throughput",
        extra={
            "events_per_second": round(frames / seconds, 1) if seconds > 0 else 0.0,
            "source_events": stats.events,
            "source_malformed": stats.malformed,
            "source_reconnects": stats.reconnects,
            "source_bytes": stats.bytes_received,
            "queue_depth": sink.queue_depth,
            "in_flight": sink.stats.in_flight,
            **sink.summary(),
        },
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)

    source = build_source(settings)
    sink = KafkaSink(settings)

    def _shutdown(signum: int, frame: FrameType | None) -> None:
        # Only sets a flag. Flushing Kafka from inside a signal handler would run
        # librdkafka callbacks on an interrupted stack; the loop exits on its own
        # and the flush happens below, on the main path.
        del frame
        log.info("shutdown requested", extra={"signal": signal.Signals(signum).name})
        source.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info(
        "producer starting",
        extra={
            "source": source.name,
            "topic": settings.kafka_topic,
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "duration_seconds": args.duration,
            "max_events": args.max_events,
        },
    )

    try:
        run(
            source,
            sink,
            settings,
            duration=args.duration,
            max_events=args.max_events,
        )
    finally:
        # In the finally block so that a fatal upstream status — a renamed stream,
        # a 403 — still delivers whatever is already queued before the process
        # exits. Losing the tail on the way out would be a self-inflicted gap.
        sink.close(args.flush_timeout)

    if sink.stats.failed:
        log.error("run completed with delivery failures", extra=sink.summary())
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
