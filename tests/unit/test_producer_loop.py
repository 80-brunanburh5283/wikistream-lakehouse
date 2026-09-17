"""Tests for the ingest loop's bounds, shutdown and liveness reporting.

The loop is dull on purpose, so these tests are about the three things that would
be expensive to get wrong: a bounded run must actually stop, stopping must tell
the source rather than just abandoning the iterator mid-frame, and a run must
leave a heartbeat behind even if it ended before the first reporting interval.

The last test in this file is not about the loop at all. It guards the producer
image's import graph, which is a build-time property that only a subprocess can
observe.
"""

from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

import pytest
from doubles import FakeClock, FakeSource, RecordingProducer

from wikistream.config import Settings
from wikistream.producer import __main__ as producer_main
from wikistream.producer.heartbeat import age_seconds
from wikistream.producer.kafka_sink import KafkaSink
from wikistream.sources.base import SourceEvent
from wikistream.sources.jetstream import JetstreamSource
from wikistream.sources.wikimedia import WikimediaSource

pytestmark = pytest.mark.unit


def _settings(tmp_path, **overrides: Any) -> Settings:
    return Settings(
        _env_file=None,
        producer_heartbeat_path=str(tmp_path / "heartbeat.json"),
        **overrides,
    )


def _sink(settings: Settings) -> KafkaSink:
    return KafkaSink(settings, producer=RecordingProducer())


def test_max_events_bounds_the_run(tmp_path):
    settings = _settings(tmp_path)
    source = FakeSource()

    count = producer_main.run(source, _sink(settings), settings, max_events=5)

    assert count == 5
    assert source.frames_yielded == 5


def test_reaching_a_bound_stops_the_source_rather_than_dropping_the_iterator(tmp_path):
    # Abandoning the generator would leave the SSE connection open until the
    # garbage collector got to it, and Wikimedia asks for one connection, closed
    # when you are done with it.
    settings = _settings(tmp_path)
    source = FakeSource()

    producer_main.run(source, _sink(settings), settings, max_events=2)

    assert source.stop_calls == 1


def test_duration_bounds_the_run(tmp_path):
    # A zero-second budget expires on the first check, which exercises the deadline
    # branch without making the test wait for a clock.
    settings = _settings(tmp_path)
    source = FakeSource()

    count = producer_main.run(source, _sink(settings), settings, duration=0.0)

    assert count == 1
    assert source.stop_calls == 1


def test_every_frame_reaches_the_sink(tmp_path):
    settings = _settings(tmp_path)
    sink = _sink(settings)

    producer_main.run(FakeSource(), sink, settings, max_events=7)

    assert sink.stats.produced == 7


def test_a_short_run_still_leaves_a_heartbeat(tmp_path):
    # Three frames is well inside the ten-second reporting interval, so the only
    # heartbeat this run can produce is the final one. Without it, a bounded run in
    # CI would look like a process that never started.
    settings = _settings(tmp_path)

    producer_main.run(FakeSource(), _sink(settings), settings, max_events=3)

    age = age_seconds(settings.producer_heartbeat_path)
    assert age is not None
    assert age < 10


def test_the_heartbeat_records_the_delivery_counters(tmp_path):
    settings = _settings(tmp_path)

    producer_main.run(FakeSource(), _sink(settings), settings, max_events=4)

    body = json.loads((tmp_path / "heartbeat.json").read_text())
    assert body["produced"] == 4
    assert body["schema"] == 1
    assert "unix_time" in body


def test_throughput_is_reported_per_interval_not_since_start_up(tmp_path, caplog, monkeypatch):
    # A since-start-up average decays slowly and still looks plausible ten minutes
    # after the stream went quiet, which is the one moment the number matters. With
    # a clock that jumps five seconds per read and a ten-second interval, every
    # report covers a known window, so the rate can be checked exactly.
    settings = _settings(tmp_path, kafka_stats_interval_seconds=10.0)
    monkeypatch.setattr(producer_main, "time", FakeClock(step=5.0))

    with caplog.at_level("INFO", logger="wikistream.producer"):
        producer_main.run(FakeSource(), _sink(settings), settings, max_events=6)

    reports = [r for r in caplog.records if r.msg == "throughput"]
    assert reports
    # Two frames per ten-second window: 0.2 events/s, not a cumulative average.
    assert [r.events_per_second for r in reports] == [0.2] * len(reports)


def test_the_heartbeat_is_refreshed_at_every_report(tmp_path, monkeypatch):
    # The healthcheck reads the file's mtime, so a loop that reports without
    # touching the heartbeat would go unhealthy while working perfectly.
    settings = _settings(tmp_path, kafka_stats_interval_seconds=10.0)
    monkeypatch.setattr(producer_main, "time", FakeClock(step=5.0))
    writes = []
    monkeypatch.setattr(
        producer_main.heartbeat, "write", lambda path, payload=None: writes.append(payload)
    )

    producer_main.run(FakeSource(), _sink(settings), settings, max_events=6)

    # Three interval writes plus the final one on the way out.
    assert len(writes) == 4
    assert writes[-1]["produced"] == 6


def test_a_malformed_frame_does_not_stop_the_loop(tmp_path):
    # The loop must not be a validation gate. It forwards what arrived; deciding
    # what is well-formed happens in silver, which has a table to put rejects in.
    settings = _settings(tmp_path)
    good = {"meta": {"id": "a", "dt": "2026-09-17T08:00:00Z", "domain": "en.wikipedia.org"}}
    frames = [
        SourceEvent(raw="{not json", payload=None),
        SourceEvent(raw=json.dumps(good), payload=good),
    ]
    sink = _sink(settings)

    count = producer_main.run(FakeSource(frames), sink, settings, max_events=4)

    assert count == 4
    assert sink.stats.produced == 4
    assert sink.stats.unkeyed == 2


def test_build_source_selects_the_configured_implementation():
    assert isinstance(producer_main.build_source(Settings(_env_file=None)), WikimediaSource)
    assert isinstance(
        producer_main.build_source(Settings(_env_file=None, source_name="jetstream")),
        JetstreamSource,
    )


def test_parse_args_defaults_to_an_unbounded_run():
    args = producer_main.parse_args([])

    assert args.duration is None
    assert args.max_events is None
    assert args.flush_timeout == 30.0


def test_the_producer_imports_no_pyspark():
    """The producer image contains no Spark, so importing it must not need any.

    This is a regression guard with a real history: `wikistream.events` once
    imported the Spark schema module for a single tuple of field names, and the
    container failed at start-up with ModuleNotFoundError. A subprocess is the only
    way to ask the question, because pytest has already imported pyspark for the
    schema tests by the time this runs.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import wikistream.producer.__main__; "
            "print([m for m in sys.modules if m.split('.')[0] == 'pyspark'])",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "[]", (
        "the producer's import graph now reaches pyspark, which is not in its "
        f"image: {result.stdout.strip()}"
    )
