"""Tests for the heartbeat file and the container healthcheck that reads it.

A healthcheck is a claim about liveness, and an untested one is a claim nobody
checked. The cases that matter are the boundaries: absent, fresh, stale — and the
atomicity of the write, because a healthcheck that reads a half-written file
reports a fault that does not exist.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from wikistream.config import Settings, get_settings
from wikistream.producer import healthcheck
from wikistream.producer.heartbeat import age_seconds, write

pytestmark = pytest.mark.unit


def test_write_then_read_gives_a_small_age(tmp_path):
    path = tmp_path / "beat"

    write(path)

    age = age_seconds(path)
    assert age is not None
    assert age < 5


def test_an_absent_heartbeat_reads_as_none_not_as_old(tmp_path):
    # The caller has to tell "never started" from "started and stopped", so these
    # cannot collapse into one very large number.
    assert age_seconds(tmp_path / "nothing") is None


def test_the_payload_is_recorded_for_a_human_reading_the_file(tmp_path):
    path = tmp_path / "beat"

    write(path, {"produced": 42, "delivered": 41})

    body = json.loads(path.read_text())
    assert body["produced"] == 42
    assert body["pid"] == os.getpid()
    assert body["schema"] == 1


def test_the_write_leaves_no_temp_file_behind(tmp_path):
    # The temp file is an implementation detail of atomicity; if it survives, the
    # rename did not happen and the heartbeat is not atomic.
    write(tmp_path / "beat", {"produced": 1})
    write(tmp_path / "beat", {"produced": 2})

    assert sorted(p.name for p in tmp_path.iterdir()) == ["beat"]


def test_missing_parent_directories_are_created(tmp_path):
    path = tmp_path / "nested" / "deeper" / "beat"

    write(path)

    assert path.exists()


def test_a_non_serialisable_payload_does_not_break_the_write(tmp_path):
    # The heartbeat is diagnostics. Raising here would take down a healthy producer
    # to protect a log file, so `default=str` covers whatever a caller passes.
    path = tmp_path / "beat"

    write(path, {"when": object()})

    assert "when" in json.loads(path.read_text())


def _configure(monkeypatch, tmp_path, *, timeout: float = 180.0) -> str:
    path = str(tmp_path / "beat")
    monkeypatch.setenv("WS_PRODUCER_HEARTBEAT_PATH", path)
    monkeypatch.setenv("WS_PRODUCER_HEARTBEAT_TIMEOUT_SECONDS", str(timeout))
    # get_settings is lru_cached for the process, so a test that changes the
    # environment has to invalidate it — both before, so it does not read a cache
    # some earlier test filled, and after, so it does not leave one behind.
    get_settings.cache_clear()
    return path


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    yield
    get_settings.cache_clear()


def test_healthcheck_passes_on_a_fresh_heartbeat(monkeypatch, tmp_path, capsys):
    path = _configure(monkeypatch, tmp_path)
    write(path)

    assert healthcheck.main() == 0
    assert "healthy" in capsys.readouterr().out


def test_healthcheck_fails_when_the_heartbeat_is_missing(monkeypatch, tmp_path, capsys):
    # Missing is unhealthy rather than "not started yet": Docker's --start-period
    # already covers start-up, and after it a process that never wrote a heartbeat
    # is genuinely broken.
    _configure(monkeypatch, tmp_path)

    assert healthcheck.main() == 1
    assert "no heartbeat" in capsys.readouterr().out


def test_healthcheck_fails_when_the_heartbeat_is_stale(monkeypatch, tmp_path, capsys):
    path = _configure(monkeypatch, tmp_path, timeout=30.0)
    write(path)
    stale = time.time() - 120
    os.utime(path, (stale, stale))

    assert healthcheck.main() == 1
    output = capsys.readouterr().out
    assert "120s old" in output
    assert "limit is 30s" in output


def test_the_default_timeout_outlasts_the_longest_legitimate_quiet_period(clean_settings_env):
    # The source's backoff caps at 60s and the producer reports every 10s, so a
    # single worst-case reconnect can legitimately leave the heartbeat 70s old.
    # A timeout at or below that would flap during a normal upstream blip.
    settings = Settings(_env_file=None)

    assert settings.producer_heartbeat_timeout_seconds > (
        settings.source_backoff_max_seconds + settings.kafka_stats_interval_seconds
    )
