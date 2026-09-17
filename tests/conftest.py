"""Shared fixtures and the marker-based skip logic.

Integration and e2e tests need services that may not be running. Rather than
having every test discover that for itself and fail with a connection error,
collection-time hooks skip them with a message that says which `make` target
would make them runnable.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURE_DIR / "recentchange_sample.jsonl"
ADVERSARIAL_FILE = FIXTURE_DIR / "adversarial.jsonl"


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    """True if something is listening. Used to decide whether to skip, not to assert."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        pytest.skip(f"fixture missing: {path.name}. Regenerate with scripts/capture_fixture.py")
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@pytest.fixture(scope="session")
def sample_events() -> list[dict[str, Any]]:
    """The 500-event capture from the live stream. Real bytes, not hand-written."""
    return _read_jsonl(SAMPLE_FILE)


@pytest.fixture(scope="session")
def adversarial_events() -> list[dict[str, Any]]:
    """Hand-built edge cases: duplicate, late, null-bearing, unknown field, invalid."""
    return _read_jsonl(ADVERSARIAL_FILE)


@pytest.fixture
def clean_settings_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Remove every ``WS_*`` variable for the duration of a test.

    Without this a developer's own exported variables silently change the meaning
    of a settings test, which is the kind of failure that only shows up in CI.
    Tests that want the pure defaults should also pass ``_env_file=None`` so a
    local `.env` does not leak in.
    """
    for key in [k for k in os.environ if k.startswith("WS_")]:
        monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture(scope="session")
def spark():
    """An in-process Spark session, shared across the whole test session.

    No Docker and no Iceberg catalog — this exists to assert what Spark's own
    parser does with the declared schema, which is a question about Spark, not
    about the lakehouse. Session-scoped because a JVM start-up costs a few
    seconds and paying that per test would push these out of the routine loop.
    """
    from pyspark.sql import SparkSession

    # WSL resolves its own hostname to a 127.0.1.1 entry that Spark's driver
    # sometimes fails to bind to. Pinning the loopback address avoids a
    # start-up failure that has nothing to do with the code under test.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("wikistream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip tests whose prerequisites are not present, with an actionable reason."""
    del config
    kafka_up = _port_open("localhost", int(os.environ.get("WS_KAFKA_PORT", "9092")))
    rest_up = _port_open("localhost", int(os.environ.get("WS_ICEBERG_REST_PORT", "8181")))
    core_up = kafka_up and rest_up
    java_present = shutil.which("java") is not None

    skip_integration = pytest.mark.skip(
        reason="core services not reachable on localhost:9092/8181 — run `make up-core`"
    )
    skip_e2e = pytest.mark.skip(reason="full stack not reachable — run `make up`")
    skip_spark = pytest.mark.skip(reason="no `java` on PATH — Spark needs a JDK (17 or 21)")

    for item in items:
        # `spark` is checked before `integration` so that a Spark-only test is
        # not skipped for the absence of Kafka, which it never touches.
        if "spark" in item.keywords:
            if not java_present:
                item.add_marker(skip_spark)
            continue
        if "integration" in item.keywords and not core_up:
            item.add_marker(skip_integration)
        if "e2e" in item.keywords and not core_up:
            item.add_marker(skip_e2e)
