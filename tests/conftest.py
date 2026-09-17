"""Shared fixtures and the marker-based skip logic.

Integration and e2e tests need services that may not be running. Rather than
having every test discover that for itself and fail with a connection error,
collection-time hooks skip them with a message that says which `make` target
would make them runnable.
"""

from __future__ import annotations

import json
import os
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


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip integration and e2e tests when the stack they need is not up."""
    del config
    kafka_up = _port_open("localhost", int(os.environ.get("WS_KAFKA_PORT", "9092")))
    rest_up = _port_open("localhost", int(os.environ.get("WS_ICEBERG_REST_PORT", "8181")))
    core_up = kafka_up and rest_up

    skip_integration = pytest.mark.skip(
        reason="core services not reachable on localhost:9092/8181 — run `make up-core`"
    )
    skip_e2e = pytest.mark.skip(reason="full stack not reachable — run `make up`")

    for item in items:
        if "integration" in item.keywords and not core_up:
            item.add_marker(skip_integration)
        if "e2e" in item.keywords and not core_up:
            item.add_marker(skip_e2e)
