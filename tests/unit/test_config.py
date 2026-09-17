from __future__ import annotations

import json
import logging

import pytest
from pydantic import ValidationError

from wikistream.config import USER_AGENT, Settings, get_settings
from wikistream.logging import JsonFormatter, configure_logging

pytestmark = pytest.mark.unit


def test_defaults_work_with_no_env_at_all(clean_settings_env):
    """A stranger with no .env must still get a runnable configuration."""
    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_topic == "wiki.recentchange"
    assert settings.s3_bucket == "lakehouse"
    assert settings.source_name == "wikimedia"


def test_env_prefix_is_applied(clean_settings_env, monkeypatch):
    monkeypatch.setenv("WS_KAFKA_TOPIC", "other.topic")
    monkeypatch.setenv("WS_TRIGGER_INTERVAL_SECONDS", "5")

    settings = Settings(_env_file=None)

    assert settings.kafka_topic == "other.topic"
    assert settings.trigger_interval_seconds == 5


def test_table_identifiers_are_catalog_qualified(clean_settings_env, monkeypatch):
    """Renaming the catalog must not require touching any job."""
    monkeypatch.setenv("WS_ICEBERG_CATALOG_NAME", "warehouse")

    settings = Settings(_env_file=None)

    assert settings.bronze_raw_table == "warehouse.bronze.recentchange_raw"
    assert settings.silver_edits_table == "warehouse.silver.edits"
    assert settings.silver_quarantine_table == "warehouse.silver.quarantine"


def test_warehouse_uri_follows_the_bucket(clean_settings_env, monkeypatch):
    monkeypatch.setenv("WS_S3_BUCKET", "other-bucket")

    assert Settings(_env_file=None).warehouse_uri == "s3://other-bucket/warehouse"


@pytest.mark.parametrize("bad", ["postgres", "kinesis", ""])
def test_unknown_source_is_rejected_at_startup(clean_settings_env, monkeypatch, bad):
    """Fail on boot with a readable message, not 200 lines into the first batch."""
    monkeypatch.setenv("WS_SOURCE_NAME", bad)

    with pytest.raises(ValidationError, match="WS_SOURCE_NAME must be one of"):
        Settings(_env_file=None)


def test_log_level_is_normalised(clean_settings_env, monkeypatch):
    monkeypatch.setenv("WS_LOG_LEVEL", "debug")

    assert Settings(_env_file=None).log_level == "DEBUG"


def test_settings_are_frozen(clean_settings_env):
    """Spark serialises closures that capture settings; mutation would not propagate."""
    settings = Settings(_env_file=None)

    with pytest.raises(ValidationError):
        settings.kafka_topic = "mutated"


def test_get_settings_is_cached(clean_settings_env):
    assert get_settings() is get_settings()


def test_user_agent_identifies_the_project_and_links_somewhere():
    """Wikimedia asks firehose clients to identify themselves; an anonymous UA is rude."""
    assert "wikistream-lakehouse" in USER_AGENT
    assert "https://github.com/" in USER_AGENT


def test_json_formatter_emits_one_parseable_object_per_record():
    record = logging.LogRecord(
        name="wikistream.test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="reconnecting after %d attempts",
        args=(3,),
        exc_info=None,
    )
    record.attempt = 3  # the kind of context passed via extra=

    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "WARNING"
    assert payload["msg"] == "reconnecting after 3 attempts"
    assert payload["logger"] == "wikistream.test"
    assert payload["attempt"] == 3


def test_configure_logging_is_idempotent():
    """Called once per process and again inside Spark's foreachBatch closure."""
    configure_logging("INFO")
    configure_logging("INFO")

    assert len(logging.getLogger().handlers) == 1
