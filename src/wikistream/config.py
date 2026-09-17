"""Every tunable in one place, driven by environment variables.

The pipeline runs in four different contexts — a developer's shell, a Docker
service, a pytest process and a Dagster run — and each reaches Kafka, MinIO and
the Iceberg catalog by a different hostname. Centralising that here means the
only difference between contexts is the environment, so a misconfiguration is a
single wrong variable rather than a wrong string buried in a job.

Defaults are chosen so that `python -m wikistream.producer` works against
`make up-core` with no `.env` at all.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Sent on every request to Wikimedia. Their operations team asks clients to
#: identify themselves and link somewhere contactable; an anonymous firehose
#: consumer is the thing they rate-limit first.
USER_AGENT = (
    "wikistream-lakehouse/0.1 "
    "(https://github.com/william-sarkar/wikistream-lakehouse; portfolio project)"
)


class Settings(BaseSettings):
    """Runtime configuration, populated from the environment or a local `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="WS_",
        extra="ignore",
        frozen=True,
    )

    # ---------------------------------------------------------------- source
    source_name: str = Field(
        default="wikimedia",
        description="Which EventSource implementation to run: wikimedia or jetstream.",
    )
    wikimedia_stream_url: str = Field(
        default="https://stream.wikimedia.org/v2/stream/recentchange",
    )
    jetstream_url: str = Field(
        default=(
            "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
        ),
    )
    #: How long to wait for the TCP+TLS handshake. Short: a dead endpoint should
    #: fail fast into the backoff loop rather than tie up the process.
    source_connect_timeout_seconds: float = 10.0
    #: How long a connection may produce nothing before we treat it as stalled.
    #: The stream emits several events a second, so 60s of silence is a fault,
    #: not a quiet period. Without this a half-open socket hangs forever.
    source_read_timeout_seconds: float = 60.0
    source_backoff_initial_seconds: float = 1.0
    source_backoff_max_seconds: float = 60.0

    # ----------------------------------------------------------------- kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="wiki.recentchange")
    kafka_topic_partitions: int = Field(default=3, ge=1)
    #: 24h is enough to replay a day of bronze from Kafka alone while keeping the
    #: laptop volume small; bronze is the long-term audit trail, not Kafka.
    kafka_retention_ms: int = Field(default=86_400_000, ge=1)
    kafka_consumer_group: str = Field(default="wikistream")
    #: Bounded buffering: 100 ms of batching cuts request count sharply at a
    #: latency cost far below the pipeline's 30-second micro-batch trigger.
    kafka_linger_ms: int = Field(default=100, ge=0)
    kafka_batch_size_bytes: int = Field(default=65_536, ge=1)
    kafka_stats_interval_seconds: float = Field(default=10.0, gt=0)

    # ------------------------------------------------------------ object store
    s3_endpoint: str = Field(default="http://localhost:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")
    s3_region: str = Field(default="us-east-1")
    s3_bucket: str = Field(default="lakehouse")

    # ----------------------------------------------------------------- catalog
    iceberg_rest_uri: str = Field(default="http://localhost:8181")
    iceberg_catalog_name: str = Field(default="lakehouse")
    bronze_namespace: str = Field(default="bronze")
    silver_namespace: str = Field(default="silver")
    gold_namespace: str = Field(default="gold")

    # --------------------------------------------------------------- streaming
    checkpoint_root: str = Field(default="/opt/spark/checkpoints")
    #: Visible knob, not a default: 30s trades end-to-end latency for fewer,
    #: larger Iceberg data files. See docs/correctness.md on small files.
    trigger_interval_seconds: int = Field(default=30, ge=1)
    #: Justified by the measured lateness distribution in docs/latency.md, not
    #: picked for roundness.
    watermark_minutes: int = Field(default=10, ge=1)
    spark_master: str = Field(default="local[*]")
    spark_driver_memory: str = Field(default="2g")

    # ------------------------------------------------------------------- trino
    trino_host: str = Field(default="localhost")
    trino_port: int = Field(default=8080)
    trino_user: str = Field(default="wikistream")

    # ----------------------------------------------------------------- logging
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=True)

    @field_validator("source_name")
    @classmethod
    def _known_source(cls, value: str) -> str:
        allowed = {"wikimedia", "jetstream"}
        if value not in allowed:
            msg = f"WS_SOURCE_NAME must be one of {sorted(allowed)}, got {value!r}"
            raise ValueError(msg)
        return value

    @field_validator("log_level")
    @classmethod
    def _known_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = value.upper()
        if upper not in allowed:
            msg = f"WS_LOG_LEVEL must be one of {sorted(allowed)}, got {value!r}"
            raise ValueError(msg)
        return upper

    @property
    def warehouse_uri(self) -> str:
        """S3 URI the Iceberg catalog writes table data under."""
        return f"s3://{self.s3_bucket}/warehouse"

    def table(self, namespace: str, name: str) -> str:
        """Fully-qualified Iceberg table identifier for Spark SQL.

        Exists so no module hard-codes `lakehouse.silver.edits`; renaming the
        catalog is then an environment change rather than a search-and-replace.
        """
        return f"{self.iceberg_catalog_name}.{namespace}.{name}"

    @property
    def bronze_raw_table(self) -> str:
        """The append-only audit table: one row per Kafka record, no dedup."""
        return self.table(self.bronze_namespace, "recentchange_raw")

    @property
    def silver_edits_table(self) -> str:
        """The deduplicated one-row-per-edit table."""
        return self.table(self.silver_namespace, "edits")

    @property
    def silver_quarantine_table(self) -> str:
        """Where rows that fail validation go instead of being dropped."""
        return self.table(self.silver_namespace, "quarantine")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings, parsed once.

    Cached because Spark executors deserialise closures that touch settings and
    re-reading `.env` per task would be both slow and a source of drift.
    """
    return Settings()
