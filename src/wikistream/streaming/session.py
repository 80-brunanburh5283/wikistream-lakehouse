"""The only place a SparkSession or an Iceberg catalog is configured.

Every streaming job, maintenance script and test that needs Spark calls
`build_session` here. That is a deliberate constraint rather than tidiness: the
Spark-plus-Iceberg-plus-REST-plus-S3 configuration is about fifteen properties
where a single wrong one produces an error that names none of the others, and
having two copies of it in a repository means debugging the wrong copy.

## The configuration, and why each part of it

Iceberg needs three things wired up and they are independent:

1. **The SQL extensions.** Without
   `spark.sql.extensions=...IcebergSparkSessionExtensions`, `MERGE INTO`,
   `CALL`-based maintenance procedures and `ALTER TABLE ... WRITE ORDERED BY` all
   fail as parse errors. The failure looks like a SQL syntax mistake, not like a
   missing jar, which is what makes it expensive.
2. **A catalog.** `type=rest` pointing at the REST fixture. Iceberg's own catalog
   implementations are plug-ins, so the catalog name (`lakehouse`) becomes the
   first identifier in every table reference: `lakehouse.bronze.recentchange_raw`.
3. **File IO.** `io-impl=S3FileIO` sends data-file reads and writes through
   Iceberg's own S3 client (the shaded AWS SDK v2 in `iceberg-aws-bundle`), not
   through Hadoop's `s3a`. That is one fewer filesystem layer, and it is the path
   Iceberg tests, but it means the MinIO endpoint has to be given to Iceberg
   rather than to Hadoop — `spark.hadoop.fs.s3a.endpoint` has no effect on it.

`s3.path-style-access=true` is not optional against MinIO. The AWS SDK defaults to
virtual-host addressing (`bucket.host`), so without it the client tries to resolve
`lakehouse.minio` and fails with what reads like a DNS fault.

## What this module deliberately does not do

It does not set `spark.jars.packages`. The jars are baked into the image (see
`docker/spark.Dockerfile`); resolving them from Maven on every submit would make a
cold start depend on someone's network. If a job here fails with
`ClassNotFoundException: org.apache.iceberg...`, the image is wrong, and adding a
`--packages` flag to work around that would hide it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from wikistream.config import get_settings
from wikistream.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from wikistream.config import Settings

log = get_logger(__name__)

#: The catalog implementation Iceberg loads for a `type=rest` catalog.
_ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO"


def catalog_properties(settings: Settings | None = None) -> dict[str, str]:
    """Spark properties that define the Iceberg catalog and its storage.

    Returned as a plain dict, separately from the session, so a test can assert
    the wiring without starting a JVM — 15 properties are 15 chances to typo a
    key, and `spark.sql.catalog.lakehouse.warehosue` is silently ignored rather
    than rejected.
    """
    cfg = settings or get_settings()
    catalog = cfg.iceberg_catalog_name
    prefix = f"spark.sql.catalog.{catalog}"
    return {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        prefix: _ICEBERG_CATALOG_IMPL,
        f"{prefix}.type": "rest",
        f"{prefix}.uri": cfg.iceberg_rest_uri,
        f"{prefix}.warehouse": cfg.warehouse_uri,
        f"{prefix}.io-impl": _S3_FILE_IO,
        f"{prefix}.s3.endpoint": cfg.s3_endpoint,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": cfg.s3_access_key,
        f"{prefix}.s3.secret-access-key": cfg.s3_secret_key,
        f"{prefix}.client.region": cfg.s3_region,
        # Makes unqualified table names resolve in this catalog, so a script can
        # say `bronze.recentchange_raw`. The fully-qualified form still works and
        # is what the jobs use; this is for interactive spark-sql sessions.
        "spark.sql.defaultCatalog": catalog,
    }


def session_properties(settings: Settings | None = None) -> dict[str, str]:
    """Spark properties that are about this machine rather than about Iceberg."""
    cfg = settings or get_settings()
    return {
        "spark.driver.memory": cfg.spark_driver_memory,
        # A laptop has no shuffle to parallelise 200 ways. The default of 200
        # partitions on a micro-batch of a few thousand rows produces 200 tiny
        # files per commit, which is the small-files problem manufactured on
        # purpose. See docs/correctness.md.
        "spark.sql.shuffle.partitions": "4",
        # Iceberg writes are already atomic through the catalog commit, and the
        # Hadoop committer's `_SUCCESS`/`_temporary` dance costs S3 round trips
        # for nothing here.
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
        # Adaptive execution coalesces post-shuffle partitions. Note what this does
        # and does not cover: Spark disables AQE inside streaming queries and says
        # so at start-up ("spark.sql.adaptive.enabled is not supported in streaming
        # DataFrames/Datasets and will be disabled"), so for the bronze and silver
        # streams the line above about shuffle partitions is the only lever. This
        # setting is doing real work only in the batch jobs that share this session
        # builder: `scripts/rebuild_silver.py`, `scripts/maintain_tables.py` and
        # `scripts/table_stats.py`.
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # UTC everywhere. The source timestamps are UTC, Iceberg stores
        # timestamptz as UTC, and a JVM defaulting to the host's zone would shift
        # every `days(event_date)` partition boundary by the offset — quietly, and
        # only for people not in UTC.
        "spark.sql.session.timeZone": "UTC",
        # Structured Streaming's default is to keep 100 progress entries per
        # query in memory for the UI. Fine, and stated so it is a choice.
        "spark.sql.streaming.ui.enabled": "true",
    }


def build_session(app_name: str, settings: Settings | None = None) -> SparkSession:
    """Return a configured SparkSession, reusing the active one if there is one.

    `getOrCreate` returns any existing session unchanged, ignoring the config
    passed here — which is the single most confusing thing about writing tests
    against Spark. A session created earlier in the same process with different
    catalog settings is what you get, silently. That is tolerable here because
    every entry point in this project builds its session through this function,
    so there is only one configuration to get.
    """
    # Imported inside the function, not at module scope: the producer imports
    # nothing from this package, and CLI tools that only read `catalog_properties`
    # should not pay a ~1s pyspark import.
    from pyspark.sql import SparkSession

    cfg = settings or get_settings()
    builder = SparkSession.builder.appName(app_name).master(cfg.spark_master)
    for key, value in {**session_properties(cfg), **catalog_properties(cfg)}.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    # WARN is Spark's default and it is too loud to read a streaming log through;
    # the job's own structured logging is the thing worth reading.
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        "spark session ready",
        extra={
            "app_name": app_name,
            "master": cfg.spark_master,
            "catalog": cfg.iceberg_catalog_name,
            "warehouse": cfg.warehouse_uri,
        },
    )
    return spark


def checkpoint_location(name: str, settings: Settings | None = None) -> str:
    """Checkpoint directory for a named query.

    Named per query rather than per job, because two queries sharing a checkpoint
    directory corrupt each other's offset log, and the symptom is a restart that
    replays or skips data rather than an error.
    """
    cfg = settings or get_settings()
    return f"{cfg.checkpoint_root.rstrip('/')}/{name}"
