"""Bronze ingest against the real broker and the real Iceberg catalog.

Nothing here is mocked. A throwaway topic is created, the 500 captured frames are
produced to it through the same `KafkaSink` the producer uses, the real streaming
job is run inside the Spark container, and the assertions are made by querying the
table through the REST catalog. The point is that the three things most likely to
be quietly wrong — the Iceberg wiring, the checkpoint, and the claim that
`raw_payload` is byte-exact — cannot be faked by a test double.

Each run isolates itself with a random suffix on the topic, the namespace and the
checkpoint directory, so two runs cannot interfere and a failed run leaves
evidence rather than poisoning the next one.

The job runs in the container rather than in-process because the Iceberg and Kafka
jars live in the image (see `docker/spark.Dockerfile`) and there is deliberately
only one Spark configuration in this project. A test that reached Iceberg some
other way would be testing a second configuration that nothing else uses.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from wikistream.config import Settings
from wikistream.producer.kafka_sink import KafkaSink
from wikistream.sources.base import SourceEvent

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "recentchange_sample.jsonl"

#: Long enough for a cold JVM start plus a micro-batch on a loaded laptop. A
#: streaming job that has not finished a bounded run in five minutes is broken,
#: not slow.
SUBMIT_TIMEOUT_SECONDS = 300


def _compose(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    """Run a `docker compose` command from the repository root."""
    # Fixed argv, no shell, `docker` resolved from PATH.
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _spark_submit(
    script: str,
    *script_args: str,
    env: dict[str, str],
    timeout: int = SUBMIT_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    """Run a script in the Spark container with `WS_*` overrides for this test run."""
    env_flags = [flag for key, value in env.items() for flag in ("-e", f"{key}={value}")]
    result = _compose(
        "exec",
        "-T",
        *env_flags,
        "spark",
        "/opt/spark/bin/spark-submit",
        script,
        *script_args,
        timeout=timeout,
    )
    if result.returncode != 0:
        # The tail, not the whole log: a Spark failure puts the useful lines last
        # and the first two hundred are class loading.
        tail = "\n".join(result.stdout.strip().splitlines()[-40:])
        pytest.fail(f"{script} exited {result.returncode}\n{tail}")
    return result


def _query(sql: str, env: dict[str, str]) -> list[dict[str, Any]]:
    """Run one statement in the container and parse the JSON rows it prints.

    Every line that starts with `{` is a row: `spark-submit` merges the Python
    process's stderr into stdout, so filtering by stream is not possible. See the
    docstring of `scripts/spark_sql.py`.
    """
    result = _spark_submit("/opt/wikistream/scripts/spark_sql.py", "--json", sql, env=env)
    return [
        json.loads(line)
        for line in result.stdout.splitlines()
        if line.startswith("{") and line.rstrip().endswith("}")
    ]


@pytest.fixture(scope="module")
def raw_frames() -> list[str]:
    """The captured frames as text, not as parsed dicts.

    Byte-exactness cannot be asserted from a dict: `json.dumps` of a parsed frame
    rewrites key order and number formatting, so a test built on `sample_events`
    would compare the pipeline against its own reserialisation and pass even if
    bronze stored something different from what the wire carried.
    """
    if not FIXTURE.exists():
        pytest.skip(f"fixture missing: {FIXTURE.name}")
    return [line for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture(scope="module")
def ingested(raw_frames: list[str]) -> Iterator[dict[str, Any]]:
    """Produce the frames to a throwaway topic and run bronze over it once.

    Module-scoped: the setup costs two JVM starts, and every assertion below reads
    the same landed table.
    """
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")

    from confluent_kafka.admin import AdminClient, NewTopic

    run_id = uuid.uuid4().hex[:8]
    topic = f"wikistream.it.bronze.{run_id}"
    namespace = f"bronze_it_{run_id}"
    settings = Settings(kafka_topic=topic, bronze_namespace=namespace)
    table = settings.bronze_raw_table

    # The container's own settings differ from the host's only in these three
    # values; everything else comes from the compose environment, so the job under
    # test is configured exactly as `make stream-bronze` configures it.
    env = {
        "WS_KAFKA_TOPIC": topic,
        "WS_BRONZE_NAMESPACE": namespace,
        "WS_CHECKPOINT_ROOT": f"/opt/spark/checkpoints/it-{run_id}",
    }

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    # Three partitions, matching production, so the ordering-by-key and
    # coordinate-uniqueness assertions are made against the real layout.
    futures = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    futures[topic].result(timeout=30)

    try:
        sink = KafkaSink(settings)
        for frame in raw_frames:
            sink.send(SourceEvent(raw=frame, payload=json.loads(frame)))
        remaining = sink.flush(timeout=60.0)
        assert remaining == 0, f"{remaining} records never reached the broker"
        assert sink.stats.delivered == len(raw_frames)
        sink.close()

        _spark_submit("/opt/wikistream/scripts/init_tables.py", env=env)
        _spark_submit(
            "/opt/wikistream/src/wikistream/streaming/bronze.py",
            "--once",
            env=env,
        )
        yield {"env": env, "table": table, "topic": topic, "namespace": namespace}
    finally:
        _compose(
            "exec",
            "-T",
            *[f for k, v in env.items() for f in ("-e", f"{k}={v}")],
            "spark",
            "bash",
            "-lc",
            f"rm -rf {env['WS_CHECKPOINT_ROOT']}",
        )
        _spark_submit(
            "/opt/wikistream/scripts/spark_sql.py", f"DROP TABLE IF EXISTS {table} PURGE", env=env
        )
        _spark_submit(
            "/opt/wikistream/scripts/spark_sql.py",
            f"DROP NAMESPACE IF EXISTS {settings.iceberg_catalog_name}.{namespace}",
            env=env,
        )
        admin.delete_topics([topic])


def test_every_produced_frame_lands_exactly_once(
    ingested: dict[str, Any], raw_frames: list[str]
) -> None:
    """500 frames in, 500 rows out, and no Kafka coordinate appearing twice.

    The coordinate check is the one that matters. `(kafka_partition, kafka_offset)`
    is unique in Kafka by construction, so a duplicate here would mean the sink
    committed the same records under two batches — the exact failure that
    checkpointing plus Iceberg's per-epoch commit is supposed to prevent.
    """
    rows = _query(
        f"""SELECT count(*) AS rows,
                   count(DISTINCT kafka_partition || ':' || kafka_offset) AS coords,
                   count(DISTINCT event_id) AS ids
            FROM {ingested["table"]}""",
        ingested["env"],
    )
    assert rows == [{"rows": len(raw_frames), "coords": len(raw_frames), "ids": len(raw_frames)}]


def test_raw_payload_is_byte_exact(ingested: dict[str, Any], raw_frames: list[str]) -> None:
    """What is in the table is what was on the wire, character for character.

    Not "parses to an equal document" — equal text. Bronze is the audit record, and
    a re-serialised payload is a different artefact that merely happens to mean the
    same thing.
    """
    landed = _query(f"SELECT raw_payload FROM {ingested['table']}", ingested["env"])
    payloads = [row["raw_payload"] for row in landed]

    assert sorted(payloads) == sorted(raw_frames)


def test_extracted_columns_match_the_payload(ingested: dict[str, Any]) -> None:
    """`event_id`, `event_time` and `schema_uri` are the payload's own values.

    Asserted in SQL against `raw_payload` rather than against the fixture, so this
    would catch a future change that extracted the wrong field just as well as one
    that dropped it.
    """
    rows = _query(
        f"""SELECT
              count_if(event_id <> get_json_object(raw_payload, '$.meta.id')) AS wrong_id,
              count_if(schema_uri <> get_json_object(raw_payload, '$."$schema"')) AS wrong_schema,
              count_if(event_time <> to_timestamp(get_json_object(raw_payload, '$.meta.dt')))
                AS wrong_time,
              count_if(event_id IS NULL OR event_time IS NULL OR schema_uri IS NULL) AS any_null
            FROM {ingested["table"]}""",
        ingested["env"],
    )
    assert rows == [{"wrong_id": 0, "wrong_schema": 0, "wrong_time": 0, "any_null": 0}]


def test_kafka_coordinates_and_ingest_metadata_are_populated(ingested: dict[str, Any]) -> None:
    """The columns that make a replay possible are actually filled in.

    `ingest_date` is the partition column: if it were null every row would land in
    Iceberg's null partition and the table would have one growing partition rather
    than one per day.
    """
    rows = _query(
        f"""SELECT count(DISTINCT kafka_topic) AS topics,
                   count(DISTINCT kafka_partition) AS partitions,
                   count_if(kafka_timestamp IS NULL) AS null_broker_time,
                   count_if(ingested_at IS NULL) AS null_ingest_time,
                   count_if(ingest_date <> to_date(ingested_at)) AS mismatched_date,
                   count(DISTINCT ingest_date) AS ingest_dates,
                   count(DISTINCT ingested_at) AS ingest_instants
            FROM {ingested["table"]}""",
        ingested["env"],
    )
    row = rows[0]
    assert row["topics"] == 1
    assert row["partitions"] == 3, "the fixture should spread across all three partitions"
    assert row["null_broker_time"] == 0
    assert row["null_ingest_time"] == 0
    assert row["mismatched_date"] == 0
    assert row["ingest_dates"] == 1
    # One `current_timestamp()` for the whole micro-batch, not one per row. This is
    # what makes `ingest_date` a property of the commit rather than of the row, and
    # what stops a batch that spans midnight from straddling two partitions.
    assert row["ingest_instants"] == 1


def test_the_batch_is_recorded_as_one_iceberg_snapshot(ingested: dict[str, Any]) -> None:
    """One micro-batch, one snapshot, tagged with the epoch that produced it.

    `spark.sql.streaming.epochId` in the snapshot summary is the mechanism behind
    the restart guarantee: Iceberg refuses to commit an epoch it has already
    committed, which is what makes a replayed batch a no-op instead of a duplicate.
    A test that only counted rows would not notice if that tag disappeared.
    """
    snapshots = _query(
        f"""SELECT summary['added-records'] AS added,
                   summary['spark.sql.streaming.epochId'] AS epoch
            FROM {ingested["table"]}.snapshots
            ORDER BY committed_at""",
        ingested["env"],
    )
    assert len(snapshots) == 1
    assert snapshots[0]["added"] == "500"
    assert snapshots[0]["epoch"] == "0"


def test_rerunning_the_job_adds_nothing(ingested: dict[str, Any], raw_frames: list[str]) -> None:
    """The checkpoint, not `startingOffsets`, decides where a restart resumes.

    Runs the bounded job a second time over a topic that has not moved. If the
    checkpoint were being ignored — a renamed query, a deleted directory, an
    `earliest` that applied on every start — this would double the table.
    """
    _spark_submit(
        "/opt/wikistream/src/wikistream/streaming/bronze.py", "--once", env=ingested["env"]
    )
    rows = _query(f"SELECT count(*) AS rows FROM {ingested['table']}", ingested["env"])
    assert rows == [{"rows": len(raw_frames)}]

    # An empty batch must not leave a snapshot behind either: an Iceberg commit per
    # idle trigger would grow the metadata forever on a quiet topic.
    snapshots = _query(f"SELECT count(*) AS n FROM {ingested['table']}.snapshots", ingested["env"])
    assert snapshots == [{"n": 1}]
