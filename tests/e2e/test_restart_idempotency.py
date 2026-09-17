"""The restart-idempotency proof: SIGKILL the producer and the stream, restart, count.

    make test-e2e

`silver.edits` claims one row per source event. The rest of the suite argues that claim
on data that behaves; this test breaks the pipeline in the middle of its work and then
asks whether the claim survived. It is the slowest test here — eight to ten minutes —
and the only one that kills a running process.

## The two failures it reproduces

**The producer dies mid-connection.** SIGKILL, not SIGTERM, so there is no flush and no
graceful close: whatever sits in librdkafka's queue is lost. A second producer process
then starts against the same topic.

**The streaming job dies between writing a micro-batch and committing it.** This is the
failure `foreachBatch` cannot rule out, and the reason duplicate suppression exists.
Spark writes a batch's Kafka offsets to `offsets/N` *before* running it and `commits/N`
*after*; a crash in between leaves rows already merged into Iceberg and offsets Spark
believes it never consumed. On restart it re-reads that exact range and applies it
again.

Landing a SIGKILL inside that window would be a flake — the window is milliseconds
wide. So the kill is real and the window is made deterministic: the job is killed while
it is streaming, and then `commits/N` is deleted, which leaves the checkpoint in
precisely the state an interrupted commit leaves it in. The re-read is therefore
guaranteed rather than hoped for, and this test prints how many Kafka records it
covered.

## What it deliberately does not claim

Not that the producer restart lost no events. It did lose some: the SSE cursor lives in
the process, so a fresh process resumes at the tip of the stream and the edits emitted
while it was dead are never fetched. That gap is real, it is in the README's
limitations, and it is a different property from this one. This test is about
duplicates.

## Isolation

A random suffix on the topic, both namespaces and the checkpoint root, so a run cannot
disturb the demo tables, the long-lived producer service, or another run. The suffix is
also the `pkill` pattern: it appears in the driver's `--name` and in the job's
`--topic`, so the kill cannot reach a process this test did not start.

The topic is fed from the live Wikimedia endpoint rather than from the captured
fixtures, because a crash-recovery proof run against hand-fed frames would mostly be a
proof about the frames.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest
from stack import SPARK_SQL_SCRIPT, compose, query, spark_submit

from wikistream.config import Settings

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.e2e

SILVER_JOB = "/opt/wikistream/src/wikistream/streaming/silver.py"
INIT_TABLES = "/opt/wikistream/scripts/init_tables.py"
VERIFY_GATE = "/opt/wikistream/scripts/verify_no_duplicates.py"

#: A bounded run of the streaming job has a backlog of a few thousand records at most,
#: so seven minutes is generous. It exists to turn a wedged job into a failure.
SUBMIT_TIMEOUT = 420

#: How long the first producer streams before it is killed. Long enough for the job to
#: commit two micro-batches at a 10-second trigger, which is what makes "delete the
#: newest commit" a meaningful thing to do — batch 0 has no predecessor to compare with.
FIRST_PRODUCER_SECONDS = 70

#: The restarted producer. Bounded, so it stops on its own and the topic is static
#: before the final measurements — the last two assertions are exact equalities and a
#: producer still appending would make them races.
SECOND_PRODUCER_SECONDS = 40

#: Micro-batch trigger for this run only. The pipeline's default is 30 seconds; 10 buys
#: three commits in the time the first producer runs.
TRIGGER_SECONDS = "10"


def _numeric_entries(directory: str) -> list[int]:
    """Batch ids in a Spark metadata log directory, ignoring `.tmp` and dotfiles.

    `HDFSMetadataLog` names each entry after its batch id and writes a temporary file
    alongside while a commit is in flight, so a numeric filter is what distinguishes a
    finished record from one being written.
    """
    listing = compose("exec", "-T", "spark", "bash", "-lc", f"ls -1 {directory} 2>/dev/null")
    return sorted(int(line) for line in listing.stdout.split() if line.isdigit())


def _read_container_file(path: str) -> str:
    result = compose("exec", "-T", "spark", "cat", path)
    if result.returncode != 0:
        pytest.fail(f"could not read {path}: {result.stderr.strip()}")
    return result.stdout


def _batch_end_offsets(checkpoint: str, batch_id: int) -> dict[str, int]:
    """Per-partition end offsets recorded for one batch.

    A Spark offset log entry is a version line, a metadata line, then one JSON object
    per source. There is a single source here, so the last JSON line is it, and it maps
    topic to partition to the offset the batch reads *up to*.
    """
    lines = [
        line
        for line in _read_container_file(f"{checkpoint}/offsets/{batch_id}").splitlines()
        if line
    ]
    payload = json.loads(lines[-1])
    partitions: dict[str, int] = next(iter(payload.values()))
    return {str(key): int(value) for key, value in partitions.items()}


def _records_in_batch(checkpoint: str, batch_id: int) -> int:
    """How many Kafka records batch `batch_id` covers.

    The topic is created empty by this test and the job starts from `earliest`, so batch
    0 starts at offset 0 in every partition and each later batch starts where its
    predecessor ended.
    """
    end = _batch_end_offsets(checkpoint, batch_id)
    start = _batch_end_offsets(checkpoint, batch_id - 1) if batch_id > 0 else {}
    return sum(end.values()) - sum(start.get(key, 0) for key in end)


def _topic_end_offsets(topic: str, settings: Settings) -> int:
    """Sum of the topic's high watermarks: how many records the producers have landed."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"e2e-watermark-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=30)
        partitions = list(metadata.topics[topic].partitions)
        return sum(
            consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30, cached=False)[1]
            for p in partitions
        )
    finally:
        consumer.close()


def _start_producer(topic: str, seconds: int) -> subprocess.Popen[str]:
    """The project's own producer, on the host, against the scratch topic.

    On the host rather than in the container because this test needs a process it can
    signal directly: `docker compose kill` would stop the shared ingest service, and
    SIGKILL to a specific PID is the whole point.
    """
    return subprocess.Popen(
        [sys.executable, "-m", "wikistream.producer", "--duration", str(seconds)],
        env={**os.environ, "WS_KAFKA_TOPIC": topic},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _wait_for_records(topic: str, settings: Settings, minimum: int, timeout: float) -> int:
    """Block until the topic holds at least `minimum` records."""
    deadline = time.monotonic() + timeout
    landed = 0
    while time.monotonic() < deadline:
        landed = _topic_end_offsets(topic, settings)
        if landed >= minimum:
            return landed
        time.sleep(5)
    pytest.fail(
        f"only {landed} records reached {topic} in {timeout:.0f}s — is the stream reachable?"
    )


def _start_stream(env_flags: list[str], app_name: str, topic: str, job_log: str) -> None:
    """Start the continuous silver stream in the Spark container, detached.

    Detached rather than in the foreground because the point is to kill it while it is
    working. `--name` and `--topic` both carry the run id, so every process this starts
    — the shell, the JVM driver, the Python driver — is reachable by one `pkill`
    pattern that matches nothing else on the machine.
    """
    started = compose(
        "exec",
        "-d",
        *env_flags,
        "spark",
        "bash",
        "-lc",
        f"/opt/spark/bin/spark-submit --name {app_name} {SILVER_JOB} "
        f"--topic {topic} >> {job_log} 2>&1",
    )
    assert started.returncode == 0, started.stderr


def _stop_stream(run_id: str, *, expect_running: bool) -> None:
    """SIGKILL every process belonging to this run, and wait for the table to drain.

    Waiting matters: a survivor could commit a batch between a measurement and the
    deletion of a commit file, and the proof would then be of nothing.
    """
    killed = compose("exec", "-T", "spark", "pkill", "-9", "-f", run_id)
    if expect_running:
        assert killed.returncode == 0, (
            "pkill matched no process, so the job was not running when it was killed"
        )
    for _ in range(30):
        if compose("exec", "-T", "spark", "pgrep", "-f", run_id).returncode != 0:
            return
        time.sleep(1)
    pytest.fail(f"a process matching {run_id} survived SIGKILL")


def _wait_for_commits(checkpoint: str, minimum: int, timeout: float, log_path: str) -> list[int]:
    """Block until the streaming job has committed at least `minimum` micro-batches."""
    return _wait_until(
        lambda committed: len(committed) >= minimum,
        checkpoint,
        timeout,
        log_path,
        f"at least {minimum} committed batches",
    )


def _wait_for_commit_of(checkpoint: str, batch_id: int, timeout: float, log_path: str) -> list[int]:
    """Block until `batch_id` appears in the commit log."""
    return _wait_until(
        lambda committed: batch_id in committed,
        checkpoint,
        timeout,
        log_path,
        f"a commit for batch {batch_id}",
    )


def _wait_for_commit_beyond(
    checkpoint: str, batch_id: int, timeout: float, log_path: str
) -> list[int]:
    """Block until a batch *later* than `batch_id` has been committed."""
    return _wait_until(
        lambda committed: bool(committed) and committed[-1] > batch_id,
        checkpoint,
        timeout,
        log_path,
        f"a commit later than batch {batch_id}",
    )


def _wait_until(
    predicate: Any, checkpoint: str, timeout: float, log_path: str, wanted: str
) -> list[int]:
    """Poll the commit log until `predicate` holds, then return the committed batch ids."""
    deadline = time.monotonic() + timeout
    committed: list[int] = []
    while time.monotonic() < deadline:
        committed = _numeric_entries(f"{checkpoint}/commits")
        if predicate(committed):
            return committed
        time.sleep(5)
    # The job runs detached, so its failure is in its log and nowhere else.
    tail = compose("exec", "-T", "spark", "bash", "-lc", f"tail -40 {log_path}").stdout
    pytest.fail(f"wanted {wanted} within {timeout:.0f}s, have {committed}\n{tail}")


def _counts(table: str, env: dict[str, str]) -> dict[str, int]:
    """Rows and distinct event ids, in one round trip. The gap between them is the bug."""
    row = query(
        f"SELECT count(*) AS row_count, count(DISTINCT event_id) AS id_count FROM {table}",
        env,
    )[0]
    return {"rows": int(row["row_count"]), "ids": int(row["id_count"])}


def _exception_line(stdout: str) -> str:
    """The exception line from a failed `spark-submit`, without the logical plan.

    A `StreamingQueryException` prints its message, then the query's state, then the
    entire logical plan — hundreds of lines. `stack.spark_submit` reports a tail, which
    for this failure is all plan and no cause.
    """
    for line in stdout.splitlines():
        if "Exception:" in line or "SQLSTATE" in line:
            return line.strip()
    return stdout.strip().splitlines()[-1] if stdout.strip() else "no output"


@pytest.fixture(scope="module")
def crash_and_restart() -> Iterator[dict[str, Any]]:
    """Run the whole scenario once and hand every measurement to the assertions below.

    Module-scoped because it takes minutes and starts several JVMs; each test below then
    names one property of the same run rather than re-running it.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    run_id = uuid.uuid4().hex[:8]
    topic = f"wikistream.e2e.{run_id}"
    app_name = f"e2e-restart-{run_id}"
    silver_ns = f"silver_e2e_{run_id}"
    bronze_ns = f"bronze_e2e_{run_id}"
    checkpoint_root = f"/opt/spark/checkpoints/e2e-{run_id}"
    checkpoint = f"{checkpoint_root}/silver_edits"
    # A path inside the Spark container, not on the host.
    job_log = f"/tmp/{app_name}.log"

    settings = Settings(kafka_topic=topic, silver_namespace=silver_ns, bronze_namespace=bronze_ns)
    env = {
        "WS_KAFKA_TOPIC": topic,
        "WS_SILVER_NAMESPACE": silver_ns,
        "WS_BRONZE_NAMESPACE": bronze_ns,
        "WS_CHECKPOINT_ROOT": checkpoint_root,
        "WS_TRIGGER_INTERVAL_SECONDS": TRIGGER_SECONDS,
    }
    env_flags = [flag for key, value in env.items() for flag in ("-e", f"{key}={value}")]

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(
        timeout=30
    )

    producer: subprocess.Popen[str] | None = None
    try:
        spark_submit(INIT_TABLES, env=env, timeout=SUBMIT_TIMEOUT)

        print(f"\n[1] producing into {topic} for up to {FIRST_PRODUCER_SECONDS}s")
        producer = _start_producer(topic, FIRST_PRODUCER_SECONDS)
        landed = _wait_for_records(topic, settings, minimum=200, timeout=120)
        print(f"    {landed} records on the topic")

        print(f"[2] starting the silver stream, {TRIGGER_SECONDS}s trigger")
        _start_stream(env_flags, app_name, topic, job_log)
        print(f"    committed batches {_wait_for_commits(checkpoint, 2, 300, job_log)}")

        print("[3] SIGKILL both, mid-flight")
        _stop_stream(run_id, expect_running=True)
        producer.kill()
        producer.wait(timeout=30)
        producer = None

        at_crash = _counts(settings.silver_edits_table, env)
        offsets_at_crash = _topic_end_offsets(topic, settings)
        committed = _numeric_entries(f"{checkpoint}/commits")
        replayed_batch = committed[-1]
        replayed_records = _records_in_batch(checkpoint, replayed_batch)
        print(
            f"    silver holds {at_crash['rows']} rows, {at_crash['ids']} distinct ids; "
            f"topic at {offsets_at_crash}"
        )

        print(f"[4] deleting commits/{replayed_batch}: {replayed_records} records unconfirmed")
        removed = compose("exec", "-T", "spark", "rm", f"{checkpoint}/commits/{replayed_batch}")
        assert removed.returncode == 0, removed.stderr

        print(f"[5] restarting the producer for {SECOND_PRODUCER_SECONDS}s")
        producer = _start_producer(topic, SECOND_PRODUCER_SECONDS)
        producer.wait(timeout=SECOND_PRODUCER_SECONDS + 120)
        producer = None
        offsets_after_restart = _topic_end_offsets(topic, settings)
        print(f"    topic at {offsets_after_restart}, and static from here")

        print("[6] trying to resume the unconfirmed batch with --once")
        once = spark_submit(
            SILVER_JOB, "--once", "--topic", topic, env=env, timeout=SUBMIT_TIMEOUT, check=False
        )
        once_error = _exception_line(once.stdout) if once.returncode != 0 else ""
        # In full, not truncated: this line is the evidence for the paragraph in
        # docs/correctness.md that tells a reader which command to restart with.
        print(f"    exit {once.returncode}: {once_error}")

        print("[7] resuming with the continuous trigger, the way the pipeline runs")
        _start_stream(env_flags, app_name, topic, job_log)
        print(f"    committed {_wait_for_commit_beyond(checkpoint, replayed_batch, 420, job_log)}")
        _stop_stream(run_id, expect_running=False)
        after_restart = _counts(settings.silver_edits_table, env)
        print(f"    silver holds {after_restart['rows']} rows, {after_restart['ids']} distinct ids")

        settled = _numeric_entries(f"{checkpoint}/commits")
        second_replay = settled[-1]
        second_replay_records = _records_in_batch(checkpoint, second_replay)
        print(
            f"[8] replaying commits/{second_replay} ({second_replay_records} records), no new data"
        )
        compose("exec", "-T", "spark", "rm", f"{checkpoint}/commits/{second_replay}")
        _start_stream(env_flags, app_name, topic, job_log)
        _wait_for_commit_of(checkpoint, second_replay, 420, job_log)
        _stop_stream(run_id, expect_running=False)
        after_replay = _counts(settings.silver_edits_table, env)
        print(f"    silver holds {after_replay['rows']} rows, {after_replay['ids']} distinct ids")

        yield {
            "env": env,
            "edits": settings.silver_edits_table,
            "at_crash": at_crash,
            "after_restart": after_restart,
            "after_replay": after_replay,
            "replayed_batch": replayed_batch,
            "replayed_records": replayed_records,
            "second_replay_records": second_replay_records,
            "offsets_at_crash": offsets_at_crash,
            "offsets_after_restart": offsets_after_restart,
            "committed_before_crash": committed,
            "once_returncode": once.returncode,
            "once_error": once_error,
        }
    finally:
        if producer is not None and producer.poll() is None:
            producer.kill()
        compose("exec", "-T", "spark", "pkill", "-9", "-f", run_id)
        # Every table `init_tables.py` created, including the bronze one this scenario
        # never writes to: `DROP NAMESPACE` without CASCADE fails on a namespace that
        # still holds a table, so forgetting one leaks the namespace as well. PURGE
        # because a plain drop against the REST catalog leaves the data files in MinIO.
        for table in (
            settings.silver_edits_table,
            settings.silver_quarantine_table,
            settings.bronze_raw_table,
        ):
            spark_submit(
                SPARK_SQL_SCRIPT, f"DROP TABLE IF EXISTS {table} PURGE", env=env, check=False
            )
        for namespace in (silver_ns, bronze_ns):
            spark_submit(
                SPARK_SQL_SCRIPT,
                f"DROP NAMESPACE IF EXISTS {settings.iceberg_catalog_name}.{namespace}",
                env=env,
                check=False,
            )
        compose("exec", "-T", "spark", "bash", "-lc", f"rm -rf {checkpoint_root} {job_log}")
        admin.delete_topics([topic])


def test_the_crash_itself_left_no_duplicates(crash_and_restart):
    """A SIGKILL mid-batch cannot half-write a row: Iceberg commits or it does not."""
    at_crash = crash_and_restart["at_crash"]
    assert at_crash["rows"] > 0, "nothing was written before the kill, so nothing was proved"
    assert at_crash["rows"] == at_crash["ids"]


def test_the_restart_reprocessed_records_it_had_already_written(crash_and_restart):
    """The precondition for the next tests: the replay was real and not empty."""
    assert crash_and_restart["replayed_records"] > 0
    assert crash_and_restart["replayed_batch"] in crash_and_restart["committed_before_crash"]


def test_both_processes_resumed(crash_and_restart):
    """A green run has to mean the pipeline recovered, not that it stayed dead."""
    assert crash_and_restart["offsets_after_restart"] > crash_and_restart["offsets_at_crash"], (
        "the restarted producer landed nothing"
    )
    assert crash_and_restart["after_restart"]["rows"] > crash_and_restart["at_crash"]["rows"], (
        "the restarted job merged nothing"
    )


def test_silver_holds_no_duplicate_event_ids_after_the_restart(crash_and_restart):
    """The headline. One row per event id, across a kill, a replay and a catch-up."""
    after = crash_and_restart["after_restart"]
    assert after["rows"] == after["ids"], f"{after['rows'] - after['ids']} duplicate rows"


def test_replaying_a_committed_batch_inserts_exactly_nothing(crash_and_restart):
    """The sharp version, with the topic static so the equality is exact.

    A batch whose commit was lost is re-read and re-merged in full; the row count may
    not move by one. This is the assertion that would fail if the writer were an
    `append` instead of a `MERGE`, and it is why both writes in the silver job are
    MERGEs (ADR-0020).
    """
    assert crash_and_restart["second_replay_records"] > 0, "the second replay covered no records"
    assert crash_and_restart["after_replay"] == crash_and_restart["after_restart"]


def test_the_duplicate_gate_agrees(crash_and_restart):
    """`make verify-no-duplicates` is what CI and `make up` call, so run that too.

    The tests above query the table directly. This one runs the script the rest of the
    project depends on, against the same table, and takes its exit code as the verdict —
    otherwise the gate itself is the untested part of the proof.
    """
    result = spark_submit(
        VERIFY_GATE, env=crash_and_restart["env"], timeout=SUBMIT_TIMEOUT, check=False
    )
    assert result.returncode == 0, "\n".join(result.stdout.splitlines()[-30:])


def test_once_cannot_resume_an_unconfirmed_batch(crash_and_restart):
    """A characterisation test: `--once` is not the way to restart a crashed stream.

    `--once` uses `Trigger.AvailableNow`, and on a checkpoint whose newest batch has
    offsets but no commit it re-runs that batch, merges it correctly, and then fails
    writing the commit file that it is in the middle of writing:

        Multiple streaming queries are concurrently using .../commits

    No second query exists. The MERGE has already happened, so no data is lost or
    duplicated — the run simply exits non-zero and the batch stays unconfirmed. The
    continuous trigger recovers the same checkpoint without complaint, which is what
    step [7] of the fixture does and what `make stream-silver` uses.

    This is asserted rather than merely written down because it decides which command a
    reader should reach for after a crash. If it ever fails, Spark has fixed the
    AvailableNow path: delete this test and the paragraph it guards in
    `docs/correctness.md`.
    """
    assert crash_and_restart["once_returncode"] != 0
    assert "concurrently using" in crash_and_restart["once_error"], crash_and_restart["once_error"]
