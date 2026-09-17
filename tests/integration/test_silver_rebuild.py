"""Silver against the real catalog: the split, the dedup, and the rebuild's idempotence.

Nothing here is mocked. The 500 captured frames and the 19 hand-built adversarial frames
are produced to a throwaway topic, bronze lands them, silver merges them, and then
`scripts/rebuild_silver.py` replays the whole window through the same code twice. The
assertions are made by querying the tables through the REST catalog.

Three things can only be proved here and not in `tests/spark/`:

* **The MERGE is real.** `tests/spark/test_silver_mapping.py` asserts the statement as
  text; only a live Iceberg v2 table shows that it commits, that the join finds the
  existing row, and that the second application inserts nothing.
* **Duplicates existed upstream.** The adversarial fixture contains two duplicate pairs.
  Bronze keeps all four rows, silver keeps two. "Zero duplicates in silver" means
  nothing without that comparison, and this is where it is made.
* **A rebuild is a rebuild.** Replaying a window must restore a row that was deleted by
  mistake, and replaying it again must change nothing. Anything less and bronze is not a
  recovery path.

Each run isolates itself with a random suffix on the topic, both namespaces and the
checkpoint directory, so two runs cannot interfere and a failure leaves its tables
behind as evidence rather than poisoning the next run.
"""

from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pytest
from stack import REPO_ROOT, SPARK_SQL_SCRIPT, compose, query, spark_submit

from wikistream.config import Settings
from wikistream.producer.kafka_sink import KafkaSink
from wikistream.sources.base import SourceEvent

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.integration

SAMPLE = REPO_ROOT / "tests" / "fixtures" / "recentchange_sample.jsonl"
ADVERSARIAL = REPO_ROOT / "tests" / "fixtures" / "adversarial.jsonl"

#: The adversarial frames that must be rejected, and by which rule. Kept here rather
#: than derived from `wikistream.quality.expectations` on purpose: deriving it would
#: make the test agree with the code by construction and assert nothing.
EXPECTED_QUARANTINE = {
    "Adversarial:missing_meta_id": "event_id_missing",
    "Adversarial:missing_meta_dt": "event_time_missing",
    "Adversarial:unparseable_meta_dt": "event_time_unparseable",
    "Adversarial:missing_meta_domain": "domain_missing",
    "Adversarial:far_future_meta_dt": "event_time_in_future",
}


def _frames(path: Path) -> list[str]:
    """A fixture file as raw text lines, not parsed dicts."""
    if not path.exists():
        pytest.skip(f"fixture missing: {path.name}")
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture(scope="module")
def merged() -> Iterator[dict[str, Any]]:
    """Produce both fixtures, land bronze, merge silver, and yield the run's coordinates.

    Module-scoped: the setup costs three JVM starts, and every assertion below reads the
    same landed tables. The adversarial frames are produced *after* the sample so their
    Kafka offsets are the higher ones, which makes the first-arrival-wins assertion read
    the way it is written.
    """
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")

    from confluent_kafka.admin import AdminClient, NewTopic

    sample = _frames(SAMPLE)
    adversarial = _frames(ADVERSARIAL)

    run_id = uuid.uuid4().hex[:8]
    topic = f"wikistream.it.silver.{run_id}"
    bronze_ns = f"bronze_it_{run_id}"
    silver_ns = f"silver_it_{run_id}"
    settings = Settings(kafka_topic=topic, bronze_namespace=bronze_ns, silver_namespace=silver_ns)
    env = {
        "WS_KAFKA_TOPIC": topic,
        "WS_BRONZE_NAMESPACE": bronze_ns,
        "WS_SILVER_NAMESPACE": silver_ns,
        "WS_CHECKPOINT_ROOT": f"/opt/spark/checkpoints/it-{run_id}",
    }

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    futures[topic].result(timeout=30)

    try:
        sink = KafkaSink(settings)
        for frame in [*sample, *adversarial]:
            # The adversarial file holds frames that are valid JSON but invalid events;
            # none of them is unparseable text, so `json.loads` is safe on all of them.
            sink.send(SourceEvent(raw=frame, payload=json.loads(frame)))
        remaining = sink.flush(timeout=60.0)
        assert remaining == 0, f"{remaining} records never reached the broker"
        sink.close()

        spark_submit("/opt/wikistream/scripts/init_tables.py", env=env)
        spark_submit("/opt/wikistream/src/wikistream/streaming/bronze.py", "--once", env=env)
        spark_submit("/opt/wikistream/src/wikistream/streaming/silver.py", "--once", env=env)

        window = query(
            "SELECT cast(min(ingest_date) AS string) AS first_day, "
            f"cast(max(ingest_date) AS string) AS last_day FROM {settings.bronze_raw_table}",
            env,
        )[0]

        yield {
            "env": env,
            "bronze": settings.bronze_raw_table,
            "edits": settings.silver_edits_table,
            "quarantine": settings.silver_quarantine_table,
            "produced": len(sample) + len(adversarial),
            "window": window,
        }
    finally:
        for table in (
            settings.silver_edits_table,
            settings.silver_quarantine_table,
            settings.bronze_raw_table,
        ):
            spark_submit(
                SPARK_SQL_SCRIPT,
                f"DROP TABLE IF EXISTS {table} PURGE",
                env=env,
                check=False,
            )
        for namespace in (bronze_ns, silver_ns):
            spark_submit(
                SPARK_SQL_SCRIPT,
                f"DROP NAMESPACE IF EXISTS {settings.iceberg_catalog_name}.{namespace}",
                env=env,
                check=False,
            )
        compose(
            "exec",
            "-T",
            "spark",
            "bash",
            "-lc",
            f"rm -rf {env['WS_CHECKPOINT_ROOT']}",
        )
        admin.delete_topics([topic])


def _counts(merged: dict[str, Any]) -> dict[str, int]:
    """Row counts for all three tables, in one round trip."""
    row = query(
        f"""SELECT (SELECT count(*) FROM {merged["bronze"]}) AS bronze,
                   (SELECT count(*) FROM {merged["edits"]}) AS edits,
                   (SELECT count(*) FROM {merged["quarantine"]}) AS quarantine""",
        merged["env"],
    )[0]
    return {key: int(value) for key, value in row.items()}


def _epoch_seconds(rendered: str) -> int:
    """A timestamp as `spark_sql.py --json` renders it, back to whole epoch seconds.

    `json.dumps(..., default=str)` gives `str(datetime)` — `2026-09-17 12:30:00.123456`,
    with no zone, because PySpark hands back naive datetimes in the session's zone. Every
    job here pins that to UTC, so attaching UTC is a restatement rather than a guess.
    Truncated rather than rounded, to match Spark's `unix_timestamp`.
    """
    return int(datetime.fromisoformat(rendered).replace(tzinfo=timezone.utc).timestamp())


def _current_snapshot(table: str, env: dict[str, str]) -> int:
    """The snapshot a rollback should return `table` to."""
    rows = query(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1", env
    )
    return int(rows[0]["snapshot_id"])


def _rollback(table: str, snapshot_id: int, env: dict[str, str]) -> None:
    """Undo every commit since `snapshot_id`.

    Used to clean up after a test that deliberately corrupts a table. A rollback is
    exact in a way a compensating `DELETE` is not — it restores the metadata pointer
    rather than trying to reconstruct the previous contents — which keeps the tests
    below independent of the ones above.
    """
    catalog, _, relative = table.partition(".")
    spark_submit(
        SPARK_SQL_SCRIPT,
        f"CALL {catalog}.system.rollback_to_snapshot("
        f"table => '{relative}', snapshot_id => {snapshot_id})",
        env=env,
    )


# --------------------------------------------------------------------------
# The split and the dedup, against a real table
# --------------------------------------------------------------------------


def test_every_frame_reached_bronze(merged: dict[str, Any]) -> None:
    """The premise of everything below: bronze holds one row per produced frame."""
    assert _counts(merged)["bronze"] == merged["produced"]


def test_silver_holds_no_duplicate_event_id(merged: dict[str, Any]) -> None:
    """The invariant, asserted against the table rather than against the statement text."""
    rows = query(
        f"""SELECT count(*) AS rows,
                   count(DISTINCT event_id) AS ids,
                   count_if(event_id IS NULL) AS null_ids
            FROM {merged["edits"]}""",
        merged["env"],
    )
    assert rows[0]["rows"] == rows[0]["ids"]
    assert rows[0]["null_ids"] == 0


def test_bronze_kept_the_duplicates_that_silver_collapsed(merged: dict[str, Any]) -> None:
    """Four rows upstream, two downstream. This is what makes the zero above evidence.

    The adversarial fixture contains `duplicate_event_id` twice and
    `divergent_duplicate` twice — the second of each pair carrying different content, as
    a producer replay after a partial send would. Bronze is the audit log and keeps all
    four; silver keeps one row per id.
    """
    rows = query(
        f"""SELECT
              (SELECT count(*) FROM {merged["bronze"]}
                WHERE get_json_object(raw_payload, '$.title') IN
                      ('Adversarial:duplicate_event_id', 'Adversarial:divergent_duplicate')
              ) AS bronze_rows,
              (SELECT count(*) FROM {merged["edits"]}
                WHERE page_title IN
                      ('Adversarial:duplicate_event_id', 'Adversarial:divergent_duplicate')
              ) AS silver_rows""",
        merged["env"],
    )
    assert rows[0]["bronze_rows"] == 4
    assert rows[0]["silver_rows"] == 2


def test_the_earlier_offset_won_the_divergent_pair(merged: dict[str, Any]) -> None:
    """First arrival wins, and the surviving row is the one from the lower Kafka offset.

    The two halves of `divergent_duplicate` differ in `length.new` — 5010 and 5042 — so
    which one survived is visible in `bytes_new` rather than having to be inferred.
    Determinism here is what makes the rebuild reproducible: a replay of the same
    offsets has to produce the same table.
    """
    rows = query(
        f"""SELECT bytes_new FROM {merged["edits"]}
            WHERE page_title = 'Adversarial:divergent_duplicate'""",
        merged["env"],
    )
    assert [row["bytes_new"] for row in rows] == [5010]


def test_each_invalid_frame_landed_in_quarantine_with_its_reason(merged: dict[str, Any]) -> None:
    """Five hand-built faults, five quarantine rows, each with the rule that caught it.

    Asserted per title so a failure names the rule that moved rather than a count that
    changed. `raw_payload` is queried for the title because a quarantined row may have
    no usable columns at all — which is the whole reason the payload is stored.
    """
    rows = query(
        f"""SELECT get_json_object(raw_payload, '$.title') AS title,
                   failure_reason
            FROM {merged["quarantine"]}""",
        merged["env"],
    )
    observed = {row["title"]: row["failure_reason"] for row in rows}

    assert observed == EXPECTED_QUARANTINE


def test_quarantined_rows_carry_their_kafka_coordinates(merged: dict[str, Any]) -> None:
    """The MERGE key for this table, so it cannot be null and cannot repeat.

    `event_id` is null for some of these rows by definition; the coordinates are what
    make them addressable, and a replay after a fix needs them.
    """
    rows = query(
        f"""SELECT count(*) AS rows,
                   count(DISTINCT kafka_partition || ':' || kafka_offset) AS coords,
                   count_if(kafka_offset IS NULL OR kafka_partition IS NULL) AS null_coords,
                   count_if(raw_payload IS NULL) AS null_payloads
            FROM {merged["quarantine"]}""",
        merged["env"],
    )
    row = rows[0]
    assert row["rows"] == row["coords"]
    assert row["null_coords"] == 0
    assert row["null_payloads"] == 0


def test_the_two_tables_account_for_every_distinct_event(merged: dict[str, Any]) -> None:
    """Nothing was dropped: bronze rows equal silver rows plus quarantine rows plus dupes.

    The two duplicate pairs collapse to one row each, so the expected identity is
    `bronze = edits + quarantine + 2`. Written as an explicit arithmetic check because a
    silent loss — a filter that matches neither branch — is otherwise invisible.
    """
    counts = _counts(merged)
    collapsed_duplicates = 2

    assert counts["bronze"] == counts["edits"] + counts["quarantine"] + collapsed_duplicates


# --------------------------------------------------------------------------
# The gate script, and the rebuild
# --------------------------------------------------------------------------


def test_the_duplicate_gate_exits_zero(merged: dict[str, Any]) -> None:
    """`make verify-no-duplicates` against the real tables, exit code and all.

    Run as a subprocess rather than by importing it, because the exit code is the
    product: `make up` and CI both depend on this failing loudly when it should.
    """
    result = spark_submit(
        "/opt/wikistream/scripts/verify_no_duplicates.py", env=merged["env"], check=False
    )

    assert result.returncode == 0, "\n".join(result.stdout.splitlines()[-30:])
    assert "all 5 checks passed" in result.stdout


def test_the_gate_fails_when_a_duplicate_is_inserted(merged: dict[str, Any]) -> None:
    """A gate that has never failed is not known to work.

    One row is copied into `silver.edits` with a plain `INSERT`, deliberately bypassing
    the MERGE that normally makes that impossible, and the gate must catch it. The
    table is rolled back to its pre-insert snapshot afterwards, so this test leaves no
    trace for the ones below.
    """
    edits, env = merged["edits"], merged["env"]
    clean = _current_snapshot(edits, env)
    try:
        spark_submit(
            SPARK_SQL_SCRIPT,
            f"""INSERT INTO {edits}
                SELECT * FROM {edits} WHERE event_id IS NOT NULL ORDER BY event_id LIMIT 1""",
            env=env,
        )
        result = spark_submit(
            "/opt/wikistream/scripts/verify_no_duplicates.py", env=env, check=False
        )

        assert result.returncode == 1, "the gate passed on a table holding a duplicate"
        assert "[FAIL] silver.edits duplicate event_id" in result.stdout
    finally:
        _rollback(edits, clean, env)

    assert (
        spark_submit(
            "/opt/wikistream/scripts/verify_no_duplicates.py", env=env, check=False
        ).returncode
        == 0
    ), "the rollback did not restore the table"


#: The two columns a rebuilt row does not reproduce, and why. Both derive from
#: `ingested_at`, and the stream and the rebuild get that value from different places:
#: the stream stamps its own micro-batch clock, while the rebuild reads what bronze
#: stamped when *it* consumed the same Kafka record. The two jobs consume the topic
#: independently, so the values differ by however far apart their batches ran.
#:
#: Taking bronze's value is the deliberate choice — see `frames_from_bronze` — because
#: judging a replayed month-old event against today's clock would report every row as a
#: month late and would fail the future-tolerance rule differently than the live path
#: did. The consequence is that `late_by_seconds` is not comparable across a rebuild
#: boundary, which `docs/data-contracts.md` states under "What you may not rely on".
ARRIVAL_DEPENDENT_COLUMNS = {"ingested_at", "late_by_seconds"}


def test_a_rebuild_replaces_a_row_deleted_by_mistake(merged: dict[str, Any]) -> None:
    """The reason bronze exists, exercised rather than described.

    A row is deleted from `silver.edits` — which is what a wrong rule looks like from
    the table's point of view — and then `rebuild_silver.py` replays the ingest window
    from bronze. The restored row is compared column by column, because "the count went
    back up" would also be true of a row rebuilt wrongly.

    Everything derived from the payload reconstructs exactly. The two columns derived
    from arrival do not, and the assertion says so rather than being loosened until it
    passes: see `ARRIVAL_DEPENDENT_COLUMNS`.
    """
    edits, env, window = merged["edits"], merged["env"], merged["window"]
    victim = query(
        f"SELECT * FROM {edits} WHERE event_id IS NOT NULL ORDER BY event_id LIMIT 1", env
    )[0]
    before = _counts(merged)

    spark_submit(
        SPARK_SQL_SCRIPT,
        f"DELETE FROM {edits} WHERE event_id = '{victim['event_id']}'",
        env=env,
    )
    assert _counts(merged)["edits"] == before["edits"] - 1, "the delete did not remove one row"

    spark_submit(
        "/opt/wikistream/scripts/rebuild_silver.py",
        "--from",
        window["first_day"],
        "--to",
        window["last_day"],
        env=env,
    )

    restored = query(f"SELECT * FROM {edits} WHERE event_id = '{victim['event_id']}'", env)
    assert len(restored) == 1, "the rebuild did not restore exactly one row"
    assert _counts(merged) == before

    differing = {key for key, value in restored[0].items() if victim[key] != value}
    assert differing == ARRIVAL_DEPENDENT_COLUMNS

    # And the two that moved moved coherently. `late_by_seconds` is
    # `unix_timestamp(ingested_at) - unix_timestamp(event_time)`, so if it shifted by
    # anything other than the shift in `ingested_at`, the rebuild would be measuring
    # lateness against a third clock. Compared in whole seconds because that is what
    # `unix_timestamp` returns; the timestamps themselves carry microseconds.
    #
    # The shift is positive because bronze consumed the record before silver did, so a
    # rebuilt row reports its event as *less* late than the stream did — the direction
    # that flatters the pipeline, which is the one worth asserting on.
    shift = _epoch_seconds(victim["ingested_at"]) - _epoch_seconds(restored[0]["ingested_at"])
    assert shift > 0, "the rebuild did not take bronze's earlier arrival time"
    assert victim["late_by_seconds"] - restored[0]["late_by_seconds"] == shift


def test_replaying_the_same_window_twice_changes_nothing(merged: dict[str, Any]) -> None:
    """Idempotence, which is the only property that makes a rebuild safe to retry.

    `MERGE INTO ... WHEN NOT MATCHED` is what provides it: applying it twice is applying
    it once. A `writeTo().append()` here would double both tables, and this is the test
    that would catch that change.
    """
    window = merged["window"]
    before = _counts(merged)

    for _ in range(2):
        spark_submit(
            "/opt/wikistream/scripts/rebuild_silver.py",
            "--from",
            window["first_day"],
            "--to",
            window["last_day"],
            env=merged["env"],
        )

    assert _counts(merged) == before


def test_the_gate_still_passes_after_the_replays(merged: dict[str, Any]) -> None:
    """The same data through the same MERGE four times over, and still one row per event."""
    result = spark_submit(
        "/opt/wikistream/scripts/verify_no_duplicates.py", env=merged["env"], check=False
    )

    assert result.returncode == 0, "\n".join(result.stdout.splitlines()[-30:])


def test_maintenance_preserves_every_row(merged: dict[str, Any]) -> None:
    """The four Iceberg procedures run against real tables and change no contents.

    This is the only place the generated `CALL` statements are executed — a misspelled
    named argument is reported by Iceberg as the procedure not existing, so the text
    assertions in `tests/unit/test_maintenance.py` cannot catch it. The compaction
    *numbers* are not asserted: at this volume there is little to compact, and a
    file-count assertion would be a claim about the fixture rather than about the code.
    """
    before = _counts(merged)

    result = spark_submit(
        "/opt/wikistream/scripts/maintain_tables.py", env=merged["env"], check=False
    )

    assert result.returncode == 0, "\n".join(result.stdout.splitlines()[-40:])
    for step in (
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
        "remove_orphan_files",
    ):
        assert step in result.stdout, f"{step} did not run"
    assert _counts(merged) == before
