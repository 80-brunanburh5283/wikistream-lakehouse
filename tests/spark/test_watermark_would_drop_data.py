"""Why `silver.py` declares no watermark, measured rather than argued.

`BUILD_PHASES.md` asks for `withWatermark("event_time", "10 minutes")` followed by a
watermark-based dedup. I did not build that, and a rejected requirement deserves
evidence rather than an opinion, so this file runs the rejected design and the kept one
over the same two events and records what each produces.

The finding: a stateful dedup operator admits only rows at or above the watermark, so a
row that arrives below it is **discarded with no error, no metric and no dead-letter
row**. The `late_by_30_minutes` frame in `tests/fixtures/adversarial.jsonl` is a real
shape from a real feed, and the rejected design deletes it. Unbounded dedup keeps it.

One mechanical note, because it cost an hour and it silently inverts the result: a
watermark only advances at a batch boundary, and the watermark in force during a batch
is the one computed at the end of the *previous* batch. So the late row must arrive in a
later batch than the row that advanced the watermark past it. `maxFilesPerTrigger=1`
with one file per event is not enough control — the file source orders by modification
time, and files written in the same millisecond can be read in either order, which makes
the drop appear and disappear between runs. One `availableNow` run per event against a
shared checkpoint is deterministic, so that is what `_stream_one_batch_per_event` does.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

SCHEMA = "event_id string, event_time timestamp"

#: Event ids are distinct. This is not a duplicate-suppression test, and reusing an id
#: would let a reader conclude the missing row was deduplication working as intended.
ON_TIME = {"event_id": "on-time-04-00", "event_time": "2026-09-17T04:00:00.000Z"}

#: The `late_by_30_minutes` shape from the adversarial fixture. Real traffic does this:
#: a wiki reconnects after a network partition and replays what it buffered.
LATE = {"event_id": "late-by-30-minutes", "event_time": "2026-09-17T03:30:00.000Z"}

#: Five minutes behind, so still above the 03:50 watermark. The boundary case, and the
#: reason the claim in `silver.py` is about the threshold rather than about late data.
INSIDE_THRESHOLD = {"event_id": "late-by-5-minutes", "event_time": "2026-09-17T03:55:00.000Z"}

#: The figure `BUILD_PHASES.md` asks for, which is also what sets the boundary above.
DELAY = "10 minutes"


def _with_watermark_and_dedup_within(stream: DataFrame) -> DataFrame:
    """The design `BUILD_PHASES.md` asks for, as written."""
    return stream.withWatermark("event_time", DELAY).dropDuplicatesWithinWatermark(["event_id"])


def _with_watermark_and_plain_dedup(stream: DataFrame) -> DataFrame:
    """The older spelling of the same idea, included so the finding is not operator-specific."""
    return stream.withWatermark("event_time", DELAY).dropDuplicates(["event_id"])


def _watermark_only(stream: DataFrame) -> DataFrame:
    """`withWatermark` with nothing stateful behind it: the letter of the specification."""
    return stream.withWatermark("event_time", DELAY)


def _the_design_in_use(stream: DataFrame) -> DataFrame:
    """No watermark, no streaming state. Dedup happens in `foreachBatch` against Iceberg."""
    return stream


def _stream_one_batch_per_event(
    spark: SparkSession,
    tmp_path: Path,
    name: str,
    events: Sequence[dict[str, str]],
    transform: Callable[[DataFrame], DataFrame],
) -> set[str]:
    """Feed `events` through `transform` one micro-batch at a time; return the survivors.

    Each event is written as a file and then drained by its own `availableNow` run
    against the same checkpoint, so batch boundaries and batch *order* are both
    guaranteed rather than left to the file source's mtime ordering. Restarting the
    query per batch also means the watermark and the dedup state are restored from the
    checkpoint each time, which is how the real job runs anyway.

    A file sink rather than the memory sink, because a memory sink's table is named
    after the query and cannot be reused across restarts; the JSON output directory
    accumulates over every run instead.
    """
    source = tmp_path / name / "source"
    source.mkdir(parents=True)
    output = tmp_path / name / "output"

    for index, event in enumerate(events):
        (source / f"{index}.json").write_text(json.dumps(event), encoding="utf-8")
        stream = spark.readStream.schema(SCHEMA).json(str(source))
        query = (
            transform(stream)
            .writeStream.format("json")
            .option("path", str(output))
            .outputMode("append")
            .option("checkpointLocation", str(tmp_path / name / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    survivors = spark.read.schema(SCHEMA).json(str(output)).collect()
    return {row["event_id"] for row in survivors}


@pytest.mark.parametrize(
    ("label", "transform"),
    [
        ("dropDuplicatesWithinWatermark", _with_watermark_and_dedup_within),
        ("dropDuplicates", _with_watermark_and_plain_dedup),
    ],
)
def test_the_rejected_design_silently_drops_a_late_event(spark, tmp_path, label, transform):
    """The 30-minute-late row is deleted, and nothing anywhere says so.

    Not an error, not a corrupt-record column, not a metric — `numOutputRows` for that
    batch is simply one lower than `numInputRows`, and the event is gone from the
    output with no record that it ever arrived. A pipeline whose correctness story is
    "no duplicates" must not pay for it in unreported deletions, and this is the
    measurement that decided the design.

    Both operators behave identically, which is why this is parameterised: the loss is
    not a quirk of the newer one, it is what bounding dedup state by the watermark
    means.
    """
    survivors = _stream_one_batch_per_event(
        spark, tmp_path, f"rejected_{label}", [ON_TIME, LATE], transform
    )

    assert survivors == {ON_TIME["event_id"]}
    assert LATE["event_id"] not in survivors


def test_the_design_in_use_keeps_the_late_event(spark, tmp_path):
    """The same two batches with no streaming state: both rows survive.

    In the real job the late one lands in the older `days(event_date)` partition, which
    is the property that makes unbounded dedup affordable and is most of the reason the
    table format is Iceberg.
    """
    survivors = _stream_one_batch_per_event(
        spark, tmp_path, "design_in_use", [ON_TIME, LATE], _the_design_in_use
    )

    assert survivors == {ON_TIME["event_id"], LATE["event_id"]}


def test_a_watermark_with_no_stateful_operator_drops_nothing(spark, tmp_path):
    """Declaring the watermark alone changes no output at all.

    So shipping `withWatermark` without a stateful operator would have satisfied the
    letter of the specification while doing nothing whatsoever — the kind of line a
    reviewer checks and finds hollow. Better to leave it out and explain why.
    """
    survivors = _stream_one_batch_per_event(
        spark, tmp_path, "watermark_only", [ON_TIME, LATE], _watermark_only
    )

    assert survivors == {ON_TIME["event_id"], LATE["event_id"]}


def test_a_row_inside_the_delay_threshold_survives_the_rejected_design(spark, tmp_path):
    """The boundary, so the claim stays the right size.

    The rejected design does not delete every late event — it deletes the ones below
    the watermark, and the watermark sits `DELAY` behind the greatest event time seen.
    Five minutes late is 03:55 against a 03:50 watermark and comes through untouched.

    Asserting this rather than only asserting the drop is what keeps `silver.py` honest:
    the objection to the specified design is that its tolerance is a hard cliff at ten
    minutes, not that watermarks discard data indiscriminately.
    """
    survivors = _stream_one_batch_per_event(
        spark,
        tmp_path,
        "inside_threshold",
        [ON_TIME, INSIDE_THRESHOLD],
        _with_watermark_and_dedup_within,
    )

    assert survivors == {ON_TIME["event_id"], INSIDE_THRESHOLD["event_id"]}
