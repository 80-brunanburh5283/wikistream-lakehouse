"""Tests that pin the declared schema to what the stream actually sends.

These run without a Spark session: building a `StructType` is pure Python, so the
contract can be checked in milliseconds rather than behind a JVM start-up.

The drift test is the important one. It fails when the upstream adds a field, and
that failure is the feature — a pipeline whose schema silently widens is a
pipeline whose bronze table quietly stops containing everything it received.
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from wikistream.events import EVENT_ID_COLUMN, REQUIRED_FIELDS
from wikistream.streaming.schema import RECENTCHANGE_SCHEMA, TOP_LEVEL_FIELDS

pytestmark = pytest.mark.unit


def _field(schema: StructType, name: str):
    return next(f for f in schema.fields if f.name == name)


def _nested(path: str):
    """Resolve a dotted path through the schema to its StructField."""
    schema: StructType = RECENTCHANGE_SCHEMA
    field = None
    for part in path.split("."):
        field = _field(schema, part)
        if isinstance(field.dataType, StructType):
            schema = field.dataType
    assert field is not None
    return field


def test_schema_covers_every_top_level_key_in_the_captured_sample(sample_events):
    observed = {key for event in sample_events for key in event}
    missing = observed - TOP_LEVEL_FIELDS
    assert not missing, (
        f"the stream sent top-level fields the schema does not declare: {sorted(missing)}. "
        "Add them to RECENTCHANGE_SCHEMA and re-run; do not widen the test."
    )


def test_schema_covers_every_meta_key_in_the_captured_sample(sample_events):
    observed = {key for event in sample_events for key in event.get("meta", {})}
    declared = {f.name for f in _field(RECENTCHANGE_SCHEMA, "meta").dataType.fields}
    assert not observed - declared


def test_schema_declares_nothing_the_sample_never_sent(sample_events):
    # The other direction. A declared column that never arrives is not harmful,
    # but it is usually a typo, and a typo'd column name is a permanently null
    # column that nobody notices.
    observed = {key for event in sample_events for key in event}
    # log_* fields appear in only 2.8% of events, so they are legitimately absent
    # from a small sample; everything else should have been seen.
    unexplained = TOP_LEVEL_FIELDS - observed
    assert not unexplained, f"declared but never observed: {sorted(unexplained)}"


@pytest.mark.parametrize(
    "path",
    ["id", "revision.old", "revision.new", "meta.offset", "timestamp", "log_id"],
)
def test_wide_integers_are_long_not_int(path):
    # Measured in a single 500-event sample: id reached 3,468,178,745,
    # revision.new reached 2,546,877,183 and meta.offset reached 6,524,211,441 —
    # all past the 2,147,483,647 int32 ceiling. Spark's JSON parser returns null
    # on overflow rather than failing, so IntegerType here would delete the
    # revision ids of the busiest wikis with no error anywhere.
    assert isinstance(_nested(path).dataType, LongType)


def test_namespace_is_a_narrow_int_on_purpose():
    # Not an oversight: MediaWiki namespace ids are a small closed set, so the
    # narrower type expresses a real constraint instead of gambling on a range.
    assert isinstance(_nested("namespace").dataType, IntegerType)


def test_byte_lengths_are_long():
    assert isinstance(_nested("length.old").dataType, LongType)
    assert isinstance(_nested("length.new").dataType, LongType)


def test_polymorphic_log_params_is_a_string():
    # log_params is a JSON object for upload/abusefilter/newusers and an empty
    # JSON array for thanks/delete. No StructType describes both. StringType
    # against a JSON structure makes Spark hand back the raw JSON text, which is
    # lossless; the integration test asserts that behaviour against real Spark.
    assert isinstance(_nested("log_params").dataType, StringType)


def test_event_identity_and_time_are_strings_not_parsed_types():
    # meta.id and meta.dt stay strings through bronze. Parsing at the edge means
    # an unparseable value becomes a null with no record of what it was; bronze
    # keeps the received bytes and silver does the casting where a failure can be
    # quarantined.
    assert isinstance(_nested("meta.id").dataType, StringType)
    assert isinstance(_nested("meta.dt").dataType, StringType)


def test_bot_and_minor_are_boolean():
    assert isinstance(_nested("bot").dataType, BooleanType)
    assert isinstance(_nested("minor").dataType, BooleanType)
    assert isinstance(_nested("patrolled").dataType, BooleanType)


def test_every_field_is_nullable():
    # from_json does not enforce non-nullability: it fills null and moves on. A
    # non-nullable declaration would therefore be a promise the parser cannot
    # keep, and would license the optimiser to remove the null check that would
    # have caught the violation. Required-ness lives in REQUIRED_FIELDS instead.
    def walk(schema: StructType, prefix: str = "") -> list[str]:
        problems = []
        for field in schema.fields:
            name = f"{prefix}{field.name}"
            if not field.nullable:
                problems.append(name)
            if isinstance(field.dataType, StructType):
                problems.extend(walk(field.dataType, f"{name}."))
        return problems

    assert walk(RECENTCHANGE_SCHEMA) == []


def test_required_fields_all_exist_in_the_schema():
    for path in REQUIRED_FIELDS:
        assert _nested(path) is not None


def test_required_fields_are_present_in_every_sampled_event(sample_events):
    # If this ever fails, the quarantine path is not hypothetical any more.
    for path in REQUIRED_FIELDS:
        parts = path.split(".")
        for event in sample_events:
            node = event
            for part in parts:
                node = node.get(part) if isinstance(node, dict) else None
            assert node is not None, f"{path} was null in a sampled event"


def test_dedup_key_is_named_event_id():
    assert EVENT_ID_COLUMN == "event_id"


def test_schema_field_names_are_sql_safe_except_the_documented_one(sample_events):
    # `$schema` is the one field name that needs quoting in SQL. It is kept
    # rather than renamed at the parse step so bronze stays a faithful copy of
    # the payload; silver renames it. Any *new* awkward name should be noticed.
    awkward = [f.name for f in RECENTCHANGE_SCHEMA.fields if not f.name.replace("_", "").isalnum()]
    assert awkward == ["$schema"]
