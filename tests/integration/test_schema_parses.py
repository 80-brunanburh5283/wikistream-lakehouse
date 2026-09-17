"""Assert what Spark's parser really does with the declared schema.

Everything here needs a JVM and nothing here needs Docker, so these carry the
`spark` marker rather than `integration`. They are the tests that turn the
comments in `wikistream.streaming.schema` from claims into checked facts — in
particular the three `from_json` behaviours the quarantine design depends on,
each of which is the opposite of the intuitive guess.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from wikistream.streaming.schema import (
    CORRUPT_RECORD_COLUMN,
    PARSE_OPTIONS,
    PARSE_SCHEMA,
    RECENTCHANGE_SCHEMA,
)

pytestmark = pytest.mark.spark

INT32_MAX = 2**31 - 1


@pytest.fixture(scope="module")
def parsed_sample(spark, sample_events):
    """The 500 captured events, parsed through the production schema."""
    raw = [(json.dumps(event, ensure_ascii=False),) for event in sample_events]
    return (
        spark.createDataFrame(raw, "raw string")
        .select(F.from_json("raw", PARSE_SCHEMA, PARSE_OPTIONS).alias("e"))
        .select("e.*")
        .cache()
    )


@pytest.fixture(scope="module")
def parsed_adversarial(spark, adversarial_events):
    raw = [(json.dumps(event, ensure_ascii=False),) for event in adversarial_events]
    return (
        spark.createDataFrame(raw, "raw string")
        .select(F.from_json("raw", PARSE_SCHEMA, PARSE_OPTIONS).alias("e"))
        .select("e.*")
        .cache()
    )


def _parse_one(spark, payload: str):
    """Parse a single raw string and return the resulting Row."""
    return (
        spark.createDataFrame([(payload,)], "raw string")
        .select(F.from_json("raw", PARSE_SCHEMA, PARSE_OPTIONS).alias("e"))
        .select("e.*")
        .first()
    )


# --------------------------------------------------------------------------
# The schema parses real traffic without loss
# --------------------------------------------------------------------------


def test_every_captured_event_parses(parsed_sample, sample_events):
    assert parsed_sample.count() == len(sample_events)


def test_no_captured_event_is_treated_as_corrupt(parsed_sample):
    assert parsed_sample.filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull()).count() == 0


def test_required_fields_survive_parsing_for_every_captured_event(parsed_sample):
    bad = parsed_sample.filter(
        F.col("meta.id").isNull() | F.col("meta.dt").isNull() | F.col("meta.domain").isNull()
    )
    assert bad.count() == 0


def test_event_ids_are_still_distinct_after_parsing(parsed_sample, sample_events):
    assert parsed_sample.select("meta.id").distinct().count() == len(sample_events)


def test_type_distribution_survives_parsing(parsed_sample, sample_events):
    from_spark = {
        row["type"]: row["n"]
        for row in parsed_sample.groupBy("type").count().withColumnRenamed("count", "n").collect()
    }
    expected: dict[str, int] = {}
    for event in sample_events:
        expected[event["type"]] = expected.get(event["type"], 0) + 1
    assert from_spark == expected


# --------------------------------------------------------------------------
# Integer widths — the failure this would otherwise cause is silent
# --------------------------------------------------------------------------


@pytest.mark.parametrize("column", ["id", "revision.new", "meta.offset"])
def test_values_beyond_int32_survive_parsing(parsed_sample, sample_events, column):
    # If any of these columns were IntegerType, Spark would return null rather
    # than raising, and the largest wikis would lose their revision ids while
    # every row count stayed correct. So the check is not "nothing is null" —
    # `id` is legitimately null on log events — but "the maximum value Python saw
    # in the raw JSON is the maximum value Spark reports after parsing".
    parts = column.split(".")
    raw_values = []
    for event in sample_events:
        node: object = event
        for part in parts:
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, int) and not isinstance(node, bool):
            raw_values.append(node)

    assert max(raw_values) > INT32_MAX, f"expected {column} to exceed the int32 ceiling"
    from_spark = parsed_sample.select(F.max(F.col(column)).alias("m")).first()["m"]
    assert from_spark == max(raw_values)
    assert parsed_sample.filter(F.col(column) > INT32_MAX).count() > 0


def test_a_value_past_int32_round_trips_exactly(spark):
    payload = json.dumps(
        {
            "meta": {
                "id": "x",
                "dt": "2026-09-17T04:00:00.000Z",
                "domain": "d",
                "offset": 6524211441,
            },
            "id": 3468178745,
            "revision": {"old": 2546877176, "new": 2546877183},
        }
    )
    row = _parse_one(spark, payload)
    assert row["id"] == 3468178745
    assert row["revision"]["new"] == 2546877183
    assert row["meta"]["offset"] == 6524211441


# --------------------------------------------------------------------------
# The polymorphic field
# --------------------------------------------------------------------------


def test_log_params_object_is_returned_as_raw_json(parsed_adversarial):
    row = parsed_adversarial.filter(F.col("log_type") == "upload").select("log_params").first()
    # Not a stringified Row and not null: the original JSON text.
    assert json.loads(row["log_params"]) == {
        "img_sha1": "0mnr4pq7wxyz1abc2def3ghi4jkl5mn",
        "img_timestamp": "20260917040003",
    }


def test_log_params_empty_array_is_returned_as_raw_json(parsed_adversarial):
    # The case no StructType can express alongside the object form.
    row = parsed_adversarial.filter(F.col("log_type") == "thanks").select("log_params").first()
    assert json.loads(row["log_params"]) == []


def test_log_params_with_mixed_value_types_is_preserved(spark):
    # `{"filter": "1245", "log": 45144167}` — string and number in one object,
    # which is also why MapType would not work here.
    params = {"action": "edit", "filter": "1245", "actions": "disallow", "log": 45144167}
    payload = json.dumps({"meta": {"id": "x"}, "log_type": "abusefilter", "log_params": params})
    row = _parse_one(spark, payload)
    assert json.loads(row["log_params"]) == params


def test_every_captured_log_params_round_trips(parsed_sample, sample_events):
    expected = [
        event["log_params"] for event in sample_events if event.get("log_params") is not None
    ]
    assert expected, "expected the sample to contain log events"
    got = [
        json.loads(row["log_params"])
        for row in parsed_sample.filter(F.col("log_params").isNotNull())
        .select("log_params")
        .collect()
    ]
    assert sorted(map(json.dumps, got), key=str) == sorted(map(json.dumps, expected), key=str)


# --------------------------------------------------------------------------
# Malformed input: the three counter-intuitive behaviours
# --------------------------------------------------------------------------


def test_malformed_json_does_not_produce_a_null_struct(spark):
    # The finding that shapes the quarantine predicate. `parsed IS NULL` looks
    # like a corruption test and is not one: garbage parses to an ordinary struct
    # whose fields are all null, so a predicate built on it admits garbage as a
    # row of nulls.
    df = spark.createDataFrame([("{not json}",)], "raw string").select(
        F.from_json("raw", PARSE_SCHEMA, PARSE_OPTIONS).alias("e")
    )
    assert df.select(F.col("e").isNull()).first()[0] is False


def test_malformed_json_leaves_every_contract_field_null(spark):
    row = _parse_one(spark, "{not json}")
    declared = [f.name for f in RECENTCHANGE_SCHEMA.fields]
    assert all(row[name] is None for name in declared)


def test_malformed_json_is_captured_in_the_corrupt_record_column(spark):
    row = _parse_one(spark, "{not json}")
    assert row[CORRUPT_RECORD_COLUMN] == "{not json}"


@pytest.mark.parametrize(
    "payload",
    ['{"meta":{"id":"x"', "[1,2,3]", "42", "null", "not json at all"],
)
def test_various_unreadable_payloads_are_captured_rather_than_dropped(spark, payload):
    row = _parse_one(spark, payload)
    assert row[CORRUPT_RECORD_COLUMN] == payload
    assert row["meta"] is None


def test_empty_string_is_the_one_input_that_yields_a_null_struct(spark):
    # Documented rather than fixed, because it is the one case where the intuitive
    # test does work — and knowing it is the *only* one is the point.
    df = spark.createDataFrame([("",)], "raw string").select(
        F.from_json("raw", PARSE_SCHEMA, PARSE_OPTIONS).alias("e")
    )
    assert df.select(F.col("e").isNull()).first()[0] is True


def test_failfast_mode_would_kill_the_query(spark):
    # Why the pipeline does not use FAILFAST. On a streaming query this exception
    # is not a rejected row, it is a stopped pipeline — one malformed byte on the
    # wire halting ingestion for every wiki.
    df = spark.createDataFrame([("{not json}",)], "raw string").select(
        F.from_json("raw", RECENTCHANGE_SCHEMA, {"mode": "FAILFAST"}).alias("e")
    )
    with pytest.raises(Exception, match=r"MALFORMED_RECORD_IN_PARSING|Malformed"):
        df.collect()


def test_a_valid_event_missing_required_fields_is_not_corrupt(parsed_adversarial):
    # The distinction the quarantine table records. This payload is well-formed
    # JSON — nothing for the parser to complain about — and still unusable,
    # because it has no deduplication key. Two different reasons for rejection
    # that a single "invalid" flag would conflate.
    row = (
        parsed_adversarial.filter(F.col("title") == "Adversarial:missing_meta_id")
        .select(CORRUPT_RECORD_COLUMN, "meta.id", "meta.dt")
        .first()
    )
    assert row[CORRUPT_RECORD_COLUMN] is None
    assert row["id"] is None
    assert row["dt"] is not None


# --------------------------------------------------------------------------
# Schema evolution and awkward values
# --------------------------------------------------------------------------


def test_unknown_fields_are_ignored_and_the_event_still_parses(parsed_adversarial):
    # Forward compatibility: a payload carrying fields the schema has never seen
    # must not be quarantined. The unknown values are dropped, which is a real
    # loss and the reason bronze also keeps the raw payload.
    row = (
        parsed_adversarial.filter(F.col("title") == "Adversarial:unknown_future_field")
        .select("meta.id", "length.new", CORRUPT_RECORD_COLUMN)
        .first()
    )
    assert row["id"] == "aaaaaaa7-0000-4000-8000-000000000007"
    assert row["new"] == 91240
    assert row[CORRUPT_RECORD_COLUMN] is None


def test_comment_with_newline_quote_tab_and_astral_emoji_round_trips(parsed_adversarial):
    row = (
        parsed_adversarial.filter(F.col("title").contains("comment_with_newline"))
        .select("comment")
        .first()
    )
    comment = row["comment"]
    assert "\n" in comment
    assert '"' in comment
    assert "\t" in comment
    assert "\\" in comment
    # An astral-plane character, which is two UTF-16 code units in the JVM.
    assert "🚀" in comment
    assert "编辑要旨" in comment


def test_null_bearing_log_event_parses_with_null_length_and_revision(parsed_adversarial):
    row = (
        parsed_adversarial.filter(F.col("log_type") == "upload")
        .select("length", "revision", "meta.id")
        .first()
    )
    assert row["length"] is None
    assert row["revision"] is None
    assert row["id"] is not None


def test_page_creation_has_null_old_length_but_a_new_length(parsed_adversarial):
    row = (
        parsed_adversarial.filter(F.col("title") == "Adversarial:new_page_null_old_length")
        .select("length.old", "length.new")
        .first()
    )
    assert row["old"] is None
    assert row["new"] == 1520


def test_meta_dt_casts_to_a_timestamp(parsed_adversarial):
    # Deferred to silver rather than done in the parse schema, but it has to be
    # possible, and the millisecond component has to survive.
    row = (
        parsed_adversarial.filter(F.col("title") == "Adversarial:duplicate_event_id")
        .select(F.col("meta.dt").cast("timestamp").alias("ts"))
        .first()
    )
    assert row["ts"].microsecond == 250_000
