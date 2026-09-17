"""The explicit Spark schema for a `recentchange` event.

Every type and every nullability decision here was read off a 500-event capture
(`tests/fixtures/recentchange_sample.jsonl`, taken 2026-09-17), not off the
stream's documentation. The two disagree: the documented field table omits
`server_script_path`, `notify_url` and all five `log_*` fields, every one of
which the stream actually sends.

Why the schema is written out instead of inferred
-------------------------------------------------
Spark can infer a JSON schema, and on a streaming source it must not.

1. Inference samples data. Two runs that sample different micro-batches infer
   different schemas, so the table's column set becomes a function of when the
   job started. With `type` distributed as measured — 52% `categorize`, 42%
   `edit`, 3% `new`, 3% `log` — a job that starts during a quiet minute can miss
   `log` events entirely and infer a schema with no `log_type` column at all.
2. Inference on a stream needs a pass over data before processing it, which is
   why `spark.readStream.json` refuses to infer without
   `spark.sql.streaming.schemaInference` being switched on.
3. An inferred width is a guess. `id` was inferred-safe as a 32-bit int in most
   samples and is not: see the width notes below.

So the schema is a declared contract. When the upstream adds a field, this file
does not silently change shape — `tests/unit/test_schema.py` fails instead,
which is the notification we want.

Why every field is nullable
---------------------------
`from_json` does not enforce non-nullability. Marking a field non-nullable does
not make Spark reject a row that lacks it; the parser fills null and moves on,
and downstream code that trusted the declaration then meets a null it was
promised could not exist. Worse, a non-nullable declaration is a licence for the
optimiser to fold away the null check that would have caught it.

Required-ness is therefore enforced where it can be — as an explicit predicate
over the parsed row, routing failures to `silver.quarantine`.
`wikistream.events.REQUIRED_FIELDS` is that predicate's input, and it is the only
place "required" means anything. It lives there rather than here so that the
producer, which has no Spark in its image, can read the same contract.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Integer widths
#
# Four fields overflow a 32-bit int, and all four were caught in a single
# 500-event sample rather than in production six months later:
#
#   field          max seen in 500 events   int32 max
#   id                       3,468,178,745  2,147,483,647
#   revision.old             2,546,877,176  2,147,483,647
#   revision.new             2,546,877,183  2,147,483,647
#   meta.offset              6,524,211,441  2,147,483,647
#
# On overflow Spark's JSON parser yields null, not an error, so an IntegerType
# here would quietly delete the revision ids of the largest wikis while every
# count and every health check stayed green. `timestamp` fits int32 today and is
# LongType anyway, because epoch seconds stop fitting in January 2038.
#
# `namespace` is IntegerType on purpose: MediaWiki namespace ids are a small
# closed set (measured range 0..106, and negative values exist for Special: and
# Media:), so the narrower type is a genuine constraint rather than a gamble.
# ---------------------------------------------------------------------------

#: Wikimedia's event envelope. `meta.id` is a 36-character UUID in 100% of the
#: sample and is the deduplication key for the whole pipeline.
_META_SCHEMA = StructType(
    [
        StructField("uri", StringType(), nullable=True),
        StructField("request_id", StringType(), nullable=True),
        StructField("id", StringType(), nullable=True),
        StructField("dt", StringType(), nullable=True),
        StructField("domain", StringType(), nullable=True),
        StructField("stream", StringType(), nullable=True),
        # `topic` carries a datacenter prefix — every event in the sample was
        # `eqiad.mediawiki.recentchange`. Wikimedia runs eqiad/codfw
        # active-passive, so this value changing is the visible signature of a
        # datacenter failover, which is also one of the moments redelivery
        # happens. Kept for exactly that diagnostic reason.
        StructField("topic", StringType(), nullable=True),
        StructField("partition", IntegerType(), nullable=True),
        StructField("offset", LongType(), nullable=True),
    ]
)

#: Byte size of the page before and after. Absent — the whole struct, not just
#: its members — for every `categorize` and `log` event, which is 55% of traffic.
_SIZE_SCHEMA = StructType(
    [
        StructField("old", LongType(), nullable=True),
        StructField("new", LongType(), nullable=True),
    ]
)

RECENTCHANGE_SCHEMA = StructType(
    [
        StructField("$schema", StringType(), nullable=True),
        StructField("meta", _META_SCHEMA, nullable=True),
        # The recentchanges row id. Null in 0.4% of the sample; both cases were
        # `log` events (an abusefilter hit and a thanks), which have no rc row.
        # This is why the dedup key is meta.id and not id.
        StructField("id", LongType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("namespace", IntegerType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("title_url", StringType(), nullable=True),
        StructField("comment", StringType(), nullable=True),
        StructField("parsedcomment", StringType(), nullable=True),
        # Epoch seconds. Duplicates meta.dt to the second; meta.dt is the one
        # used for event time because it carries milliseconds.
        StructField("timestamp", LongType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("bot", BooleanType(), nullable=True),
        # Present only on edits: absent for 55% (categorize/log) and 65%
        # respectively. Absent is not the same as false, which is why neither is
        # coalesced to a default here — that judgement belongs to the mart that
        # asks the question, not to the parser.
        StructField("minor", BooleanType(), nullable=True),
        StructField("patrolled", BooleanType(), nullable=True),
        StructField("length", _SIZE_SCHEMA, nullable=True),
        StructField("revision", _SIZE_SCHEMA, nullable=True),
        StructField("server_url", StringType(), nullable=True),
        StructField("server_name", StringType(), nullable=True),
        StructField("server_script_path", StringType(), nullable=True),
        StructField("wiki", StringType(), nullable=True),
        StructField("notify_url", StringType(), nullable=True),
        # --- log events only (2.8% of the sample) ---
        StructField("log_id", LongType(), nullable=True),
        StructField("log_type", StringType(), nullable=True),
        StructField("log_action", StringType(), nullable=True),
        # Polymorphic in the wild: a JSON object for upload/abusefilter/newusers
        # (12 of 14 log events) and an empty JSON array for thanks/delete (2 of
        # 14). No StructType can describe both, and a MapType cannot hold the
        # mixed value types inside it (`{"filter": "1245", "log": 45144167}` is
        # string and number in one object).
        #
        # Declaring StringType against a JSON object or array is not a mistake
        # here: Spark's JacksonParser, on a StringType target, copies the raw
        # JSON structure back out as text. So this column holds the original
        # substring, losslessly, and callers that care can parse it themselves.
        # `tests/integration/test_schema_parses.py` asserts that behaviour
        # against a real Spark session rather than trusting the description.
        StructField("log_params", StringType(), nullable=True),
        StructField("log_action_comment", StringType(), nullable=True),
    ]
)

#: Every top-level key the schema declares. Used by the drift test.
TOP_LEVEL_FIELDS: frozenset[str] = frozenset(field.name for field in RECENTCHANGE_SCHEMA.fields)

# ---------------------------------------------------------------------------
# Parse-time envelope
#
# `RECENTCHANGE_SCHEMA` above is the contract: exactly the fields the source
# sends. What gets handed to `from_json` is that contract plus one column, for a
# reason established by measurement rather than assumed.
#
# Three behaviours of `from_json`, each verified against Spark 4.0.4 in
# `tests/integration/test_schema_parses.py`:
#
# 1. A malformed payload does NOT produce a null struct. `from_json("{not json}")`
#    returns a perfectly ordinary struct whose every field happens to be null.
#    Only a zero-length string yields an actual null struct. So `parsed IS NULL`
#    is not a corruption test, and any quarantine predicate built on it would let
#    garbage through as a row of nulls.
# 2. Naming a corrupt-record column makes the parser put the offending raw text
#    in it. That turns "something was wrong" into "here is exactly what arrived",
#    which is the difference between a dead-letter table worth reading and one
#    that only restates that a failure happened.
# 3. `mode=FAILFAST` raises and kills the query. On a streaming job that is a
#    single malformed byte on the wire stopping the pipeline, so PERMISSIVE plus
#    an explicit quarantine step is the design, and this is the evidence for it.
#
# The extra column is kept out of `RECENTCHANGE_SCHEMA` so that the drift test
# keeps comparing the declared contract against what the wire sent, with no
# locally-invented field to explain away.
# ---------------------------------------------------------------------------

#: Where the parser puts the raw text of a payload it could not read.
CORRUPT_RECORD_COLUMN = "_corrupt_record"

#: The contract plus the corrupt-record sidecar. This is what `from_json` gets.
PARSE_SCHEMA = StructType(
    [*RECENTCHANGE_SCHEMA.fields, StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
)

#: Options that go with `PARSE_SCHEMA`. PERMISSIVE is the default, stated
#: explicitly because the alternative is a streaming query that dies on one bad
#: record and the choice should be visible at the call site.
PARSE_OPTIONS: dict[str, str] = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN,
}
