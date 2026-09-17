# Data contracts

Three tables, one per layer of the lakehouse. This document is the human-readable
half of the contract; the machine-readable half is `src/wikistream/streaming/tables.py`,
which holds the DDL as SQL and is the only thing that creates a table. If the two
disagree, the DDL is right and this file is stale — the column lists here are meant
to be checkable against it by eye.

All three tables are written. `bronze.recentchange_raw` comes from
`make stream-bronze`; `silver.edits` and `silver.quarantine` come from
`make stream-silver`, which reads the same Kafka topic rather than reading bronze —
see `src/wikistream/streaming/silver.py` for why, and "What you may not rely on"
below for what that costs.

```bash
make init-tables                                              # create all three
make sql SQL="DESCRIBE TABLE lakehouse.bronze.recentchange_raw"
make table-stats                                              # rows, files, partitions
make verify-no-duplicates                                     # the uniqueness gate
```

## The layering rule

Bronze interprets almost nothing. Silver interprets everything. That split is the
contract that matters more than any column list, because it is what makes the
pipeline recoverable: a bug in the silver transformation is repaired by dropping
silver and replaying bronze, and that is only possible if bronze applied no
judgement worth doubting.

| | bronze | silver |
|---|---|---|
| Row identity | one Kafka record | one source event |
| Duplicates | kept, expected | removed by `MERGE` on `event_id` |
| Invalid frames | stored as-is | routed to `quarantine` with a reason |
| Late events | indistinguishable from on-time ones | measured, in `late_by_seconds` |
| Write mode | append only | merge |
| Partitioned by | `days(ingest_date)` — when we read it | `days(event_date)` — when it happened |
| Rebuildable from | Kafka, while retention lasts (24h) | bronze, indefinitely |

The partitioning asymmetry is deliberate and it is the one design point in this
document that a reviewer should push on. Ingest date is monotonic: a replay of last
week's events writes into today's bronze partition and rewrites nothing, so the
audit log's old partitions are immutable once written and stay compacted. Event
date is not monotonic: a late event writes into an old silver partition, so silver
pays a rewrite cost that bronze does not. Silver pays it because every analytical
question — edits per hour, bytes changed per day — is asked about when an edit
happened, and partitioning an analytics table by ingest time makes every one of
those questions a full scan.

## `bronze.recentchange_raw`

One row per Kafka record. Append-only, never updated, never deduplicated.

| Column | Type | Source | Null? | Notes |
|---|---|---|---|---|
| `event_id` | string | `meta.id` in the payload | yes | Not unique here. A producer restart or an upstream SSE replay legitimately lands the same id twice. |
| `event_time` | timestamp | `meta.dt` | yes | The instant the wiki recorded the change. UTC. |
| `schema_uri` | string | `$schema` | yes | `/mediawiki/recentchange/1.0.0` for every row observed so far. Kept so upstream version drift is a `SELECT DISTINCT`. |
| `raw_payload` | string | the frame | no | The frame verbatim, byte for byte. |
| `kafka_topic` | string | Kafka | no | |
| `kafka_partition` | int | Kafka | no | |
| `kafka_offset` | bigint | Kafka | no | With `kafka_partition`, the natural key of the row. |
| `kafka_timestamp` | timestamp | Kafka | no | Broker append time. Later than `event_time` by the ingest path's latency. |
| `ingested_at` | timestamp | Spark | no | Processing time. One value per micro-batch, not per row. |
| `ingest_date` | date | derived | no | `to_date(ingested_at)`. The partition column. |

The three interpreted columns are an index over `raw_payload`, not a substitute for
it. A frame whose JSON does not parse still lands: `from_json` yields a null struct,
all three columns come out null, and the row is appended anyway. Bronze counting a
malformed frame as data is the reason the quarantine path in silver can ever fire.

### What you may rely on

* **`(kafka_partition, kafka_offset)` is unique.** Enforced by Kafka's own
  semantics plus Spark's checkpoint and Iceberg's per-epoch commit, and asserted by
  `test_every_produced_frame_lands_exactly_once`.
* **`raw_payload` is what the wire carried.** Not a reserialisation. Asserted
  character-for-character by `test_raw_payload_is_byte_exact`.
* **Every row of one commit shares one `ingested_at`.** So `ingest_date` is a
  property of the batch, and a batch running across midnight does not split across
  two partitions. Asserted by
  `test_kafka_coordinates_and_ingest_metadata_are_populated`.
* **A restart resumes from the checkpoint,** not from the topic's start. Asserted
  by `test_rerunning_the_job_adds_nothing`.

### What you may not rely on

* **`event_id` being unique.** It is not, by design.
* **Completeness against the upstream source.** Bronze holds what Kafka still had
  when the job read it. The job runs with `failOnDataLoss=false`, so records that
  passed the topic's 24-hour retention while the job was down are skipped
  silently, and nothing in the table distinguishes that from a quiet period. See
  the module docstring in `src/wikistream/streaming/bronze.py` for why that trade
  was made and when it would be the wrong one.
* **Ordering by offset meaning ordering by event time.** Within one partition the
  offsets are in the order the producer read the stream, which is close to but not
  identical to event order, and across partitions there is no relationship at all.
* **`event_time` being trustworthy for a hostile payload.** It is whatever the
  frame said. Range checking belongs to silver.

## `silver.edits`

One row per source event, keyed on `event_id`.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string, not null | Unique by `MERGE`, not by constraint — see below. |
| `event_time` | timestamp, not null | From `meta.dt`, parsed with `try_to_timestamp`. |
| `wiki`, `domain` | string | `enwiki`, `en.wikipedia.org`. `domain` is the Kafka partition key. |
| `change_type` | string | `edit`, `new`, `log`, `categorize`. |
| `namespace_id`, `is_article` | int, boolean | `is_article` is `namespace_id = 0`, and is **null** when the namespace is unknown: "we do not know" and "not an article" are different answers. |
| `page_title`, `page_url` | string | |
| `editor`, `is_bot`, `is_minor`, `is_anonymous` | string, boolean × 3 | `is_anonymous` covers IP editors and MediaWiki temporary accounts. |
| `bytes_old`, `bytes_new`, `bytes_delta` | bigint | `bytes_delta` is null when `bytes_new` is null, which happens for log events, and equals `bytes_new` when only `bytes_old` is null, which is a page creation. |
| `rev_old`, `rev_new` | bigint | bigint, not int: the live values already exceed int32. |
| `comment` | string | |
| `late_by_seconds` | bigint | `ingested_at - event_time`, stored so lateness is queryable rather than recomputed. Negative means the wiki's clock is ahead of ours. |
| `ingested_at` | timestamp | Processing time, one value per micro-batch. On a rebuild it is bronze's stored value, not the replay's clock. |
| `event_date` | date | The partition column, from `event_time`. |

**Iceberg has no primary key and no uniqueness constraint.** Nothing in the table
prevents a duplicate `event_id`. The only thing that makes the uniqueness claim true
is that every write goes through `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`,
which is why the writer has one code path and not two: both the stream and the
rebuild call `merge_batch`.

An invariant with no test is a wish, so there is a gate, and it exits non-zero:

```bash
make verify-no-duplicates    # duplicate event_id, null keys, duplicate quarantine offsets
make table-stats             # prints distinct and repeated event_id per table
```

`make table-stats` is the more interesting of the two, because it prints the same
number for bronze, where the expected answer is the opposite: bronze is an audit log
and a repeated `event_id` there is correct. Zero duplicates in silver only means
something next to evidence that duplicates existed upstream.

### What you may rely on

* **`event_id` is unique.** Asserted three ways: on the statement text by
  `test_the_merge_joins_only_on_the_declared_key`, against a real table by
  `test_silver_holds_no_duplicate_event_id`, and against a table that has had a
  duplicate forced into it by `test_the_gate_fails_when_a_duplicate_is_inserted` —
  because a gate that has never failed is not known to work.
* **Every bronze row reaches exactly one of the two silver tables.** The split is a
  partition of the input, not two filters that happen to look complementary;
  `test_every_frame_lands_in_exactly_one_table` asserts it on the frame level and
  `test_the_two_tables_account_for_every_distinct_event` asserts the arithmetic on
  landed tables.
* **Which duplicate wins is deterministic.** The lowest `(kafka_partition,
  kafka_offset)` — first arrival. Not "arbitrary but consistent": stable across a
  replay of the same offsets, which is what makes a rebuild reproducible.
  `test_the_earlier_offset_won_the_divergent_pair`.
* **A rebuild reconstructs every column derived from the payload exactly.**
  `test_a_rebuild_replaces_a_row_deleted_by_mistake` deletes a row, replays bronze,
  and compares the restored row field by field rather than counting rows. The two
  columns derived from *arrival* rather than from the payload are the exception, and
  they are the next bullet down.

### What you may not rely on

* **Silver being a subset of bronze at any instant.** The two jobs read the same
  topic independently and hold separate checkpoints, so either can be ahead. A set
  difference between them is a race, not a defect, which is why the duplicate gate
  deliberately does not check completeness.
* **`late_by_seconds` being comparable across a rebuild.** It is `ingested_at -
  event_time`, and the two writers get `ingested_at` from different places: the
  stream stamps its own micro-batch clock, the rebuild reads what bronze stamped when
  bronze consumed the same record. Since the jobs consume the topic independently,
  a rebuilt row's lateness differs from the streamed row's by however far apart the
  two batches ran — smaller, normally, because bronze usually gets there first.
  `test_a_rebuilt_row_carries_bronzes_arrival_time_not_silvers` measures the shift
  and asserts it is exactly the gap between the two arrival stamps. Taking bronze's
  value is the deliberate choice: judging a replayed week-old event against today's
  clock would report every row as a week late. A lateness histogram is therefore a
  statement about the stream, not about the table.
* **Late events being present.** They are, for any lateness — there is no watermark
  and no `dropDuplicatesWithinWatermark`, deliberately, and
  `tests/spark/test_watermark_would_drop_data.py` measures what the rejected design
  would have discarded. The cost is that write amplification grows with table size
  instead of staying flat; ADR-0019 states the trade and when it would be wrong.
* **`is_bot` meaning "not a human".** It is the flag the wiki set, which bots set on
  themselves. An unflagged bot is indistinguishable from a person here.

## `silver.quarantine`

Rows that could not be made into a silver row, with the reason.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | Nullable: the reason may be that there is no id. |
| `raw_payload` | string | The frame, so a fix can be replayed rather than merely counted. |
| `failure_reason` | string | Which rule rejected it. One of seven values; `FAILURE_REASONS` in `quality/expectations.py` is the closed set, which is what makes a `GROUP BY` on this column finite. |
| `kafka_partition`, `kafka_offset` | int, bigint | The MERGE key of this table, and the address to go and look at the record. |
| `failed_at` | timestamp | The batch's `ingested_at`. |
| `ingest_date` | date | Partition column. The question asked of this table is always "what went wrong recently". |

A pipeline that drops bad rows silently is broken. A pipeline that dies on one bad
row is also broken. This table is the third option, and it stores the payload so
that a day's rejects can be repaired and replayed rather than only counted.

**It keys on `(kafka_partition, kafka_offset)`, not on `event_id`,** because "the
payload has no `meta.id`" is one of the reasons a row lands here. A MERGE on a null
key matches nothing and would insert the row again on every retry — the one table in
the pipeline whose job is to record broken data would be the one that duplicates.
ADR-0021 has the alternatives.

That key is a transport address rather than an event identity, and the consequence is
worth stating: the same logical event replayed by the source arrives at two offsets
and occupies two quarantine rows. For a log of what arrived that is the behaviour I
want, but it means `count(*)` here answers "how many bad frames arrived", not "how
many distinct events are broken".

```bash
make sql SQL="SELECT failure_reason, count(*) FROM lakehouse.silver.quarantine GROUP BY 1"
```

## Types worth explaining

`timestamp` in Spark maps to Iceberg's `timestamptz` — an instant, stored as UTC.
Every timestamp in every table here is an instant. A zone-less local timestamp
would make `days(...)` partitioning ambiguous for one hour twice a year in any zone
that observes daylight saving, and the bug would only appear for people not running
in UTC.

`raw_payload` is a string, not binary. The frames are UTF-8 JSON, and keeping them
as text means `SELECT raw_payload` is readable in Trino without a decode step. The
cost is that a frame which is not valid UTF-8 cannot be stored verbatim: the source
decodes with `errors="replace"`, so such a frame is stored with replacement
characters instead of being rejected, and the JSON parse failure downstream is what
sends it to quarantine.

**Every integer that comes from the source or is computed from one is `bigint`.**
Four fields in the payload already overflow int32 — observed values include
`id = 3,468,178,745` and `meta.offset = 6,524,211,441`, both from the 500-event
sample — so `id`, `revision.old`, `revision.new` and `meta.offset` have to be wide.
`meta.dt`'s epoch-seconds twin is `bigint` for the ordinary reason that epoch seconds
stop fitting in int32 in January 2038. Only `namespace` is narrow, and that is a
genuine constraint rather than a gamble: MediaWiki namespace ids are a small closed
set, measured range 0..106 with negatives for `Special:` and `Media:`.

Declaring one of those as `int` does not truncate and does not null the column.
Measured under PERMISSIVE mode on Spark 4.0.4: a value too wide for the declared type
sets `_corrupt_record`, which this pipeline treats as an unreadable payload — so the
**whole event** is quarantined, not just the offending field. Loud rather than silent,
which is the better failure, but the loss is bigger than it looks: every edit on the
largest wikis would land in `silver.quarantine` and nowhere else.

`bytes_old`, `bytes_new`, `bytes_delta` and `late_by_seconds` are `bigint` for a
different reason, and I got this one wrong first. The argument for `int` was that
`event_time_before_wikipedia` bounds how far an event's clock can drift, so lateness
is bounded too. It bounds drift in one direction only. A payload stamped 2099 gives
`late_by_seconds = -2,281,290,900`, which overflows int32 — and the value is computed
for every row *before* the rule that rejects that row gets to run, so under ANSI mode
the narrowing cast raised, the micro-batch died, and it would have died again on
every replay of the same offsets. A validation rule cannot protect a column computed
upstream of it. `test_every_frame_lands_in_exactly_one_table` is what found it.

## Table properties

Applied to all three tables by `tables.py`:

| Property | Value | Why |
|---|---|---|
| `format-version` | `2` | v2 enables row-level deletes, which `MERGE INTO` needs. v1 would rewrite whole files instead. |
| `write.format.default` | `parquet` | Iceberg's current default, stated so a future default change does not silently alter the file format. |
| `write.parquet.compression-codec` | `zstd` | Same reasoning as the Kafka topic — see `docs/throughput.md`. Measured on bytes, not on CPU. |
| `write.target-file-size-bytes` | 128 MiB | The 512 MiB default is unreachable at this ingest rate, so every file would stay small forever. |
| `history.expire.max-snapshot-age-ms` | 7 days | Long enough for time travel to be demonstrable, short enough that metadata does not grow without bound on a laptop. |
| `write.metadata.delete-after-commit.enabled` | `true` | Iceberg keeps every `metadata.json` by default: one per commit, 2,880 a day at a 30-second trigger. |
| `write.metadata.previous-versions-max` | `20` | |

Sort orders are set as table properties rather than at write time, so that
compaction produces files sorted the way the streaming writes were:
`silver.edits` by `event_time`, `bronze.recentchange_raw` by
`(kafka_partition, kafka_offset)` — the order bronze data already arrives in, so it
costs nothing at write time and makes an offset-range replay skip files on
statistics.

## Schema evolution

The source can add a field at any time; MediaWiki's `recentchange` schema is
versioned but the version has not moved during this project.

* **Bronze needs no change.** A new field is inside `raw_payload` the moment it
  appears, and `schema_uri` records which version produced it.
* **Silver needs a column,** added with `ALTER TABLE ... ADD COLUMN` and a change
  to the projection. Iceberg tracks columns by id rather than by position, so an
  added column reads as null for old files and no data is rewritten.
* **A removed or retyped upstream field** is the case that breaks: `from_json`
  returns null for a field it cannot coerce, so the failure is silent — the
  pipeline stays green and the column quietly empties. Detecting it needs a check
  on the *shape* of the data rather than on the pipeline's health, which is what
  the quarantine reason codes and the orchestration layer's asset checks are for.
  In a system where you own both the producer and the consumer, the better answer
  is a schema registry that refuses the incompatible change at the source. Here
  the producer is Wikimedia's, so detection after the fact is the only option
  available.
