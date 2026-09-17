# Data contracts

Three tables, one per layer of the lakehouse. This document is the human-readable
half of the contract; the machine-readable half is `src/wikistream/streaming/tables.py`,
which holds the DDL as SQL and is the only thing that creates a table. If the two
disagree, the DDL is right and this file is stale — the column lists here are meant
to be checkable against it by eye.

Status as of the bronze layer landing: `bronze.recentchange_raw` is written by
`make stream-bronze` and has data. `silver.edits` and `silver.quarantine` are
created empty by `make init-tables`; their columns are declared below because the
declaration is what the bronze layer was designed against, but nothing writes to
them yet.

```bash
make init-tables                                              # create all three
make sql SQL="DESCRIBE TABLE lakehouse.bronze.recentchange_raw"
make table-stats                                              # rows, files, partitions
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

One row per source event, keyed on `event_id`. Declared now, written by the silver
layer.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string, not null | Unique by `MERGE`, not by constraint — see below. |
| `event_time` | timestamp, not null | The watermark column. |
| `wiki`, `domain` | string | `enwiki`, `en.wikipedia.org`. `domain` is the Kafka partition key. |
| `change_type` | string | `edit`, `new`, `log`, `categorize`. |
| `namespace_id`, `is_article` | int, boolean | `is_article` is `namespace_id = 0`. |
| `page_title`, `page_url` | string | |
| `editor`, `is_bot`, `is_minor`, `is_anonymous` | string, boolean × 3 | `is_anonymous` covers IP editors and MediaWiki temporary accounts. |
| `bytes_old`, `bytes_new`, `bytes_delta` | int | `bytes_delta` is null when either side is null, which happens for log events. |
| `rev_old`, `rev_new` | bigint | bigint, not int: the live values already exceed int32. |
| `comment` | string | |
| `late_by_seconds` | int | `ingested_at - event_time`, stored so lateness is queryable rather than recomputed. |
| `ingested_at` | timestamp | |
| `event_date` | date | The partition column, from `event_time`. |

**Iceberg has no primary key and no uniqueness constraint.** Nothing in the table
prevents a duplicate `event_id`; the only thing that will make the uniqueness claim
true is that every write goes through `MERGE INTO ... WHEN MATCHED`. Until the
silver layer exists, the claim is a design intention and this paragraph is the only
thing supporting it. The invariant needs a test that fails when it is violated, and
that test ships with the writer.

Today the same question can be asked of bronze, where the expected answer is the
opposite:

```bash
make table-stats   # prints distinct and repeated event_id per table
```

## `silver.quarantine`

Rows that could not be made into a silver row, with the reason. Declared now,
written by the silver layer.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | Nullable: the reason may be that there is no id. |
| `raw_payload` | string | The frame, so a fix can be replayed rather than merely counted. |
| `failure_reason` | string | Which rule rejected it. |
| `failed_at` | timestamp | |
| `ingest_date` | date | Partition column. The question asked of this table is always "what went wrong recently". |

A pipeline that drops bad rows silently is broken. A pipeline that dies on one bad
row is also broken. This table is the third option, and it stores the payload so
that a day's rejects can be repaired and replayed rather than only counted.

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

Four fields in the source payload overflow int32 and are declared `bigint`
accordingly: `id`, `revision.old`, `revision.new`, `meta.offset`. Observed live
values include `id = 3,468,178,745` and `meta.offset = 6,524,211,441`. Declaring
those as `int` produces silent nulls under `from_json`, not an error.

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
