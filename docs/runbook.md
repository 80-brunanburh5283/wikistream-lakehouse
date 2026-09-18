# Runbook

What to do when the pipeline is not doing what it should. Every entry is
**symptom → the command that tells you which cause you have → the fix**, and every
command in it is a real target in this repository.

Scope: the local Docker stack. `infra/aws/` and `k8s/` are design artefacts that
have never been applied ([their README says so](../infra/aws/README.md)), so nothing
here is about operating them — where the AWS module *would* change an answer, the
entry says so and stops there.

Numbers below were measured on one machine — a 12 GB WSL2 instance — on the dates
given. They are there to show the shape of a healthy system, not as a benchmark.

## Orientation

Three commands, in this order, before any diagnosis:

```bash
make ps                # is every container up, and does each call itself healthy
make dagster-observe   # runs the streaming layer's asset checks against the tables
make table-stats       # row counts, file counts, snapshot counts, bytes
```

They answer three different questions. `make ps` answers *is it running*.
`make dagster-observe` answers *is what it wrote correct*. `make table-stats` answers
*is it still writing*, which a healthy-looking container can stop doing.

Read the check results, not the exit code. All six checks are non-blocking, so a
failed one is recorded against the asset without failing the run, and
`make dagster-observe` exits 0 either way. This is the line that matters:

```bash
make dagster-observe 2>&1 | grep ASSET_CHECK_EVALUATION
```

A healthy run, 2026-09-18 00:12 UTC: five evaluations, all `passed`, with
`bronze_offsets_are_contiguous` logging `checked 3 partitions, 1212445 rows, 0 gaps`.
The sixth, `marts_are_not_empty`, belongs to the gold layer and runs under
`make dagster-marts`.

The two Spark queries are foreground processes, not services: `make stream-bronze`
and `make stream-silver` each run in their own terminal. `make ps` therefore says
nothing about whether they are alive. This does:

```bash
docker compose exec -T spark bash -lc \
  "ps -eo etime=,args= | grep -E 'streaming/(bronze|silver)\.py' | grep -v grep"
```

The first column is how long each has been running.

**One trap worth knowing before you need it.** `make kafka-lag` prints nothing at
all while the pipeline is healthy, and that is correct: it lists consumer groups
that commit offsets, and Spark Structured Streaming does not commit any — it keeps
its position in its own checkpoint, which is the property that makes the restart
proof work. Lag is the difference between the topic's end offsets and what bronze
holds:

```bash
make kafka-offsets
make sql SQL="SELECT kafka_partition, max(kafka_offset) AS ingested_through
              FROM lakehouse.bronze.recentchange_raw GROUP BY 1 ORDER BY 1"
```

Measured 2026-09-18 00:06 UTC with both streams running: end offsets 269,907 /
226,683 / 703,649 against ingested-through 269,822 / 226,578 / 703,461 — 378 records
behind in total, about seven seconds of stream at the observed rate. With a 30-second
trigger, a few hundred records of lag is the steady state. Tens of thousands is not.

`make sql` prints Spark's start-up log before the result; the rows are the last
thing on stdout.

## Symptom index

| symptom | entry |
|---|---|
| `wikistream-producer` is `unhealthy`; nothing new in Kafka | [1](#1-the-source-is-unreachable) |
| producer log says the send queue is full; offsets frozen | [2](#2-kafka-is-down-or-unreachable) |
| Iceberg commits fail; MinIO logs I/O errors; `docker` complains about space | [3](#3-the-disk-is-full) |
| `silver.quarantine` is growing; `quarantine_rate_is_low` fails | [4](#4-a-poison-message) |
| `quarantine_reasons_are_known` fails, or a new field appeared upstream | [5](#5-the-upstream-schema-changed) |
| a stream restarts from the beginning of the topic, or refuses to start | [6](#6-the-checkpoint-is-lost-or-corrupt) |
| silver is missing rows bronze has, or a rule changed | [7](#7-backfilling-a-window) |
| bronze grows and never shrinks | [8](#8-bronze-is-growing-without-bound) |
| `ICEBERG_CATALOG_ERROR` or `ICEBERG_COMMIT_ERROR` with no cause | [9](#9-a-catalog-error-whose-cause-is-three-layers-down) |
| a mart's numbers are wrong after a model change | [10](#10-a-marts-grain-or-a-column-changed) |
| Trino query killed; `wikistream-trino` restarts | [11](#11-trino-runs-out-of-memory) |

## 1. The source is unreachable

**Symptom.** `make ps` shows `wikistream-producer` as `unhealthy`. Kafka end offsets
stop advancing. Both streams keep running and commit empty micro-batches;
`silver_edits_are_fresh` fails once the newest event is more than 15 minutes old.

The health signal is a heartbeat file, not a port: the producer touches
`/tmp/wikistream-producer.heartbeat` once per 10-second stats interval, and the
container's healthcheck fails when it is older than 180 seconds. 180 is a full
backoff delay (60 s) plus headroom, so a reconnecting producer is not reported as
sick.

**Diagnose.**

```bash
make logs SERVICE=producer | tail -30
make smoke-live      # touches the public internet: one connection to stream.wikimedia.org
```

`make smoke-live` runs from the host and splits the two cases. If it fails too, the
source or the path to it is the problem. If it succeeds while the container cannot
connect, the fault is inside the container — DNS in the compose network, or a
`WS_WIKIMEDIA_STREAM_URL` override in `.env`.

**Fix.** Usually nothing. The producer reconnects by itself, with a delay that
doubles from 1 s to a 60 s cap, and it resumes from the `Last-Event-ID` it last
saw, so a short outage costs no events. Cases that do need a hand:

| in the log | cause | what to do |
|---|---|---|
| HTTP 403 or 429 | the `User-Agent` | Wikimedia asks for a descriptive one and this project sends one naming the repository. A build that stripped it gets throttled rather than served — restore it. |
| repeated reads timing out after 60 s | a half-open socket | Nothing: `source_read_timeout_seconds` exists for exactly this, and the reconnect is the recovery. A connection producing nothing for 60 s is a fault, because the stream emits several events a second. |
| connects, then no events and no error | upstream is quiet or filtered | Compare with `make smoke-live`. Wikimedia's traffic has a strong daily cycle but never stops. |

If Wikimedia is unavailable for longer than you want to wait, `WS_SOURCE_NAME=jetstream`
switches the producer to Bluesky's Jetstream firehose, which is public and
unauthenticated in the same way. Be clear about what that buys: the Iceberg schema,
the dbt marts and the asset checks are all written for `recentchange`, so the
fallback gets bytes into Kafka and proves the producer works — it does not keep the
wiki marts fed. It is an escape hatch, not a second supported mode.

## 2. Kafka is down or unreachable

**Symptom.** In the producer log, the send queue filling: `BufferError` waits
followed by a resumed send. The producer's local buffer is capped at 64 MB rather
than librdkafka's 1 GB default, so a broker outage backpressures the reader instead
of being absorbed silently until the process is OOM-killed. When the buffer stays
full, the event loop stops touching its heartbeat and the container goes unhealthy
inside three minutes — which looks identical to entry 1 from `make ps` alone, and is
why the log is the first thing to read.

On the Spark side, micro-batches fail and the query retries. The checkpoint does not
advance, so nothing is lost or double-committed.

**Diagnose.**

```bash
make ps                                   # is kafka up, and does it call itself healthy
docker compose logs kafka --tail=50
make kafka-offsets                        # fails outright if the broker is unreachable
```

Kafka's healthcheck is `kafka-topics.sh --list`, so a container reported healthy is
one that answered a metadata request within the last 15 seconds.

**Fix.**

```bash
docker compose up -d --wait kafka
make create-topics     # idempotent: reconciles partition count and retention
```

Then restart the two streams if they exited. They resume from their checkpoints, and
`make verify-no-duplicates` afterwards is the assertion that the restart cost
nothing.

The one case that does lose data is an outage longer than the topic's retention.
Retention is 24 hours (`WS_KAFKA_RETENTION_MS`, default 86,400,000), and the streams
run with `failOnDataLoss=false` — ADR-0016 explains why: with `true`, a rebalance or a
retention-aged offset kills the query, and on a laptop that is far more often a
nuisance than a signal. The cost of that choice is that a gap is silent, so the gap
is checked for instead:

```bash
make dagster-observe 2>&1 | grep ASSET_CHECK_EVALUATION   # bronze_offsets_are_contiguous
```

That check compares each partition's row count with `max(offset) - min(offset) + 1`.
Fewer rows means records were lost; more means a batch was written twice.

## 3. The disk is full

**Symptom.** Iceberg commits fail, MinIO logs I/O errors, or Docker itself starts
refusing to write. On WSL2 the usual cause is not MinIO: it is the Docker VM disk on
the Windows drive, and Docker stops being able to write anywhere at once.

**Diagnose.**

```bash
docker system df          # images, containers, volumes, build cache
docker system df -v       # per-volume and per-image sizes
make table-stats          # what the tables themselves cost
```

Measured 2026-09-18 00:14 UTC after 1,218,424 events: bronze 193.6 MiB across 825
data files, silver 86.2 MiB across 826 — 280 MiB of table data in total. The images
are an order of magnitude larger: 2.37 GB for `wikistream/spark`, 1.14 GB for
`wikistream/dagster`, 871 MB for `wikistream/dbt`. If a laptop is short of space,
the tables are almost never the reason, and deleting them is almost never the fix.

**Fix,** cheapest first:

```bash
make maintain            # compact, expire snapshots, remove orphans
docker builder prune     # build cache, which grows with every image rebuild
docker image prune       # dangling layers from earlier builds
make down                # stops every profile; keeps volumes, so tables and offsets survive
```

`make maintain` is the one that reclaims lakehouse space, because a streaming write
leaves every superseded file behind for time travel. Its defaults are conservative —
snapshots older than 24 h, orphans older than the safe age — so on a young table it
correctly does almost nothing. `MAINTAIN_ARGS="--snapshot-age-hours 0"` makes it
expire everything but the retain-last floor; `docs/lakehouse.md` has the before and
after figures from a run with exactly that argument.

Last resort, and it destroys data:

```bash
make clean    # down --volumes: the next `make up` starts from an empty lakehouse
```

On the AWS side the equivalent controls are lifecycle rules rather than a command,
and their ordering is a trap of its own — see entry 8.

## 4. A poison message

**Symptom.** `silver.quarantine` is growing, and `quarantine_rate_is_low` fails. The
budget is a rate, not a count — 0.5% of rows ingested in the last 24 hours — because
a count threshold has to be re-tuned every time throughput changes, and Wikimedia's
throughput at 04:00 UTC is not its throughput at 14:00.

Bronze is unaffected. It stores the frame verbatim and never rejects anything, which
is what makes the rest of this entry possible.

**Diagnose.**

```bash
make sql SQL="SELECT failure_reason, count(*) AS rows, min(failed_at) AS first_seen,
                     max(failed_at) AS last_seen
              FROM lakehouse.silver.quarantine GROUP BY 1 ORDER BY 2 DESC"
```

`failure_reason` is one of a closed set defined in
`wikistream.quality.expectations.FAILURE_REASONS`, and each value points at a
different upstream problem:

| reason | what it means |
|---|---|
| `payload_not_json` | Spark could not parse the frame at all |
| `event_id_missing` | `meta.id` absent or blank, so the row cannot be deduplicated |
| `event_time_missing` | `meta.dt` absent — a contract change |
| `event_time_unparseable` | `meta.dt` present but not a timestamp — usually a bug at one wiki |
| `domain_missing` | `meta.domain` absent; it is the Kafka partition key |
| `event_time_before_wikipedia` | a clock fault: the timestamp predates the project |
| `event_time_in_future` | a clock fault the other way |

Every quarantined row keeps its `raw_payload` and its `(kafka_partition,
kafka_offset)`, so the exact frame is recoverable rather than merely counted:

```bash
make sql SQL="SELECT raw_payload FROM lakehouse.silver.quarantine
              WHERE failure_reason = 'payload_not_json' LIMIT 1"
```

**Fix.** Decide which of two things is true.

*The event really is invalid.* Then nothing is broken: quarantine is the design, the
stream did not stall, and the count is the observability. A parse failure never fails
a micro-batch on purpose — Spark 4.0 runs ANSI mode by default, so a malformed
timestamp raises rather than yielding null, and an exception inside a streaming batch
is a poison pill: the batch fails, the checkpoint replays the same offsets, and the
job dies on the same record for ever. `try_to_timestamp` and a null-tolerant
`from_json` are what stop that, and `tests/spark/` pins the behaviour.

*The rule is wrong.* Then change it in `src/wikistream/quality/expectations.py` —
where each rule exists twice, as a Python predicate and as the Spark SQL it compiles
to, with a parity test asserting the two agree — add a test for the frame that was
wrongly rejected, and replay the affected window from bronze:

```bash
make rebuild-silver FROM=2026-09-17 TO=2026-09-18
```

That is entry 7, and it works because bronze kept the payload.

## 5. The upstream schema changed

**Symptom.** Either a new field appears in the payloads, or a field this pipeline
reads stops appearing. The two have very different consequences.

A **new** field costs nothing and breaks nothing. Bronze stores the whole frame in
`raw_payload`, and silver projects the columns it knows about, so an added key is
retained and ignored until someone asks for it.

A **removed or renamed** field shows up as quarantine, with the reason naming the
field: `event_id_missing`, `event_time_missing`, `domain_missing`. If it is one that
`silver.edits` merely reports, the rows still land and the column goes null.
`quarantine_reasons_are_known` failing means something stranger: a reason in the
table that the current code does not define, which happens when the table is older
than the code.

**Diagnose.**

```bash
make kafka-tail N=1                      # what a frame looks like right now
make dagster-observe                     # which checks fail, and with what counts
```

Compare the frame with [docs/data-contracts.md](data-contracts.md), which lists every
field this project reads and states which are required.

**Fix.** Adding a column to `silver.edits` is metadata-only in Iceberg — no rewrite,
no downtime, and readers that do not know the column keep working. The mechanics are
demonstrated rather than asserted:

```bash
make prove-schema-evolution   # add, rename, widen and drop on a scratch table
```

The sequence for a real change:

1. Add the column to the DDL in `src/wikistream/streaming/tables.py`, then apply it to
   the live table with `ALTER TABLE lakehouse.silver.edits ADD COLUMN ...` through
   `make sql`. The DDL is `CREATE TABLE IF NOT EXISTS`, so editing it alone changes a
   table that already exists not at all. Iceberg assigns the new column a fresh field
   id, so the addition cannot resurrect a dropped column's data.
2. Add the field to the parse schema in `src/wikistream/streaming/schema.py` and the
   projection in `src/wikistream/streaming/silver.py`, with a test. The parse schema is
   written out rather than inferred on purpose — inference samples data, so two runs
   could give the table two different column sets.
3. Restart `make stream-silver`. New rows carry the column; old rows read null.
4. Backfill the old rows from bronze if the column matters historically — entry 7.
   This is the whole reason bronze keeps the raw payload rather than only the parsed
   columns.
5. Update `docs/data-contracts.md` in the same change. The pull request template asks
   for that, and it is the document a future reader will trust.

## 6. The checkpoint is lost or corrupt

**Symptom.** A stream refuses to start, or starts and re-reads the entire retained
topic. Each query has its own checkpoint directory, named after the query, in the
`spark-checkpoints` volume:

```bash
docker compose exec -T spark ls /opt/spark/checkpoints
# bronze_recentchange_raw
# silver_edits
```

`startingOffsets=earliest` applies only on a checkpoint's *first* run, so a stream
that suddenly starts from the beginning of the topic has lost its checkpoint — most
often because someone ran `make clean`, which deletes the volume along with
everything else.

**Diagnose.**

```bash
docker compose exec -T spark ls /opt/spark/checkpoints/silver_edits/offsets | tail -3
docker compose exec -T spark ls /opt/spark/checkpoints/silver_edits/commits | tail -3
```

A healthy query has the same newest batch id in both, or `offsets` one ahead — that
one is the batch in flight. A missing `commits` entry for an old batch id, or an
empty `offsets` directory on a table that already has data, is the corrupt case.

**Fix.** Delete the checkpoint and restart the query. What that costs is different
for the two tables, and the difference is the point of the architecture:

| table | after a checkpoint loss and replay | why |
|---|---|---|
| `bronze.recentchange_raw` | may hold a Kafka record twice | It is an append-only audit log with no key. Two rows with the same `(kafka_partition, kafka_offset)` is the visible cost, and `bronze_offsets_are_contiguous` reports it as *more* rows than the offset range. |
| `silver.edits` | unchanged | `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` looks each `event_id` up in the table itself, so a replayed event matches an existing row and is not inserted. No watermark, no state store, no window in which a duplicate can slip through. |

```bash
make stream-silver           # not stream-silver-once, after a crash — see docs/correctness.md
make verify-no-duplicates    # exits non-zero if silver.edits has any repeated event_id
```

Measured 2026-09-18 on 1,218,424 silver rows: zero repeated `event_id`. The hard
version of this — kill the producer and both streams mid-flight, restart, assert zero
duplicates — is `make test-e2e`, and it is not run in CI because it consumes the live
stream.

If bronze did pick up duplicate rows, they are not repaired in place: bronze is
deliberately immutable, and silver was never wrong. The rewrite is available
(`DELETE` plus a re-ingest from Kafka while the records are still retained) but it
trades the audit log's one guarantee — that it holds what arrived — for tidiness.
Prefer leaving it, and let the contiguity check carry the record.

## 7. Backfilling a window

**Symptom, or rather occasion.** Silver is missing rows that bronze has, or a
validation rule changed, or a mapping bug means a column is wrong for a period.

**Diagnose** the size of the gap before rebuilding anything. Count both tables by the
day the pipeline ingested them, which is the axis `make rebuild-silver` works on:

```bash
make sql SQL="SELECT b.d AS ingest_day, b.rows AS bronze_rows, s.rows AS silver_rows
              FROM (SELECT ingest_date AS d, count(*) AS rows
                    FROM lakehouse.bronze.recentchange_raw GROUP BY 1) b
              FULL OUTER JOIN (SELECT date(ingested_at) AS d, count(*) AS rows
                               FROM lakehouse.silver.edits GROUP BY 1) s USING (d)
              ORDER BY 1"
```

A finished day should match exactly. Measured 2026-09-18 00:13 UTC: 1,183,150 rows on
both sides for 2026-09-17, and 33,058 against 31,798 for the day still in progress —
the two streams read Kafka independently, so the current day differs by however far
apart their positions are. A *closed* day where silver is short is a real gap.

Note that silver is being counted by `ingested_at`, not by its `event_date` partition
column, so this query scans rather than prunes. It is a diagnosis to run when
something looks wrong, not a dashboard.

**Fix.**

```bash
make rebuild-silver FROM=2026-09-17 TO=2026-09-18
```

Both dates are inclusive, they filter bronze's `ingest_date` partition column so the
scan is pruned rather than full, and the rebuild uses the same `MERGE` as the stream.
It is therefore idempotent, which `tests/integration/test_silver_rebuild.py` asserts
directly — `test_replaying_the_same_window_twice_changes_nothing`,
`test_a_rebuild_replaces_a_row_deleted_by_mistake`, and
`test_the_gate_still_passes_after_the_replays`.

Two things it does not do:

- It does not touch bronze. If bronze is missing the records, they must come from
  Kafka, and only if they are still inside the 24-hour retention.
- It does not refresh the marts. Do that after, and read entry 10 first if the
  backfill reaches back past the incremental models' lower bound:

```bash
make dbt-build
```

## 8. Bronze is growing without bound

**Symptom.** Row count and file count climb and never come down. This is normal for a
week and then it is not. The arithmetic is unforgiving: a 30-second trigger is 2,880
commits per stream per day, every commit is a snapshot, and every snapshot pins the
files it referenced against deletion.

**Diagnose.**

```bash
make table-stats
make sql SQL="SELECT count(*) AS snapshots, min(committed_at) AS oldest, max(committed_at) AS newest
              FROM lakehouse.bronze.recentchange_raw.snapshots"
```

Measured 2026-09-18 00:14 UTC: 825 data files and 211 retained snapshots on bronze,
average file 240.4 KiB against a 69.4 MiB largest — the small-files problem in one
line, and [docs/lakehouse.md](lakehouse.md) is about it. The retained snapshots span
2026-09-17 12:54:15 to 2026-09-18 00:13:01, which is longer than the streams were
actually up: a snapshot is created per commit, not per unit of time, so the window is
a record of when the pipeline ran rather than of how long it has existed.

**Fix, locally.**

```bash
make maintain
```

Four procedures in one pass, in an order that matters: compact data files, rewrite
manifests, expire snapshots older than 24 hours (subject to a retain-last floor), then
remove orphans. Expiry has to come after compaction — it is what frees the disk space
compaction just doubled — and orphan removal has to come last, because before expiry
there is nothing unreferenced for it to find.

**Fix, on AWS — and the order is a trap.** The Terraform module has a lifecycle rule
`bronze-data-backstop-expiry` on `warehouse/bronze/data/`. It is **off by default**,
and off is the recommendation, because S3 expiry and Iceberg retention do not measure
the same thing:

| control | works on | consequence of using it alone |
|---|---|---|
| Iceberg `expire_snapshots` | snapshots, and the files they reference | Correct: it only deletes a file after no live snapshot needs it. |
| S3 lifecycle expiry | object age | Deletes a data file the current snapshot still references. Every query against the table then fails — not degrades — until the object is restored from a noncurrent version. |

So the two are enabled together, table retention first: turn on Iceberg snapshot
expiry with a retention shorter than the S3 rule, let it run through at least one
full cycle, confirm no live snapshot is older than the S3 rule's age, and only then
set `bronze_retention_days` above 0. The rule is a backstop against files Iceberg
lost track of, not a retention policy.

Versioning on the bucket is the undo button if that ordering goes wrong, and
`expire-noncurrent-versions` keeps it 30 days — long enough to notice, short enough
that compaction's inputs do not accumulate for ever. `bronze-data-to-infrequent-access`
transitions data files only, never `metadata/`: manifests are read by every query
plan, and Standard-IA charges more per request, so moving them would make planning
slower and dearer to save cents on kilobytes.

None of that has been applied. It is a design under static analysis, and
[infra/aws/README.md](../infra/aws/README.md) is explicit about it.

## 9. A catalog error whose cause is three layers down

**Symptom.** dbt or Trino reports a catalog failure with no cause attached:

```
Database Error in model stg_edits
  TrinoExternalError(name=ICEBERG_CATALOG_ERROR, message="Failed to create view 'stg_edits'")
```

Trino is reporting a 500 from the REST catalog and has nothing else to tell you. The
same shape appears as `ICEBERG_COMMIT_ERROR` on a write.

**Diagnose.** Read the catalog's log, not the client's error.

```bash
docker compose logs iceberg-rest --tail=200 | grep -A5 'Caused by'
```

The real exception is there. The instance this project hit was:

```
org.apache.iceberg.jdbc.UncheckedSQLException: Unknown failure
  at org.apache.iceberg.jdbc.JdbcViewOperations.doCommit(JdbcViewOperations.java:136)
Caused by: org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked
```

— two JDBC connections against a SQLite catalog, fixed by giving it one
(`CATALOG_CLIENTS: "1"` in the compose file). ADR-0039 has the full reasoning,
including why SQLite is the right catalog for a laptop and what changes if it is not.

**Fix.** Whatever the log says. The generalisable part is the diagnosis habit: for any
5xx from the catalog, the client's message is a wrapper and the cause is one log away.
A second habit that pays here — the streams and Trino write through the same catalog,
so an error that only appears when two things run at once is usually about the catalog
rather than about either writer.

## 10. A mart's grain or a column changed

**Symptom.** A mart's numbers are wrong, or duplicated, after a model change that
looked correct. Three of the five gold models are incremental —
`mart_edits_per_minute`, `mart_bot_vs_human_hourly`, `mart_top_pages_hourly` — so a
`dbt run` merges into an existing table rather than rebuilding it, which means a
changed grain leaves the old grain's rows in place beside the new ones and a changed
expression leaves history computed the old way. (`dim_wikis` and
`mart_pipeline_health` are full tables, rebuilt on every run, so they cannot have this
problem.)

**Diagnose.** Count the grain against itself. `mart_edits_per_minute` is keyed on
`(event_minute, wiki, change_type)`, so all three columns go in the `GROUP BY`:

```bash
make sql SQL="SELECT event_minute, wiki, change_type, count(*) AS rows
              FROM lakehouse.gold.mart_edits_per_minute
              GROUP BY 1, 2, 3 HAVING count(*) > 1 ORDER BY 1 DESC LIMIT 5"
```

Zero rows is the healthy answer, and is what it returned on 2026-09-18 00:14 UTC. Any
row means the table holds two records for one key, which is what a grain change looks
like from the outside. dbt's own tests catch this on the next build —
`make dbt-test` — and the same tests run as Dagster asset checks through
`make dagster-marts`.

**Fix.**

```bash
make dbt-build DBT_ARGS="--full-refresh --select mart_edits_per_minute+"
```

`--full-refresh` drops and recreates rather than merging, which is the only way to
change a grain honestly. The `+` suffix includes everything downstream, because a mart
that feeds another one leaves the second stale otherwise.

Then, in the same change: update the model's tests, update the grain in
[docs/data-contracts.md](data-contracts.md), and if a consumer's column disappeared,
say so there rather than silently. The pull request template asks for exactly this,
which is why it links here.

A full refresh of every mart is cheap at laptop volumes — the marts are aggregates
over a table measured in hundreds of megabytes — so the incremental strategy is about
demonstrating the pattern honestly, including its footgun, rather than about needing
it. `DECISIONS.md` records that trade.

## 11. Trino runs out of memory

**Symptom.** A query fails with `EXCEEDED_LOCAL_MEMORY_LIMIT`, or
`wikistream-trino` disappears and comes back. Trino runs with `mem_limit: 2g` so the
whole stack fits a 12 GB box, and that headroom is thin by design.

**Diagnose.**

```bash
docker stats --no-stream
docker compose logs trino --tail=100
```

Measured over 328 samples on 2026-09-18 with both streams running, the producer live
and the full profile up: Trino peaked at 1,676 MiB of its 2 GiB — Spark 2,701 MiB,
Kafka 734, MinIO 678, the two Dagster containers 563 and 283, the Iceberg catalog 273,
the producer 49. The peaks sum to 6,957 MiB, which is where the documented requirement
of about 7 GB available to Docker comes from. They are peaks over the sample window
rather than simultaneous, so the sum is an upper bound, not a reading.

**Fix,** in order of preference:

1. Give the query less to do. `WHERE ingest_date >= current_date - INTERVAL '1' DAY`
   on a bronze scan is the difference between reading a partition and reading
   everything.
2. Run it somewhere else. `make query-duckdb` reads the same Iceberg tables with no
   JVM and no Trino, which is also the answer if Trino will not start at all.
3. Raise `mem_limit` for `trino` in `docker-compose.yml`. Only worth doing if the box
   has the RAM: on a 12 GB machine with the full profile up, it does not.

Compaction helps here too, for a reason worth naming: a query against 825 small files
spends memory on planning that a query against a few large ones does not.
`make maintain` is the fix for a slow query as often as for a full disk.

## What this runbook does not cover

- **Anything on AWS or Kubernetes.** Entry 8 describes what the Terraform module
  would do because the module promises this page will; it is still a design that has
  never been applied.
- **Multi-node anything.** Spark runs `local[*]`, Kafka is a single broker, MinIO is a
  single node. Rebalances, broker failover and erasure-set repair are real operational
  concerns that this stack cannot have, and pretending otherwise would be worse than
  the gap.
- **Alerting.** The asset checks are the detection mechanism and `make dagster-observe`
  is how they run; nothing pages anyone. Wiring Dagster's failure hooks to a
  notification channel would be the next step, and it is not implemented — so every
  entry above starts from a human noticing.
- **Security incidents.** There are no credentials to rotate: the only ones in the
  tree are MinIO's documented local defaults, and CI scans every commit in the history
  to keep that true.
