# Correctness: five claims, and the command that checks each one

A streaming pipeline is easy to demonstrate and hard to trust. This page lists what
this one claims, and next to each claim the command that would fail if the claim were
false. Nothing here is argued from first principles alone — every section ends in an
exit code.

| Claim | Command | Where the logic is |
|---|---|---|
| `silver.edits` holds one row per source event | `make verify-no-duplicates` | `MERGE INTO ... WHEN NOT MATCHED` |
| A crash and restart adds no duplicates | `make test-e2e` | Spark's checkpoint plus the same MERGE |
| A late event is stored, never dropped | `uv run pytest -m spark -k watermark` | no watermark at all |
| Schema changes rewrite no data, and history stays readable | `make prove-schema-evolution` | Iceberg column ids and snapshots |
| Unusable data is quarantined with a reason, never silently discarded | `uv run pytest -m integration -k quarantine` | `wikistream.quality.expectations` |

Storage mechanics — file counts, compaction, what a commit costs — are in
[lakehouse.md](lakehouse.md). Source lag and the watermark measurement are in
[latency.md](latency.md).

## 1. Duplicate suppression

### The source really does redeliver

This is not a hypothetical defended with a synthetic test. Wikimedia EventStreams is
server-sent events, and a reconnect resumes at or before the `Last-Event-ID` the
client offers, so the client receives events it has already seen. One reconnect was
observed in a single 180-second measurement run ([latency.md](latency.md)).

Three sources of duplication exist, and only the first is handled by Kafka:

| Source of duplicate | Handled by |
|---|---|
| Producer's internal retry after a lost acknowledgement | `enable.idempotence` on the producer |
| SSE replay after a reconnect | silver's `MERGE`. Kafka cannot see it — the duplication happened upstream of the producer |
| Spark re-running a micro-batch whose commit it could not confirm | silver's `MERGE`. See section 2 |

`src/wikistream/producer/kafka_sink.py` opens with what `enable.idempotence` does and
does not buy, because it is the most over-claimed setting in Kafka and the other two
rows of that table are the reason this repository exists.

### Two mechanisms, because there are two scopes

**Within a micro-batch**, `row_number()` over a window partitioned by `event_id` and
ordered by `(kafka_partition, kafka_offset)` keeps the earliest arrival. First arrival
wins: two frames carrying the same `meta.id` and different content resolve to the
lower Kafka offset, and the later one is discarded.

**Across micro-batches**, `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` looks each key
up in `silver.edits` itself. There is no `WHEN MATCHED` clause, so a row already in
the table is never updated. Two consequences worth stating:

- The deduplication window is the table's entire history. No arrival is too late to be
  recognised as a duplicate, which is the property a watermark would have taken away.
- The table is append-only in practice, so a downstream incremental model keyed on
  `event_id` never has to handle a changed row.

The cost is real and is not hidden: a genuine upstream correction under the same
`meta.id` would be ignored. Bronze still holds both versions, and
`make rebuild-silver` against a changed rule is how such a correction would be
applied. And because the `MERGE` reads the target table on every batch, write
amplification grows with table size rather than staying flat — ADR-0019 and ADR-0020
have the full trade.

### The gate, and the reason it is a script

```bash
make verify-no-duplicates
```

`scripts/verify_no_duplicates.py` exits non-zero when `silver.edits` contains any
`event_id` more than once. It is a script and not a `.sql` file because a query prints
a number and a gate has to fail; `make up`, the restart proof and CI all depend on its
exit code.

Two design points that matter more than the query:

**An empty table is a failure.** Zero duplicates in zero rows is not evidence of
anything, and a gate that goes green on an empty lakehouse goes green on precisely the
failure it exists to catch — a silver stream that died before writing. `--allow-empty`
exists for the fresh-stack path and nothing else uses it.

**It prints bronze's duplicate count as context.** "Zero duplicates in silver" means
nothing without evidence that duplicates existed upstream. Bronze is append-only, so
its duplicate count is the size of the problem silver solved.

A gate nobody has seen fail is not a gate, so
`tests/integration/test_silver_rebuild.py::test_the_gate_fails_when_a_duplicate_is_inserted`
inserts a duplicate row into a scratch table and asserts the script exits non-zero.

### The backfill path is the same proof at a larger scale

```bash
make rebuild-silver FROM=2026-09-17 TO=2026-09-17
```

The rebuild reads a date range out of `bronze.recentchange_raw` and pushes it through
the same projection and the same `MERGE` the live stream uses — the code path is shared
deliberately, via `frames_from_bronze`, so a bug in one cannot be absent from the other.
Which makes it a replay test with no fixtures involved. On 2026-09-17, against a silver
table already holding every one of those events:

```
silver batch merged   batch_id: -1   valid_rows: 450725   quarantined_rows: 0
silver.edits now holds           721,366 rows
```

450,725 rows re-merged, and the row count before and after is 721,366 either way. The
`batch_id: -1` marks the batch as a rebuild rather than a micro-batch in the log.

## 2. Restart idempotency

### The window Spark cannot close for you

`foreachBatch` is at-least-once, not exactly-once. Spark writes a micro-batch's Kafka
offsets to `<checkpoint>/offsets/N` **before** running the batch and
`<checkpoint>/commits/N` **after** it. A crash in between leaves rows already merged
into Iceberg and offsets Spark believes it never consumed, so on restart it re-reads
that exact range and applies it again.

Iceberg's own streaming sink defends against this by stamping
`spark.sql.streaming.epochId` into the snapshot and refusing an epoch it has seen. A
batch write performed *inside* `foreachBatch` carries no such tag, so
`writeTo(...).append()` would duplicate on retry. `MERGE INTO ... WHEN NOT MATCHED`
needs no tag: applying it twice is applying it once. That is why both writes in the
silver job are MERGEs, including the quarantine write.

### The proof

```bash
make test-e2e
```

`tests/e2e/test_restart_idempotency.py` SIGKILLs the producer and the streaming job
while both are working, then deletes the newest commit file so the checkpoint is in
exactly the state an interrupted commit leaves it in. Landing a signal inside a
millisecond-wide window would be a flake; deleting `commits/N` makes the replay
certain. ADR-0025 records why that is the honest version of the test and what the two
rejected designs were.

One run on 2026-09-18, with the single long exception line wrapped and nothing else
changed:

```
[1] producing into wikistream.e2e.71b12f18 for up to 70s
    285 records on the topic
[2] starting the silver stream, 10s trigger
    committed batches [0, 1, 2]
[3] SIGKILL both, mid-flight
    silver holds 893 rows, 893 distinct ids; topic at 1046
[4] deleting commits/2: 98 records unconfirmed
    offsets [0, 1, 2], so batches [2] are now unconfirmed
[5] restarting the producer for 40s
    topic at 2470, and static from here
[6] trying to resume the unconfirmed batch with --once
    exit 1: pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED]
    Query [id = 4e35ff4a-..., runId = 94af6ec5-...] terminated with exception: Multiple
    streaming queries are concurrently using
    file:/opt/spark/checkpoints/e2e-71b12f18/silver_edits/commits. SQLSTATE: XXKST
[7] resuming with the continuous trigger, the way the pipeline runs
    committed [0, 1, 2, 3]
    silver holds 2470 rows, 2470 distinct ids
[8] replaying commits/3 (1577 records), no new data
    silver holds 2470 rows, 2470 distinct ids
.......
================ 7 passed, 383 deselected in 229.31s (0:03:49) =================
```

Reading it in order: the crash left 893 rows and 893 distinct ids; deleting
`commits/2` put 98 already-merged records back into the unconfirmed state; the
restarted producer took the topic to 2,470 records; and after the restart the table
held 2,470 rows and 2,470 distinct ids — one per record on the topic, no more and no
fewer. Step 8 then replays a second already-applied batch and the two counts do not
move.

Step 8 is the sharp one. With the producer stopped and the topic static, a committed
batch of 1,577 records is replayed in full, and the row count moves by **exactly
zero**. An append instead of a MERGE would have added 1,577 rows there.

The numbers differ from run to run — the topic is fed from a live firehose, so how many
records arrive and how many batches commit before the kill are not fixed. What is fixed
is the shape: rows equal distinct ids at every measurement, and the replay changes
nothing.

### What this does not claim

**Not that the producer restart lost no events.** It did. The SSE cursor lives in
process memory, so a fresh producer resumes at the tip of the stream and the edits
emitted while it was dead are never fetched. That is a real gap, it is listed under
"What none of this proves" below, and it is a different property from this one. This
test is about duplicates.

**Not exactly-once end to end.** Kafka's idempotence does not survive a producer
restart, and nothing in this pipeline deduplicates *bronze*. Bronze is append-only on
purpose: duplicates there are data about the source, not corruption. The
exactly-once-looking result is effectively-once at the silver boundary, achieved by an
idempotent write rather than by a distributed transaction.

### After a crash, restart with `make stream-silver`, not `make stream-silver-once`

This came out of the proof and is worth knowing before you need it.
`--once` uses `Trigger.AvailableNow`. Against a checkpoint whose newest batch has
offsets but no commit, it re-reads that batch, merges it correctly, and then *may* fail:

```
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED] ...
terminated with exception: Multiple streaming queries are concurrently using
file:/opt/spark/checkpoints/.../silver_edits/commits. SQLSTATE: XXKST
```

No second query exists. The MERGE has already happened, so nothing is lost or
duplicated — the run exits non-zero and the batch stays unconfirmed.

The hedge in "may" is the honest part, and it was not there first. Four consecutive runs
produced that error, so the test asserted it. The fifth, on 2026-09-18, resumed cleanly
and exited 0 — and the assertion was wrong rather than the pipeline. The difference is
how many batches the crash left unconfirmed: a SIGKILL landing *inside* a batch writes
`offsets/N+1` before it dies, so deleting `commits/N` leaves two unconfirmed batches
instead of one, and `AvailableNow` handled that case without complaint. The test now
prints the unconfirmed set on every run for exactly this reason — the transcript above is
a one-batch run (`offsets [0, 1, 2], so batches [2] are now unconfirmed`) and it failed;
the run that resumed cleanly had two.

What did not vary across any of the five runs: the continuous trigger recovered the
checkpoint, and the table held one row per event id afterwards. So the advice stands, on
a weaker premise than the one it started with — restart with the continuous trigger not
because `--once` always fails, but because it sometimes does and the alternative never
has.

This is asserted, not merely written down:
`test_once_is_not_a_reliable_way_to_resume_an_unconfirmed_batch` fails if a non-zero exit
ever arrives with a *different* error, and fails if the recovered table ever holds a
duplicate.

A *graceful* stop is the other case and behaves the opposite way. Ctrl-C on
`make stream-silver` runs Spark's shutdown hook, which finishes the batch in flight and
commits it, so the newest checkpoint entry is complete and `--once` resumes from it
without the error above — measured 2026-09-18 on the bronze stream, which came back at
batch 218 with zero offsets behind. Whether the checkpoint's last batch is confirmed is
what separates the two, not whether a process died. `scripts/stream.sh` exists so that
Ctrl-C reaches the driver at all inside the container
([ADR-0050](../DECISIONS.md#adr-0050--wrap-the-streaming-targets-in-a-script-so-ctrl-c-reaches-the-driver));
[runbook entry 12](runbook.md#12-a-stream-is-still-running-after-you-stopped-it) covers
the case where it did not.

## 3. Late-arriving events are stored, not dropped

The silver stream declares no watermark. That is a decision with a measurement behind
it, and [latency.md](latency.md) is the measurement; the short version is that a
watermark only does something when a stateful operator reads it, and the operator that
would read one here — `dropDuplicatesWithinWatermark` — deletes a late row with no
exception, no `_corrupt_record`, no quarantine entry and no metric that names it.

```bash
uv run pytest -m spark -k watermark
```

Four designs, five test cases, all given the same two events — one on time, one thirty
minutes behind it:

| design under a 10-minute watermark | the row 30 minutes late |
|---|---|
| `dropDuplicatesWithinWatermark("event_id")` | silently dropped |
| `dropDuplicates("event_id")` | silently dropped |
| a watermark with no stateful operator | kept |
| no watermark, `MERGE INTO` against the table (the design in use) | kept |

Instead of a watermark, lateness is recorded and left for the reader to act on.
`silver.edits.late_by_seconds` is `ingest_time - event_time` per row, and Iceberg
absorbs a write into an old day-partition without complaint — which is most of the
reason an open table format is here at all. The ten-minute figure from the original
design survives as a service level objective rather than as a mechanism: something has
to watch `late_by_seconds`, and that belongs in the orchestration layer's asset
checks, not in a stateful operator that deletes the evidence.

## 4. Schema evolution and time travel

```bash
make prove-schema-evolution
```

Iceberg tracks columns by id, not by position or name, so adding, renaming, widening
and dropping a column are metadata operations that rewrite no data file. That is the
claim; the script builds a scratch table in its own namespace, performs all four
changes, checks the file count after each one, reads the table's own history, and drops
everything it made. One real run:

```
lakehouse.schema_evolution_demo.edits
  baseline snapshot 3994805093751851321, 2 rows, 1 file(s)
  columns: 22
  narrowing bigint to int was refused: [NOT_SUPPORTED_CHANGE_COLUMN] ALTER TABLE
  ALTER/CHANGE COLUMN is not supported for changing
  `lakehouse`.`schema_evolution_demo`.`edits`'s column `namespace_id` with type
  "BIGINT" to `namespace_id` with type "INT". SQLSTATE: 0A000; line 1 pos 0;

checks
  [pass] ADD COLUMN rewrote no data file                                     1  expected 1
  [pass] rows written before the column read null                            2  expected 2
  [pass] a row written after it carries the value                       'demo'  expected 'demo'
  [pass] old and new files coexist in one query                              3  expected 3
  [pass] FOR VERSION AS OF sees the pre-change table                         2  expected 2
  [pass] FOR TIMESTAMP AS OF resolves to the same snapshot                   2  expected 2
  [pass] the live table is unchanged by reading history                      3  expected 3
  [pass] RENAME COLUMN rewrote no data file                                  2  expected 2
  [pass] the value survived the rename                                  'demo'  expected 'demo'
  [pass] edit_source is gone from the schema                             False  expected False
  [pass] namespace_id widened to bigint                               'bigint'  expected 'bigint'
  [pass] widening rewrote no data file                                       2  expected 2
  [pass] the widened values are unchanged                                    4  expected 4
  [pass] narrowing bigint to int is refused  'NOT_SUPPORTED_CHANGE_COLUMN'  expected 'NOT_SUPPORTED_CHANGE_COLUMN'
  [pass] DROP COLUMN rewrote no data file                                    2  expected 2
  [pass] every row still reads                                               3  expected 3
  [pass] edit_channel is gone from the schema                            False  expected False

all 17 checks passed
dropped lakehouse.schema_evolution_demo.edits and its namespace
```

Two of those lines carry most of the value. **"old and new files coexist in one
query"** is the point of column ids: a file written before `ADD COLUMN` and a file
written after it are read together, the older rows returning null for the new column,
with no rewrite and no migration step. And **"narrowing bigint to int is refused"** is
the boundary — Iceberg permits only widening promotions, because a narrowing cast could
fail on data already written, and it refuses at DDL time rather than at read time. The
script asserts the refusal with the same weight as it asserts the successes, since a
constraint you have not seen enforced is a constraint you are guessing about.

### Time travel, and why it still works after maintenance

Every one of those snapshots is queryable on the live table. Two statements, run
through `make sql`, which takes one statement at a time:

```bash
make sql SQL="SELECT snapshot_id, committed_at, operation
              FROM lakehouse.silver.edits.snapshots ORDER BY committed_at"
```

```
|snapshot_id        |committed_at              |operation|
|4383069819034846387|2026-09-17 12:53:10.962000|append   |
|316451574106629156 |2026-09-17 12:53:14.937000|append   |
|1429939475774916741|2026-09-17 12:53:17.400000|append   |
|3563253739473736839|2026-09-17 13:21:12.057000|replace  |
|5938876578140601884|2026-09-17 13:21:14.998000|replace  |
```

Take a `snapshot_id` from that list and read the table as it was:

```bash
make sql SQL="SELECT count(*) AS n FROM lakehouse.silver.edits
              FOR VERSION AS OF 1429939475774916741"
```

```
|n     |
|445395|
```

One count proves nothing on its own — it has to differ from the present. Both numbers
in one statement, on a later run of the same table:

```bash
make sql SQL="SELECT 'current' AS snapshot, count(*) AS rows FROM lakehouse.silver.edits
              UNION ALL
              SELECT 'oldest retained', count(*) FROM lakehouse.silver.edits
              VERSION AS OF 7674839218057691856
              ORDER BY rows DESC"
```

```
|snapshot       |rows  |
|current        |222410|
|oldest retained|791   |
```

Measured 2026-09-18 at 03:47 UTC. The 791 is not a round number for a reason: it is
this table's first commit — one micro-batch, `added-records` 791 and `total-records`
791 — still addressable 218 snapshots and 222,410 rows later.

`FOR TIMESTAMP AS OF '2026-09-17 13:00:00'` addresses the same snapshot by wall-clock
time, and fails with `Cannot find a snapshot older than ...` for any timestamp before
the oldest retained snapshot — which is the honest behaviour, and the reason the floor
below exists.

The two `replace` operations in that history are `make maintain`: compaction rewrote
the data files and the manifests. The three `append` snapshots before it are still
addressable, and still reference the pre-compaction files —
[lakehouse.md](lakehouse.md) has what that costs in disk.

Time travel is the first thing a maintenance run breaks, because an expired snapshot
is a `FOR VERSION AS OF` that no longer resolves. Two guards, both in
`src/wikistream/maintenance.py`:

- `history.expire.max-snapshot-age-ms` is 7 days, so age alone will not take
  yesterday's snapshot.
- `RETAIN_LAST_SNAPSHOTS = 5` is a floor on *count*, independent of age. It is why the
  queries above still resolve immediately after `make maintain`, even when the run is
  given `--snapshot-age-hours 0`. A demonstration that only works before you run
  maintenance is not a demonstration.

### The evolution case that would break silently

A field the source *removes* or *retypes*. `from_json` returns null for a field it
cannot coerce, so the pipeline stays green and the column quietly empties. Nothing in
the sections above catches that, because every check here is about the pipeline's
health and this is a change in the shape of the data. It needs a check on the data
itself — a non-null rate on a column that should always be populated — which is what
the quarantine reason codes and the orchestration layer's asset checks are for. Where
you own the producer, a schema registry that rejects the incompatible change is the
better answer; here the producer is Wikimedia's, so detection after the fact is the
only option available.

## 5. Bad data is loud

```bash
uv run pytest -m integration -k quarantine
```

Nothing is dropped. Every frame lands in exactly one of `silver.edits` or
`silver.quarantine`, and
`tests/spark/test_silver_mapping.py::test_every_frame_lands_in_exactly_one_table`
is the test that says so —
`tests/integration/test_silver_rebuild.py::test_the_two_tables_account_for_every_distinct_event`
repeats it against the real catalog.

### Spark decides readable, the rules decide usable

The split matters. `from_json` with a declared schema decides whether the bytes were
JSON at all; six named rules then decide whether the readable result can be used. Seven
reason codes, because the parse verdict is one of them:

| `failure_reason` | Meaning |
|---|---|
| `payload_not_json` | Spark could not read the payload |
| `event_id_missing` | `meta.id` absent or blank, so the row cannot be deduplicated |
| `event_time_missing` | `meta.dt` absent or blank, so the row cannot be placed in time |
| `event_time_unparseable` | `meta.dt` was sent but is not a timestamp this pipeline can read |
| `domain_missing` | `meta.domain` absent or blank; it is the Kafka partition key |
| `event_time_before_wikipedia` | `event_time` predates 2001-01-15, so the clock is wrong |
| `event_time_in_future` | `event_time` is more than an hour ahead of ingest time |

That list is closed, which is what makes a `GROUP BY failure_reason` dashboard finite.
Two of the reasons look redundant and are not: **absent** means the source stopped
sending a field, **garbage** means the source changed its format. The first is a
contract change and the second is usually a bug at one wiki, and they need different
responses.

The rules exist twice — once in Python, once as Spark SQL — and
`tests/spark/test_expectation_parity.py` runs the same fixture frames through both and
demands identical verdicts. Only the *rules* are duplicated; the parse verdict is
Spark's alone, because mirroring Jackson's leniency flags in Python would be wrong on
the first version bump. ADR-0022.

### Why it is deliberately loud

A corrupt-flagged row keeps whatever sibling fields did parse, so a row can be flagged
and still carry a perfectly good `event_id`. It is quarantined anyway.

The alternative — salvage what parsed, write it to `silver.edits`, log a warning — is
how a table becomes untrustworthy. Once some rows in `silver.edits` are partial, every
downstream consumer needs to know which, and no column tells them. Better that
`silver.edits` means "this row parsed cleanly and passed every rule" without
qualification, and that everything else is one query away in a table whose name says
what it is.

Quarantine keys on `(kafka_partition, kafka_offset)` rather than on `event_id`,
because the most common reason to be in that table is not having a usable `event_id`.
The Kafka coordinates are the only identifier every quarantined row is guaranteed to
have, and they make the quarantine write a MERGE — so it is idempotent under replay
for the same reason the main write is. ADR-0021.

### The live stream has never triggered a single rule

Worth saying, because it is the sort of thing a page like this usually leaves out. In
721,366 events ingested on 2026-09-17, `silver.quarantine` received **zero rows**.
Wikimedia's `recentchange` payloads are well-formed, every one of them carries
`meta.id`, `meta.dt` and `meta.domain`, and none arrived with a broken clock.

Live traffic has therefore tested only the passing side of every rule, 721,366 times.
The failing side is exercised by `tests/fixtures/adversarial.jsonl` and by nothing
else: the parity test proves the Python and SQL versions agree, and
`test_each_invalid_frame_landed_in_quarantine_with_its_reason` proves each reason code
fires on the frame built to trigger it. That is a limitation of the evidence rather
than of the rules, and the honest reading is that the quarantine is a working mechanism
whose threat model has not yet materialised.

### What loud does not mean

There is no alerting. Nothing pages anyone when the quarantine rate rises; the table
fills up and waits to be queried. Wiring a quarantine-rate check into the
orchestration layer is the obvious next step.

## What none of this proves

Stated plainly, because a correctness page that only lists successes is marketing.

- **Exactly-once end to end.** See section 2. The result is effectively-once at the
  silver boundary through idempotent writes. Bronze can and does hold duplicates.
- **No event loss.** The producer's SSE cursor is in memory, so a producer restart
  loses whatever the source emitted while it was down. Nothing in this repository
  measures that gap, and closing it would mean persisting the cursor.
- **Correctness at volume.** Every figure here is from one laptop at roughly 40
  events/second. The mechanisms transfer; the numbers do not, and the `MERGE`-reads-
  the-target design is the first thing that would need revisiting at a hundred times
  the rate.
- **Delete correctness.** Nothing in this pipeline deletes or updates a row, so
  Iceberg v2's equality and position deletes — the part that makes compaction genuinely
  hard — are enabled in the table format and never exercised.
- **That the tests would catch a regression I have not thought of.** They check the
  claims on this page. A pipeline fails in ways its author did not enumerate; that is
  what the quarantine table and the asset checks are for, and they detect rather than
  prevent.
