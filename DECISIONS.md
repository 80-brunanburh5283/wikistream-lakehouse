# Architecture decision records

Short records of the decisions that were not obvious, written when the decision
was made rather than reconstructed afterwards. Each one names what was rejected,
because a decision with no rejected alternative is not a decision.

Six are exceptions and say so in their own headers. ADR-0040 to ADR-0045 record the
platform choices — the queue, the table format, the stream processor, the
orchestrator, the query engine, the object store — which were settled before the
first commit and written down afterwards, when the README needed each row of its
decisions table to point at an argument. A backfilled record is worth less than a
contemporaneous one, so they are marked rather than dated as if they were written on
the day.

Several records quote a percentage of "the job adverts this project was designed
against". That is one specific dataset, and this is what it is: 31 data-engineering
adverts collected in September 2026 from four public job boards (arbeitnow,
himalayas, remoteok, jobicy), counted by whole-word keyword match. It is a small
sample of a regional slice of the market, so treat every percentage from it as an
indication of where I chose to spend my time and not as a measurement of the
industry. The technical arguments in these records stand or fall on their own.

Newest last.

---

## ADR-0001 — Apache License 2.0

**Date:** 2026-09-17 · **Status:** accepted

### Context

The repository needs a license before the first commit, and the choice is
visible: a reviewer looking at a data-engineering project reads the license as a
signal about whether the author has worked around open source.

### Options

| Option | Note |
|---|---|
| Apache-2.0 | Permissive, includes an explicit patent grant and a contribution clause. The license of every major component here — Kafka, Spark, Iceberg, Trino, Airflow, dbt-core's core. |
| MIT | Shorter and equally permissive, but silent on patents. |
| No license | Means "all rights reserved" by default. A portfolio repository with no license is not usable and reads as an oversight. |
| GPL / AGPL | Copyleft would be an odd fit for a reference implementation intended to be copied from. |

### Decision

Apache-2.0.

The patent grant is the substantive difference from MIT, and matching the license
of the surrounding ecosystem means anyone vendoring a file from here faces no new
compatibility question. The `NOTICE`-and-attribution machinery is more than a
project this size needs, which is the cost.

### Consequence

`LICENSE` holds the canonical text fetched from apache.org with the copyright
line filled in. `pyproject.toml` declares `license = "Apache-2.0"`. Contributions
are inbound under the same terms via clause 5, so there is no CLA to administer.

---

## ADR-0002 — A real-time lakehouse, not a batch ELT project

**Date:** 2026-09-17 · **Status:** accepted

### Context

I already work with Airflow, dbt, Snowflake and PySpark daily. A batch ELT
repository would be faster to build and would look competent, but it would
demonstrate nothing that my day job does not already demonstrate. The gap in what
I can show is the streaming and lakehouse half of the field: Kafka, stream
processing semantics, and open table formats.

### Options

| Option | Rejected because |
|---|---|
| Batch ELT: API → S3 → warehouse → dbt marts, on a schedule | Proves nothing I cannot already prove, and the interesting failure modes (duplicates, lateness, restart safety) do not arise. |
| Streaming pipeline with generated synthetic events | Synthetic data cannot demonstrate watermarking honestly. You choose the lateness distribution, so any watermark you pick is trivially correct, and a reviewer knows it. |
| Streaming from a live public source into an open table format | Chosen. |
| Batch + streaming (a full Lambda architecture) | Twice the surface area for one extra talking point, and the batch half would be the uninteresting half. |

### Decision

Consume a live public event stream and land it in Apache Iceberg through Spark
Structured Streaming, with dbt marts over Trino and Dagster as the control plane.

Wikimedia's `recentchange` firehose is the source. It is public, unauthenticated,
free, explicitly intended for this kind of consumption, and it produces genuine
data problems at no cost: real duplicates on reconnect (the SSE `Last-Event-ID`
mechanism is at-least-once by construction), real out-of-order arrivals, real
schema variation between event types, real bursts.

### Consequence

The correctness work becomes the point of the repository rather than a footnote:
if the source can redeliver, the sink must be idempotent, and that has to be
proven rather than asserted. It also means the project cannot be demonstrated
offline. The unit, `spark` and `integration` suites therefore run against captured
fixtures, and exactly two targets reach the public internet: `make smoke-live`,
which is a connectivity check, and `make test-e2e`, the restart proof — which
feeds itself from the live endpoint because a crash-recovery proof run on hand-fed
frames would mostly be a proof about the frames.

The scope excluded by this choice is recorded plainly: no ML layer, no frontend,
no second cloud implementation. See the README's limitations section.

---

## ADR-0003 — The version pin set, verified rather than assumed

**Date:** 2026-09-17 · **Status:** accepted

### Context

Spark, Iceberg and Trino only work in tested combinations, and the Scala suffix
on an Iceberg runtime jar has to match the Spark build exactly. A portfolio
repository that cannot be cloned and run a year later is worth nothing, so every
pin is exact and every pin was checked against the registry rather than taken
from memory.

### What I verified, on 2026-09-17

| Component | Pinned | How checked |
|---|---|---|
| Spark | `apache/spark:4.0.4-scala2.13-java17-python3-ubuntu` | Docker Hub tag list; `docker manifest inspect` |
| PySpark | `4.0.4` | PyPI JSON API — must equal the image, or job submission fails on a py4j protocol mismatch |
| Iceberg | `1.10.1` (`iceberg-spark-runtime-4.0_2.13`, `iceberg-aws-bundle`) | Maven Central `HEAD` on both jar URLs, 200 |
| Iceberg REST catalog | `apache/iceberg-rest-fixture:1.10.1` | Docker Hub tag list |
| Kafka | `apache/kafka:4.1.2` | Docker Hub tag list |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.13:4.0.4` | Maven Central `HEAD`, 200 |
| Object store | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` | quay.io tag API |
| Trino | `trinodb/trino` 4xx, confirmed in Phase 6 | Docker Hub tag list |
| Python | 3.12 | matches the system interpreter and every library in the set |

### Three findings that changed the plan

**`minio/minio` no longer exists on Docker Hub.** `docker pull minio/minio:...`
returns `pull access denied for minio/minio, repository does not exist`, and the
Docker Hub API returns `object not found` for the repository. The images are
served from `quay.io/minio/minio`. Anything in this repository referring to the
Docker Hub path would fail on a fresh machine, so the compose file uses the
quay.io path with a comment saying why.

**Spark 4.x is Scala 2.13 only.** The `_2.12` Iceberg runtime jars that most
Spark-plus-Iceberg material still shows apply to the 3.x line. Using `_2.12`
against Spark 4 fails at class-load time with a `NoSuchMethodError` that does not
mention Scala, which is an expensive hour if you have not met it before.

**Spark 4.2.0 has an image but no Iceberg runtime.** Iceberg 1.11.0 ships
runtimes for Spark 4.0 and 4.1; there is no `iceberg-spark-runtime-4.2`. Taking
the newest Spark tag would have produced an unresolvable jar.

### Decision

Spark 4.0.4 with Iceberg 1.10.1, all three Iceberg artefacts on the same patch
version, and the jars baked into a custom image at build time.

Iceberg 1.11.0 was available and rejected: the REST catalog fixture image is only
published up to 1.10.1, and running a 1.11 client against a 1.10 catalog server
adds a variable to the most version-sensitive part of the stack for no gain.
Keeping client and server on the same patch release is worth more than being one
minor version newer.

Spark 4.0.4 was chosen over 4.1.3 for the same reason in the other direction:
`iceberg-spark-runtime-4.1_2.13` exists only in Iceberg 1.11.0, so Spark 4.1
would have forced the 1.11 client. Spark 4.0's runtime exists across four Iceberg
releases, which is the evidence that the combination is exercised.

Jars are downloaded in the `Dockerfile` rather than resolved by
`spark-submit --packages`. Resolution at startup means a reviewer on a slow
connection waits for 105 MB of Maven traffic on every single run, sees a timeout,
and blames the repository. Baking them in makes a cold start a one-time image
build.

### Consequence

`docker/spark.Dockerfile` pins each jar URL by full coordinate. Upgrading Spark
means changing the image tag, the PySpark pin, the connector jar and possibly the
Iceberg runtime together — which is why `dependabot.yml` ignores major-version
bumps on `apache/spark` and `apache/iceberg-rest-fixture`. That is deliberate:
those are compatibility-matrix exercises, not version bumps.

**Amended in Phase 4.** Two details above are out of date and the reason is worth
keeping rather than editing away. The Spark *version* held, but the publisher did
not: `apache/spark:4.0.4-scala2.13-java17-python3-ubuntu` turned out to ship 41
zero-byte jars, so the image is now the Docker Official Images build pinned by
digest — ADR-0015. And the jar count grew from four to six once
`spark-token-provider-kafka-0-10` and `commons-pool2` proved to be transitive
dependencies that `--packages` would have resolved silently; the Maven traffic
figure measured 115 MB rather than 105 MB.

---

## ADR-0004 — `uv` for Python packaging

**Date:** 2026-09-17 · **Status:** accepted

### Context

The project needs a reproducible Python environment across a laptop, four
container images and GitHub Actions. It has an awkward dependency set: PySpark,
dbt-core and Dagster in one resolution, each with opinions about `click`,
`jinja2`, `protobuf` and `pydantic`.

### Options

| Option | Rejected because |
|---|---|
| `pip` + `requirements.txt` | No lockfile with hashes, no resolution of the full graph, and `pip freeze` output does not distinguish a direct dependency from a transitive one. |
| Poetry | A real lockfile, but slow on a graph this size and its dependency-group model is now non-standard next to PEP 735. |
| `pip-tools` | Works, but needs a separate file per environment and no Python version management. |
| `uv` | Chosen. |

### Decision

`uv`, with PEP 735 `[dependency-groups]` splitting `dev`, `analytics` and
`orchestration`, and `uv.lock` committed.

The deciding factor was that the resolution actually succeeded: PySpark 4.0.4,
dbt-core 1.12.5 and Dagster 1.13.23 co-resolve into 152 packages with no
conflict, which I did not assume in advance. Resolution took 3m49s cold and the
lock makes it free thereafter.

### Consequence

`uv.lock` is committed and CI installs from it, so CI and my laptop run the same
bytes. `uv run` means no target in the `Makefile` depends on a virtualenv being
activated. Contributors need `uv` installed, which is one extra prerequisite —
documented in `CONTRIBUTING.md`.

---

## ADR-0005 — Derive the event schema from a capture, and declare it explicitly

**Date:** 2026-09-17 · **Status:** accepted

### Context

The pipeline needs a Spark schema for `recentchange` events. Two questions had to
be answered: where the field list comes from, and whether Spark should infer it.

I captured 500 live events (`tests/fixtures/recentchange_sample.jsonl`,
2026-09-17) and profiled them before writing any schema. The capture disagreed
with the field list I had been working from: `server_script_path`, `notify_url`
and five `log_*` fields are sent by the stream and were absent from my notes.

### Options considered

| Option | Why not |
|---|---|
| Runtime schema inference | Inference samples data, so the table's column set becomes a function of when the job started. With `log` events at 2.8% of traffic, a job starting in a quiet minute can infer a schema with no `log_type` column at all. Spark also requires `spark.sql.streaming.schemaInference` to be switched on before it will infer on a stream, which is a fair warning. |
| Schema from the upstream documentation | Measurably incomplete: it omits seven fields the stream actually sends. |
| Explicit schema derived from a capture | Chosen. |

### Decision

`RECENTCHANGE_SCHEMA` in `src/wikistream/streaming/schema.py` is written out by
hand, with every type and nullability decision traceable to a measured rate in
the capture. `tests/unit/test_schema.py` compares the declared field set against
the captured field set in both directions and fails on either kind of drift.

Four integer widths were decided by measurement rather than by guess. In a single
500-event sample, `id` reached 3,468,178,745, `revision.new` reached
2,546,877,183 and `meta.offset` reached 6,524,211,441 — all past the int32
ceiling of 2,147,483,647. Spark's JSON parser returns null on overflow instead of
raising, so `IntegerType` on those columns would have deleted the revision ids of
the busiest wikis while every row count and every health check stayed green.

### Consequence

Upstream adding a field breaks a test instead of silently widening the table. The
cost is that adding a field is a code change, which is the trade I want: bronze
also retains the raw payload, so a field discovered late is recoverable by
backfill rather than lost.

---

## ADR-0006 — Keep the polymorphic `log_params` as raw JSON text

**Date:** 2026-09-17 · **Status:** accepted

### Context

`log_params` is not one shape. In the captured `log` events it is a JSON object
for `upload`, `abusefilter` and `newusers` (12 of 14) and an empty JSON array for
`thanks` and `delete` (2 of 14). The object's own values are mixed types —
`{"action": "edit", "filter": "1245", "actions": "disallow", "log": 45144167}`
holds both strings and numbers.

### Options considered

| Option | Why not |
|---|---|
| `StructType` with the union of observed keys | Cannot also be an array. Would need a new key every time a log type appears that the capture missed, and there are dozens of MediaWiki log types. |
| `MapType(StringType, StringType)` | Cannot hold the array form, and coerces `45144167` to `"45144167"` — a silent type change inside a field whose whole purpose is fidelity. |
| Drop the field | It is the only place the detail of a log action lives. |
| `StringType`, holding the raw JSON | Chosen. |

### Decision

`log_params` is `StringType`. Declaring a string against a JSON object or array
is deliberate, not a mistake: Spark's `JacksonParser`, given a `StringType`
target and a JSON structure, copies the raw JSON back out as text. The column
therefore holds the original substring losslessly and a caller who wants
structure can parse it with `from_json` at read time, choosing a shape
appropriate to the log type they are asking about.

This is asserted rather than assumed —
`tests/integration/test_schema_parses.py` round-trips every `log_params` value
in the capture through real Spark 4.0.4, including the empty-array form and the
mixed-type object.

### Consequence

Querying inside `log_params` needs an explicit parse in SQL. Acceptable: log
events are 2.8% of traffic and none of the marts read this field. The alternative
was a column that is null exactly when it is interesting.

---

## ADR-0007 — Parse permissively and quarantine explicitly

**Date:** 2026-09-17 · **Status:** accepted

### Context

Malformed records must not stop a streaming query, and unusable records must not
enter the deduplicated table. I probed what `from_json` actually does before
designing this, and two of the three behaviours were the opposite of my guess.

Verified against Spark 4.0.4:

1. A malformed payload does **not** produce a null struct. `from_json("{not json}")`
   returns an ordinary struct whose every field happens to be null. Only a
   zero-length string yields an actual null struct.
2. Naming a corrupt-record column makes the parser store the offending raw text
   in it.
3. `mode=FAILFAST` raises and kills the query.

Finding (1) matters most: `parsed IS NULL` reads like a corruption check and is
not one. A quarantine predicate built on it would admit garbage as a row of
nulls.

### Options considered

| Option | Why not |
|---|---|
| `FAILFAST` | One malformed byte on the wire stops ingestion for every wiki. Unacceptable on a stream. |
| `PERMISSIVE` and test the struct for null | Does not work, per finding (1). This is the bug the probe caught before it was written. |
| `PERMISSIVE` + corrupt-record column + explicit required-field predicate | Chosen. |

### Decision

`PARSE_SCHEMA` is the declared contract plus a `_corrupt_record` column, parsed
with `mode=PERMISSIVE`. The sidecar column is kept out of `RECENTCHANGE_SCHEMA`
so the drift test keeps comparing the declared contract against the wire with no
locally-invented field to explain away.

Rejection then has two distinguishable reasons, and `silver.quarantine` records
which one applied:

- **unparseable** — `_corrupt_record` is not null. The bytes were not JSON.
- **incomplete** — the payload is valid JSON but a field in `REQUIRED_FIELDS`
  (`meta.id`, `meta.dt`, `meta.domain`) is null. There is no deduplication key,
  no event time, or no partition key, so the row cannot be placed, ordered or
  deduplicated.

A single "invalid" flag would conflate the two, and they need different
responses: the first is a wire or upstream-encoding problem, the second is a
contract change.

### Consequence

The quarantine table stores the raw payload alongside the reason, so a rejected
record can be replayed after a fix rather than merely counted. Cost: one extra
column through the parse step, and a quarantine table that needs its own
retention policy.

---

## ADR-0008 — Partition Kafka by wiki domain, accepting the skew

**Date:** 2026-09-17 · **Status:** accepted, amended below the same day

### Context

The topic needs a partitioning key. `meta.domain` (the wiki) is the natural
candidate; round-robin is the alternative.

### Options considered

| Option | Why not |
|---|---|
| Round-robin / no key | Even partitions, but no ordering guarantee for any subject. "The most recent edit to this page" then needs a global sort. |
| Key by `meta.id` | Perfectly even, and useless — every event is its own key, so ordering is guaranteed for nothing. |
| Key by `meta.domain` | Chosen, with a measured cost. |

### Decision

Partition by `meta.domain`, giving per-wiki ordering.

The cost is skew, and it is measured rather than hand-waved: in the 500-event
capture, `commons.wikimedia.org` alone was 31.8% of events, and the top three
domains were 67.2%. With three partitions, one runs hot. `tests/unit/test_events.py`
asserts the skew is still above 20% so that this entry cannot quietly go stale.

### Consequence

Accepted, because per-wiki ordering is what makes the silver table's
last-write-wins semantics meaningful, and because at 40 events/second a hot
partition is not a throughput problem on a laptop. At scale the fix is a
composite key (`domain` plus a bucket of `page_id`) which trades strict per-wiki
ordering for per-page ordering — a better trade at volume, and an unnecessary
complication here. Named in the README's limitations rather than pretended away.

### Amendment, 2026-09-17: the 31.8% figure was the wrong quantity

Once the topic existed and could be measured, the busiest partition held **58.3%**
of a 5,000-record run (749 / 1,338 / 2,913 across three partitions), not the ~32%
this entry implied. Both numbers are correct and they are not the same number:
31.8% is the busiest *wiki's* share of traffic, which is only a lower bound on the
busiest *partition*, because a partition receives every key that hashes to it.

`make explain-partitions` reproduces the mechanism offline from the capture.
librdkafka's default partitioner is `crc32(key) % partition_count`, and with three
partitions `commons.wikimedia.org` (31.8%) and `id.wikipedia.org` (21.8%) collide
on partition 2 — predicting 61.2% against the 58.3% measured hours later on a
different sample. So this is two heavy keys landing together, not poor hash
quality, and no partition count can go below 31.8% while one key is indivisible.

Three partitions are kept regardless, which is a laptop-shaped decision rather
than the general answer: Spark reads one Kafka partition per task and there are
two executor cores, so six partitions would queue half the tasks and move the
constraint rather than remove it. The full table of predicted skew per partition
count, and the reason the real fix is a composite key rather than more partitions,
is in `docs/throughput.md`.

Corrected here rather than silently rewritten above, because the original estimate
being off by 26 points is the more useful thing to record.

---

## ADR-0009 — A 10-minute watermark for a sub-second stream

**Date:** 2026-09-17 · **Status:** superseded by [ADR-0019](#adr-0019--no-watermark-on-the-silver-stream)

**Superseded because** the watermark was sized here before the silver writer existed.
When it was built, the operator that would have read the watermark turned out to drop
data silently, so the whole setting went. The lag measurement below is still the real
one and still worth keeping; the conclusion drawn from it is not.

### Context

The silver stream needs a watermark. I measured the input rather than guessing:
7,234 events over 180 seconds gave a median lag of −0.36 s, p99 of 1.32 s and a
maximum of 2.74 s, with 98.6% of events arriving inside one second
(`docs/latency.md`). 82% of the samples were negative, which is clock skew
between WSL2 and Wikimedia's producers, and is reported rather than corrected.

### Options considered

| Option | Why not |
|---|---|
| 30 seconds (~23x the measured p99) | Sized for the network, not for recovery. On a restart after a two-minute outage, Kafka hands back a backlog whose event times are minutes old, and everything older than 30 s behind the batch maximum is dropped as late. A restart would silently lose most of what it read. |
| 1 hour | Costs 240,000 events of retained state to protect against an outage length that the 24-hour Kafka retention already bounds, and delays nothing usefully. |
| 10 minutes | Chosen. |

### Decision

`watermark_minutes = 10`, roughly 450x the measured p99. The watermark is sized
for restart replay, not for steady-state jitter: it tolerates an outage of up to
ten minutes with no dropped events.

### Consequence

Spark retains about `40 events/s x 600 s = 24,000` events of deduplication state
— small enough to be uninteresting on an 11 GB laptop, which is the honest reason
this trade-off was cheap. An outage longer than ten minutes drops the oldest
events on restart, and the README says so. On a stream two orders of magnitude
larger the answer would flip to a tighter watermark plus a correction table for
late arrivals.

---

## ADR-0010 — `confluent-kafka` as the Python client

**Date:** 2026-09-17 · **Status:** accepted

### Context

The producer needs a Kafka client. The pipeline's whole argument is about
duplicate suppression, so the client's idempotence support is not a detail — it
decides which of the three duplicate sources can be closed at the producer and
which have to be closed downstream.

### Options considered

| Option | Why not |
|---|---|
| `kafka-python` | Pure Python, no build step, widely used. But it has no idempotent producer: no producer id, no per-partition sequence numbers, so an internal retry after a lost acknowledgement writes the record twice. That is exactly the duplicate class this repository claims to handle. |
| `aiokafka` | Async, and it does implement idempotence. Rejected because the source is a blocking SSE iterator; adopting it would mean an event loop wrapped around synchronous I/O for no throughput gain at 51 events/s. |
| `confluent-kafka` | Chosen. |

### Decision

`confluent-kafka` 2.15.x, the official librdkafka binding, with
`enable.idempotence=true`.

The cost is a compiled dependency — wheels exist for CPython on manylinux and
macOS, so this is only felt on unusual platforms — and an API that is C-shaped
rather than Pythonic: delivery is a callback, `poll()` must be called to serve
those callbacks, and a full send queue raises `BufferError` rather than blocking.
Each of those is a place to get it wrong quietly, so each has a test in
`tests/unit/test_kafka_sink.py`, the load-bearing one being that a full queue makes
`send` wait rather than drop.

### Consequence

Idempotence removes duplicates from producer-internal retries only, and that
boundary is documented at the top of `kafka_sink.py` rather than left to be
assumed. Restart duplicates (a new session gets a new producer id) and upstream
replay duplicates (Wikimedia resumes SSE at or before `Last-Event-ID`) both remain,
which is why bronze is append-only and silver MERGEs on `meta.id`. A client without
idempotence would have made no difference to the pipeline's guarantees, only to how
many duplicate classes it has to absorb — but "we chose the client that could not do
it" is not a sentence worth writing.

---

## ADR-0011 — Advertise Kafka on two listeners, not one

**Date:** 2026-09-17 · **Status:** accepted

### Context

Kafka clients bootstrap by connecting to any broker, receiving the cluster's
*advertised* addresses, and then reconnecting to those. The address the client
dialled is discarded. Containers on the compose network resolve `kafka`; pytest and
`uv run` on the WSL host resolve `localhost:9092`. One advertised address cannot be
correct for both.

### Options considered

| Option | Why not |
|---|---|
| Advertise `kafka:29092` only | Containers work; every host-side test and script fails at the second connection with a DNS error, after an apparently successful bootstrap. The failure mode is confusing enough that it is worth engineering around rather than documenting. |
| Advertise `localhost:9092` only | The reverse, and worse: inside a container `localhost` resolves to the container itself. |
| Add `kafka` to the host's `/etc/hosts` | Works, needs root, and makes a fresh clone fail until a human edits a system file. Fails the "stranger runs one command" test. |
| Two listeners | Chosen. |

### Decision

`INTERNAL://kafka:29092` and `HOST://localhost:9092`, with
`KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL`. Only the host listener is published
to the host. `WS_KAFKA_BOOTSTRAP_SERVERS` defaults to `localhost:9092` for the host
context and is overridden per service in compose.

### Consequence

Every context reaches the broker with no host-file editing and no root. The cost is
that "which port" now has two answers, so the Makefile keeps `KAFKA_INTERNAL` as a
variable and a comment saying why, and a wrong bootstrap address produces a clear
timeout rather than a resolvable-but-wrong connection. This is also the single
detail most likely to break if the stack ever moves to Kubernetes, where the
equivalent is a headless service plus per-pod advertised names — noted in `k8s/`
rather than discovered later.

---

## ADR-0012 — Send Kafka the original frame, not a re-serialised one

**Date:** 2026-09-17 · **Status:** accepted

### Context

The producer already parses each SSE frame, because it needs `meta.domain` for the
partition key and it wants to count frames that fail to decode. Having a `dict` in
hand, the obvious next step is to `json.dumps` it into the Kafka value.

### Options considered

| Option | Why not |
|---|---|
| Re-serialise the parsed dict | Normalises key order and whitespace, which sounds tidy and is a quiet loss of fidelity: Python's JSON round-trip is not byte-identical to the source, it coerces some numeric forms, and it would silently repair malformed input that bronze is supposed to preserve as evidence. Bronze then stops being able to answer "what did Wikimedia actually send?" |
| Parse in the producer, forward the parsed columns | Moves schema decisions into ingest, so a schema change requires a producer redeploy and old data cannot be reinterpreted. |
| Forward the raw frame body | Chosen. |

### Decision

The Kafka value is the exact UTF-8 body the stream sent. The parse result is used
only for the key and for counters, and a frame that fails to decode is still
produced — unkeyed, so it round-robins — rather than dropped.

### Consequence

Bronze is a faithful byte-level record, which is what makes replaying it through a
changed schema meaningful and what makes the quarantine table's stored payload
worth storing. Parsing is deferred to Spark, where `from_json` on a declared schema
handles it (ADR-0005, ADR-0007). Two costs, both accepted: the producer parses JSON
it then throws away, which at this volume is free; and malformed frames reach the
lakehouse, which is deliberate — a validation gate at ingest would mean the only
copy of a bad record is a log line.

---

## ADR-0013 — `pyspark` is a dependency group, not a runtime dependency

**Date:** 2026-09-17 · **Status:** accepted

### Context

`make test-spark` runs schema tests against a real in-process Spark, so pyspark
must be installable from this project. The streaming jobs also import it. The
default move is to put it in `[project] dependencies`.

### Options considered

| Option | Why not |
|---|---|
| Runtime dependency | The producer image is the only image built from this repo and it never touches Spark. This would add roughly 400 MB of jars to a service whose job is to send JSON to Kafka, and would install a second pyspark alongside the one already in the Spark image — a driver/cluster version mismatch that surfaces as an opaque py4j traceback rather than a version error. |
| Not a dependency at all; test Spark only in Docker | Makes the fastest correctness tests in the repo require the whole stack to be up, which is the wrong incentive. |
| A `spark` dependency group | Chosen. |

### Decision

`spark = ["pyspark==4.0.4"]` under `[dependency-groups]`, pinned exactly rather
than with a range, because it has to match the Spark image tag in
`docker-compose.yml` for `make test-spark` to be testing the same engine that runs
in production-shaped conditions.

### Consequence

The producer image stays small and Spark-free, and that is enforced rather than
hoped for: `test_the_producer_imports_no_pyspark` starts a subprocess and imports
the producer package with no pyspark available. That test exists because of a real
failure — `wikistream.events` once imported the Spark schema module for a single
tuple of field names, and the container died at start-up with `ModuleNotFoundError`.
The cost is a second pin to keep in step with the image tag, and one place (the
group definition) where that requirement is written down.

---

## ADR-0014 — zstd for topic compression, though gzip measured better

**Date:** 2026-09-17 · **Status:** accepted

### Context

`recentchange` payloads are verbose JSON averaging 1,368 bytes with a small
vocabulary of keys, so the codec choice is worth a measurement rather than a
default. `make measure-compression` runs 2,000 frames through each codec into its
own topic and reads what the broker has on disk.

### Options considered

Measured, not argued (ratio is payload bytes divided by bytes on disk, so it
includes Kafka's record framing — which is why `none` comes out at 0.97x rather
than 1.00x, understating every row by about 3% uniformly):

| Codec | Ratio | Stored bytes/record |
|---|---|---|
| none | 0.97x | 1,401 |
| **gzip** | **4.37x** | 312 |
| zstd | 4.18x | 326 |
| snappy | 3.25x | 426 |
| lz4 | 3.19x | 422 |

### Decision

zstd, despite gzip winning the measurement by 4.5%.

That margin is inside the run-to-run variance: zstd measured 4.18x here and 4.33x
on the separate 5,000-frame run, on different samples of the same stream. The
honest reading is that gzip and zstd are indistinguishable on ratio for this data
and both are ~30% better than lz4 and snappy. What separates them is CPU per byte,
and **this project did not measure CPU** — at 51 events/s the producer is idle
waiting on the network, so neither codec is close to being the constraint. zstd is
chosen for the property that survives a volume change: it has a tunable level, so
the same codec spans "cheap" to "small" without a topic migration.

### Consequence

An earlier comment in `config.py` claimed zstd had the better ratio. It did not,
and the measurement is what corrected the comment rather than the reverse; both the
comment and `docs/throughput.md` now say gzip won on bytes and why zstd is still
the default. The unmeasured half — codec CPU — is named as the most significant gap
on that page instead of being papered over. At 50,000 events/s this decision should
be reopened, and the reopening starts with the measurement that was skipped here.

---

## ADR-0015 — The Spark base image is the Docker Official one, pinned by digest

**Date:** 2026-09-17 · **Status:** accepted

### Context

Phase 4 needs Spark 4.0.4 with the Iceberg runtime, the AWS bundle and the Kafka
connector available to `spark-submit`. The obvious base image is `apache/spark`,
published by the project itself, and the obvious pin is the version tag.

The first container built that way would not start:

```
exec /opt/entrypoint.sh: exec format error
```

Which reads like an architecture mismatch and is not one. `/opt/entrypoint.sh` in
that image is **0 bytes**, and an empty file with a shebang-less body is what
`exec format error` looks like. Overriding the entrypoint got further and then hit
`ClassNotFoundException: org.apache.spark.launcher.Main` from `spark-submit`
itself — because `spark-launcher_2.13-4.0.4.jar` is also 0 bytes, and Java's
`-cp "dir/*"` skips an empty jar silently rather than reporting an I/O error. In
total **41 of 277 jars** in the published image are zero-length.

Ruled out as local corruption: 906 GB free on the disk, and a fresh
`docker image rm` plus re-pull reproduced 41 of 277 exactly. The two images side by
side, with the entrypoint overridden so that the empty `/opt/entrypoint.sh` could
not mask the count:

```bash
docker run --rm --entrypoint sh apache/spark:4.0.4-scala2.13-java17-python3-ubuntu \
  -c 'find /opt/spark/jars -size 0 | wc -l'      # -> 41
docker run --rm --entrypoint sh spark:4.0.4-scala2.13-java17-python3-ubuntu \
  -c 'find /opt/spark/jars -size 0 | wc -l'      # -> 0
```

### Options considered

| Option | Why not |
|---|---|
| Fix it in the Dockerfile: re-download the 41 jars over the base image | Builds a working image on top of a broken one, and the list of 41 would have to be maintained by hand against every future tag. |
| Build Spark from source | Hours of build time in a repo whose selling point is that it starts in minutes. |
| Downgrade to an older `apache/spark` tag | Might work, but pins the project to whichever tag happened to publish cleanly, and gives no way to notice if the next one does not. |
| `spark:4.0.4-scala2.13-java17-python3-ubuntu` — the Docker Official Images build | Chosen. Same Spark version, same variant name, different publisher and different build pipeline. 0 zero-byte jars; entrypoint 4,735 bytes. |

### Decision

Use the Docker Official Images build, and pin it by **digest as well as tag**:

```dockerfile
FROM spark:4.0.4-scala2.13-java17-python3-ubuntu@sha256:8fc690e18426aa04ae92e7709c34fdc0be5ea848840e216fa6091ab9436fd37d
```

The six jars the pipeline adds are downloaded from Maven Central in the build and
each verified against a sha256 digest computed on 2026-09-17. They are baked in
rather than resolved by `spark-submit --packages`, so a cold start does not depend
on Ivy resolution and 115 MB of Maven traffic.

### Consequence

This is the argument for digest pinning, made by the failure rather than by
principle. The tag resolved, the manifest was valid, the layers unpacked, and the
contents were not what the version number promised — a digest is the only part of
an image reference that can detect that. The reproduction commands live in a comment
at the top of `docker/spark.Dockerfile` so the next person to read that odd-looking
`FROM` line gets the reason and can check whether it still holds.

The cost is a manual step on every Spark upgrade: the digest has to be looked up
and the six jar digests recomputed. That is the intended trade. An upgrade that
silently pulls different bytes under the same tag is exactly the event this pin
exists to make loud.

---

## ADR-0016 — `failOnDataLoss=false` on the bronze Kafka source

**Date:** 2026-09-17 · **Status:** accepted

### Context

Kafka's retention on `wiki.recentchange` is 24 hours — `kafka_retention_ms` in
`config.py`, applied by `scripts/create_topics.sh`. That is short on purpose:
Kafka is a buffer in this design and bronze is the durable audit trail, so paying
laptop disk to hold a second copy of history in the broker buys nothing.

Spark's Kafka source defaults to
`failOnDataLoss=true`: if the offsets the checkpoint recorded no longer exist on
the broker, the query fails to start.

On a laptop that combination fires for the most ordinary reason there is. Close
the lid on Friday, open it on Monday, and the checkpoint points at offsets that
expired on Saturday.

### Options considered

| Option | Why not |
|---|---|
| Keep the default and fail loudly | Correct for a system where Kafka is the record of truth. Here it turns "I closed my laptop" into a manual checkpoint deletion, and a reviewer's second `make up` into a stack trace. |
| Raise retention to 7 days | Moves the failure rather than removing it, and spends laptop disk on data that bronze already holds durably. |
| Delete the checkpoint automatically on this error | Silently re-reads the whole topic and duplicates everything already in bronze. Worse than either alternative. |
| `failOnDataLoss=false`, documented | Chosen. |

### Decision

Set it to false, and write down the cost where the flag is set rather than in a
commit message: data that expired while the job was down is skipped rather than
reported, and **nothing in the pipeline distinguishes that from a quiet period**.

### Consequence

Bronze's guarantee is precisely "everything Kafka still had when the job read it",
not "everything Wikimedia ever sent", and `docs/data-contracts.md` states that
under "what you may not rely on". A system where Kafka were the system of record
would have to choose the other way, and the sentence explaining that is in the
`bronze.py` module docstring so that anyone copying the flag out of this repo reads
the condition attached to it.

---

## ADR-0017 — Bronze partitions by ingest date, silver by event date

**Date:** 2026-09-17 · **Status:** accepted

### Context

Both tables hold the same events and both need a partition column. Using the same
one in both places is the obvious choice and it is wrong in one of the two.

### Options considered

| Option | Why not |
|---|---|
| Both by `days(event_date)` | A late event, or any replay of older data, writes into a bronze partition that was already written and compacted. The audit log stops being append-only in the physical sense even though it is append-only logically, and every backfill rewrites history. |
| Both by `days(ingest_date)` | Makes every analytical question ("edits per hour on Tuesday") a full-table scan, because ingest time and event time only coincide while nothing has ever been replayed. |
| Bronze by ingest date, silver by event date | Chosen. |

### Decision

`bronze.recentchange_raw PARTITIONED BY (days(ingest_date))`,
`silver.edits PARTITIONED BY (days(event_date))`.

### Consequence

Ingest date is monotonic, so bronze's old partitions are immutable in practice and
stay compacted: a replay of last week lands in today's partition and rewrites
nothing. Event date is not monotonic, so silver pays a rewrite cost on late data —
accepted, because it is the table analytics actually reads, and because Iceberg's
row-level deletes make the rewrite a partition-local operation rather than a
table-wide one.

The visible consequence is that the two tables do not line up partition for
partition, so a "compare bronze and silver for yesterday" query has to name which
kind of yesterday it means. `late_by_seconds` in silver exists so that the
difference is measurable rather than a matter of inference.

---

## ADR-0018 — Keep ANSI mode on and parse with `try_*`, rather than disabling it

**Date:** 2026-09-17 · **Status:** accepted

### Context

Spark 4.0 enables `spark.sql.ansi.enabled` by default. Under ANSI, a malformed
cast raises instead of returning null. Measured in this project's container,
2026-09-17:

```sql
SELECT to_timestamp(s)     FROM VALUES ('2026-09-17T10:00:00Z'), ('nonsense');
-- [CAST_INVALID_INPUT] SparkDateTimeException, raised from inside
-- GeneratedIteratorForCodegenStage1 — a per-row failure, not constant folding
SELECT try_to_timestamp(s) FROM VALUES ('2026-09-17T10:00:00Z'), ('nonsense');
-- one timestamp, one null
```

Bronze was written with `to_timestamp(meta.dt)`. That is a poison pill in a
streaming job: the micro-batch fails, the checkpoint does not advance, the retry
reads the same offsets, and the job dies on the same record forever. One malformed
`meta.dt` would stop ingestion for the whole topic — and because Spark retries
before failing the query, it would do so after a delay, which is the worst kind of
outage to diagnose.

### Options considered

| Option | Why not |
|---|---|
| `spark.sql.ansi.enabled=false` in `session_properties` | Fixes every cast in the project with one line, and throws away the thing ANSI is for. Silent nulls from a bad cast are how a column empties without anything going red — the exact failure mode `docs/data-contracts.md` names as the one that breaks schema evolution. Also makes this project's SQL behave differently from the Trino and dbt layers, which do not have a mode to turn off. |
| Keep ANSI on, use `to_timestamp`, catch the failure per batch | There is nothing useful to do with the exception. It names neither the offset nor the payload, so the handler could only skip the whole batch. |
| Keep ANSI on, parse with `try_to_timestamp`, quarantine the nulls | Chosen. |

### Decision

ANSI mode stays at Spark's default of enabled. Every cast that touches source data
uses the `try_*` form, and a null result is a validation failure that
`silver.quarantine` records with a reason — the same treatment as a missing field.

### Consequence

Bad data becomes a row in a table instead of an exception in a log, which is the
behaviour the whole quarantine design already assumed. The cost is that `try_*`
must be remembered at every such call site; it is not the default, and a reviewer
adding a `to_timestamp` later would reintroduce the poison pill.

Two things guard against that: `tests/spark/test_bronze_mapping.py` asserts the
malformed case yields null rather than raising, and it first asserts that ANSI mode
is actually on, so the guard cannot quietly stop testing anything if a future Spark
changes the default back.

---

## ADR-0019 — No watermark on the silver stream

**Date:** 2026-09-17 · **Status:** accepted

### Context

The design this project was written to called for `withWatermark("event_time", "10
minutes")` and `dropDuplicatesWithinWatermark` on the silver stream. That is the
textbook answer for streaming deduplication: it bounds the state store, so the job's
memory does not grow with uptime.

Wikimedia's `recentchange` stream does not behave the way that design assumes. A
wiki that loses its connection to the event bus reconnects and replays, and the
replay can be tens of minutes behind. Steady-state source lag is sub-second — p99
1.32 s over 7,234 events, `docs/latency.md` — but that distribution says nothing
about the reconnect tail, which is not bounded by anything I control, and a
watermark is a data-loss setting sized against exactly the part I cannot measure.

### Options considered

| Option | Why not |
|---|---|
| `withWatermark` + `dropDuplicatesWithinWatermark("event_id")` | Measured, not assumed: `tests/spark/test_watermark_would_drop_data.py` feeds a unique event 30 minutes behind the watermark through this exact operator and the row is **gone**. No exception, no `_corrupt_record`, no quarantine row, no metric — `numOutputRows` is simply one lower than `numInputRows`. A reconnecting wiki would lose edits and nothing in the pipeline would say so. |
| `withWatermark` with no stateful operator | Satisfies the specification and changes no output at all — the same test proves it drops nothing, because a watermark is only a filter in combination with an operator that reads it. Declaring one to tick a box would be decoration, and the README would be claiming a guarantee that no code provides. |
| `withWatermark` with a threshold wide enough to be safe | Any threshold is a cliff somewhere. The same test shows 5 minutes late survives a 10-minute threshold and 30 does not; picking 24 hours to be safe keeps 24 hours of keys in state, which is the cost the watermark existed to avoid. |
| No watermark; deduplicate with `MERGE INTO` against the table | Chosen. |

### Decision

The silver stream declares no watermark. Duplicate suppression is the table's job:
`MERGE INTO ... WHEN NOT MATCHED THEN INSERT` looks the key up in `silver.edits`
itself, so the deduplication window is the table's whole history rather than a
tunable number of minutes. Within a micro-batch, `row_number()` collapses duplicates
before the MERGE, because a MERGE whose source has two rows matching one target row
raises rather than picking one.

### Consequence

An event arriving arbitrarily late is still deduplicated correctly, and lateness is
recorded rather than discarded — `late_by_seconds` is a column, so "how late does
this source actually get" is a query and not a guess.

The cost is real and belongs in the README's limitations rather than in a footnote:
the MERGE reads the target table on every micro-batch, so write cost grows with
table size instead of staying flat, and Iceberg's partition pruning is the only thing
keeping that sublinear. A stream that has to keep up with a firehose would need the
watermark back, plus a Bloom filter or a bounded lookback predicate on the MERGE.
This pipeline handles roughly 30 events a second, where the trade lands the other way.

The objection to the rejected design is not that watermarks discard data
indiscriminately. It is that ten minutes is a hard cliff with no diagnostic on the
far side of it, and I would rather pay for correctness in write amplification, where
it is measurable, than in silent loss.

---

## ADR-0020 — `MERGE INTO` inside `foreachBatch`, not the Iceberg streaming sink

**Date:** 2026-09-17 · **Status:** accepted

### Context

Iceberg's Spark streaming sink is idempotent on its own: it records the
`spark.sql.streaming.epochId` of each committed batch and refuses to apply the same
epoch twice, so an at-least-once retry from Structured Streaming does not duplicate
rows. Writing silver with `writeStream.format("iceberg")` would get that for free.

Silver needs to write two tables from one batch — `edits` and `quarantine` — and it
needs an upsert rather than an append, neither of which the streaming sink does.

### Options considered

| Option | Why not |
|---|---|
| Two `writeStream` queries, one per table, both using the Iceberg sink | Two queries means two checkpoints reading the same topic, so the split between valid and invalid rows would be computed twice and could disagree across a code change. It also doubles the Kafka consumers for one logical job. |
| One `foreachBatch`, `writeTo(...).append()` for each table | This is the trap. The epoch-id idempotency belongs to the Iceberg *sink*; a batch write issued inside `foreachBatch` carries no epoch tag, so a retried batch appends its rows a second time. The result is a pipeline that looks exactly like the correct one and duplicates on every failure. |
| One `foreachBatch`, `MERGE INTO ... WHEN NOT MATCHED` for each table | Chosen. |

### Decision

`foreachBatch` writes both tables with a generated `MERGE INTO`, joining on that
table's declared key. `WHEN NOT MATCHED THEN INSERT` and no `WHEN MATCHED` clause at
all: the merge inserts what is new and leaves what exists untouched, which is
idempotent by construction rather than by bookkeeping. A replayed batch re-executes
the same MERGE and inserts nothing.

### Consequence

Retries are safe without relying on a sink feature the code is not using, and the
same function serves the stream and the rebuild — `rebuild_from_bronze` calls
`merge_batch` with `batch_id=-1`, so the replay path cannot drift from the live path.

It is also testable in a way epoch bookkeeping is not:
`test_replaying_the_same_window_twice_changes_nothing` runs the whole window through
the MERGE twice more and asserts every row count is unchanged, and
`test_a_rebuild_replaces_a_row_deleted_by_mistake` deletes a row, replays, and
compares the restored row column by column.

The cost is the target-table read on every batch, which is the same cost ADR-0019
accepts, and one sharp edge worth naming: `MERGE` raises when the source contains two
rows matching one target row, so the intra-batch `row_number()` dedup is not an
optimisation. Remove it and the first duplicated `event_id` in a batch fails the write.

---

## ADR-0021 — `silver.quarantine` keys on the Kafka coordinates, not on `event_id`

**Date:** 2026-09-17 · **Status:** accepted

### Context

Both silver tables are written with a MERGE, so both need a key that identifies a
row. `silver.edits` keys on `event_id`. The obvious move is to do the same for
quarantine.

It cannot: "the payload has no `meta.id`" is one of the six reasons a row lands in
quarantine. A MERGE on a null key matches nothing, so every retry of that batch would
insert the row again — the table whose purpose is to record broken data would be the
one place in the pipeline that duplicates.

### Options considered

| Option | Why not |
|---|---|
| Key on `event_id` | Null for the `event_id_missing` reason, which is the most common shape of upstream breakage. Idempotency would hold for every quarantine reason except the one that matters most. |
| Key on a hash of `raw_payload` | Stable and never null, but wrong: two genuinely distinct events with identical payloads would collapse into one quarantine row, and the point of the table is to hold every rejected frame for replay. It also makes the key unreadable — you cannot go and look at offset `sha256:…`. |
| Key on `(kafka_partition, kafka_offset)` | Chosen. |

### Decision

`QUARANTINE_KEY = ("kafka_partition", "kafka_offset")`, and both columns are in the
table. Kafka guarantees the pair is unique within a topic, it is never null for a row
that arrived, and it is the address an operator uses to go and look at the record.

### Consequence

The quarantine table is idempotent under retry for every failure reason including a
missing id, which `test_quarantined_rows_carry_their_kafka_coordinates` and the
duplicate gate both assert. `raw_payload` is stored next to the coordinates so a
day's rejects can be fixed and replayed rather than only counted.

One consequence to be honest about: the key is a *transport* address, not an event
identity. The same logical event replayed by the source lands at two offsets and
would occupy two quarantine rows. For an error table that is the behaviour I want —
it is a log of what arrived, not a set of distinct problems — but it means
`count(*)` on quarantine answers "how many bad frames arrived", not "how many
distinct events are broken", and a dashboard should group by `failure_reason` rather
than trusting the total.

---

## ADR-0022 — The parse verdict is Spark's alone; only the rules have a Python twin

**Date:** 2026-09-17 · **Status:** accepted

### Context

`wikistream.quality.expectations` deliberately expresses every validation rule twice,
once as a Python predicate and once as Spark SQL, and
`tests/spark/test_expectation_parity.py` runs both over all 519 fixture frames and
fails on any disagreement. That is what makes the duplication safe.

The natural next step is to extend the same treatment to "is this payload readable at
all" — a `json.loads` twin for `from_json`. I tried, and it does not hold.

### Options considered

| Option | Why not |
|---|---|
| A Python `_parse_failed` twin using `json.loads` | Measured on Spark 4.0.4: the two disagree on 9 of 21 payload shapes. `from_json` in PERMISSIVE mode accepts trailing content, accepts a bare scalar, and returns a struct of nulls with `_corrupt_record` set rather than raising; a blank payload gives a null struct *and* a null `_corrupt_record`, so the check has to be `_parsed IS NULL OR _corrupt_record IS NOT NULL`. A twin would encode Spark's parser quirks in Python and the parity test would pass by copying the bug. |
| Make the SQL side stricter so the twin can be simple | Means rejecting frames Spark can in fact read. Data loss to make a test tidy. |
| Split the responsibility: Spark decides readability, the rules decide usability | Chosen. |

### Decision

There is one reason code, `payload_not_json`, that has no Python counterpart and is
produced only by inspecting `from_json`'s output. Every other reason — six rules
about presence, parseability and range — exists in both languages and is parity
tested. `RULE_INPUT_COLUMNS` is the interface between the two halves, and
`to_candidates` raises if the projection stops producing a column some rule reads.

### Consequence

The parity test keeps its value, because it now covers exactly the rules where two
implementations can be compared and nothing else. The parser's behaviour is pinned by
a table of measured payload shapes in `quality/expectations.py` rather than by a
second implementation — a comment that cites measurements, next to a test that
asserts the current verdicts.

The gap this leaves is honest and worth stating: if a future Spark changes what
PERMISSIVE mode accepts, the parity test will not notice, and the symptom would be
frames moving between `silver.edits` and `silver.quarantine` after a version bump.
The measured-shapes test is what would catch it, so it asserts the verdict for each
shape rather than merely that parsing "works".

---

## ADR-0023 — Target the Spark image's Python 3.10, not the dev environment's 3.12

**Date:** 2026-09-17 · **Status:** accepted

### Context

`uv` builds this project's environment on Python 3.12, the producer image is
`python:3.12.14-slim`, and both were chosen. The Spark image's interpreter was not: it
comes with the Docker Official image and it is **Python 3.10.12**.

`spark-submit` runs the streaming jobs with that interpreter, against the source tree
bind-mounted into the container. So a 3.11+ idiom anywhere in shared code passes every
local test on 3.12 and then fails at import time inside the container.

It did. `wikistream.quality.expectations` used `from datetime import UTC`, added in
Python 3.11. 285 unit and Spark tests were green, and the silver job died on its first
run in the container with `ImportError: cannot import name 'UTC'`. The first
integration test to run the job under `spark-submit` is what found it.

### Options considered

| Option | Why not |
|---|---|
| Install Python 3.12 into the Spark image | The bundled pyspark lives under `$SPARK_HOME/python` and the image's entrypoints resolve `python3` from PATH. Pointing `PYSPARK_PYTHON` at a second interpreter means re-installing every dependency into it and taking on a driver/worker mismatch that surfaces as a py4j traceback. A large change to the one part of the stack that currently works, to avoid writing `timezone.utc`. |
| Keep 3.12 as the target and rely on integration tests to catch drift | They do catch it — this ADR exists because one did — but only after a full stack is up, which is the slowest and least frequently run tier. A five-character mistake should not need Docker to find. |
| Fence off a "container-safe" subset of `src/` with its own target | Two Python dialects in one source tree, with the boundary maintained by memory. `config`, `logging` and `events` are imported by both runtimes; the boundary would move with every refactor. |
| Target 3.10 for all first-party Python | Chosen. |

### Decision

Ruff's `target-version` and mypy's `python_version` are both `py310`/`3.10`, matching
the Spark image. The dev environment stays on 3.12 — `requires-python` is unchanged,
because that governs what the tooling runs on, not what the code may use.

### Consequence

The failure is now caught by `make lint`, with no containers involved: typeshed gates
`datetime.UTC` behind `sys.version_info >= (3, 11)`, so mypy rejects it, and ruff at
`py310` stops *suggesting* it — UP017 rewrites `timezone.utc` into `datetime.UTC` at
py311+, which would have reintroduced the bug on the next `ruff --fix`. That detail is
the reason both settings had to move rather than just one.

The cost is a slightly older dialect in code that runs on a newer interpreter, which
is a fair price for the constraint being checked rather than remembered. `vermin
--target=3.10-` was used to survey the tree and found `datetime.UTC` to be the only
violation; it is not a permanent dependency, because ruff and mypy now cover the same
ground on every commit.

## ADR-0024 — Orphan cleanup lists storage through Iceberg's FileIO, not Hadoop's

**Date:** 2026-09-17 · **Status:** accepted

### Context

Three of the four Iceberg maintenance procedures read the table's metadata to decide
what to do. `remove_orphan_files` cannot: an orphan is by definition a file the metadata
does not mention, so the procedure has to list the object store directly and diff the
listing against the manifests.

It does that through Hadoop's `FileSystem` API by default, and this catalog's tables
live at `s3://lakehouse/warehouse/…`. Hadoop has no filesystem registered for the `s3`
scheme — its S3 connector is `s3a` — because `streaming.session` routes storage through
Iceberg's own `S3FileIO` instead, one filesystem layer fewer and the path Iceberg tests.
The result, measured against Iceberg 1.10.1 on the live catalog:

```
UnsupportedFileSystemException: No FileSystem for scheme "s3"
  at FileSystemWalker.listDirRecursivelyWithHadoop(FileSystemWalker.java:122)
  at DeleteOrphanFilesSparkAction.listedFileDS(DeleteOrphanFilesSparkAction.java:329)
```

The first three procedures had already succeeded in the same run, which is what makes
this worth an ADR: "Iceberg works against MinIO" and "every Iceberg procedure works
against MinIO" are different claims, and only the first was true.

### Options considered

| Option | Why not |
|---|---|
| Add `hadoop-aws` plus the AWS SDK to the Spark image and configure `fs.s3.impl` | Two S3 clients in one JVM, each with its own credentials, endpoint and path-style settings, to serve one procedure. `hadoop-aws` on Hadoop 3.4 pulls the AWS SDK v2 bundle — several hundred MiB on an image that has to be pullable on a laptop — and the shaded SDK already inside `iceberg-aws-bundle` cannot be shared with it. |
| Rewrite the table locations as `s3a://` | Changes the warehouse URI the REST catalog hands out, so every existing table's metadata points at the old scheme. Migrating paths to work around a listing implementation is the tail wagging the dog. |
| Build the file listing in Python and pass it as `file_list_view` | The procedure does accept a pre-computed view of `(file_path, last_modified)`, which would work. It also means writing and testing an S3 lister — pagination, clock handling, a `boto3` dependency in the Spark image — to reproduce something Iceberg already has. |
| Drop the procedure and document the gap | Tempting, and it would have been honest. But it leaves the pipeline with no answer for the files its own restart-idempotency proof strands, and the fix turned out to be one argument. |
| `prefix_listing => true` | Chosen. |

### Decision

`remove_orphan_files_sql` always passes `prefix_listing => true`, which switches the
walk to `listDirRecursivelyWithFileIO` and lists through the table's own FileIO. The
argument is not conditional: this repository configures the catalog in exactly one
place, `S3FileIO` supports prefix operations, and a flag that is sometimes set is a flag
whose failure mode has to be discovered twice.

Finding the argument needed reading the procedure's bytecode
(`javap -c` on `RemoveOrphanFilesProcedure`, whose string constants are its parameter
names), because Spark reports an unknown named argument as the procedure not existing.
That is recorded here because it is the reproducible way to answer "what arguments does
this procedure take" for any Iceberg version.

### Consequence

`make maintain` runs all four procedures and exits 0, and
`tests/integration/test_silver_rebuild.py::test_maintenance_preserves_every_row` is what
proves it — the argument names cannot be checked any other way.
`tests/unit/test_maintenance.py::test_orphan_listing_goes_through_the_table_io_not_hadoop`
pins the flag itself, because nothing in the statement suggests it is load-bearing and
the failure only appears against real storage.

The trade this accepts: on a warehouse using `HadoopFileIO` — a local `file://` path,
or HDFS — `prefix_listing => true` is the argument that would break, since it requires a
FileIO implementing `SupportsPrefixOperations`. The assumption is tied to the catalog
configuration, and both live in the two modules that a reader of either would open.

Iceberg's own 24-hour floor on `older_than` stays in place, so the procedure cannot be
demonstrated deleting a file in a test that runs in seconds: the orphans it would remove
have to be a day old. The integration test therefore asserts that the call succeeds and
removes nothing, which is the assertion that would have caught this failure.

---

## ADR-0025 — Make the crash window deterministic by deleting `commits/N`

**Date:** 2026-09-17 · **Status:** accepted

### Context

`silver.edits` claims one row per source event across restarts. The failure that
claim exists to survive is narrow: Spark writes a micro-batch's Kafka offsets to
`<checkpoint>/offsets/N` *before* running the batch and `<checkpoint>/commits/N`
*after* it. A crash between those two writes leaves rows already merged into
Iceberg and offsets Spark believes it never consumed, so on restart it re-reads
that exact range and applies it a second time. That window is milliseconds wide.

A test that tries to land a SIGKILL inside it is a coin flip. It would pass most
of the time by killing the job somewhere harmless, and the green tick would mean
nothing.

### Options

| Option | Rejected because |
|---|---|
| SIGKILL the job and hope the timing lands mid-commit | Passes for the wrong reason almost every time. The assertion holds trivially when no batch was in flight, and nothing in the output distinguishes the two cases. |
| Inject a fault into the job — a flag that raises after the MERGE, before the commit | Test-only code in the production path, and it proves the fault injector works rather than that the recovery works. |
| Mock the checkpoint layer entirely | Then the subject is a mock of Spark's contract, not Spark's behaviour. The whole point is that this contract is Spark's, not mine. |
| SIGKILL for real, then delete `commits/N` | Chosen. |

### Decision

`tests/e2e/test_restart_idempotency.py` kills both processes with SIGKILL while
they are working, and then deletes the newest commit file. Deleting `commits/N`
puts the checkpoint in exactly the state an interrupted commit leaves it in —
`offsets/N` present, `commits/N` absent — so the replay is guaranteed rather than
hoped for. The test prints how many Kafka records the replayed batch covered, which
is the number that makes the result meaningful: 858 on the run recorded in
`docs/correctness.md`.

The sharpest assertion is the second replay. With the producer stopped and the
topic static, re-running a committed batch of 2,640 records must move the row count
by exactly zero — an equality, not a bound.

### Consequence

The proof is deterministic, and the two halves of it are honest about what each
one does: SIGKILL demonstrates that a hard kill cannot half-write an Iceberg
commit, and the file deletion demonstrates that a genuine re-read of already-merged
offsets inserts nothing. Neither claims to be the other.

The cost is that the test reaches into Spark's checkpoint layout, which is internal.
If Spark renames those directories the test breaks — loudly, at the `rm`, not
silently. `_numeric_entries` and `_batch_end_offsets` are commented with the layout
they assume for that reason.

---

## ADR-0026 — Ship no dbt packages; write the one generic test that is missing

**Date:** 2026-09-17 · **Status:** accepted

### Context

The gold layer needs a uniqueness assertion on a composite grain: `(event_minute,
wiki, change_type)` for the minutely mart, `(event_hour, wiki)` for the hourly one,
and two of them for the top-N mart. dbt core's `unique` test takes a single column,
so the usual answer is `dbt_utils.unique_combination_of_columns`, which means a
`packages.yml` and a `dbt deps` step.

That is the only thing this project would use `dbt_utils` for. Everything else the
marts assert is either a core generic test — `unique`, `not_null`,
`accepted_values`, `relationships` — or a singular test whose logic is specific
enough that no package could supply it (`assert_top_pages_rank_is_contiguous` is
not a general-purpose idea).

`dbt deps` fetches from `hub.getdbt.com` at build time. This stack has one
deliberate network dependency, the Wikimedia stream, and `make dbt-run` on a fresh
clone reaching a second host to run four tests is a failure mode bought for very
little.

### Options

| Option | Rejected because |
|---|---|
| Add `dbt_utils` to `packages.yml` and run `dbt deps` in the image build | Pulls several hundred macros to get one test, pins a second dependency tree that has to resolve against dbt-trino on every upgrade, and makes a clone-and-run depend on a package registry being up. |
| Vendor the whole of `dbt_utils` into `dbt/macros/` | Thousands of lines nobody in the repo wrote, that a reader has to skip past, and that quietly stop matching upstream the day after they are copied. |
| Skip the composite test and assert `unique` on a concatenated surrogate key | Needs a column in every mart that exists only to be tested, and concatenation makes uniqueness depend on the delimiter never appearing in a page title. It also fails silently on nulls. |
| Assert nothing about the composite grain | The grain *is* the correctness property of an incremental model. An incremental key typo produces duplicate rows on the second run and nothing else notices. |
| Write the test | Chosen. |

### Decision

`dbt/tests/generic/unique_combination_of_columns.sql` — ten lines of SQL: group by
the column list, count, return the groups with more than one row. The name is
deliberately the same as `dbt_utils`'. If a future need ever justifies the package,
the migration is to delete this file and add the `dbt_utils.` prefix at four call
sites, with no change to the assertion.

`packages.yml` does not exist. `dbt_packages` stays in `clean-targets` anyway, so
the day it does exist `dbt clean` already knows about it.

### Consequence

`make dbt-run` on a fresh clone touches no host but Trino, and the dbt image build
has one dependency resolver in it rather than two. The test is in the repository
where a reviewer reads it, next to the models it guards.

The cost is real and worth naming: the next generic test this project needs will
also have to be written by hand, and if `dbt_utils` fixes a bug in its version of
this test, nothing here learns about it. That trade is acceptable at four call
sites and would not be at forty.

---

## ADR-0027 — One `gold` schema; the layer lives in the model name

**Date:** 2026-09-17 · **Status:** accepted

### Context

The dbt convention is a schema per layer, configured with `+schema: staging` and
`+schema: marts` in `dbt_project.yml`. dbt *appends* that value to the target
schema rather than replacing it, so with a `gold` target the result is
`gold_staging` and `gold_marts`.

Under Trino's Iceberg connector a schema is a catalog namespace, and namespaces
here are created by `scripts/init_tables.py` — `bronze`, `silver`, `gold` — before
anything queries the lakehouse. dbt would create its two at run time.

### Options

| Option | Rejected because |
|---|---|
| `+schema: staging` / `+schema: marts`, giving `gold_staging` and `gold_marts` | The catalog's namespace list then depends on whether dbt has ever run, so `make init-tables` stops being a complete description of the lakehouse layout and two tools own overlapping parts of it. The names also say "gold" twice: `lakehouse.gold_marts.mart_edits_per_minute`. |
| A separate `analytics` catalog for dbt output | Two catalogs over one warehouse root, and the marts stop being reachable from the same three-part name the rest of the project uses. Buys isolation this stack has no use for. |
| Staging views in `silver`, marts in `gold` | Puts dbt-managed objects inside the namespace Spark writes, where a `drop schema cascade` during a reset would take the source tables with them. |
| Everything in `gold` | Chosen. |

### Decision

Every model dbt builds lands in `gold`. The layer is carried by the model name —
`stg_`, `dim_`, `mart_` — and by the `models/` subdirectory, both of which appear
in `dbt ls`, in the docs site, and in the lineage graph. `init_tables.py` remains
the single place that creates namespaces.

### Consequence

`lakehouse.gold.mart_edits_per_minute` is the name in the marts, in
`scripts/query_trino.sh`, and in the README, with nothing to translate between
them. Dropping and recreating the whole analytics layer is one `drop schema
lakehouse.gold cascade` away, and it cannot reach the source tables.

What is given up is dbt's schema-level grant and permission story, which is the
real reason the convention exists. This stack has one user and no authentication
(see `SECURITY.md`), so there is nothing to grant. On a shared warehouse the split
would earn its keep and this ADR would be the wrong decision.

---

## ADR-0028 — Choose each mart's incremental strategy from the shape of its aggregate

**Date:** 2026-09-17 · **Status:** accepted

### Context

Five gold models over a table that is still being written to. They are not the same
shape:

- `mart_edits_per_minute` and `mart_bot_vs_human_hourly` are additive aggregates at
  a fixed grain. A bucket's value depends only on the rows in that bucket.
- `mart_top_pages_hourly` is a top-N. Which rows belong in the output depends on how
  the other rows in the bucket compare, so a row can stop belonging.
- `dim_wikis` and `mart_pipeline_health` are a few hundred rows over the whole
  history.

Every incremental run also lands mid-bucket. A run at 10:00:30 sees half of the
10:00 minute; the next run sees all of it. Whatever strategy is used has to say what
happens to that partial bucket, and the answer decides whether `count(distinct
editor)` in these marts is a real number or a lie.

### Options

| Option | Rejected because |
|---|---|
| `append` everywhere, filtering `> max(bucket)` | Writes the partial boundary bucket once partial and never corrects it, so half a minute of activity is permanently missing. Filtering `>=` instead makes it worse: the bucket is then written twice and every count doubles. |
| `merge` everywhere | Correct for the additive marts, wrong for top-N. When a page falls out of the ten, merge has no row to update and no reason to delete: the stale row stays, the hour ends up with eleven "top ten" entries, and two of them claim the same rank. |
| `delete+insert` everywhere | Correct, but dbt-trino materialises a temp *table* for this strategy rather than the view it uses for merge, so every additive mart would pay a full write of its incremental slice to buy a delete it does not need. |
| Full rebuild everywhere | Simplest and always right, and it re-reads the entire silver table once per model per run — minutes on a laptop, growing with the data. It also skips the part of the problem worth demonstrating. |
| One strategy per shape | Chosen. |

### Decision

| Model | Strategy | Key | Why this one |
|---|---|---|---|
| `mart_edits_per_minute` | `merge` | `(event_minute, wiki, change_type)` | Additive; the boundary bucket's row is overwritten in place. |
| `mart_bot_vs_human_hourly` | `merge` | `(event_hour, wiki)` | Same shape, coarser grain. |
| `mart_top_pages_hourly` | `delete+insert` | `event_hour` | The affected hours have to be emptied before they are rewritten, because membership of the top ten is not stable. |
| `dim_wikis` | table | — | Cumulative shares over a few hundred rows; an incremental version would re-read everything anyway. |
| `mart_pipeline_health` | table | — | Same, plus a full outer join across two clocks that an incremental filter would have to be applied to twice. |

The second half of the decision is the filter. Every incremental model reads
`>= max(bucket)` from its own target, not `>`. The bucket a previous run stopped
inside is therefore recomputed from all of its rows and overwritten — which is what
makes `merge` mandatory rather than merely convenient, and what makes a distinct
count in these marts correct.

For `mart_top_pages_hourly`, `unique_key='event_hour'` names the delete predicate,
not a key: dbt-trino renders it as `delete from target where (event_hour) in (select
event_hour from tmp)`, and the target legitimately holds up to ten rows per hour per
wiki. The ordering inside `row_number()` breaks ties on `page_title` so that
recomputing an hour produces the same table rather than a differently shuffled one.

### Consequence

Each incremental run re-reads one bucket of data it has already processed. That is
the price of the boundary being correct, and it is bounded — one minute or one hour
of the firehose, not a growing tail.

Every incremental read carries two predicates that look redundant. The bucket
predicate is for correctness; the `event_date >=` beside it exists only so Iceberg
can prune partitions, because Trino cannot infer that a truncated timestamp in a
view is related to the partition column underneath it. Both are scalar subqueries
rather than a cross join to the same aggregate: Trino evaluates a scalar subquery
once and pushes a constant into the scan, where the cross join would need dynamic
filtering and would prune nothing.

`mart_events = silver_events` is not an invariant and no test asserts it — silver
keeps growing while the marts are built, and late data lands in a bucket already
written. `mart_events > silver_events` has no legitimate cause, so
`assert_minute_mart_never_overcounts` asserts that direction only. A one-sided test
that is true is worth more than a two-sided one that has to be marked `warn`.

---

## ADR-0029 — Name the Trino catalog `lakehouse`, matching Spark's

**Date:** 2026-09-17 · **Status:** accepted

### Context

Trino takes a catalog's name from its properties filename; Spark takes it from the
`spark.sql.catalog.<name>` property prefix. Both here point at the same Iceberg REST
catalog over the same MinIO bucket, and nothing makes them agree. The Trino Iceberg
connector's documentation and nearly every example call the catalog `iceberg`.

If they disagree, one table has two fully-qualified names: `iceberg.silver.edits` in
the Trino CLI and `lakehouse.silver.edits` in Spark. A query copied from one to the
other fails with `Catalog 'lakehouse' does not exist`, which reads like a broken
configuration rather than a naming difference.

### Options

| Option | Rejected because |
|---|---|
| Trino `iceberg`, Spark `lakehouse` — each engine's convention | Two names for one table. Every snippet in the README, `docs/`, and the runbooks would have to say which engine it is for, and the failure when someone gets it wrong points at the wrong thing. |
| Both `iceberg` | Names the file format rather than the store. It also reads as though there is a non-Iceberg copy of `silver` somewhere, and leaves nothing to distinguish a second Iceberg catalog with different settings if one is ever added for comparison. |
| Both `lakehouse` | Chosen. |

### Decision

`lakehouse` on both sides. `docker/trino/catalog/lakehouse.properties` gives Trino
the name; `WS_ICEBERG_CATALOG_NAME` gives it to Spark, to dbt's profile, to the
source definition in `_silver__sources.yml`, and to `scripts/query_trino.sh`.

### Consequence

`lakehouse.silver.edits` is one string that works in `spark-sql`, in the Trino CLI,
in a dbt model, and in the README, so no document has to name an engine before it can
name a table.

The one seam is that Trino's catalog name comes from a filename and cannot read an
environment variable. Changing `WS_ICEBERG_CATALOG_NAME` without renaming
`lakehouse.properties` puts the two out of step. That fails immediately and loudly
on the first Trino query rather than producing wrong results, and the file is named
after the value precisely so the connection is visible in a directory listing.

---

## ADR-0030 — Dagster observes the streaming half; it does not run it

**Date:** 2026-09-17 · **Status:** accepted

### Context

Dagster is the orchestrator, and three of the five things in the ingest path are not
orchestratable in the sense Dagster means. The producer holds one long-lived HTTP
connection to Wikimedia. The bronze and silver jobs are Spark Structured Streaming
queries with checkpoints, running until stopped. For all three, "materialise" has no
meaning: there is no run that starts, produces a table and finishes.

But leaving them out of the asset graph would break the graph. The marts are built by
dbt from `silver.edits`, so a lineage that begins at silver starts halfway through the
pipeline and says nothing about where the data came from — which is most of what an
orchestrator is for here.

### Options

| Option | Rejected because |
|---|---|
| Wrap each streaming query in an asset that submits `spark-submit` and returns | The asset would go green the moment the query started and stay green after it died. It would also make Dagster the supervisor of a process Compose already restarts, with two restart policies disagreeing about who owns the query. |
| Give the streaming jobs a schedule and run them in bounded windows | Turns a streaming pipeline into a micro-batch one to satisfy the orchestrator. The restart-idempotency proof — kill mid-flight, restart, no duplicates — is about a continuous query; scheduling it away would delete the thing this repository exists to demonstrate. |
| Leave them out of the graph entirely | The dbt source becomes a root with no parent. Nothing then connects Kafka to the marts, and the lineage diagram in the UI stops being evidence of anything. |
| Model them as external assets and observe them | Chosen. |

### Decision

The producer, the Kafka topic, `bronze.events`, `silver.edits` and `silver.quarantine`
are `AssetSpec`s with no compute — external assets. A single
`@multi_observable_source_asset` (`lakehouse_state`, `can_subset=True`) queries Trino
and Kafka for the row count, the newest event time and the topic's high watermarks,
and yields an `ObserveResult` per table.

Each observation's data version is the row count, `DataVersion(str(rows))`. That is
what makes the arrangement useful rather than decorative: Dagster compares an
observed asset's data version against the version its downstream consumed, so the
moment silver grows, every dbt mart shows as stale in the UI — without Dagster having
any part in writing silver.

### Consequence

Stopping Dagster does not stop the pipeline. It stops the pipeline being watched,
which is the honest description of what an observability layer over a streaming
system is.

Two things follow that are easy to get wrong. `AssetSelection.all()` selects only
*materializable* assets, so it silently excludes all five of these — which is why
`make dagster-observe` names a job rather than selecting everything, and why the
Makefile target that used `--select '*'` was deleted rather than fixed. And an
observation is not a materialisation, so freshness cannot be asserted with
`build_last_update_freshness_checks`: that measures the age of the last observation
event, which stays fresh on a dead pipeline as long as the observation keeps running.
`silver_edits_are_fresh` measures `max(event_time)` against the wall clock instead.

---

## ADR-0031 — Each engine maintains the tables it writes

**Date:** 2026-09-17 · **Status:** accepted

### Context

Iceberg tables need compaction and snapshot expiry. This project has two writers:
Spark writes bronze and silver from the streaming jobs, dbt-through-Trino writes the
gold marts. `scripts/maintain_tables.py` already handles bronze and silver through
Spark's `rewrite_data_files` and `expire_snapshots` procedures, run by `make maintain`.

The gold tables were unmaintained, and they are the ones that need it most often: the
two incremental marts rewrite rows on every `dbt build`, so a fifteen-minute schedule
produces a steady stream of small delete files and snapshots.

The question is which process does it. Dagster is the obvious owner of a scheduled
maintenance job, and the Dagster container has no Spark in it.

### Options

| Option | Rejected because |
|---|---|
| Mount the Docker socket into the Dagster container and `docker exec` into Spark | Hands the container that launches arbitrary runs full control of the daemon, which is root on the host. It is also unvalidatable in CI, so the one part of the stack with a privilege-escalation shape would be the part no test covers. |
| Add Spark to the Dagster image and compact from there | A second 1.5 GB image and a second Spark driver competing for the same 10 GB, in order to write tables that were written through Trino. |
| Run the compaction inside a Spark session that is also running the silver stream | Two writers on the same table, one of them rewriting files the other is committing against. Iceberg resolves that with a commit conflict and a retry, which is correct and is still a fight this project can simply not have. |
| Compact the gold tables through Trino, from Dagster | Chosen. |

### Decision

`gold_table_maintenance` is a Dagster asset that runs
`ALTER TABLE ... EXECUTE optimize` and
`ALTER TABLE ... EXECUTE expire_snapshots(retention_threshold => '7d')` through the
Trino resource, once a day at 03:00 UTC, for each dbt model materialised as a table.
The table list is read from the dbt manifest, not written down, so a new mart is
maintained without a Python change.

Bronze and silver stay with Spark and `make maintain`. Each engine maintains what it
writes, and neither reaches into the other's tables.

### Consequence

The retention threshold is 7 days and not less, because Trino refuses any value below
its `iceberg.expire-snapshots.min-retention` default of 7 days rather than clamping
it. Spark's procedure has no such floor, which is why `make maintain` can demonstrate
expiry immediately with `MAINTAIN_ARGS="--snapshot-age-hours 0"` and this job cannot.

The job is on its own schedule and its own cost class, one run at a time, because
rewriting data files through a 2 GB coordinator while `build_marts` is running is the
one way this stack can make Trino thrash.

---

## ADR-0032 — Attach multi-parent singular tests with `meta.dagster.ref`

**Date:** 2026-09-17 · **Status:** accepted

### Context

`enable_asset_checks` turns every dbt test into a Dagster asset check, and for
generic tests it is exact: the test knows the node it is attached to. For a singular
test — a `.sql` file that must return no rows — dagster-dbt has to guess, and
`get_asset_check_key_for_test` in `dagster_dbt/asset_utils.py` guesses by counting
upstream refs: exactly one parent means the check goes on that parent, and more than
one means it goes nowhere.

Two of the four singular tests here have two parents, because that is what makes them
worth writing. `assert_minute_mart_never_overcounts` compares the mart against
`stg_edits`; `assert_dim_wikis_primary_domain_was_observed` recounts the dimension's
domains straight off the staging view. A test that reads only the model it is testing
can only restate the model.

Nothing fails when the guess comes up empty. Both tests still run under `dbt build`
and still fail the build when they should. They are simply attached to no asset, so
the UI shows 59 checks where the project has 61, and the two most interesting
assertions are the missing ones.

### Options

| Option | Rejected because |
|---|---|
| Rewrite both tests to read one model each | Deletes the independent yardstick, which is the entire assertion. A mart compared against itself is a tautology. |
| Reimplement them as hand-written Dagster checks in Python | Two copies of the same SQL, in two languages, to be kept in step by nobody. |
| Accept the gap and note the count | The count is what a reviewer reads. "every dbt test runs, two of them attached to nothing" is a footnote nobody will find at the moment it matters. |
| Name the asserted model with `meta.dagster.ref` | Chosen. |

### Decision

Each of the two tests carries a config block naming the model the assertion is
*about*:

```sql
{{ config(meta={'dagster': {'ref': {'name': 'mart_edits_per_minute'}}}) }}
```

The other `ref` is the yardstick it is measured against, and stays unnamed. Every dbt
test in the manifest now maps to an asset check, which
`test_every_dbt_test_becomes_an_asset_check` in `tests/unit/test_dagster_lineage.py`
asserts by name rather than by count.

### Consequence

The hint is one line in a file a reader is already looking at, and each of the two
carries a comment saying why it is there.

The failure it replaces has a sibling that is worth naming, because it was measured
rather than assumed: change the hint to name a model that does not exist — a typo, or
a model since renamed — and dagster-dbt drops the check entirely rather than
attaching it to a missing key. `dagster definitions validate` still reports success.
So neither Dagster nor dbt will tell anyone, and the test above is the only thing that
notices; that is why the mapping is asserted in the suite rather than described in a
comment.

---

## ADR-0033 — SQLite for the Dagster instance, and no separate code server

**Date:** 2026-09-17 · **Status:** accepted

### Context

A Dagster deployment has three moving parts to choose: where run and event storage
lives, how runs are launched, and how the code location is served. Dagster's
documented production answer is Postgres storage, a container per code location
served over gRPC, and a run launcher that starts a container per run.

This project's constraint is that `make up` must bring the whole stack up inside
10 minutes on an 11 GB machine that is already running Kafka, MinIO, a Spark driver
and a 2 GB Trino coordinator.

### Options

| Option | Rejected because |
|---|---|
| Postgres for run and event storage | Another stateful container and another JVM-free-but-not-free resident set, to absorb the event stream of three short jobs. It would also make `make up` wait on one more healthcheck before the UI is reachable. |
| A `dagster api grpc` code-location container | The recommended isolation, and its benefit is that a code-location import error cannot affect the webserver. That is worth a container in a deployment where code changes without the image changing. Here the code and the image are one artefact, and a third long-lived Python process is memory this machine does not have. |
| A container-per-run launcher (`DockerRunLauncher`) | Needs the Docker socket mounted into the webserver and the daemon. Rejected for the same reason as in ADR-0031: it makes the process that launches arbitrary runs able to control the host's daemon. |
| SQLite, the default run launcher, code loaded in-process with `-m` | Chosen. |

### Decision

Two containers from one image. Both load the code location in-process with
`-m wikistream_dagster.definitions`, and both mount the same named volume at
`$DAGSTER_HOME`, so they share one SQLite database and one `dagster.yaml`. Runs are
launched as subprocesses by the default run launcher, and the run queue is capped at
one concurrent run — the cap is about Trino's heap, not about Dagster.

### Consequence

The whole control plane costs two Python processes and one SQLite file, and adds no
container that `make up` has to wait on beyond the two it starts.

What is given up is real and belongs in the README's limitations rather than being
glossed: an import error in `definitions.py` shows up as a failed code location in
the UI instead of being isolated in another container, SQLite means one writer at a
time and would not survive a second daemon, and a run executes beside the process that
launched it — so a run launched from the UI runs in the webserver container and a
scheduled run runs in the daemon container. On one machine that distinction has no
consequence. On several it would be the first thing to change, and the change is
`dagster.yaml` plus one Compose service, not a rewrite.

---

## ADR-0034 — Scan the Terraform with Trivy, not tfsec or Checkov

**Date:** 2026-09-18 · **Status:** accepted

### Context

`infra/aws/` is never applied, so `terraform validate` is the only correctness check
it can get from Terraform itself — and validate checks the provider's schema, which
catches a misspelled attribute and nothing about whether the configuration is safe.
A static analyser is what turns the module from a plausible-looking artefact into one
whose claims about least privilege and encryption are checked by something other than
the person who wrote them.

### Options

| Option | Rejected because |
|---|---|
| tfsec | The obvious choice, and archived. Its last release was 2025-05-02 and its own README points users at Trivy, into which Aqua merged the rule set. Adopting a tool that has stopped shipping means the rule set stops learning about new services, which for infrastructure scanning is the whole value. |
| Checkov | Actively developed, more rules than Trivy, and a Python package — which is the problem. It is ~100 transitive dependencies into `uv.lock`, in a repository where the lock file is otherwise the pipeline's own dependency graph. A reader running `uv tree` should not have to work out why a data project depends on `boto3`, `cyclonedx-python-lib` and `pycep-parser`. |
| Trivy | Chosen. |

### Decision

`make infra-scan` runs `trivy config --quiet --exit-code 1 --disable-telemetry
infra/aws`. A single Go binary, no Python dependencies, and the same rule identifiers
(`AVD-AWS-####`) the tfsec documentation uses, so a suppression comment written
against tfsec's docs still means what it says.

`--exit-code 1` is the part that matters: the scan fails the build on any finding at
any severity, so the only way to pass with a finding is an inline suppression that
states its reason on the same line. There are two, both in `modules/s3`, both listed
in `infra/aws/README.md`.

### Consequence

One more non-Python binary a contributor needs, alongside `terraform`,
`kubeconform` and `kustomize`. The Makefile target prints the release URL rather
than failing with "command not found", and CI installs it from the vendor's action.

Two consequences that were surprises and are worth recording, because both changed
the code rather than the tooling:

- **Trivy cannot resolve `for_each`.** The first version of `modules/s3` created the
  public access blocks, encryption and versioning with one resource each looped over
  a map of bucket ids. Trivy reported both buckets as having none of the three,
  because it cannot follow `bucket = each.value` back to the resource it names. The
  controls are written out per bucket now: six extra lines, and a reviewer can see
  each control next to the bucket it protects. A control a static analyser cannot see
  is a control someone has to verify by hand.
- **A suppression attaches to the resource the rule fires on, not the resource the
  rule is about.** `AVD-AWS-0132` is "use a customer-managed KMS key", and the
  comment has to sit above `aws_s3_bucket_server_side_encryption_configuration`,
  not above the `aws_s3_bucket` it configures. Put it on the bucket and the scan
  still fails, which reads like a broken suppression syntax.

---

## ADR-0035 — The AWS module consumes a VPC; it does not create one

**Date:** 2026-09-18 · **Status:** accepted

### Context

MSK Serverless and EMR Serverless both require private subnets. A module that
provisions them therefore needs a network, and the default instinct is to create one
— a VPC, subnets across three availability zones, route tables, and the VPC
endpoints that let workers reach S3, Glue and STS without a NAT gateway.

### Options

| Option | Rejected because |
|---|---|
| Create the VPC, subnets, route tables and endpoints | It doubles the module and it is the part of the module that would be wrong in every real account. Networks are shared, they have an owner who is not this module, and a second VPC in an account is usually a mistake someone has to unpick. It would also mean the module's blast radius includes the network, which is the one resource whose deletion takes everything else with it. |
| Create the VPC endpoints only, taking the VPC as input | Closer, and still wrong in the same direction: an interface endpoint is an account-level shared resource priced per hour per AZ, and creating a second Glue endpoint next to an existing one is a waste that is invisible until the bill. |
| Take `vpc_id` and `private_subnet_ids` as required inputs | Chosen. |

### Decision

`vpc_id` and `private_subnet_ids` are required variables with no defaults;
`private_subnet_ids` validates that at least two were given, because a single-AZ
deployment of a service billed by cluster-hour is a decision nobody makes on
purpose. `modules/network` creates two security groups and nothing else.

The endpoints are documented as prerequisites in `infra/aws/README.md` rather than
created, and the egress rules assume them: there is no `0.0.0.0/0` rule anywhere in
the module, so a worker's only routes are to MSK inside the VPC, to S3 through the
gateway endpoint's managed prefix list, and to the VPC's own CIDR on 443 where the
interface endpoints live.

### Consequence

The module cannot be applied into an empty account, which is honest — it is a
component, not a landing zone. The failure mode when a prerequisite is missing is
poor and worth knowing in advance: a Spark job with no route to STS hangs on
`AssumeRole` and eventually times out, with nothing in the logs that mentions the
network. `infra/aws/README.md` says so under "What it deliberately does not create".

The gain is that the interesting parts of the module — the IAM policies, the S3
lifecycle rules, the security group egress — are all things this pipeline actually
needs, rather than 200 lines of network boilerplate that any account already has.

---

## ADR-0036 — Commit `.terraform.lock.hcl`

**Date:** 2026-09-18 · **Status:** accepted

### Context

The first version of `.gitignore` in this repository ignored `.terraform.lock.hcl`
along with `.terraform/`, `*.tfstate` and `*.tfplan`. Three of those four are
correct. The lock file is not state and not a plan: it is the provider dependency
lock, recording each provider's version and the checksums of every platform's
package.

### Options

| Option | Rejected because |
|---|---|
| Keep ignoring it | `terraform init` then resolves `~> 6.65` freshly on every machine, so CI and a contributor can validate against different provider builds, and a provider release between two runs changes the result of `terraform validate` with no commit to explain it. It also gives up the supply-chain property the file exists for: init verifies the downloaded package against a committed hash. |
| Commit it with only the CI platform's hashes | What `terraform init` writes by default — hashes for the platform that ran it, and nothing else. A contributor on an Apple Silicon laptop then gets a checksum error, and the documented fix is `terraform providers lock`, which changes a committed file as a side effect of doing nothing. |
| Commit it with hashes for the platforms anyone will use | Chosen. |

### Decision

`.terraform.lock.hcl` is committed, and generated with

```
terraform providers lock \
  -platform=linux_amd64 -platform=linux_arm64 -platform=darwin_arm64
```

so CI, a WSL laptop and an Apple Silicon laptop all verify against the same file.
`.gitignore` carries a comment saying the omission is deliberate, because the next
person to tidy that section will otherwise re-add it.

### Consequence

Provider upgrades become explicit: `terraform init -upgrade` changes a committed
file, which shows up in review as a version bump rather than happening silently
between two CI runs. The cost is remembering the `-platform` flags when the provider
is upgraded, which is one line in `infra/aws/README.md`.

---

## ADR-0037 — `k8s/` deploys the control plane only, with the dbt project in the image

**Date:** 2026-09-18 · **Status:** accepted

### Context

Two of the three job adverts this repository is written against name Kubernetes and
GitOps, so `k8s/` exists. The question is what it should contain. The pipeline has six
components in `docker-compose.yml` — Kafka, MinIO, an Iceberg REST catalog, Spark,
Trino and Dagster — and charts or operators exist for all of them.

### Options

| Option | Rejected because |
|---|---|
| Port the whole stack: Strimzi for Kafka, the Spark operator, a MinIO tenant, the Trino chart, Dagster | Two deployments of the same pipeline, one of which is exercised by every test in this repository and one of which is exercised by nothing. The second would drift from the first, and a reader checking any claim about it would find the drift. It is also several thousand lines to review. |
| A single Helm chart instead of kustomize overlays | Helm is the more common answer and it is the wrong shape for the point being made. The interesting content here is the difference between a laptop deployment and a cluster deployment; two kustomize overlays *are* that difference, readable as a diff, whereas in a chart it is scattered across `values-*.yaml` and `{{ if }}` blocks. |
| Only Dagster, as a kustomize base with `local` and `prod` overlays | Chosen. |

### Decision

`k8s/` deploys the Dagster webserver and daemon, and nothing else. The addresses of
everything they talk to come from a ConfigMap. `k8s/README.md` states in its first
paragraph that the manifests have never been applied and lists what would have to
happen before they could be trusted.

The dbt project is baked into the image by `docker/dagster-k8s.Dockerfile` rather
than mounted. Under Compose it is a read-only bind mount of the working tree, which
is deliberate — the entrypoint parses the project at start-up, so editing a model
changes the SQL dbt runs and the graph Dagster displays without a rebuild. A cluster
has no working tree. The rejected alternatives were an init container cloning this
repository, which makes every pod start depend on GitHub and needs egress the pod
otherwise does not have, and a ConfigMap, which cannot hold `models/staging/` at all
because a ConfigMap key cannot contain a slash.

### Consequence

`k8s/` is about 400 lines including comments, and every line of it is a decision
rather than boilerplate. The manifests are checked in CI by `kustomize build` and
`kubeconform -strict`, which prove they are well-formed Kubernetes and prove nothing
about whether the pods start — stated as the first item on the README's list.

Baking the project in means the image is a single artefact whose models and whose
asset graph cannot disagree, and that a rollback rolls back both. It also means a
model change requires an image build for the cluster, which is the normal cost of
immutability and is why the Compose path keeps the bind mount.

---

## ADR-0038 — Postgres for the Dagster instance on Kubernetes, reversing ADR-0033 there

**Date:** 2026-09-18 · **Status:** accepted

### Context

ADR-0033 chose SQLite for the Dagster run and event storage under Compose, and named
the condition that would reverse it: more than one process needing the run store from
more than one filesystem. Kubernetes is that condition. The webserver and the daemon
are separate pods and must share the store — the daemon writes a run, the launcher
executes it, the webserver reads its events back — and two pods cannot share a file.

### Options

| Option | Rejected because |
|---|---|
| Keep SQLite on a `ReadWriteMany` volume | It would appear to work. SQLite's locking depends on filesystem locks that NFS and most RWX CSI drivers implement incompletely, so the failure is a corrupted database days later rather than an error on the first write. This is the option worth naming explicitly because it is the one someone porting the Compose deployment would reach for. |
| One pod running both processes with a shared `emptyDir` | Honest, and it keeps SQLite. It also merges two lifecycles that should be separate: the daemon must be a singleton and the webserver benefits from replicas, and this option makes replicating the UI impossible. |
| Postgres, run in the cluster for `local` and managed for `prod` | Chosen. |

### Decision

`k8s/base/dagster.yaml` configures `storage.postgres`, with the host, database and
username from the `wikistream-endpoints` ConfigMap and the password from a Secret this
repository does not contain. `overlays/local` runs a single-replica Postgres
StatefulSet; `overlays/prod` ships no database at all and expects a managed one.

The two `dagster.yaml` files therefore differ in exactly one substantive stanza, and
both carry a comment saying why the same argument reaches opposite conclusions on the
two platforms.

### Consequence

Under Kubernetes the deployment gains a database to operate and `overlays/prod` gains
two webserver replicas, which is what Postgres buys: either replica can serve any
run's event log because neither owns it. The daemon stays at one replica with
`strategy: Recreate`, because a rolling update would briefly run two schedulers
against the same tick table.

The local Postgres pins `PGDATA=/var/lib/postgresql/18/docker` explicitly and mounts
its volume one level up at `/var/lib/postgresql`. Postgres 18's official image moved
both, and a manifest that mounts a PVC at the old `/var/lib/postgresql/data`
initialises the database onto the container filesystem instead: it starts, it passes
its readiness probe, and it is empty after the first restart.

## ADR-0039 — Give the SQLite catalog one JDBC connection instead of two

**Date:** 2026-09-18 · **Status:** accepted

### Context

The Iceberg REST catalog keeps its table pointers in SQLite — a file on a volume
rather than the fixture image's in-memory default, so that tables survive a restart,
which is the property this project exists to demonstrate. The first time
`make dbt-run` ran against a stack with data in it, dbt failed:

```
Database Error in model stg_edits
  TrinoExternalError(name=ICEBERG_CATALOG_ERROR, message="Failed to create view 'stg_edits'")
```

Trino reports a 500 from the catalog and nothing else, so the cause is only visible in
the catalog's own log:

```
org.apache.iceberg.jdbc.UncheckedSQLException: Unknown failure
  at org.apache.iceberg.jdbc.JdbcViewOperations.doCommit(JdbcViewOperations.java:136)
Caused by: org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked
```

`dbt/profiles.yml` sets `threads: 4`, so two models commit at the same time. SQLite
allows one writer, which is not by itself the problem — the problem is that it does
not always *queue* the second one. When a connection has already read inside its
transaction and then tries to write, SQLite returns `SQLITE_BUSY` immediately without
consulting the busy handler, because blocking there could deadlock with the
connection it is waiting for. A busy timeout has no effect on that path. Iceberg's
`JdbcClientPool` opens two connections by default, both writing the same file, which
is exactly that path.

### Options

| Option | Rejected because |
|---|---|
| `dbt/profiles.yml threads: 1` | It narrows the race without closing it. dbt is not the only writer: both Spark streams commit every micro-batch and would still collide with dbt, so the same error would come back as an occasional failed mart rather than a reproducible one. Intermittent is worse. |
| Postgres as the catalog backend | The real answer for a real deployment, and what ADR-0038 does for Dagster on Kubernetes. Here it buys a container and about 200 MB of resident set on an 11 GB laptop to serialise a few writes a second, and the thing it would fix is fixed for nothing below. |
| `transaction_mode=IMMEDIATE` on the JDBC URL | Tried, measured, did not work. It makes the driver open transactions with `BEGIN IMMEDIATE`, which would take the write lock up front and turn the upgrade into a wait — but the Iceberg calls that fail run in autocommit, so there is no `BEGIN` for the setting to apply to. The error changed from `SQLITE_BUSY` to `SQLITE_BUSY_SNAPSHOT` and the models kept failing. Removed again. |
| `clients: 1` on the catalog, plus WAL and a busy timeout | Chosen. |

### Decision

Three settings on the `iceberg-rest` service in `docker-compose.yml`:

- `CATALOG_CLIENTS: "1"` — one JDBC connection for the whole catalog. This is the one
  that fixed it. Every writer in the deployment reaches the catalog through this one
  process, so its connection pool is the only place their commits can be ordered, and
  a pool of one orders them: the second caller waits on a Java monitor, which has a
  queue, instead of on a file lock, which returns an error.
- `journal_mode=WAL` and `busy_timeout=30000` in the JDBC URL. WAL alone was enough to
  get the two views created and not enough for the five marts, which is how the
  sequence above was measured. They stay because they are the right settings for a
  file written to continuously — readers no longer block the writer, and the timeout
  applies on the paths where the busy handler *is* consulted.

`CATALOG_JDBC_SCHEMA__VERSION: V1` was already set and is unrelated, but it is the
other thing that has to be right for views to work at all: under V1 a view is a row
in `iceberg_tables` with `iceberg_type = 'VIEW'`, and without the setting that column
does not exist and the first `CREATE VIEW` fails.

### Consequence

Verified after the change: `make dbt-run` green on six consecutive runs, two of them
with the bronze and silver streams both committing throughout, and zero `SQLITE_BUSY`
lines in the catalog log across all six. `make dbt-test` passes 62 tests.

Every catalog operation in the stack now serialises through one connection. At this
scale that is invisible — a commit is a single-row insert and Trino's metadata reads
are sub-millisecond — but it is a real ceiling, and the shape of it is worth being
clear about: it is a ceiling on catalog *operations*, not on data volume, because no
row of the lakehouse passes through SQLite.

The residual risk is a hang rather than an error. `ClientPoolImpl.run` blocks when the
pool is empty, so an Iceberg code path that borrowed a second connection while holding
the first would wait for itself. None of the paths this project exercises does, across
the runs above, but a pool of one is the configuration where that bug would appear as
a stuck query rather than as contention. Moving to Postgres is the fix if it ever
does, and the connection string is the only thing that would change.

---

## ADR-0040 — Kafka over Redpanda, and KRaft because there is no longer a choice

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

The queue is one of the four skills this repository exists to demonstrate, and it is
the one with the clearest market signal: Kafka is named in 32.3% of the job adverts
this project was designed against, and no alternative broker is named in any of them.
That is a reason to think hard about it, not a reason on its own — a broker chosen for
a keyword would show. The technical question is whether a laptop-scale pipeline should
run a JVM broker.

### Options

| Option | Rejected because |
|---|---|
| Apache Kafka, `apache/kafka:4.1.2` | Chosen. |
| Redpanda | Wire-compatible, one static binary, no JVM, and it would have cost less memory. Rejected on what it would remove from the demonstration rather than on merit: the operational surface I want to be able to discuss in an interview — consumer group rebalances, `kafka-consumer-groups.sh` output, log segments, retention — is Kafka's own, and protocol compatibility is not the same as running the thing. Kafka's measured peak here is 761 MiB (`make measure-resources`), which an 11 GB box can afford. |
| Kinesis or Pub/Sub | Both cost money, which hard rule 1 forbids, and Pub/Sub has no offsets, which would delete the completeness check described in `docs/architecture.md`. |
| No queue — SSE straight into Spark | Spark has no SSE source, so it would mean a custom receiver. More importantly it removes the replayable buffer, and without a buffer the restart proof has nothing to replay from: `make test-e2e` works because killing the writer loses nothing that Kafka still holds. |

### Decision

One Kafka broker in KRaft mode, three partitions, 24-hour retention, zstd compression
(ADR-0014), keyed by wiki domain (ADR-0008).

**KRaft is not a decision, and that is the interesting part.** Kafka 4.0 completed
KIP-500 and removed ZooKeeper entirely, so `apache/kafka:4.1.2` has no ZooKeeper mode
to choose. It is recorded here because most Kafka-on-Docker material still shows a
two-container compose file with a `zookeeper` service, and a reviewer who has seen that
shape may read its absence as an omission. It is the opposite: the single-container
broker is what the current release supports.

### Consequence

Every Kafka command in the `Makefile` — `kafka-offsets`, `kafka-lag`, `kafka-tail`,
`create-topics` — is a call to Kafka's own shell scripts rather than to a wrapper, so
the commands a reviewer runs locally are the commands that would work against MSK. The
cost is a JVM: 761 MiB resident at peak, second only to Spark and Trino, and the reason
`make up-core` exists at all is that this container plus Spark plus MinIO is the
smallest set that can still land an Iceberg table.

---

## ADR-0041 — Iceberg over Delta Lake and Hudi

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

The table format is the layer that makes this a lakehouse rather than a directory of
Parquet, and all three candidates provide the thing that matters: atomic commits over
object storage, so a reader never sees half a write. Choosing between them is
therefore not a question about ACID. It is a question about which properties this
particular pipeline needs to *demonstrate*.

### Options

| Option | Rejected because |
|---|---|
| Apache Iceberg 1.10.1 | Chosen. |
| Delta Lake | The format I would expect at a Databricks shop, and Databricks appears in 16.1% of the advert sample, so this was a real candidate. Rejected on the reader story: this repository's central interoperability claim is that three engines read the same bytes, and `make query` (Trino) plus `make query-duckdb` (DuckDB, no JVM, no configuration) is how it is checked. Delta outside Spark is possible and is a narrower path. |
| Apache Hudi | The strongest of the three at upsert-heavy CDC, with record-level indexes this workload would not use. Rejected because the write path is a configuration exercise — table type, index type, compaction mode — and because its reader ecosystem is the narrowest of the three. If the source were a database's change log rather than an event stream, this row would read differently. |
| Plain Parquet with Hive-style partition directories | No atomic commit, so no `MERGE`, so no deduplication guarantee and no restart proof. This is what the project would be without a table format, and naming it is the clearest statement of what the format buys. |

### Decision

Iceberg, on four specific properties rather than on general preference:

1. **Snapshot metadata is queryable SQL.** `.snapshots`, `.files`, `.manifests` and
   `.history` are tables. Every file-count, size and compaction figure in
   `docs/lakehouse.md` is a `SELECT`, which is what makes the maintenance story
   measurable instead of assertable.
2. **Hidden partitioning.** Bronze partitions by ingest date and silver by event date
   (ADR-0017), and no query in the repository — Spark, Trino, dbt or DuckDB — names a
   partition column to get partition pruning. A Hive-style layout leaks that into every
   `WHERE` clause.
3. **`MERGE INTO` as a first-class statement** on the format's own primitives, which is
   where the deduplication guarantee lives (ADR-0020).
4. **A REST catalog specification with a working reference implementation**, so the
   catalog is a documented HTTP protocol rather than a vendor runtime — and the AWS
   substitution to Glue changes one client property.

### Consequence

The bill for this choice is visible in the same documents that praise it. Small files:
1,177 files for 214 MiB on bronze, because a 30-second trigger commits four files per
partition every time. That needs a maintenance job (`make maintain`), and the
compaction it performs is measured — 73 files to 1, 92 to 1 — in `docs/lakehouse.md`.
Every query plan reads manifests before it reads data, which is why the lifecycle rule
in `docs/cost.md` deliberately leaves the `metadata/` prefix in the expensive storage
class. A format that commits atomically commits *something* atomically, and that
something is a file per partition per batch.

---

## ADR-0042 — Spark Structured Streaming over Flink

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

Flink is the better stream processor on the merits: genuine per-record processing
rather than micro-batch, a lower latency floor, richer and more explicit state
handling, and event-time semantics that were designed in rather than added. A
streaming portfolio project that picks Spark should be able to say why, because "I
already know PySpark" is a reason about the author and not about the system.

### Options

| Option | Rejected because |
|---|---|
| Spark Structured Streaming 4.0.4 | Chosen. |
| Apache Flink | Its advantage is latency the workload does not need, and its Iceberg integration would move the correctness argument somewhere harder to show. Detail below. |
| Kafka Streams or ksqlDB | No Iceberg sink. The lakehouse is the point. |
| A Python consumer writing Parquet with PyIceberg | Genuinely lighter, and it would remove 2.8 GB of resident set. Rejected because it removes the engine that makes the guarantee: no `MERGE`, no checkpointed offsets, no exactly-once commit protocol — those would all have to be hand-rolled, and hand-rolled exactly-once is how pipelines lose data quietly. |

Two specifics on the Flink row, since it is the row a reviewer would push on.

**The latency argument does not favour Flink here.** Measured end-to-end p99 is 30
seconds, and that number is the trigger interval I chose, not a limit the engine
imposed. The 30-second trigger is a file-count decision — every trigger is an Iceberg
commit, and every commit writes files and metadata — so the binding constraint on
freshness is the table format, which Flink shares. Where Flink's sub-second floor
would matter, this pipeline would still be committing to Iceberg every few seconds and
drowning in small files.

**The deduplication pattern has no clean Flink equivalent.** Silver deduplicates with
`MERGE INTO … WHEN NOT MATCHED THEN INSERT` inside `foreachBatch` (ADR-0020): the
guarantee is a SQL statement against the table's current state, which is why it holds
across restarts, across a rebuild, and across a mid-batch kill. The Flink version keeps
keys in RocksDB state and relies on the sink's equality deletes, which moves the
argument from something a reader can execute into something they have to trust me about
the state backend.

### Decision

Spark, with one engine doing streaming, maintenance and ad-hoc SQL — which is also
2.8 GB of resident set once rather than twice.

### Consequence

Micro-batch quantises latency to the trigger, and the measured distribution shows it
exactly: p50 of 15.3 s against a 30 s trigger is what a uniform arrival rate inside a
fixed window looks like. Both facts a reviewer might hold against the choice — the
quantised latency and the 3.99 files per commit — are consequences of the same
parameter, and both are measured rather than described.

---

## ADR-0043 — Dagster over Airflow, deliberately and against the market

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

I use Airflow daily and have for years. In the advert sample this project was designed
against, Airflow appears in 41.9% and Dagster in 16.1%. Choosing Airflow would have
matched more adverts and taken less time. I chose Dagster anyway, and the reasoning
matters more than the outcome because it is the one place where this repository
deliberately does not optimise for keyword coverage.

### Options

| Option | Rejected because |
|---|---|
| Dagster | Chosen. |
| Airflow | Rejected for this repository, not on merit. It demonstrates something my CV already claims, so building with it would have added nothing a reader could not already infer. And the model fits badly: Airflow schedules tasks, and the three things at the centre of this pipeline are not scheduled. Expressing "this table is being continuously written by a process I do not start" needs a sensor pretending to be a dependency. |
| Prefect | Closer to Dagster's model than to Airflow's, and a smaller answer to the same question. It does not appear in the advert sample; Dagster appears in 16.1% of it. |
| Temporal | Named in some of the adverts I am aiming at, and the wrong tool: a durable workflow engine for application logic, with no asset graph, no data-quality checks and no dbt integration. |
| No orchestrator | The streams run themselves, so this is tempting. But then nothing owns the marts, nothing owns the health checks, and "is the pipeline healthy?" has no answer that is not a human running a query. |

### Decision

Dagster, using the parts of its model that Airflow has no equivalent for:

- **External assets.** The producer and the two Spark queries are real nodes with real
  edges that Dagster does not execute (ADR-0030). That is an honest picture of the
  system rather than a scheduling fiction.
- **Asset checks as artefacts.** `silver_edits_are_fresh` and
  `bronze_offsets_are_contiguous` are objects in the graph with their own history, not
  tasks that raise. A reviewer clicking an asset sees whether its checks have been
  passing.
- **The dbt integration.** 7 models and 62 tests map into the same graph as the
  streaming tables, so one lineage view spans Kafka to marts.

### Consequence

The repository shows both halves without pretending: Airflow is on the CV, Dagster is
in the code, and the README says which is which. The cost is 745 MiB across two
containers (webserver 195, daemon 550) and one design constraint that keeps surprising
me as the right call — the schedules ship *stopped*, because a laptop that wakes up at
03:00 to compact a table is a laptop whose owner turns the project off.

---

## ADR-0044 — Trino as the engine dbt compiles against

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

The marts have to be built by something. Spark is already running and can execute SQL,
so adding a second query engine needs a justification beyond variety — it is the
second-largest memory consumer in the stack at 1,668 MiB peak.

### Options

| Option | Rejected because |
|---|---|
| Trino 483, via `dbt-trino` | Chosen. |
| Spark SQL via `dbt-spark` | One engine for everything, no extra container, and a Thrift server to run and keep alive. Rejected because it would make the interoperability claim untestable: if Spark writes and Spark reads, "the storage is open" is an assertion. It also couples mart builds to the machine running the streams. |
| DuckDB via `dbt-duckdb` | The lightest option by far. Rejected as the primary path because the marts must be *written* to Iceberg for Dagster to maintain them and for the checks to read them, and the DuckDB Iceberg extension in this stack is a reader — `scripts/query_duckdb.py` says so in its own docstring. Kept as the third reader instead, which is a better use of it. |
| ClickHouse | Named in some of the same adverts, and a different storage model: fast because the data is in its own format, which would mean copying out of the lakehouse and giving up the single-copy property. |

### Decision

Trino, on three grounds: it reads the Iceberg catalog Spark commits to with no copy and
no sync job; it is named explicitly in the adverts I am aiming at; and Athena *is*
Trino, so the AWS substitution in
`docs/architecture.md` is an adapter swap plus three connection values rather than a
rewrite of seven models.

### Consequence

1,668 MiB of resident set for a container that does nothing until a query arrives,
which is why it sits in the `full` profile and not the default one. `make up-core`
skips it, and `make query-duckdb` reads the same tables there — so the low-memory path
is not a degraded path for a reader who only wants to see the data. dbt's `threads: 4`
against this catalog is also what surfaced the SQLite locking problem in ADR-0039.

---

## ADR-0045 — MinIO over LocalStack for the object store

**Date:** 2026-09-18 · **Status:** accepted · **Recorded retrospectively:** the choice
was made when the stack was assembled on 2026-09-17

### Context

Iceberg needs object storage, and the pipeline must cost nothing, so S3 is out. The
question is which S3 substitute, and the answer shapes more than it looks: object
storage semantics are why the small-file problem exists, why `docs/cost.md` can price
requests rather than only bytes, and why the Spark catalog needs ten properties.

### Options

| Option | Rejected because |
|---|---|
| MinIO | Chosen. |
| LocalStack | Emulates a hundred AWS services when this project needs one, and its S3 is the free-tier part of a product whose value proposition is testing infrastructure code against a fake AWS. That is precisely the line hard rule 2 draws: `infra/aws/` is a design artefact that has never been applied, and pointing it at an emulator would let the repository imply otherwise. Being clear about what has not been run is worth more than a green plan against a simulator. |
| `moto` or `s3mock` | Test doubles. Fine inside a test, not a store to run a lakehouse on for a day and then measure. |
| A local filesystem warehouse (`file:///`) | Simplest of all, and it would delete the interesting part. No request costs, no eventual-consistency reasoning, no path-style access, no `S3FileIO` — and the file-count problem stops being a cost story and becomes an inode story. The measurements in `docs/cost.md` exist because the local store behaves like the remote one. |

### Decision

MinIO from `quay.io/minio/minio`, one node, one bucket, with the container's own
documented default credentials.

The credentials deserve a sentence because a reviewer will grep for them.
`minioadmin` / `minioadmin` is what the image ships with and what its documentation
prints; it is in `.env.example` and in `docker-compose.yml` in plain text on purpose,
because a fake credential that is obviously the vendor default is safer than one that
looks real enough for someone to wonder. There is no secret in this repository to
leak, and CI runs `gitleaks` to keep it that way.

### Consequence

658 MiB resident, a console on port 9001 that a reviewer can click through to see the
Parquet, and four of the ten Spark catalog properties existing only because MinIO is
not S3 — the endpoint override, path-style access and the two static credentials, all
of which `docs/architecture.md` lists as *removed* in its Glue column. That table is
the clearest thing I can show about how much of a local lakehouse is scaffolding.

---

## ADR-0046 — Bound every gold model on one ingest-time cutoff per dbt run

**Date:** 2026-09-18 · **Status:** accepted

### Context

`make dbt-build` failed. Not intermittently in some future CI run — on 2026-09-18 at
01:48, on a stack that had been up for eleven minutes:

```
ERROR: relationships_mart_top_pages_hourly_wiki__wiki__ref_dim_wikis_
Got 3 results, configured to fail if != 0
```

Three rows in `mart_top_pages_hourly` referenced a wiki that `dim_wikis` did not
contain. Both models are derived from the same staging view over the same table, so
one of them looked wrong. Neither was:

```sql
select m.wiki, min(e.ingested_at) as first_ingested, min(e.event_time) as first_event
from (select distinct wiki from gold.mart_top_pages_hourly) m
left join lakehouse.silver.edits e on e.wiki = m.wiki
where m.wiki not in (select wiki from gold.dim_wikis)
group by m.wiki
```

| wiki | first_ingested | first_event |
|---|---|---|
| `bewiktionary` | 2026-09-18 01:48:30 | 2026-09-18 01:48:15 |

The Belarusian Wiktionary emitted its first event of this pipeline's life at 01:48:15
and the silver stream committed it at 01:48:30 — after `dim_wikis` was built and
before `mart_top_pages_hourly` was. Three edits to three pages, so three top-ten rows,
pointing at a wiki the dimension had never seen.

This is the shape of bug that a repository built on a *bounded* dataset never
produces, and it is worth stating plainly because it is the whole reason a streaming
project is a different discipline: **the input changed while the DAG was running.**
Spark commits a snapshot to `silver.edits` every 30 seconds. A full `dbt build` takes
72 seconds here and runs models on four threads. `dim_wikis` and the four marts have
no dependency on each other — they all depend only on `stg_edits` — so dbt is free to
build them concurrently, each against whatever snapshot Trino resolves at its own
`select`. dbt gives a run one transaction per model. Nothing in dbt, Trino or Iceberg
gives a run one *view of the world*.

A test that compares two models is the only thing that notices. That is not a reason
to weaken the test; it is the test doing its job, and the reason to keep it is that
the same divergence is invisible in every dashboard built on those two tables.

### Options

| Option | Rejected because |
|---|---|
| One cutoff per run, `where ingested_at < run_started_at` | Chosen. |
| `severity: warn` on the relationships tests | The fastest fix and the worst one. It converts a real inconsistency between two published tables into a line of yellow text that everybody learns to scroll past, and it would still be there when the inconsistency had a different cause. |
| Iceberg time travel — pin the staging views with `for timestamp as of` | Says the same thing more directly and breaks something else. `stg_edits` and `stg_quarantine` are views, so the pin would be baked into the view definition and an ad-hoc `select * from gold.stg_edits` would return whatever was current at the last dbt run until the next one. The gold layer is allowed to be as-of; a staging view over a live table is not. |
| Build `dim_wikis` first and force the marts to depend on it | Reverses the race rather than removing it: the marts then read a *later* snapshot than the dimension by construction, which is exactly the failing direction. Serialising the graph would also cost the four-thread build for no correctness gain. |
| Derive `dim_wikis` from the union of the marts | Makes the dimension a summary of its own consumers, so a wiki that appears in no mart's top ten drops out of the dimension entirely. That is a worse table for the sake of a green test. |
| Accept it and retry the failing run | It fails whenever a new wiki, or a new domain for a known wiki, first appears mid-run. On this source that is often enough to be noise and rare enough to look random, which is the worst frequency a flaky test can have. |

### Decision

`dbt/macros/run_cutoff.sql` renders `run_started_at` — one value per dbt invocation,
identical in every model and every test of that run — as a zoned Trino literal. Every
model that reads a staging view adds `and <alias>.ingested_at < {{ run_cutoff() }}`;
`mart_pipeline_health` adds it twice, because it reads both staging views and the
quarantine's ingest clock is called `failed_at`. Snapshot isolation expressed as a
predicate, which is what a lakehouse offers in place of a transaction spanning five
tables.

Three details carry the correctness:

1. **The bound is on ingest time, not event time.** A late event has an old
   `event_time` and a new `ingested_at`. Bounding on event time would still admit a
   row that arrived mid-run into a model built after one that had already read past
   it, so the set would not be closed — and lateness is the one property of this
   source the whole pipeline is built around.
2. **Nothing is dropped.** Every incremental mart recomputes its boundary bucket with
   `>=` rather than `>` (ADR-0028), so rows excluded by one run's cutoff are picked up
   by the next. The cost is at most one run of latency on the newest bucket, not data.
3. **`dim_wikis` publishes the cutoff it was built to**, as a `built_through` column.
   `dbt test` on its own is a *separate invocation* with a later `run_started_at`, so
   the singular test that recomputes the dimension from `stg_edits` reads its bound
   off the row under test rather than from its own run. Without that, the same race
   reappears one level up and only on the runs where `dbt test` is called alone —
   which is how it would have reached CI.

`tests/unit/test_dbt_run_cutoff.py` is a static read of the model SQL: five models
must call the macro, six predicates must compare against an ingest-time column, and
the singular test must not use `run_cutoff()`. Static because the failure it guards is
not reproducible on demand — it needs a wiki to be born at the right second — and a
test that can only fail by luck protects nothing. `.sqlfluff` restates the macro the
way it already restates `epoch_utc`, with an arbitrary value and the same shape.

### Consequence

`make dbt-build` is green twice in a row against a live stream, and `make dbt-test`
alone is green, which is the invocation that would have flaked. The gold layer is now
explicitly *as of* a timestamp rather than implicitly as of whenever each model
happened to run, and `dim_wikis.built_through` publishes that timestamp so a consumer
can tell staleness from absence.

The honest cost: the newest bucket in every mart lags the newest row in silver by up
to one run. `mart_pipeline_health` gained something from that — two runs minutes apart
now agree on the percentiles for the current hour, where before they disagreed and
neither was wrong — and `dbt source freshness` still measures silver, not gold, so the
freshness signal is unaffected.

What this does not fix: two *separate* dbt invocations still see different cutoffs, so
a mart built at 01:48 and another built at 01:52 do not agree. Nothing here is a
distributed transaction. The claim is narrower and worth stating exactly: within one
dbt run, every model reads the same set of events.

---

## ADR-0047 — Check Markdown links with a script in this repository, not with lychee

**Date:** 2026-09-18 · **Status:** accepted

### Context

This repository is judged largely on its prose, and the prose is heavily
cross-linked: the README points at 19 of the 47 architecture decision records by
heading anchor, and the seven pages under `docs/` reference each other and the ADRs. A link
to a heading that has since been reworded still renders as a link. It sends the
reader to the top of the page instead of to the section they were promised, and
nothing in `make lint`, `make test` or CI notices.

That failure mode is specific and it is the one that matters here: the reviewer this
repository is written for clicks an ADR link out of the README, lands nowhere in
particular, and concludes that the document is decoration. So the anchors need to be
checked, not just the file paths.

### Options

| Option | Rejected because |
|---|---|
| A script in `scripts/`, with unit tests | Chosen. |
| [lychee](https://github.com/lycheeverse/lychee) | The obvious answer, and the one `BUILD_PHASES.md` suggested. It is a Rust binary, so it means a `cargo`-built tool or a GitHub Action in CI and a separate install for a contributor running `make lint` locally — and the checks in this repository are deliberately the same command in both places. It also resolves external URLs by default, which makes the lint fail when somebody else's site is down. |
| A marketplace action (`markdown-link-check` and similar) | Runs in CI only. A check a contributor cannot run before pushing is a check they discover by breaking the build. |
| `github-slugger` through Node | The correct slug algorithm, from the package GitHub's own tooling uses — at the cost of a `package.json` and a Node toolchain in a repository that otherwise has one language. |
| Check file paths only, skip anchors | Half the value. Every broken link found while writing this was an anchor; the paths were fine, because a missing file is obvious the first time you click it. |

### Decision

`scripts/check_doc_links.py`, wired into `make lint` (so CI runs it without a new
job), into pre-commit, and tested by `tests/unit/test_doc_links.py`.

It reimplements GitHub's slug algorithm — drop everything that is not a letter,
digit, `_` or `-`, lowercase, turn spaces into hyphens, then suffix repeats with
`-1`, `-2` — and the test cases are real headings from this repository rather than
invented ones, because the failure mode of a hand-written slugifier is that it agrees
with GitHub on `## Simple Heading` and disagrees on exactly the punctuation in use
here. The em dash in every ADR heading is why the anchors carry a double hyphen; the
backticks around every identifier vanish; `Spark's` becomes `sparks`. Three separate
attempts at this got it wrong before there was a test.

Two details are there because the repository needs them rather than because a general
tool would have them. Fenced blocks are blanked before parsing, keeping the line count
intact, because the README quotes console output and a mermaid diagram and both
contain text shaped like a heading or a link. And `../blob/main/…`, the form
`.github/PULL_REQUEST_TEMPLATE.md` uses so that its links resolve from a pull-request
page, is understood and checked against the repository root rather than skipped.

External URLs are out of scope, deliberately. A lint that fails because a third party
is having an outage teaches people to pass `--no-verify`.

### Consequence

18 more unit tests, one more thing in `make lint`, and no new dependency in
`pyproject.toml`. The cost is that the slug rules are now this repository's problem:
if GitHub changes them, the tests pass and the links break. That is a real risk and a
small one — the algorithm has been stable for years — and it is cheaper than the Node
toolchain the alternative brings.

---

## ADR-0048 — Run the restart proof on a nightly schedule, not on every pull request

**Date:** 2026-09-18 · **Status:** accepted

### Context

`make test-e2e` is the strongest argument this repository makes. It SIGKILLs the
producer and the streaming job while both are working, deletes a Spark commit file so
that the replay of an already-applied batch is guaranteed rather than hoped for, and
then asserts that `silver.edits` still holds one row per event
([ADR-0025](#adr-0025--make-the-crash-window-deterministic-by-deleting-commitsn)). It
is also the only test that consumes the live Wikimedia stream, and it takes four
minutes.

Those two facts pull against each other. A test that proves the central claim of the
README should not be something a reader has to take on trust. A test that goes red
because a volunteer-funded public endpoint had a bad night must not be what decides
whether an unrelated commit can merge. Until now the resolution was "run it by hand",
which is honest but means the proof quietly rots the first time nobody remembers to.

The same is true of `make smoke-live`, for a different reason: its last check fails
when the stream carries a field the declared schema does not, which is drift rather
than an outage — and drift by definition happens without anyone committing anything.

### Options

| Option | Rejected because |
|---|---|
| A scheduled `nightly.yml` | Chosen. |
| Add both to `ci.yml` | Four minutes onto every pull request, and a red build whenever `stream.wikimedia.org` is unavailable or has changed a field. The signal would be about the world and would look like a verdict on the commit. |
| Add them to `ci.yml` with `continue-on-error: true` | A check that cannot fail is a check nobody reads. It would also make the overall run green while hiding a real regression in the duplicate-suppression path, which is the one thing here worth being loud about. |
| Feed the e2e test from the captured fixtures so it can be a gate | The test's own docstring is the reason: a crash-recovery proof run against hand-fed frames is mostly a proof about the frames. The fixture-fed version of this argument already exists and already runs on every push — it is `tests/integration/test_silver_rebuild.py`. |
| Leave it manual, document that it is manual | What the repository did before this. It survives exactly as long as somebody keeps running it. |

### Decision

`.github/workflows/nightly.yml`, at 03:17 UTC, with two independent jobs: `live source
contract` (`make smoke-live`, no Docker, one HTTPS connection) and
`restart-idempotency proof` (`make up-core` then `make test-e2e`). Both write their
result into the run summary, because a scheduled run nobody opens is a scheduled run
nobody reads. Neither blocks anything: a failure here is information about the world,
and GitHub emails the repository owner, which is the whole notification system this
needs at this size.

Three details are deliberate. The cron minute is off the hour, because GitHub queues
scheduled workflows and the top of the hour is the most contended slot — a run there
is delayed by tens of minutes or dropped. The jobs do not `needs:` each other, so a
field renamed upstream reports as drift *and* the proof still runs, instead of being
skipped by a dependency. And the proof brings up the core profile rather than the full
stack: it was measured against `make up-core` alone before the workflow was written —
seven tests, 4m03s, green — which is 2,413 MiB and two health checks saved for the
same assertion.

### Consequence

The proof stops depending on somebody remembering it, and `ci.yml` keeps the property
that a red run means a bad commit.

The honest limitation is that the e2e job has never run on a GitHub-hosted runner and
cannot until the repository is published, so its first scheduled run is also its first
test of whether a 4-vCPU runner is enough. What is known rather than hoped: the
identical `make up-core` step is already green in `ci.yml`'s integration job, and the
core profile's measured peak is 4,561 MiB of the 16 GB `ubuntu-latest` has.

GitHub also disables scheduled workflows in a public repository after 60 days without
activity. That is the right behaviour here rather than a problem to work around — a
dormant repository should stop asking Wikimedia for data.

---

## ADR-0049 — Lint the workflows with actionlint in CI, not through a make target

**Date:** 2026-09-18 · **Status:** accepted

### Context

There are now three workflow files and about 400 lines of YAML in `.github/workflows/`,
and until this entry nothing in the repository read them. `ruff` and `mypy` do not
parse YAML; pre-commit's `check-yaml` proves a file is well-formed, not that a step
refers to an output some earlier step actually declares. So a typo in an `if:`
expression, an action pinned to a tag that does not exist, or a shell mistake inside a
`run:` block was discoverable only by running the workflow and watching it fail.

That is tolerable for `ci.yml`, which runs on every push. It is a bad deal for
`nightly.yml` ([ADR-0048](#adr-0048--run-the-restart-proof-on-a-nightly-schedule-not-on-every-pull-request)),
which by design nobody runs before it runs itself.

### Options

| Option | Rejected because |
|---|---|
| A `workflows` job in `ci.yml` that downloads the pinned actionlint release | Chosen. Same shape as the `secrets` job, which pins and downloads gitleaks for the same reason. |
| A `make lint-actions` target, so local matches CI | The property this repository otherwise keeps, and the one place it does not fit: actionlint is a Go binary, so `make lint` would start failing for every contributor who does not have it. The files it checks are only ever executed by GitHub, so unlike every other lint here there is nothing to catch on a laptop that CI would miss. |
| The `rhysd/actionlint` pre-commit hook | Its default hook needs a Go toolchain to build the binary; the alternative runs it in Docker, which is slow enough on a pre-commit hook to be bypassed. |
| `reviewdog/action-actionlint` | A third-party action with more permissions than this needs — it posts review comments — for a check whose output is three lines of stderr. |

### Decision

A five-minute `workflows` job pinned to actionlint 1.7.12, downloaded from the GitHub
release the same way `secrets` downloads gitleaks.

The half of actionlint that earns its place is that it shells out to `shellcheck` for
every `run:` block, and `ubuntu-latest` has shellcheck where a laptop may not. Running
it locally with shellcheck installed found one finding on the existing workflows —
SC2016 on the backticks inside a single-quoted markdown heading, which is a false
positive and is now suppressed inline with the reason next to it rather than by
loosening the rule set.

### Consequence

One more required check, five minutes of free-tier runner per push, and no new
dependency for a contributor. The gap it leaves is that a workflow edit made without
pushing is unchecked locally — accepted, because pushing is the only way to run a
workflow anyway.

## ADR-0050 — Wrap the streaming targets in a script so Ctrl-C reaches the driver

**Date:** 2026-09-18 · **Status:** accepted

### Context

`make stream-bronze` submitted the job with `docker compose exec -T spark spark-submit`,
which is how every other stack target in the Makefile is written and is deliberate: what
the README shows is literally what runs.

For a long-running foreground process it is wrong. `docker exec` does not forward signals
to the process it starts, and `-T` removes the TTY, so the terminal's own Ctrl-C has no
path down either. Measured on 2026-09-18: pressing Ctrl-C returned the shell prompt and
left the `SparkSubmit` JVM and its `bronze.py` running inside the container fifteen
seconds later — still committing to Iceberg, with its stdout attached to a client that no
longer existed, and nothing about it in `docker compose logs spark`. The next
`make stream-bronze` failed with `Multiple streaming queries are concurrently using
/opt/spark/checkpoints/...` naming a query the operator had no way to see.

Nothing was corrupted by this. The checkpoint protocol survives a SIGKILL —
`tests/e2e/test_restart_idempotency.py` is the proof — so the orphan's committed batches
were valid and the invisible stream was, technically, working. That is what made it worth
fixing rather than documenting: a fault that leaves no damage and no log line is one an
operator diagnoses by watching row counts move on a stopped pipeline.

### Options

| Option | Rejected because |
|---|---|
| `scripts/stream.sh`: submit in the background, trap `INT`/`TERM`, `pkill -TERM` the driver by app name over a second exec | Chosen. Costs one file and one indirection on four of the 62 targets; the rest stay plain compose commands. |
| Drop `-T` from `SPARK_SUBMIT` so the TTY carries the signal | Breaks every non-interactive caller of the same variable — CI, `make sql`, the acceptance script, `make maintain` — with `the input device is not a TTY`. A fix for the interactive case that breaks the automated one. |
| `docker compose run` instead of `exec`, which does propagate signals | A second Spark container per stream, each with its own driver heap, on a box where the measured peak is already 6.9 GiB. It would also bypass the healthchecked long-lived container the other targets share. |
| Ship `make stop-streams` alone and document Ctrl-C as a known wart | Rejected on the grounds in hard rule 6 of this project's own brief: an operator's first instinct is the correct one, and a stack where Ctrl-C silently does not stop the thing you are looking at is a defect, not a footnote. |
| Run the streams as compose services with `restart: unless-stopped` | The right answer for a deployment and the wrong one for a demo. `docker compose stop` would then be the stop command, but the reviewer following the README loses the streaming log on their terminal, which is the most legible thing in the whole quickstart. |

### Decision

Four targets call `scripts/stream.sh <layer> [--once]`. The script names the Spark
application `<layer>-<shell pid>` so its trap stops the run it started and not a stream in
another terminal, backgrounds the submit and `wait`s (bash runs a trap only between
commands, so a foreground submit would defer the handler until the JVM had already
exited), and re-`wait`s until the driver is reaped rather than exiting mid-shutdown.

`make stop-streams` covers what a trap cannot: a closed terminal, a `kill -9`, or a
`docker compose exec` typed by hand. It matches `SparkSubmit --name (bronze|silver)`, so
an interactive `make sql`, a maintenance run or an integration test's own submit in the
same container is not collateral damage — the tests name their applications
`e2e-restart-<run id>`. It prints what it found before it signals anything.

### Consequence

The Makefile's "every target is a compose command in plain sight" property now has one
documented exception, with the reason in the script's header and in the comment above the
targets. A graceful stop leaves a complete batch in the checkpoint, which is why
`make stream-bronze-once` works immediately afterwards — measured resuming at batch 218
with zero offsets behind — where after a crash it hits the concurrent-query error and
`make stream-bronze` is the documented recovery. Runbook entry 12 covers the orphan case.
