# Architecture decision records

Short records of the decisions that were not obvious, written when the decision
was made rather than reconstructed afterwards. Each one names what was rejected,
because a decision with no rejected alternative is not a decision.

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
offline — so the test suite runs against captured fixtures and only one target
(`make smoke-live`) touches the network.

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

`docker/spark/Dockerfile` pins four jar URLs by full coordinate. Upgrading Spark
means changing the image tag, the PySpark pin, the connector jar and possibly the
Iceberg runtime together — which is why `dependabot.yml` ignores major-version
bumps on `apache/spark` and `apache/iceberg-rest-fixture`. That is deliberate:
those are compatibility-matrix exercises, not version bumps.

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

**Date:** 2026-09-17 · **Status:** accepted

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
— small enough to be uninteresting on a 12 GB laptop, which is the honest reason
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
