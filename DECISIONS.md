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
replay can be tens of minutes behind. Measured source lag has a p99 of 4m12s
(`docs/latency.md`), but the tail is not bounded by anything I control.

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
