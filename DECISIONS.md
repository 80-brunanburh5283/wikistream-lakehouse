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
