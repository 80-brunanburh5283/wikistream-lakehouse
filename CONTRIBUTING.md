# Contributing

Thanks for looking. This is a small project with a narrow scope, so the most
useful contributions are bug reports with a reproduction and focused pull
requests.

## Scope

The project is deliberately bounded. Things that are in scope: correctness of
the streaming and lakehouse layers, new dbt marts, better tests, documentation
that removes a surprise, portability to another object store or query engine.

Things that are out of scope, and why, are listed under
[What I would do differently](README.md#what-i-would-do-differently). A pull
request adding a machine-learning layer, a web frontend framework, or a second
cloud implementation will be declined — not because those are bad, but because
they would make this repository about something else.

## Setting up

You need Docker with about 7 GB available to it, and `uv`.

```bash
git clone https://github.com/william-sarkar/wikistream-lakehouse
cd wikistream-lakehouse
cp .env.example .env

uv sync                 # creates .venv and installs everything, including dev tools
uv run pre-commit install

bash scripts/bootstrap.sh   # checks your machine can run the stack
make up                     # 2m28s cold once the images are built
```

`make help` lists every target.

## Running the tests

There are four levels, and they have different requirements:

| Command | Needs | Roughly |
|---|---|---|
| `make test-unit` | nothing — no network, no Docker, no JVM | 285 tests, 4 s warm |
| `make test-spark` | a JDK, but no Docker | 75 tests, 67 s |
| `make test-integration` | `make up-core` | 22 tests, 10 min |
| `make test-e2e` | `make up-core`, plus the public internet | 7 tests, 4 min |

Those four figures were measured on one machine on 2026-09-18 and are there to set
expectations, not as a benchmark. The integration suite is slow for one reason: most
of its ten minutes is JVM start-up, because every assertion about a MERGE has to
ingest to bronze with one `spark-submit` and then rebuild with another. CI runs it in
two shards for that reason; `PYTEST_ARGS="tests/integration/test_bronze.py"` narrows
it the same way locally.

Unit tests must stay offline and fast. If a test needs Kafka, MinIO or Spark, it
is an integration test and belongs behind the `integration` marker so that
someone without Docker running can still get a useful signal from `pytest`.

Five targets talk to the public internet, and only these five. Three of them touch
the pipeline itself: `make smoke-live`, which checks the source is reachable;
`make test-e2e`, which feeds its scratch topic from the live stream; and
`make query-duckdb`, whose first run downloads DuckDB's `httpfs` and `iceberg`
extensions from extensions.duckdb.org and is offline after that. The other two fetch
tooling and then cache it: `make infra-validate` downloads the Terraform AWS
provider into `infra/aws/.terraform`, and `make k8s-validate` downloads Kubernetes
JSON schemas into `.cache/kubeconform`. Neither of those two reaches AWS or a
cluster, which is the point of them — see `infra/aws/README.md`.

Everything else runs against captured fixtures and local containers.

## Before you open a pull request

```bash
make lint         # ruff check, ruff format --check, sqlfluff, Markdown links
make typecheck    # mypy
make dbt-parse    # compiles the dbt graph; no warehouse needed
make test         # unit + Spark + integration
```

`uv run pre-commit run --all-files` runs the same checks the hooks run, plus
secret scanning.

These are the commands CI runs — `.github/workflows/ci.yml` calls the same targets
rather than restating them, so a green run there means the same command is green
here. What CI adds is a scan of every commit in the history for credentials, a lint
of the workflow files themselves, and, in `infra.yml`, validation of the Terraform
and the Kubernetes manifests.

What it leaves out is `make test-e2e`, which depends on the live Wikimedia stream: an
outage upstream would fail a pull request that had nothing to do with it. That test
and `make smoke-live` run on a schedule in `nightly.yml` instead, where a red run is
information about the world rather than a verdict on your change. If you touch the
streaming path, run `make test-e2e` yourself and say so in the pull request — it
takes four minutes and it is the only check that proves the restart guarantee.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). The
prefixes in use are `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `ci`,
`perf`, `build`.

Keep the subject line under 72 characters, in the imperative mood, with no
trailing period. Use the body to say *why*, and what you rejected — that is the
part a future reader cannot reconstruct from the diff.

```
feat: dedupe within the micro-batch before the Iceberg MERGE

MERGE INTO errors when the source relation contains two rows matching the
same target row. Kafka is at-least-once, so a micro-batch can legitimately
contain the same event_id twice after a producer retry.

Rejected: making the MERGE condition tolerant with a window function inside
the USING clause. It works, but hides the duplicate count, and the count is
something we want to observe.
```

## Pull requests

One concern per pull request. Fill in the template — particularly the section on
how you verified the change, because "tests pass" and "I ran the pipeline for ten
minutes and the duplicate count stayed at zero" are very different claims.

If a change affects a guarantee documented in
[docs/correctness.md](docs/correctness.md), update that document in the same pull
request.

## Architecture decisions

Non-trivial choices are recorded as short ADRs in [DECISIONS.md](DECISIONS.md).
If your change reverses one of them, add a new entry rather than editing the old
one — the history of a decision is more useful than its latest state.

## Code of conduct

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
