#  The only interface a reviewer needs. `make` on its own prints the menu.
#
#  Design note: every target that touches the stack goes through `docker compose`
#  rather than through a shell script wrapper, so that what the README shows is
#  literally what runs. Targets that need the Python toolchain go through `uv run`
#  so they work without an activated virtualenv.

.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE       ?= docker compose
UV            ?= uv
PROJECT       ?= wikistream
SPARK_SUBMIT  := $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit
DBT           := $(COMPOSE) run --rm dbt

# Every profile, for the targets that must not miss a container: down, clean, ps.
ALL_PROFILES  := --profile full --profile producer

# Containers reach the broker on the in-network listener. localhost:9092 is the
# host listener and is not resolvable from inside the compose network.
KAFKA_INTERNAL := kafka:29092
KAFKA_BIN      := /opt/kafka/bin
TOPIC          := $${WS_KAFKA_TOPIC:-wiki.recentchange}

# Bounded run length for the CI-friendly producer target.
PRODUCER_SECONDS ?= 60
PRODUCER_EVENTS  ?= 2000

# Extra arguments for the test targets, e.g. PYTEST_ARGS="-k bronze -x". Passed as a
# variable rather than as trailing make goals because make parses a bare `-k` as its
# own keep-going flag, and `make test -- -k bronze` looks for a target named `-k`.
PYTEST_ARGS ?=

# Sampling window for the source-lag measurement. Not named SECONDS: that is a
# bash builtin holding the shell's own uptime, and a recipe reading it gets 0.
LAG_SECONDS ?= 120

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------------- stack

.PHONY: up
up: ## Bring up the whole stack (full profile: + Trino, Dagster)
	$(COMPOSE) --profile full up -d --wait
	@$(MAKE) --no-print-directory create-topics

.PHONY: up-core
up-core: ## Bring up the always-on core only (Kafka, MinIO, Iceberg REST)
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory create-topics

.PHONY: down
down: ## Stop everything. Preserves volumes, so offsets and tables survive.
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans

.PHONY: clean
clean: ## Stop everything and delete volumes. Destroys all local data.
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans --volumes
	@echo "Volumes removed. The next 'make up' starts from an empty lakehouse."

.PHONY: ps
ps: ## Show service status and health
	$(COMPOSE) $(ALL_PROFILES) ps

.PHONY: logs
logs: ## Tail logs for all running services (SERVICE=name to narrow)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

.PHONY: bootstrap
bootstrap: ## Check this machine can run the stack, before you try
	bash scripts/bootstrap.sh

# ------------------------------------------------------------------- ingest

.PHONY: build
build: ## Build the producer image
	$(COMPOSE) build producer

.PHONY: create-topics
create-topics: ## Create or reconcile the ingest topic (idempotent)
	@bash scripts/create_topics.sh

.PHONY: ingest
ingest: ## Start the producer as a background service that restarts on failure
	$(COMPOSE) up -d producer
	@echo "Ingesting. Follow it with: make logs SERVICE=producer"

.PHONY: producer
producer: ## Run the producer in the foreground, unbounded (Ctrl-C to stop)
	$(COMPOSE) run --rm producer

.PHONY: producer-bounded
producer-bounded: ## Run the producer for PRODUCER_SECONDS or PRODUCER_EVENTS, whichever first
	@# The long-lived service is stopped first so the offsets this run adds are
	@# attributable to this run. Harmless when it was never started.
	@$(COMPOSE) stop producer >/dev/null 2>&1 || true
	$(COMPOSE) run --rm producer \
	  python -m wikistream.producer --duration $(PRODUCER_SECONDS) --max-events $(PRODUCER_EVENTS)

.PHONY: kafka-offsets
kafka-offsets: ## Print per-partition end offsets for the topic
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-get-offsets.sh \
	  --bootstrap-server $(KAFKA_INTERNAL) --topic $(TOPIC)

.PHONY: kafka-tail
kafka-tail: ## Print the first N records on the topic (N=5 by default)
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-console-consumer.sh \
	  --bootstrap-server $(KAFKA_INTERNAL) --topic $(TOPIC) \
	  --from-beginning --max-messages $${N:-5} \
	  --property print.key=true --property print.partition=true

.PHONY: kafka-lag
kafka-lag: ## Print consumer group lag for the streaming jobs
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-consumer-groups.sh \
	  --bootstrap-server $(KAFKA_INTERNAL) --describe --all-groups

# -------------------------------------------------------------- measurement
#
# Every number in docs/ comes from one of these four targets. They are here so a
# reader can regenerate a figure rather than take it on trust, and so a figure that
# has gone stale can be spotted by rerunning it.

.PHONY: measure-throughput
measure-throughput: ## Ingest rate, compression ratio and partition balance. Needs `make up-core`.
	@bash scripts/measure_throughput.sh $(PRODUCER_SECONDS) $(PRODUCER_EVENTS)

.PHONY: measure-compression
measure-compression: ## Compare compression codecs on live payloads. Needs `make up-core`.
	@bash scripts/compare_compression.sh $(PRODUCER_EVENTS) $(PRODUCER_SECONDS)

.PHONY: measure-lag
measure-lag: ## Source lag distribution, which is where the watermark comes from
	$(UV) run python scripts/measure_source_lag.py --seconds $(LAG_SECONDS)

.PHONY: explain-partitions
explain-partitions: ## Why one Kafka partition takes most of the traffic. No network.
	$(UV) run python scripts/analyse_partitioning.py

# ---------------------------------------------------------------- lakehouse

.PHONY: init-tables
init-tables: ## Create namespaces and the bronze/silver Iceberg tables (idempotent)
	$(SPARK_SUBMIT) /opt/wikistream/scripts/init_tables.py

.PHONY: stream-bronze
stream-bronze: ## Run the bronze append stream continuously
	$(SPARK_SUBMIT) --name bronze /opt/wikistream/src/wikistream/streaming/bronze.py

.PHONY: stream-bronze-once
stream-bronze-once: ## Run one bronze micro-batch over all available data, then exit
	$(SPARK_SUBMIT) --name bronze-once /opt/wikistream/src/wikistream/streaming/bronze.py --once

.PHONY: stream-silver
stream-silver: ## Run the silver MERGE stream continuously
	$(SPARK_SUBMIT) --name silver /opt/wikistream/src/wikistream/streaming/silver.py

.PHONY: stream-silver-once
stream-silver-once: ## Run one silver micro-batch over all available data, then exit
	$(SPARK_SUBMIT) --name silver-once /opt/wikistream/src/wikistream/streaming/silver.py --once

.PHONY: rebuild-silver
rebuild-silver: ## Rebuild silver from bronze for a date range: FROM=YYYY-MM-DD TO=YYYY-MM-DD
	$(SPARK_SUBMIT) /opt/wikistream/scripts/rebuild_silver.py --from "$(FROM)" --to "$(TO)"

.PHONY: maintain
maintain: ## Compact files, expire snapshots, rewrite manifests, remove orphans
	$(SPARK_SUBMIT) /opt/wikistream/scripts/maintain_tables.py

.PHONY: verify-no-duplicates
verify-no-duplicates: ## Exit non-zero if silver.edits contains any duplicate event_id
	$(SPARK_SUBMIT) /opt/wikistream/scripts/verify_no_duplicates.py

.PHONY: prove-schema-evolution
prove-schema-evolution: ## Add, rename, widen and drop a column on a scratch table; check the cost
	$(SPARK_SUBMIT) /opt/wikistream/scripts/prove_schema_evolution.py

.PHONY: table-stats
table-stats: ## Row counts and file statistics for bronze and silver
	$(SPARK_SUBMIT) /opt/wikistream/scripts/table_stats.py

.PHONY: sql
sql: ## Run one statement against the Iceberg catalog: SQL="SELECT ..." [JSON=--json]
	@$(SPARK_SUBMIT) /opt/wikistream/scripts/spark_sql.py $(JSON) "$(SQL)"

# ------------------------------------------------------------------ analytics

.PHONY: query
query: ## Run the headline queries through Trino
	bash scripts/query_trino.sh

.PHONY: query-duckdb
query-duckdb: ## Read the same Iceberg tables with DuckDB, no JVM (core profile path)
	$(UV) run python scripts/query_duckdb.py

.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	$(DBT) deps

.PHONY: dbt-run
dbt-run: ## Build the gold marts
	$(DBT) build --exclude-resource-type test

.PHONY: dbt-test
dbt-test: ## Run dbt schema and singular tests
	$(DBT) test

.PHONY: dbt-docs
dbt-docs: ## Generate dbt docs (output is gitignored)
	$(DBT) docs generate

# --------------------------------------------------------------- orchestration

.PHONY: dagster
dagster: ## Open the Dagster UI URL
	@echo "Dagster UI: http://localhost:$${WS_DAGSTER_PORT:-3000}"

.PHONY: dagster-materialise-all
dagster-materialise-all: ## Materialise every Dagster asset from the CLI
	$(COMPOSE) exec -T dagster-webserver \
	  dagster asset materialize --select '*' -m wikistream_dagster.definitions

.PHONY: dagster-check-all
dagster-check-all: ## Run every Dagster asset check from the CLI
	$(COMPOSE) exec -T dagster-webserver \
	  dagster job execute -j asset_checks_job -m wikistream_dagster.definitions

# ------------------------------------------------------------------- quality

.PHONY: lint
lint: ## ruff check + ruff format --check + sqlfluff
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	@sql=$$(git ls-files '*.sql'); \
	if [ -n "$$sql" ]; then $(UV) run sqlfluff lint $$sql; \
	else echo "sqlfluff: no tracked .sql files yet"; fi

.PHONY: format
format: ## Rewrite files with ruff format and ruff --fix
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

.PHONY: fmt-check
fmt-check: ## Formatting check only
	$(UV) run ruff format --check .

.PHONY: typecheck
typecheck: ## mypy
	$(UV) run mypy

.PHONY: test
test: ## Unit + Spark + integration tests
	$(UV) run pytest -m "unit or spark or integration" $(PYTEST_ARGS)

.PHONY: test-unit
test-unit: ## Unit tests only. No network, no Docker, no JVM.
	$(UV) run pytest -m unit $(PYTEST_ARGS)

.PHONY: test-spark
test-spark: ## Schema tests against a real in-process Spark. Needs a JDK, not Docker.
	$(UV) run pytest -m spark $(PYTEST_ARGS)

.PHONY: test-integration
test-integration: ## Integration tests. Needs `make up-core`. PYTEST_ARGS="-k bronze" to narrow.
	$(UV) run pytest -m integration $(PYTEST_ARGS)

.PHONY: test-e2e
test-e2e: ## The restart-idempotency proof. Needs `make up` and takes minutes.
	$(UV) run pytest -m e2e -s $(PYTEST_ARGS)

.PHONY: coverage
coverage: ## Unit test coverage report for src/wikistream
	$(UV) run pytest -m unit --cov --cov-report=term-missing

.PHONY: smoke-live
smoke-live: ## The only target that touches the public internet
	$(UV) run python scripts/smoke_live.py
