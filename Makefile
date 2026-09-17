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

# Bounded run length for the CI-friendly producer target.
PRODUCER_SECONDS ?= 60
PRODUCER_EVENTS  ?= 2000

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------------- stack

.PHONY: up
up: ## Bring up the whole stack (full profile: + Trino, Dagster)
	$(COMPOSE) --profile full up -d --wait
	@$(MAKE) --no-print-directory init-tables

.PHONY: up-core
up-core: ## Bring up the low-memory profile (Kafka, MinIO, Iceberg REST, Spark, producer)
	$(COMPOSE) --profile core up -d --wait
	@$(MAKE) --no-print-directory init-tables

.PHONY: down
down: ## Stop everything. Preserves volumes, so offsets and tables survive.
	$(COMPOSE) --profile full --profile core down --remove-orphans

.PHONY: clean
clean: ## Stop everything and delete volumes. Destroys all local data.
	$(COMPOSE) --profile full --profile core down --remove-orphans --volumes
	@echo "Volumes removed. The next 'make up' starts from an empty lakehouse."

.PHONY: ps
ps: ## Show service status and health
	$(COMPOSE) --profile full --profile core ps

.PHONY: logs
logs: ## Tail logs for all running services (SERVICE=name to narrow)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

.PHONY: bootstrap
bootstrap: ## Check this machine can run the stack, before you try
	bash scripts/bootstrap.sh

# ------------------------------------------------------------------- ingest

.PHONY: producer
producer: ## Run the producer in the foreground, unbounded (Ctrl-C to stop)
	$(COMPOSE) --profile core run --rm producer

.PHONY: producer-bounded
producer-bounded: ## Run the producer for PRODUCER_SECONDS or PRODUCER_EVENTS, whichever first
	$(COMPOSE) --profile core run --rm producer \
	  python -m wikistream.producer --duration $(PRODUCER_SECONDS) --max-events $(PRODUCER_EVENTS)

.PHONY: kafka-offsets
kafka-offsets: ## Print per-partition end offsets for the topic
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server localhost:9092 --topic $${WS_KAFKA_TOPIC:-wiki.recentchange}

.PHONY: kafka-lag
kafka-lag: ## Print consumer group lag for the streaming jobs
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 --describe --all-groups

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

.PHONY: table-stats
table-stats: ## Row counts and file statistics for bronze and silver
	$(SPARK_SUBMIT) /opt/wikistream/scripts/table_stats.py

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
	$(UV) run pytest -m "unit or spark or integration"

.PHONY: test-unit
test-unit: ## Unit tests only. No network, no Docker, no JVM.
	$(UV) run pytest -m unit

.PHONY: test-spark
test-spark: ## Schema tests against a real in-process Spark. Needs a JDK, not Docker.
	$(UV) run pytest -m spark

.PHONY: test-integration
test-integration: ## Integration tests. Needs `make up-core`.
	$(UV) run pytest -m integration

.PHONY: test-e2e
test-e2e: ## The restart-idempotency proof. Needs `make up` and takes minutes.
	$(UV) run pytest -m e2e -s

.PHONY: coverage
coverage: ## Unit test coverage report for src/wikistream
	$(UV) run pytest -m unit --cov --cov-report=term-missing

.PHONY: smoke-live
smoke-live: ## The only target that touches the public internet
	$(UV) run python scripts/smoke_live.py
