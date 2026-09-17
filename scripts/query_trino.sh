#!/usr/bin/env bash
#
# The headline queries, run through Trino against tables Spark wrote.
#
# What this script is for: the claim "Spark writes Iceberg, Trino reads the same
# tables with no export step" is the sort of sentence every lakehouse README
# contains and few of them can demonstrate. This one runs and prints the result,
# so a reader can check it in the time it takes to run `make query`.
#
# Trino is given nothing but a REST catalog URI and a bucket. It has never been
# told which engine wrote these files, and there is no sync job between the two.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck disable=SC1091
[[ -f .env ]] && set -a && source .env && set +a

CATALOG="${WS_ICEBERG_CATALOG_NAME:-lakehouse}"
BRONZE="${WS_BRONZE_NAMESPACE:-bronze}"
SILVER="${WS_SILVER_NAMESPACE:-silver}"
GOLD="${WS_GOLD_NAMESPACE:-gold}"

if ! docker compose ps --status running --services 2>/dev/null | grep -qx trino; then
  echo "error: the trino service is not running. Start it with 'make up'." >&2
  exit 1
fi

# --output-format ALIGNED because the audience for this output is a human. The
# machine-readable path is `docker compose exec -T trino trino --output-format
# CSV`, which is what the tests use.
trino_query() {
  docker compose exec -T trino trino --output-format ALIGNED --execute "$1"
}

heading() {
  printf '\n%s\n' "── $1"
}

# One argument per line of commentary, blank line after the last. Taking a list
# rather than one string keeps the wrapping decision here instead of in every
# call site's embedded newlines.
explain() {
  printf '   %s\n' "$@"
  printf '\n'
}

# ---------------------------------------------------------------- cross-engine

heading "1. Trino sees the tables Spark wrote"
explain "One catalog, two engines, no copy. Spark commits; Trino reads the commit."
trino_query "
  SELECT
    table_schema AS namespace,
    table_name,
    (SELECT count(*) FROM ${CATALOG}.information_schema.columns c
      WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS columns
  FROM ${CATALOG}.information_schema.tables t
  WHERE table_schema IN ('${BRONZE}', '${SILVER}', '${GOLD}')
  ORDER BY 1, 2
"

heading "2. Row counts across the three tables"
explain "bronze and silver are independent consumers of the same topic, so the" \
        "gap between them is consumer progress and nothing else."
trino_query "
  SELECT
    (SELECT count(*) FROM ${CATALOG}.${BRONZE}.recentchange_raw)      AS bronze_rows,
    (SELECT count(*) FROM ${CATALOG}.${SILVER}.edits)                 AS silver_rows,
    (SELECT count(DISTINCT event_id) FROM ${CATALOG}.${SILVER}.edits) AS silver_distinct_ids,
    (SELECT count(*) FROM ${CATALOG}.${SILVER}.quarantine)            AS quarantined_rows
"
# Worth being explicit about, because the intuitive claim — "bronze is the raw
# log, so bronze >= silver" — is false here, and printing it would be the kind of
# plausible-sounding invariant a reviewer checks first. The two streaming jobs are
# separate Kafka consumers with separate checkpoints reading the same topic. Kill
# one and the other keeps going, so either table can be ahead. What *is* invariant
# is that silver's row count never exceeds the number of distinct event ids in the
# offsets it has consumed, and `make verify-no-duplicates` is the gate on that.
printf '   %s\n' "Neither count bounds the other: whichever stream ran last is ahead."

heading "3. Iceberg metadata, read through Trino's \$-suffixed tables"
explain "Snapshot history is part of the table, not a side file. This is the audit trail."
trino_query "
  SELECT
    snapshot_id,
    operation,
    date_diff('second', committed_at, current_timestamp) AS age_seconds,
    CAST(summary['total-records'] AS bigint)             AS total_records,
    CAST(summary['total-data-files'] AS integer)         AS data_files
  FROM ${CATALOG}.${SILVER}.\"edits\$snapshots\"
  ORDER BY committed_at DESC
  LIMIT 5
"

printf '\n'
