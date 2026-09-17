#!/usr/bin/env bash
#
# Parse the dbt project, then hand off to the webserver or the daemon.
#
# Why this exists at all: `@dbt_assets(manifest=...)` in
# src/wikistream_dagster/dbt_project.py reads manifest.json at *import* time, so
# the manifest must be on disk before either Dagster process starts. Without it,
# the code location fails to load and the UI shows an empty deployment with an
# error in the corner — which reads like Dagster is broken rather than like a
# missing build step.
#
# Two rejected alternatives, both worse:
#
#   1. Bake the manifest in at image build time. That is what Dagster's own docs
#      recommend for a deployment, and it is wrong here: docker-compose.yml bind
#      mounts ./dbt from the host, so an image-time manifest would describe the
#      models as they were when the image was built. Editing a model would then
#      change the SQL dbt runs without changing the graph Dagster shows, which is
#      the most confusing failure this project could ship.
#   2. Call `DbtProject.prepare_if_dev()` and let Dagster handle it. It already
#      does — but only under `dagster dev`, deliberately, and neither of these
#      containers is that.
#
# The cost is about two seconds per container start, and it is paid twice because
# the webserver and the daemon each parse into their own scratch directory. That
# is cheaper than the two processes sharing a writable path and racing on it.
set -euo pipefail

PROJECT_DIR="${WS_DBT_PROJECT_DIR:-/opt/dbt}"
TARGET_PATH="${WS_DBT_TARGET_PATH:-/tmp/dbt-target}"
LOG_PATH="${DBT_LOG_PATH:-/tmp/dbt-logs}"

if [[ ! -f "${PROJECT_DIR}/dbt_project.yml" ]]; then
  echo "entrypoint: no dbt_project.yml under ${PROJECT_DIR}." >&2
  echo "entrypoint: the ./dbt bind mount is missing or points somewhere else." >&2
  exit 1
fi

# dbt creates its target directory but not the parents of its log path, and the
# project directory it would otherwise use for both is mounted read-only.
mkdir -p "${TARGET_PATH}" "${LOG_PATH}"

echo "entrypoint: parsing ${PROJECT_DIR} into ${TARGET_PATH}" >&2
# --no-use-colors because this output goes to a Docker log, where the escape
# sequences are noise. No `dbt deps`: the project has no packages.yml, on purpose
# (DECISIONS.md ADR-0026), so there is nothing to fetch and this container needs
# no network access to become ready.
dbt parse \
  --project-dir "${PROJECT_DIR}" \
  --target-path "${TARGET_PATH}" \
  --no-use-colors

exec "$@"
