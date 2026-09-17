# syntax=docker/dockerfile:1
#
# The Dagster image with the dbt project inside it, for the manifests in k8s/.
#
# This exists because of one difference between Compose and a cluster, and it is a
# difference worth being explicit about rather than papering over.
#
# docker-compose.yml bind mounts ./dbt into the container read-only. That is
# deliberate and docker/dagster/entrypoint.sh explains why: the entrypoint parses
# the project at start-up, so editing a model changes both the SQL dbt runs and the
# asset graph Dagster shows, without a rebuild. On a laptop that is the whole
# feedback loop.
#
# A cluster has no working tree to mount. The options are to fetch the project at
# run time — a git-sync sidecar, or an init container cloning this repository, which
# makes every pod start depend on GitHub being up and on the pod having egress it
# otherwise does not need — or to put the project in the image. The second is also
# what makes a deployment reproducible: the image is then a single artefact whose
# models and whose asset graph cannot disagree with each other, and rolling back the
# image rolls back both.
#
# A ConfigMap was the third option and it does not work. dbt models live in
# subdirectories (`models/staging/`, `models/marts/`) and a ConfigMap key cannot
# contain a slash, so representing the project would mean flattening it and losing
# the layout dbt uses to find things.
#
# Built from the image docker/dagster.Dockerfile produces, rather than repeating it.
# The base has to exist locally first — `make build-dagster-k8s` does both in order.

ARG BASE_IMAGE=wikistream/dagster:local
FROM ${BASE_IMAGE}

# Root-owned and world-readable, which reproduces the `:ro` bind mount the Compose
# version gets. The process runs as uid 10001 and has no business writing here: dbt
# is pointed at /tmp for its target directory and its logs by the base image's ENV,
# and if that ever regresses this ownership is what turns it into an error at
# start-up instead of a surprise write into the image layer.
COPY --chown=root:root --chmod=444 dbt/ /opt/dbt/

# The directories themselves need the execute bit that --chmod=444 does not set on
# them, or nothing can traverse into models/.
USER root
RUN find /opt/dbt -type d -exec chmod 555 {} +
USER wikistream

LABEL org.opencontainers.image.title="wikistream-dagster-k8s" \
      org.opencontainers.image.description="Dagster webserver and daemon with the dbt project baked in, for k8s/" \
      org.opencontainers.image.source="https://github.com/william-sarkar/wikistream-lakehouse" \
      org.opencontainers.image.licenses="Apache-2.0"
