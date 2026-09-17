#!/usr/bin/env bash
#
# Host preflight. Run before `make up` on a machine you have not run this on.
#
# The point is to fail with a sentence a person can act on, before ten minutes of
# image pulls, rather than to fail thirty seconds into Spark start-up with a JVM
# stack trace about an address already in use. Every check prints a line; the
# script exits non-zero only for the ones that genuinely stop the stack.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck disable=SC1091
[[ -f .env ]] && set -a && source .env && set +a

RED=$'\033[31m'; YEL=$'\033[33m'; GRN=$'\033[32m'; OFF=$'\033[0m'
[[ -t 1 ]] || { RED=''; YEL=''; GRN=''; OFF=''; }

failures=0
warnings=0

ok()   { printf '  %sok%s    %s\n'   "${GRN}" "${OFF}" "$1"; }
warn() { printf '  %swarn%s  %s\n'   "${YEL}" "${OFF}" "$1"; ((warnings++)); }
bad()  { printf '  %sFAIL%s  %s\n'   "${RED}" "${OFF}" "$1"; ((failures++)); }

# A TCP connect rather than `ss` or `lsof`: bash has /dev/tcp built in, and
# neither of those tools is guaranteed to be installed in a minimal WSL image.
port_in_use() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1
}

echo "wikistream-lakehouse preflight"
echo

# ------------------------------------------------------------------- docker
echo "docker"
if ! command -v docker >/dev/null 2>&1; then
  bad "docker is not on PATH. Install Docker Desktop with the WSL2 backend, or docker-ce inside WSL."
elif ! docker info >/dev/null 2>&1; then
  bad "the docker daemon is not reachable. Start Docker Desktop, or 'sudo service docker start'."
else
  ok "docker $(docker version --format '{{.Server.Version}}') daemon reachable"
  if docker compose version >/dev/null 2>&1; then
    ok "docker compose $(docker compose version --short)"
  else
    bad "'docker compose' (v2) is missing. The v1 'docker-compose' script will not work: this project uses profiles and 'up --wait'."
  fi
fi
echo

# -------------------------------------------------------------------- ports
# Only the ports this project publishes. A port already in use is fatal unless it
# is in use by this project, which is the common case when re-running preflight
# against a stack that is already up.
echo "ports"
already_up=""
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  already_up=$(docker compose ps --status running --services 2>/dev/null | tr '\n' ' ')
fi

check_port() {
  local port="$1" what="$2"
  if ! port_in_use "${port}"; then
    ok "${port} free (${what})"
  elif [[ -n "${already_up}" ]]; then
    ok "${port} in use by this project's stack (${what}) — expected, it is already up"
  else
    bad "${port} is in use by something else (${what}). Free it, or set the matching WS_*_PORT in .env."
  fi
}

check_port "${WS_KAFKA_PORT:-9092}"         "kafka"
check_port "${WS_MINIO_S3_PORT:-9000}"      "minio s3"
check_port "${WS_MINIO_CONSOLE_PORT:-9001}" "minio console"
check_port "${WS_ICEBERG_REST_PORT:-8181}"  "iceberg rest catalog"
check_port "${WS_TRINO_PORT:-8080}"         "trino, full profile only"
check_port "${WS_DAGSTER_PORT:-3000}"       "dagster, full profile only"
check_port "${WS_SPARK_UI_PORT:-4040}"      "spark ui, full profile only"
echo

# ---------------------------------------------------------------- resources
echo "resources"
mem_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
mem_gb=$(( mem_kb / 1024 / 1024 ))
if (( mem_gb >= 10 )); then
  ok "${mem_gb} GB RAM visible to this kernel"
elif (( mem_gb >= 6 )); then
  warn "${mem_gb} GB RAM. The core profile fits; the full profile (Trino + Dagster) will not. Use 'make up-core'."
else
  bad "${mem_gb} GB RAM is not enough for Kafka and Spark together. Raise it in %UserProfile%\\.wslconfig — see WSL setup notes in the README."
fi

cpus=$(nproc 2>/dev/null || echo 1)
if (( cpus >= 4 )); then
  ok "${cpus} CPUs"
else
  warn "${cpus} CPUs. It will run, but micro-batches will be slow."
fi

# Docker's data root, not the repository: images and volumes are what fill up.
docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)
avail_gb=$(df -BG --output=avail "${docker_root}" 2>/dev/null | tail -1 | tr -dc '0-9' || echo 0)
avail_gb=${avail_gb:-0}
if (( avail_gb >= 20 )); then
  ok "${avail_gb} GB free on ${docker_root}"
elif (( avail_gb >= 10 )); then
  warn "${avail_gb} GB free on ${docker_root}. Images alone are about 4 GB; a day of Kafka retention plus Iceberg data will want more."
else
  bad "${avail_gb} GB free on ${docker_root} is not enough. About 15 GB is needed for images and a day of data."
fi
echo

# ------------------------------------------------------------------ limits
echo "limits"
# Kafka opens a file per log segment per partition, and Spark opens a lot more.
# The soft limit can be raised by this shell up to the hard limit with no root;
# only raising the hard limit needs privileges, which this script never asks for.
soft=$(ulimit -Sn)
hard=$(ulimit -Hn)
if (( soft >= 4096 )); then
  ok "open files soft limit ${soft}"
elif [[ "${hard}" == "unlimited" ]] || (( hard >= 4096 )); then
  warn "open files soft limit is ${soft}. Raise it for this shell with 'ulimit -n 4096' (the hard limit is ${hard}, so no root is needed)."
else
  warn "open files hard limit is ${hard}, below the 4096 Kafka and Spark want. Raising it needs root: add 'nofile=4096' to /etc/security/limits.conf."
fi

# WSL only, and only a warning: the repository working on /mnt/c is possible but
# every file read crosses a 9p mount and Spark shuffles become unusably slow.
if grep -qi microsoft /proc/version 2>/dev/null; then
  if [[ "$(pwd -P)" == /mnt/* ]]; then
    bad "this checkout is on a Windows drive ($(pwd -P)). Move it under your Linux home; 9p filesystem latency makes Spark shuffles pathologically slow."
  else
    ok "checkout is on the Linux filesystem"
  fi
fi
echo

# ---------------------------------------------------------------- toolchain
echo "toolchain"
if command -v uv >/dev/null 2>&1; then
  ok "uv $(uv --version | awk '{print $2}')"
else
  warn "uv is not installed. Needed only for 'make test' and 'make lint'; the stack itself runs without it. See https://docs.astral.sh/uv/."
fi
if command -v java >/dev/null 2>&1; then
  ok "java $(java -version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
else
  warn "no java on PATH. 'make test-spark' will skip; everything in Docker is unaffected."
fi
echo

# ------------------------------------------------------------------ verdict
if (( failures > 0 )); then
  printf '%s%d check(s) failed%s, %d warning(s). Fix the failures before running make up.\n' \
    "${RED}" "${failures}" "${OFF}" "${warnings}"
  exit 1
fi
if (( warnings > 0 )); then
  printf '%sReady%s, with %d warning(s) above.\n' "${GRN}" "${OFF}" "${warnings}"
else
  printf '%sReady.%s Run: make up-core\n' "${GRN}" "${OFF}"
fi
