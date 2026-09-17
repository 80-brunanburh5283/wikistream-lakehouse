#!/usr/bin/env bash
#
# Compare Kafka compression codecs on this project's actual payloads.
#
# `compression.type=zstd` is a default in config.py, and a default nobody measured
# is a default nobody can defend. This script produces the same number of live
# frames through each codec into its own throwaway topic, reads what the broker
# stored, and prints the ratios.
#
# Caveats, stated because they affect how much the numbers mean:
#
#   * Each codec gets a different sample of the live stream. The ratio is stable
#     to within a few percent across runs on this data, which is far narrower than
#     the gaps between codecs, so the ordering is reliable and the third digit is
#     not.
#   * Ratios depend on batch size, because a codec compresses a whole record batch
#     and a bigger batch shares more dictionary. Every run here uses the same
#     linger.ms and batch.size, so they are comparable to each other and to the
#     main measurement.
#   * This measures bytes and nothing else. CPU cost per byte is real and is not
#     measured here; at ~50 events/s it is not the constraint.
#
#   usage: scripts/compare_compression.sh [MAX_EVENTS] [SECONDS]
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

EVENT_LIMIT="${1:-2000}"
SECONDS_LIMIT="${2:-120}"
CODECS=(none gzip snappy lz4 zstd)
PREFIX="wiki.codectest"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
PARTITIONS="${WS_KAFKA_TOPIC_PARTITIONS:-3}"
BOOTSTRAP="kafka:29092"
KAFKA_BIN="/opt/kafka/bin"

for tool in docker jq; do
  command -v "${tool}" >/dev/null 2>&1 || { echo "error: ${tool} is required" >&2; exit 1; }
done

if ! docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  echo "error: the kafka service is not running. Start it with 'make up-core'." >&2
  exit 1
fi

in_kafka() { docker compose exec -T kafka "$@"; }

cleanup() {
  for codec in "${CODECS[@]}"; do
    in_kafka "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
      --delete --topic "${PREFIX}.${codec}" >/dev/null 2>&1 || true
  done
}
# The scratch topics exist only for this measurement; leaving them behind would
# make `make kafka-offsets` on the real topic harder to read.
trap cleanup EXIT

results=()
for codec in "${CODECS[@]}"; do
  topic="${PREFIX}.${codec}"
  echo "== ${codec}: ${EVENT_LIMIT} frames into ${topic}" >&2

  in_kafka "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions "${PARTITIONS}" --replication-factor 1 \
    --config retention.ms=600000 \
    --config compression.type=producer >/dev/null

  # `docker compose run -e` overrides the service's own environment, so the
  # producer sees a different codec and topic without editing .env.
  log=$(docker compose run --rm \
    -e "WS_KAFKA_TOPIC=${topic}" \
    -e "WS_KAFKA_COMPRESSION_TYPE=${codec}" \
    producer python -m wikistream.producer \
    --max-events "${EVENT_LIMIT}" --duration "${SECONDS_LIMIT}" 2>&1)

  summary=$(grep -h '"kafka sink flushed"' <<<"${log}" | tail -1)
  if [[ -z "${summary}" ]]; then
    echo "error: no flush report for ${codec}:" >&2
    echo "${log}" | tail -20 >&2
    exit 1
  fi
  produced=$(jq -r .produced <<<"${summary}")
  uncompressed=$(jq -r .bytes_produced <<<"${summary}")

  stored=$(in_kafka "${KAFKA_BIN}/kafka-log-dirs.sh" --bootstrap-server "${BOOTSTRAP}" \
    --topic-list "${topic}" --describe 2>/dev/null \
    | tail -1 | jq -r '[.brokers[].logDirs[].partitions[].size] | add // 0')

  results+=("${codec}	${produced}	${uncompressed}	${stored}")
done

printf '\n## Compression codecs, measured %s\n\n' "$(date -u +%Y-%m-%d)"
printf '| Codec | Frames | Uncompressed bytes | Stored bytes | Ratio | Stored per record |\n'
printf '|---|---|---|---|---|---|\n'
printf '%s\n' "${results[@]}" | awk -F'\t' '{
  ratio = ($4 > 0) ? $3 / $4 : 0
  per   = ($2 > 0) ? $4 / $2 : 0
  printf "| %s | %d | %d | %d | %.2fx | %.0f |\n", $1, $2, $3, $4, ratio, per
}'
printf '\n`none` is not literally uncompressed on disk: the stored figure still\n'
printf 'includes Kafka record-batch framing, so its ratio is slightly under 1.00x\n'
printf 'rather than exactly 1.00x.\n'
