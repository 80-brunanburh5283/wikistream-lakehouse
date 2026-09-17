#!/usr/bin/env bash
#
# Measure ingest: throughput, on-disk compression, and partition balance.
#
# Runs a bounded producer, then compares what it says it sent against what the
# broker actually stored. Nothing here is estimated — the uncompressed total comes
# from the producer's own counter and the stored total from kafka-log-dirs.sh, so
# the ratio is the real one for this data on this broker configuration.
#
# Output is a markdown table, because it goes into docs/throughput.md and the
# README, and a number in a document that cannot be regenerated is a number
# nobody can check.
#
#   usage: scripts/measure_throughput.sh [SECONDS] [MAX_EVENTS]
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

SECONDS_LIMIT="${1:-60}"
EVENT_LIMIT="${2:-2000}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
TOPIC="${WS_KAFKA_TOPIC:-wiki.recentchange}"
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

# Bytes the broker has on disk for this topic's partitions, after compression.
log_bytes() {
  in_kafka "${KAFKA_BIN}/kafka-log-dirs.sh" --bootstrap-server "${BOOTSTRAP}" \
    --topic-list "${TOPIC}" --describe 2>/dev/null \
    | tail -1 \
    | jq -r '[.brokers[].logDirs[].partitions[].size] | add // 0'
}

# End offset per partition, as "partition<TAB>offset".
offsets() {
  in_kafka "${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server "${BOOTSTRAP}" \
    --topic "${TOPIC}" | awk -F: '{print $2"\t"$3}' | sort -n
}

echo "measuring ${SECONDS_LIMIT}s / ${EVENT_LIMIT} events into ${TOPIC}" >&2

bytes_before=$(log_bytes)
offsets_before=$(offsets)

# The long-lived service would otherwise contribute offsets this run did not
# produce, and the whole point is attribution.
docker compose stop producer >/dev/null 2>&1 || true

run_log=$(mktemp)
trap 'rm -f "${run_log}"' EXIT
docker compose run --rm producer \
  python -m wikistream.producer \
  --duration "${SECONDS_LIMIT}" --max-events "${EVENT_LIMIT}" 2>&1 | tee "${run_log}" >&2

# The flush line is the authoritative one: it is written after the queue drained,
# so `delivered` there is what the broker acknowledged rather than what was queued.
summary=$(grep -h '"kafka sink flushed"' "${run_log}" | tail -1)
if [[ -z "${summary}" ]]; then
  echo "error: the producer did not report a flush; see the output above" >&2
  exit 1
fi

produced=$(jq -r .produced <<<"${summary}")
delivered=$(jq -r .delivered <<<"${summary}")
failed=$(jq -r .failed <<<"${summary}")
uncompressed=$(jq -r .bytes_produced <<<"${summary}")
elapsed=$(grep -h '"ingest loop finished"' "${run_log}" | tail -1 | jq -r .seconds)

bytes_after=$(log_bytes)
offsets_after=$(offsets)
stored=$((bytes_after - bytes_before))

printf '\n## Ingest, measured %s\n\n' "$(date -u +%Y-%m-%d)"
printf '| Metric | Value |\n|---|---|\n'
printf '| Frames produced | %s |\n' "${produced}"
printf '| Frames acknowledged by the broker | %s |\n' "${delivered}"
printf '| Delivery failures | %s |\n' "${failed}"
printf '| Wall clock | %s s |\n' "${elapsed}"
printf '| Throughput | %s events/s |\n' "$(awk -v p="${produced}" -v e="${elapsed}" 'BEGIN{printf "%.1f", p/e}')"
printf '| Uncompressed payload | %s bytes |\n' "${uncompressed}"
printf '| Stored on the broker | %s bytes |\n' "${stored}"
printf '| Compression ratio (%s) | %sx |\n' \
  "${WS_KAFKA_COMPRESSION_TYPE:-zstd}" \
  "$(awk -v u="${uncompressed}" -v s="${stored}" 'BEGIN{printf "%.2f", (s>0)? u/s : 0}')"
printf '| Stored bytes per record | %s |\n' \
  "$(awk -v s="${stored}" -v p="${produced}" 'BEGIN{printf "%.0f", (p>0)? s/p : 0}')"
printf '| Mean payload per record | %s bytes |\n' \
  "$(awk -v u="${uncompressed}" -v p="${produced}" 'BEGIN{printf "%.0f", (p>0)? u/p : 0}')"

printf '\n### Partition balance\n\n'
printf '| Partition | Records this run | Share |\n|---|---|---|\n'
# awk rather than join(1): join wants lexically sorted keys, which stops matching
# numeric partition ids at 10, and this stack is one `--partitions` flag away
# from having more than ten.
awk -v total="${produced}" -F'\t' '
  NR == FNR { before[$1] = $2; next }
  { delta = $2 - before[$1]
    printf "| %s | %d | %.1f%% |\n", $1, delta, (total > 0) ? 100 * delta / total : 0 }
' <(echo "${offsets_before}") <(echo "${offsets_after}")
printf '\nThe key is the wiki domain, so the balance is `hash(domain) %% partitions`\n'
printf 'and it is not expected to be even; see DECISIONS.md ADR-0008.\n'
