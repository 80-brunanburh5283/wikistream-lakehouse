#!/usr/bin/env bash
#
# Peak resident memory per container, sampled.
#
# `docker stats` reports an instant, and the number that decides whether this stack
# fits in a 12 GB WSL box is the peak — which happens during a Spark micro-batch or a
# Trino query, not while nothing is running. So this samples and keeps the maximum
# per container rather than printing one reading.
#
# The sum at the bottom is a sum of peaks and not a peak of sums: no two containers
# are guaranteed to hit their maximum at the same moment, so treat it as an upper
# bound on the stack's footprint rather than as a measurement of it.

set -euo pipefail

SECONDS_TO_SAMPLE="${1:-300}"
INTERVAL="${2:-10}"

command -v docker >/dev/null || {
  echo "docker not found" >&2
  exit 1
}

samples=0
deadline=$(($(date +%s) + SECONDS_TO_SAMPLE))
raw=$(mktemp)
trap 'rm -f "$raw"' EXIT

echo "Sampling every ${INTERVAL}s for ${SECONDS_TO_SAMPLE}s. Run the stack under load for a useful peak."

while [ "$(date +%s)" -lt "$deadline" ]; do
  # No --no-trunc: names are short here. MemUsage is "used / limit"; only the first
  # field is this container's.
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' >>"$raw" || true
  samples=$((samples + 1))
  sleep "$INTERVAL"
done

echo
echo "samples: $samples"
echo

printf '%-30s %10s\n' "container" "peak MiB"

# Sorted by peak, descending, so the container to argue with is the first line.
# `asorti` would be neater and is gawk-only; mawk is what Debian images ship.
awk '
  function mib(v,   n, u) {
    n = v + 0
    u = v
    sub(/^[0-9.]+/, "", u)
    if (u ~ /^GiB/) return n * 1024
    if (u ~ /^MiB/) return n
    if (u ~ /^KiB/) return n / 1024
    if (u ~ /^B/)   return n / 1048576
    return n
  }
  { v = mib($2); if (v > peak[$1]) peak[$1] = v }
  END { for (c in peak) printf "%s %.0f\n", c, peak[c] }
' "$raw" | sort -k2 -nr | awk '
  { total += $2; printf "%-30s %10d\n", $1, $2 }
  END { printf "%-30s %10d\n", "sum of peaks", total }
'
