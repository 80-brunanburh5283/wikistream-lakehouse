#!/usr/bin/env bash
#
# Run one of the streaming jobs in the Spark container, and stop it when this
# terminal stops.
#
#   scripts/stream.sh bronze          # continuous, 30-second trigger
#   scripts/stream.sh silver --once   # one batch over everything available, then exit
#
# Why this exists rather than a plain `docker compose exec` line in the Makefile, which
# is how every other stack target is written. `docker exec` does not forward signals to
# the process it starts, and the Makefile runs it with `-T`, so there is no TTY for the
# terminal's own Ctrl-C to travel down either. Pressing Ctrl-C on `make stream-bronze`
# killed the local client and left the JVM inside the container writing to Iceberg: a
# stream still committing, with nowhere left to print, which the next `make
# stream-bronze` meets as `Multiple streaming queries are concurrently using
# .../commits`. Measured on 2026-09-18: the driver was still there 15 seconds after the
# client exited, and the container's logs held nothing about it.
#
# The trap below sends the signal over a second exec instead. Spark's shutdown hook
# stops the query, so the last thing in the checkpoint is a complete batch. That is also
# true after a SIGKILL — `tests/e2e/test_restart_idempotency.py` is the proof — but
# "recoverable from a hard kill" is not a reason to leave Ctrl-C broken.
set -euo pipefail

layer=${1:?usage: stream.sh bronze|silver [--once]}
shift

case "$layer" in
bronze | silver) ;;
*)
    echo "stream.sh: unknown layer '$layer'; expected bronze or silver" >&2
    exit 2
    ;;
esac

# `docker compose` is two words, so it cannot be one quoted variable.
read -r -a compose <<<"${COMPOSE:-docker compose}"

# The Spark app name carries this shell's pid, so the trap stops this run and not a
# stream somebody else started in another terminal — for the other layer or for this
# one. It is also what shows up in the Spark UI on port 4040.
app_name="${layer}-$$"
job="/opt/wikistream/src/wikistream/streaming/${layer}.py"
stopped=0

# shellcheck disable=SC2329  # invoked by the trap below, which shellcheck cannot see
stop() {
    stopped=1
    # `pkill -f` matches the whole command line, where the app name appears exactly
    # once. TERM rather than KILL: a graceful stop is the entire point of the trap.
    "${compose[@]}" exec -T spark pkill -TERM -f "SparkSubmit --name ${app_name}" || true
}
trap stop INT TERM

"${compose[@]}" exec -T spark /opt/spark/bin/spark-submit --name "$app_name" "$job" "$@" &
submit=$!

# Backgrounded and waited on, because bash runs a trap only between commands: with
# spark-submit in the foreground the handler would not run until the JVM had already
# exited on its own. A signal makes `wait` return early, so it is called again until the
# job is really reaped — otherwise this script exits while the JVM is mid-shutdown.
set +e
wait "$submit"
status=$?
while ((stopped)) && kill -0 "$submit" 2>/dev/null; do
    wait "$submit"
    status=$?
done
set -e

if ((stopped)); then
    echo "stream stopped. The checkpoint is at the last complete batch;" \
        "'make stream-${layer}' resumes from it." >&2
    exit 0
fi
exit "$status"
