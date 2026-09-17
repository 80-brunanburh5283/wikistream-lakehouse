#!/usr/bin/env bash
#
# Create (or reconcile) the ingest topic. Idempotent: safe on every `make up`.
#
# Topics are created here rather than by letting the broker auto-create them,
# because auto-creation is off in docker-compose.yml. A typo in a topic name
# should be an error, not a new one-partition topic that behaves almost right.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck disable=SC1091
[[ -f .env ]] && set -a && source .env && set +a

TOPIC="${WS_KAFKA_TOPIC:-wiki.recentchange}"
PARTITIONS="${WS_KAFKA_TOPIC_PARTITIONS:-3}"
RETENTION_MS="${WS_KAFKA_RETENTION_MS:-86400000}"

# The in-network listener. The host listener advertises localhost:<published
# port>, which is unreachable from inside the container this runs in.
BOOTSTRAP="kafka:29092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"

compose() { docker compose "$@"; }
in_kafka() { compose exec -T kafka "$@"; }

if ! compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  echo "error: the kafka service is not running. Start it with 'make up-core'." >&2
  exit 1
fi

echo "topic:       ${TOPIC}"
echo "partitions:  ${PARTITIONS}"
echo "retention:   ${RETENTION_MS} ms"
echo

# --if-not-exists makes the create idempotent. The --config flags apply only on
# creation, which is why the retention is set again below for the already-exists
# path: otherwise a topic created before someone edited .env keeps the old value
# forever and the file lies about what is running.
in_kafka "${KAFKA_TOPICS}" \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}" \
  --config cleanup.policy=delete \
  --config compression.type=producer

current=$(in_kafka "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
  --describe --topic "${TOPIC}" | grep -c $'\tPartition:' || true)

if (( current < PARTITIONS )); then
  # Kafka can add partitions but never remove them. Worth knowing before you do
  # it: the key-to-partition mapping is hash(key) % partitions, so adding
  # partitions sends an existing key to a different partition from then on. Old
  # records stay where they are, so per-wiki ordering holds within each side of
  # the change but not across it. Acceptable here because the consumer
  # deduplicates on event id rather than relying on offset order.
  echo "growing ${TOPIC} from ${current} to ${PARTITIONS} partitions"
  in_kafka "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
    --alter --topic "${TOPIC}" --partitions "${PARTITIONS}"
elif (( current > PARTITIONS )); then
  echo "warning: ${TOPIC} has ${current} partitions, more than the requested" \
       "${PARTITIONS}. Kafka cannot shrink a topic; delete it with" \
       "'make clean' if you need fewer." >&2
fi

# Reconcile retention on an existing topic. Separate call because --config on
# --create is ignored when the topic is already there.
in_kafka "${KAFKA_CONFIGS}" --bootstrap-server "${BOOTSTRAP}" \
  --alter --entity-type topics --entity-name "${TOPIC}" \
  --add-config "retention.ms=${RETENTION_MS}" >/dev/null

in_kafka "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
