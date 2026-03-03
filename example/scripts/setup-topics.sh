#!/usr/bin/env bash
# setup-topics.sh — creates the Kafka topics required by the flowgate example.
# Run from the project root: bash example/scripts/setup-topics.sh
#
# Requirements: Docker Compose stack running (docker compose up -d)
# Override the Kafka service name if your Compose project uses a different prefix:
#   KAFKA_SERVICE=my-kafka bash example/scripts/setup-topics.sh

set -euo pipefail

KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
BOOTSTRAP="localhost:9092"

# Topic configuration
declare -A TOPICS=(
  ["payments.order"]="3"   # topic_name => partition_count
)
REPLICATION_FACTOR=1
RETENTION_MS=604800000     # 7 days

echo "==> Waiting for Kafka broker to be ready..."
for i in $(seq 1 30); do
  if docker compose exec -T "$KAFKA_SERVICE" \
       kafka-topics --bootstrap-server "$BOOTSTRAP" --list &>/dev/null; then
    echo "    Kafka is ready."
    break
  fi
  echo "    Attempt $i/30 — broker not ready yet, retrying in 2s..."
  sleep 2
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Kafka broker did not become ready in time." >&2
    exit 1
  fi
done

echo ""
echo "==> Creating topics..."
for topic in "${!TOPICS[@]}"; do
  partitions="${TOPICS[$topic]}"
  echo "    Topic: $topic  (partitions=$partitions, replication=$REPLICATION_FACTOR)"
  docker compose exec -T "$KAFKA_SERVICE" \
    kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor "$REPLICATION_FACTOR" \
      --config retention.ms="$RETENTION_MS"
done

echo ""
echo "==> Current topic list:"
docker compose exec -T "$KAFKA_SERVICE" \
  kafka-topics --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "==> Topic details:"
for topic in "${!TOPICS[@]}"; do
  docker compose exec -T "$KAFKA_SERVICE" \
    kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic"
done

echo ""
echo "Done. Topics are ready."
