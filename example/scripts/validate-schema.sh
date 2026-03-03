#!/usr/bin/env bash
# validate-schema.sh — registers Order v1 and v2 schemas with Confluent Schema Registry
# and confirms v2 is BACKWARD compatible with v1.
#
# Run from the project root: bash example/scripts/validate-schema.sh
#
# Requirements: Schema Registry running (docker compose up -d)
# Override registry URL:
#   SCHEMA_REGISTRY_URL=http://my-registry:8081 bash example/scripts/validate-schema.sh

set -euo pipefail

REGISTRY="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SUBJECT="payments.order-value"
V1_SCHEMA_FILE="example/schemas/order/v1/order.avsc"
V2_SCHEMA_FILE="example/schemas/order/v2/order.avsc"

# ── helpers ──────────────────────────────────────────────────────────────────

check_deps() {
  for cmd in curl jq; do
    if ! command -v "$cmd" &>/dev/null; then
      echo "ERROR: '$cmd' is required but not installed." >&2
      exit 1
    fi
  done
}

wait_for_registry() {
  echo "==> Waiting for Schema Registry to be ready at $REGISTRY..."
  for i in $(seq 1 30); do
    if curl -sf "$REGISTRY/subjects" &>/dev/null; then
      echo "    Schema Registry is ready."
      return 0
    fi
    echo "    Attempt $i/30 — not ready yet, retrying in 2s..."
    sleep 2
  done
  echo "ERROR: Schema Registry did not become ready in time." >&2
  exit 1
}

# Wraps a raw Avro .avsc file into the JSON body required by the Confluent REST API:
#   {"schema": "<escaped JSON string>"}
schema_payload() {
  local file="$1"
  jq -Rs '{"schema": .}' "$file"
}

register_schema() {
  local subject="$1" file="$2" label="$3"
  echo ""
  echo "==> Registering $label schema ($file) under subject '$subject'..."
  local payload
  payload=$(schema_payload "$file")
  local response
  response=$(curl -sf \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$payload" \
    "$REGISTRY/subjects/$subject/versions")
  local schema_id
  schema_id=$(echo "$response" | jq -r '.id')
  echo "    Registered with schema ID: $schema_id"
  echo "$schema_id"
}

check_compatibility() {
  local subject="$1" file="$2" label="$3"
  echo ""
  echo "==> Checking BACKWARD compatibility of $label schema against registered versions..."
  local payload
  payload=$(schema_payload "$file")
  local response
  response=$(curl -sf \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$payload" \
    "$REGISTRY/compatibility/subjects/$subject/versions/latest")
  local is_compatible
  is_compatible=$(echo "$response" | jq -r '.is_compatible')
  if [ "$is_compatible" = "true" ]; then
    echo "    COMPATIBLE: $label schema is backward compatible. ✓"
  else
    echo "    INCOMPATIBLE: $label schema is NOT backward compatible!" >&2
    echo "    Response: $response" >&2
    exit 1
  fi
}

list_versions() {
  local subject="$1"
  echo ""
  echo "==> Registered versions for subject '$subject':"
  curl -sf "$REGISTRY/subjects/$subject/versions" | jq .
}

# ── main ─────────────────────────────────────────────────────────────────────

check_deps
wait_for_registry

# Set compatibility mode to BACKWARD (idempotent if already set)
echo ""
echo "==> Setting compatibility mode to BACKWARD for subject '$SUBJECT'..."
curl -sf \
  -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' \
  "$REGISTRY/config/$SUBJECT" | jq .

# Register v1
register_schema "$SUBJECT" "$V1_SCHEMA_FILE" "v1"

# Check v2 compatibility before registering
check_compatibility "$SUBJECT" "$V2_SCHEMA_FILE" "v2"

# Register v2
register_schema "$SUBJECT" "$V2_SCHEMA_FILE" "v2"

# Show all registered versions
list_versions "$SUBJECT"

echo ""
echo "Schema validation complete. Both schemas are registered and compatible."
