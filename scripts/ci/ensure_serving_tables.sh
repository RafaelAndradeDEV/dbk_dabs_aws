#!/usr/bin/env bash
#
# Makes sure the gold tables a Genie space reads exist before its bundle deploys
# (Genie validates its tables at create time).
#
# Only acts when the bundle deploys a Genie space for DATABRICKS_BUNDLE_TARGET.
# If tables are missing, runs the DLT seed job (seed -> pipeline refresh) once,
# then polls until every table exists or WAIT_TIMEOUT_SECONDS elapses.
#
# Usage:
#   ensure_serving_tables.sh <bundle_name>
#
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <bundle_name>" >&2
  exit 1
fi

BUNDLE="$1"
TARGET="${DATABRICKS_BUNDLE_TARGET:-dev}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-30}"
ROOT_DIR="$(git rev-parse --show-toplevel)"
UPSTREAM_BUNDLE_DIR="$ROOT_DIR/bundles/marketing_rfm_ltv_dlt"
UPSTREAM_JOB="marketing_rfm_ltv_seed_data"

bundle_json=$(cd "$ROOT_DIR/bundles/$BUNDLE" && databricks bundle validate -t "$TARGET" -o json)

# Fully-qualified table identifiers referenced by the Genie space(s) in this target
mapfile -t TABLES < <(
  jq -r '.resources.genie_spaces // {} | .[].serialized_space | fromjson | .data_sources.tables[].identifier' \
    <<< "$bundle_json"
)

if [[ ${#TABLES[@]} -eq 0 ]]; then
  echo "✔ '$BUNDLE' deploys no Genie space for target '$TARGET' — nothing to wait for."
  exit 0
fi

missing_tables() {
  local table
  for table in "${TABLES[@]}"; do
    databricks tables get "$table" > /dev/null 2>&1 || echo "$table"
  done
}

mapfile -t MISSING < <(missing_tables)
if [[ ${#MISSING[@]} -eq 0 ]]; then
  echo "✔ All Genie tables exist in target '$TARGET'."
  exit 0
fi

echo "▶ Missing tables: ${MISSING[*]}"
echo "▶ Running '$UPSTREAM_JOB' (seed -> pipeline refresh) in target '$TARGET'..."
(cd "$UPSTREAM_BUNDLE_DIR" && databricks bundle run -t "$TARGET" "$UPSTREAM_JOB")

deadline=$(( SECONDS + WAIT_TIMEOUT_SECONDS ))
while true; do
  mapfile -t MISSING < <(missing_tables)
  if [[ ${#MISSING[@]} -eq 0 ]]; then
    echo "✔ All Genie tables exist in target '$TARGET'."
    exit 0
  fi
  if (( SECONDS >= deadline )); then
    echo "❌ Timed out after ${WAIT_TIMEOUT_SECONDS}s waiting for: ${MISSING[*]}" >&2
    exit 1
  fi
  echo "  waiting ${POLL_INTERVAL_SECONDS}s for: ${MISSING[*]}"
  sleep "$POLL_INTERVAL_SECONDS"
done
