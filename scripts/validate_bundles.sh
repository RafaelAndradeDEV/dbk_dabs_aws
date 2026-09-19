#!/usr/bin/env bash
#
# Loops through bundle projects and runs `databricks bundle validate` on each bundle.
#
# Usage:
#   ./validate_bundles.sh [--bundle <name>]
#

# Parse optional --bundle flag to validate a single named bundle
SINGLE_BUNDLE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle) SINGLE_BUNDLE="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# Use provided profile and target, fallback to defaults for consistency with other scripts
PROFILE_TO_USE="${DATABRICKS_PROFILE:-DEFAULT}"
TARGET_TO_USE="${DATABRICKS_BUNDLE_TARGET:-dev}"

ROOT_DIR="$(git rev-parse --show-toplevel)"
FIND_BUNDLES_SCRIPT="$ROOT_DIR/scripts/helpers/find_bundles.sh"

mapfile -t ALL_BUNDLE_PATHS < <(bash "$FIND_BUNDLES_SCRIPT" "$ROOT_DIR/bundles")

for bundle_path in "${ALL_BUNDLE_PATHS[@]}"; do
  bundle_name="${bundle_path#$ROOT_DIR/bundles/}"   # relative name for display
  if [[ -n "$SINGLE_BUNDLE" && "$bundle_name" != "$SINGLE_BUNDLE" ]]; then
    continue
  fi
  echo "Validating bundle: $bundle_name (profile: $PROFILE_TO_USE, target: $TARGET_TO_USE)"
  (cd "$bundle_path" && databricks bundle validate -t "$TARGET_TO_USE")
done
