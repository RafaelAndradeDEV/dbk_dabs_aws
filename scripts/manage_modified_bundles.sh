#!/usr/bin/env bash
#
# Unified script to deploy or destroy modified bundles between two branches.
# The profile is determined by the DATABRICKS_PROFILE environment variable.
#
# Usage:
#   ./manage_modified_bundles.sh <deploy|destroy> <CURRENT_BRANCH> <TARGET_BRANCH> [--debug-script]
#
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Argument Parsing
# ──────────────────────────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
  echo "❌ Error: Action required." >&2
  echo "Usage: $0 <deploy|destroy> <CURRENT_BRANCH> <TARGET_BRANCH> [--debug-script]" >&2
  exit 1
fi

ACTION="$1"
shift

# Validate action
if [[ "$ACTION" != "deploy" && "$ACTION" != "destroy" ]]; then
  echo "❌ Error: Invalid action '$ACTION'. Must be 'deploy' or 'destroy'." >&2
  exit 1
fi

DEBUG_MODE=0
SINGLE_BUNDLE=""
REMAINING_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug-script) DEBUG_MODE=1; shift ;;
    --bundle) SINGLE_BUNDLE="$2"; shift 2 ;;
    *) REMAINING_ARGS+=("$1"); shift ;;
  esac
done
set -- ${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}

if [[ $# -ne 2 ]]; then
  echo "❌ Error: Invalid arguments." >&2
  echo "Usage: $0 <deploy|destroy> <CURRENT_BRANCH> <TARGET_BRANCH> [--bundle <name>] [--debug-script]" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
ROOT_DIR="$(git rev-parse --show-toplevel)"
ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# Destroy should never run on merges to main/master; exit gracefully if that happens.
if [[ "$ACTION" == "destroy" && ("$CURRENT_BRANCH" == "main" || "$CURRENT_BRANCH" == "master") ]]; then
  echo "✔ Skipping destroy: current branch is 'main'."
  exit 0
fi

git checkout -q "$CURRENT_BRANCH"
trap 'git checkout -q "$ORIGINAL_BRANCH"' EXIT

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────

# --- Generate Action Plan ---
ACTION_VERB="${ACTION^}"  # Capitalize first letter for display
declare -a BUNDLES_TO_PROCESS=()
declare -A BUNDLE_PATH_MAP=()
BUNDLES_FOUND=false

if [[ -n "$SINGLE_BUNDLE" ]]; then
  echo "▶ Single-bundle mode: ${ACTION} ${SINGLE_BUNDLE}"
  BUNDLES_TO_PROCESS=("$SINGLE_BUNDLE")
  BUNDLE_PATH_MAP["$SINGLE_BUNDLE"]="$ROOT_DIR/bundles/$SINGLE_BUNDLE"
  BUNDLES_FOUND=true
else
  echo "▶ Analyzing differences to identify bundles to ${ACTION}..."
  mapfile -t MODIFIED < <(
    bash "$ROOT_DIR/scripts/helpers/find_modified_bundles.sh" "$CURRENT_BRANCH" "$TARGET_BRANCH"
  )
  for bundle_name in "${MODIFIED[@]}"; do
    echo "  Found changes in bundle: $bundle_name"
    BUNDLES_TO_PROCESS+=("$bundle_name")
    BUNDLE_PATH_MAP["$bundle_name"]="$ROOT_DIR/bundles/$bundle_name"
    BUNDLES_FOUND=true
  done
fi

# --- Execute Action Plan ---
if [[ $BUNDLES_FOUND == false ]]; then
  echo "✔ No modified bundles to ${ACTION}."
  exit 0
fi

PROFILE_TO_USE="${DATABRICKS_PROFILE:-DEFAULT}"
echo -e "\n▶ Executing 'databricks bundle ${ACTION}' for modified bundles (profile: $PROFILE_TO_USE)..."
for bundle_name in "${BUNDLES_TO_PROCESS[@]}"; do
  bundle_path="${BUNDLE_PATH_MAP[$bundle_name]:-"$ROOT_DIR/bundles/$bundle_name"}"
  if [[ ! -d "$bundle_path" ]]; then
    echo "⚠ Skipping bundle '$bundle_name': directory not found at $bundle_path"
    continue
  fi
  COMMAND="(cd '$bundle_path' && databricks bundle ${ACTION} --auto-approve --target '$PROFILE_TO_USE')"
  if (( DEBUG_MODE == 1 )); then
    echo "DEBUG: $COMMAND"
  else
    echo "EXEC: $COMMAND"
    eval "$COMMAND"
  fi
done

printf '\n✔ Done.\n'
