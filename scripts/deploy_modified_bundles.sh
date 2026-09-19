#!/usr/bin/env bash
#
# Checks for modified bundles between two branches and runs `databricks bundle deploy`
# on each bundle that has changes. The profile is determined by the
# DATABRICKS_PROFILE environment variable.
#
# Usage:
#   ./deploy_modified_bundles.sh <CURRENT_BRANCH> <TARGET_BRANCH> [--debug-script]
#
set -euo pipefail

# --- Argument Parsing ---
DEBUG_MODE=0
if [[ "${@: -1}" == "--debug-script" ]]; then
  DEBUG_MODE=1
  set -- "${@:1:$(($#-1))}"
fi

if [[ $# -ne 2 ]]; then
  echo "❌ Error: Invalid arguments." >&2
  echo "Usage: $0 <CURRENT_BRANCH> <TARGET_BRANCH> [--debug-script]" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
ROOT_DIR="$(git rev-parse --show-toplevel)"
ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

git checkout -q "$CURRENT_BRANCH"
trap 'git checkout -q "$ORIGINAL_BRANCH"' EXIT

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────

# --- Generate Action Plan ---
echo "▶ Analyzing differences to identify bundles to deploy..."
declare -a BUNDLES_TO_DEPLOY=()
BUNDLES_FOUND=false

for bundle_path in "$ROOT_DIR"/bundles/*; do
  [[ -d "$bundle_path" && -f "$bundle_path/databricks.yml" ]] || continue

  bundle_name=$(basename "$bundle_path")
  rel_bundle_path="bundles/$bundle_name"

  # Only consider relevant extensions within this bundle (compare target ⇄ current)
  if mapfile -t __changed < <(git diff --name-only "$TARGET_BRANCH" "$CURRENT_BRANCH" -- "$rel_bundle_path" | grep -E '\.(yml|yaml|py|sql|ipynb)$'); then
    if [[ ${#__changed[@]} -gt 0 ]]; then
    echo "  Found changes in bundle: $bundle_name"
    BUNDLES_TO_DEPLOY+=("$bundle_path")
    BUNDLES_FOUND=true
    fi
  fi
done

# --- Execute Action Plan ---
if [[ $BUNDLES_FOUND == false ]]; then
  echo "✔ No modified bundles to deploy."
  exit 0
fi

PROFILE_TO_USE="${DATABRICKS_PROFILE:-DEFAULT}"
echo -e "\n▶ Executing 'databricks bundle deploy' for modified bundles (profile: $PROFILE_TO_USE)..."
for bundle_path in "${BUNDLES_TO_DEPLOY[@]}"; do
  COMMAND="(cd '$bundle_path' && databricks bundle deploy --auto-approve --target '$PROFILE_TO_USE')"
  if (( DEBUG_MODE == 1 )); then
    echo "DEBUG: $COMMAND"
  else
    echo "EXEC: $COMMAND"
    eval "$COMMAND"
  fi
done

printf '\n✔ Done.\n'
