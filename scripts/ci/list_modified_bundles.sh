#!/usr/bin/env bash
#
# Outputs modified bundle names as a JSON array.
# Only bundles with relevant changes (*.yml, *.yaml, *.py, *.sql, *.ipynb) are included.
#
# Usage:
#   list_modified_bundles.sh <CURRENT_BRANCH> <TARGET_BRANCH>
#
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <CURRENT_BRANCH> <TARGET_BRANCH>" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
ROOT_DIR="$(git rev-parse --show-toplevel)"

declare -A MODIFIED_SET=()
mapfile -t MODIFIED < <(
  bash "$ROOT_DIR/scripts/helpers/find_modified_bundles.sh" "$CURRENT_BRANCH" "$TARGET_BRANCH"
)
for bundle in "${MODIFIED[@]}"; do
  MODIFIED_SET["$bundle"]=1
done

if [[ ${#MODIFIED_SET[@]} -eq 0 ]]; then
  echo "[]"
  exit 0
fi

printf '%s\n' "${!MODIFIED_SET[@]}" | jq -R . | jq -cs .
