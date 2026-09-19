#!/usr/bin/env bash
# Print modified bundle names (one per line) between two refs.
# A bundle is "modified" when at least one .yml/.yaml/.py/.sql/.ipynb file
# under bundles/<name>/ changed between TARGET_BRANCH and CURRENT_BRANCH.
#
# Usage: find_modified_bundles.sh <CURRENT_BRANCH> <TARGET_BRANCH>
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <CURRENT_BRANCH> <TARGET_BRANCH>" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
ROOT_DIR="$(git rev-parse --show-toplevel)"
FIND_BUNDLES_SCRIPT="$ROOT_DIR/scripts/helpers/find_bundles.sh"

mapfile -t ALL_BUNDLE_PATHS < <(bash "$FIND_BUNDLES_SCRIPT" "$ROOT_DIR/bundles")

for bundle_path in "${ALL_BUNDLE_PATHS[@]}"; do
  rel_bundle_path="${bundle_path#"$ROOT_DIR"/}"
  bundle_name="${rel_bundle_path#bundles/}"
  mapfile -t __changed < <(
    git diff --name-only "$TARGET_BRANCH" "$CURRENT_BRANCH" -- "$rel_bundle_path" \
      | grep -E '\.(yml|yaml|py|sql|ipynb)$' || true
  )
  if [[ ${#__changed[@]} -gt 0 ]]; then
    echo "$bundle_name"
  fi
done
