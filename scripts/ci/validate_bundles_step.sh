#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "bundle validation" pr_dev pr_main merge_dev merge_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

ensure_target_branch_fetched

echo "Validating modified bundles (context: $(current_context))"
mapfile -t bundles < <(
  bash "${SCRIPT_DIR}/list_modified_bundles.sh" "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}" | jq -r '.[]'
)

if [[ "${#bundles[@]}" -eq 0 ]]; then
  echo "No modified bundles found."
  exit 0
fi

for bundle in "${bundles[@]}"; do
  uv run ./scripts/validate_bundles.sh --bundle "$bundle"
done
