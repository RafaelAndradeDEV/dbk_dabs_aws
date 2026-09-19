#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "destroy modified bundles" pr_dev pr_main merge_dev

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

ensure_target_branch_fetched

target="${DATABRICKS_BUNDLE_TARGET:-unknown}"
echo "Destroying modified bundles for ${target} (context: $(current_context))"
uv run scripts/manage_modified_bundles.sh destroy "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}"
