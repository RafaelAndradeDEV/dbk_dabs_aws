#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "compare bundle resources" pr_dev pr_main merge_dev merge_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

ensure_target_branch_fetched

target="${DATABRICKS_BUNDLE_TARGET:-unknown}"
echo "Comparing bundle resources: ${BITBUCKET_BRANCH} ⇄ ${TARGET_BRANCH} (target: ${target}, context: $(current_context))"
uv run ./scripts/compare_bundle_resources.sh "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}"
