#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "deploy modified bundles" pr_dev pr_main merge_dev merge_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

ensure_target_branch_fetched

target="${DATABRICKS_BUNDLE_TARGET:-unknown}"
echo "Deploying modified bundles to ${target} (context: $(current_context))"
uv run scripts/manage_modified_bundles.sh deploy "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}"
