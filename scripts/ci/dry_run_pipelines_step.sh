#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "dry-run pipelines" pr_dev pr_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

ensure_target_branch_fetched

target="${DATABRICKS_BUNDLE_TARGET:-unknown}"
echo "Running dry-run validation on modified pipelines: ${target} (context: $(current_context))"
uv run scripts/run_on_modified_resources.sh "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}" "run --validate-only"
