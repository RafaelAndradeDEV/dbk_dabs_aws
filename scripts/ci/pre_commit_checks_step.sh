#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "pre-commit checks" pr_dev pr_main merge_dev merge_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

echo "Running pre-commit checks (context: $(current_context))"
uv run pre-commit run --all-files
