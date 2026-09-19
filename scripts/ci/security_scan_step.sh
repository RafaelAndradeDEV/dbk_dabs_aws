#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_context "security scan" pr_dev pr_main merge_dev merge_main

source "${SCRIPT_DIR}/bootstrap_tools.sh"
uv sync --locked --dev

echo "Running security scan (context: $(current_context))"
mkdir -p ci-artifacts

rc=0
uv run bandit -c pyproject.toml -r bundles/ -f json -o ci-artifacts/bandit-report.json || rc=$?
uv run pip-audit -f json -o ci-artifacts/pip-audit.json || {
  audit_rc=$?
  if [[ "$rc" -eq 0 ]]; then
    rc=$audit_rc
  fi
}

exit "$rc"
