#!/usr/bin/env bash
# Combined deploy -> dry-run -> integration -> destroy lifecycle for Bitbucket
# Pipelines, which lacks a cross-step "always run" / "after stage" mechanism.
#
# Each inner step.sh self-gates on PIPELINE_CONTEXT via require_context, so we
# can call them all unconditionally here and let them skip when not applicable.
# A bash trap guarantees destroy fires whether deploy / dry-run / integration
# succeed or fail.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  rc=$?
  # Don't let destroy mask the original failure code, but always attempt it.
  bash "${SCRIPT_DIR}/destroy_modified_bundles_step.sh" || true
  exit "${rc}"
}
trap cleanup EXIT

bash "${SCRIPT_DIR}/deploy_modified_bundles_step.sh"
bash "${SCRIPT_DIR}/dry_run_pipelines_step.sh"
bash "${SCRIPT_DIR}/integration_tests_step.sh"
