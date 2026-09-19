#!/usr/bin/env bash
set -euo pipefail

# Utility helpers shared by CI pipeline step scripts.

current_context() {
  echo "${PIPELINE_CONTEXT:-unset}"
}

should_run_for_context() {
  local ctx allowed
  ctx="${PIPELINE_CONTEXT:-skip}"
  for allowed in "$@"; do
    if [[ "$ctx" == "$allowed" ]]; then
      return 0
    fi
  done
  return 1
}

skip_step() {
  local desc="${1:-step}"
  printf "Skipping %s for context: %s\n" "$desc" "$(current_context)"
  exit 0
}

require_context() {
  local desc="$1"
  shift
  if ! should_run_for_context "$@"; then
    skip_step "$desc"
  fi
}

ensure_target_branch_fetched() {
  if [[ -z "${TARGET_BRANCH:-}" ]]; then
    echo "TARGET_BRANCH is not set; skipping fetch"
    return 0
  fi
  local fetch_branch
  fetch_branch="${TARGET_BRANCH%%~*}"
  if [[ -z "$fetch_branch" ]]; then
    echo "Unable to derive fetch branch from TARGET_BRANCH=${TARGET_BRANCH}"
    return 0
  fi
  git fetch origin "${fetch_branch}:${fetch_branch}" || git fetch origin "${fetch_branch}"
}
