# CI Step Scripts

This directory centralizes per-step shell wrappers. Each file handles the
repeatable logic for deciding whether a step runs for the current
`PIPELINE_CONTEXT`, performs any shared setup, and then calls the underlying
“core” script in `scripts/`.

> CI now runs on GitHub Actions (`.github/workflows/ci.yml`), which invokes the
> core `scripts/*.sh` directly per bundle (matrix jobs) rather than these
> `*_step.sh` wrappers. The wrappers are retained for local use and other CI
> backends that still drive steps through `PIPELINE_CONTEXT`.

## Shared utilities

- `bootstrap_tools.sh`
  Installs or verifies the baseline tools expected by Bitbucket steps: `curl`,
  `git`, `jq`, `uv`, and the pinned Databricks CLI. Wrappers source this script
  after their context guard because Bitbucket steps run in fresh containers.

- `common.sh`
  Functions exported for the wrappers:
  - `current_context`: echoes the current `PIPELINE_CONTEXT` or `unset`.
  - `should_run_for_context "$@"`: returns success if the context is in the
    provided list.
  - `skip_step "description"`: logs that the step is skipped and exits 0.
  - `require_context "description" ctx…`: exits early unless the step should
    run for one of the contexts.
  - `ensure_target_branch_fetched`: derives the fetch branch from
    `TARGET_BRANCH` (ignoring any `~N` suffix) and fetches it from origin.

- `ensure_serving_tables.sh <bundle>`
  Used by the GitHub `deploy-serving` stage before deploying a serving bundle
  (listed in `SERVING_BUNDLES` in `ci.yml`). If the bundle deploys a Genie space
  for `DATABRICKS_BUNDLE_TARGET` and its tables are missing, it runs the
  `marketing_rfm_ltv_seed_data` job (seed → DLT refresh), then polls every
  `POLL_INTERVAL_SECONDS` (30) until the tables exist or `WAIT_TIMEOUT_SECONDS`
  (900) elapses.

## Step wrappers

- `security_scan_step.sh`
  Uses `require_context` to guard execution, then runs `bandit` and `pip-audit`
  into `ci-artifacts/`. Both tools run, and the wrapper exits non-zero if either
  tool reports a failure.

- `pre_commit_checks_step.sh`
  Wraps `uv run pre-commit run --all-files`, only running for the permitted
  contexts.

- `unit_tests_step.sh`
  Runs pytest with `pytest-custom-exit-code` so no-test exits match the GitHub
  workflow behavior.

- `validate_bundles_step.sh`
  Fetches the target branch, lists modified bundles, and validates each modified
  bundle with `./scripts/validate_bundles.sh --bundle <name>`.

- `compare_bundle_resources_step.sh`
  Guards the context, calls `ensure_target_branch_fetched`, then executes
  `./scripts/compare_bundle_resources.sh` with branch arguments.

- `dry_run_pipelines_step.sh`
  Uses `ensure_target_branch_fetched` and runs
  `scripts/run_on_modified_resources.sh "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}" "run --validate-only"`.

- `deploy_modified_bundles_step.sh`
  Fetches the target branch first, then calls
  `scripts/manage_modified_bundles.sh deploy "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}"`.

- `integration_tests_step.sh`
  Guarded execution that runs integration tests via
  `scripts/run_on_modified_resources.sh "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}" "run"`.

- `destroy_modified_bundles_step.sh`
  Fetches the target branch and triggers
  `scripts/manage_modified_bundles.sh destroy "${BITBUCKET_BRANCH}" "${TARGET_BRANCH}"`.

The Bitbucket pipeline simply references these scripts in each step, ensuring
the conditional logic lives in one place while the reusable operational scripts
remain in `scripts/`.
