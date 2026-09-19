Scripts Overview

Concise guide to the CI/CD helper scripts. Assumptions:
- Git available and repository checked out
- Databricks CLI installed and configured (OAuth m2m via `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`)
- Optional `DATABRICKS_PROFILE` set to `ci|qa|prod|dev` (fallback to DEFAULT profile); optionally `DATABRICKS_BUNDLE_TARGET`

ci/list_modified_bundles.sh
- Output modified bundle names as a JSON array.
- Usage:
  ```bash
  ./ci/list_modified_bundles.sh <CURRENT_BRANCH> <TARGET_BRANCH>
  # Example output: ["vba_claims_pipeline_bundle/vba_claims_pipeline","eldorado_srp_pipeline_bundle/eldorado_srp_pipeline"]
  ```
- Notes:
  - Only includes bundles with relevant changes (yml/yaml/py/sql/ipynb)
  - Used by CI to build the matrix of bundles for per-bundle deploy/run/destroy jobs

compare_bundle_resources.sh
- Compare Databricks bundle resources between two branches and show a color-coded diff.
- Usage:
  ```bash
  ./compare_bundle_resources.sh <SOURCE_BRANCH> <TARGET_BRANCH>
  ```
- Notes:
  - Validates bundles on both branches (honors `DATABRICKS_BUNDLE_TARGET`) and diffs resource lists
  - Shows `+` added, `-` removed, `~` modified
  - Lists changed files within the specific bundle (filtered to yml/yaml/py/sql/ipynb)
  - Ignores untracked files and file mode-only changes; sanitizes branch names for temp files

manage_modified_bundles.sh
- Unified deploy/destroy workflow for bundles that changed between branches.
- Usage:
  ```bash
  ./manage_modified_bundles.sh <deploy|destroy> <CURRENT_BRANCH> <TARGET_BRANCH> [--bundle <name>] [--debug-script]
  ```
- Notes:
  - Without `--bundle`: detects changed bundles via git diff and processes all of them
  - With `--bundle <name>`: skips diff detection and operates on the named bundle only (used by CI matrix jobs)
  - Runs `databricks bundle <action> --auto-approve`; skips destruction on merges to main/master
  - Uses `DATABRICKS_PROFILE`/`DATABRICKS_BUNDLE_TARGET` when provided

run_on_modified_resources.sh
- Run a Databricks CLI command on modified pipelines/jobs in changed bundles.
- Usage:
  ```bash
  ./run_on_modified_resources.sh <CURRENT_BRANCH> <TARGET_BRANCH> "<DATABRICKS_COMMAND>" [--bundle <name>] [--debug-script]
  # Example:
  ./run_on_modified_resources.sh feature/xyz dev "run --validate-only" --debug-script
  ./run_on_modified_resources.sh feature/xyz dev "run" --bundle my_bundle_name
  ```
- Notes:
  - Without `--bundle`: detects bundle-level changes between branches and processes all changed bundles
  - With `--bundle <name>`: scopes processing to the named bundle only (used by CI matrix jobs)
  - Executes `databricks bundle <DATABRICKS_COMMAND>` for pipelines/jobs in changed bundles, using `DATABRICKS_BUNDLE_TARGET` to select the target
  - Skips jobs when paired with `run --validate-only` (CLI limitation)
  - Skips resources matching `.bundlerunignore` patterns on real runs (dry-run validation still covers them)
  - Uses `DATABRICKS_PROFILE` if set; defaults to `DEFAULT`

validate_bundles.sh
- Validate all (or one) bundle via Databricks CLI.
- Usage:
  ```bash
  ./validate_bundles.sh [--bundle <name>]
  ```
- Notes:
  - Without `--bundle`: iterates all bundles with `databricks.yml` and runs validate
  - With `--bundle <name>`: validates only the named bundle (used by CI matrix jobs)
  - Uses `DATABRICKS_PROFILE`/`DATABRICKS_BUNDLE_TARGET` when provided to align with pipeline context

Tips
- Ensure the target branch is available locally or fetch it first
- For local testing, export a profile:
  ```bash
  export DATABRICKS_PROFILE=dev  # or qa/prod
  ```
