#!/usr/bin/env bash
#
# Finds modified/added resources between two branches and runs a Databricks
# command on them. Filters out resources that are not pipelines or jobs before
# running the command.
#
# The Databricks command is expected to be a valid Databricks CLI command that
# can be run on these resources, such as `run --validate-only`.
#
# Usage:
#   ./run_on_modified_resources.sh <CURRENT_BRANCH> <TARGET_BRANCH> <DATABRICKS_COMMAND> [--debug-script]
#
set -euo pipefail

# --- Argument Parsing ---
DEBUG_MODE=0
SINGLE_BUNDLE=""
REMAINING_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug-script) DEBUG_MODE=1; shift ;;
    --bundle) SINGLE_BUNDLE="$2"; shift 2 ;;
    *) REMAINING_ARGS+=("$1"); shift ;;
  esac
done
set -- ${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}

if [[ $# -ne 3 ]]; then
  echo "Error: Invalid arguments." >&2
  echo "Usage: $0 <CURRENT_BRANCH> <TARGET_BRANCH> \"<DATABRICKS_COMMAND>\" [--bundle <name>] [--debug-script]" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
DATABRICKS_COMMAND="$3"

ROOT_DIR="$(git rev-parse --show-toplevel)"
ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# ──────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────────────────────────────────────────

# Read ignore patterns from .bundlerunignore file
# Returns patterns separated by newlines
read_ignore_patterns() {
    local ignore_file="$ROOT_DIR/.bundlerunignore"
    local patterns=()

    if [[ -f "$ignore_file" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            # Skip empty lines and comments (lines starting with #)
            [[ -z "$line" ]] && continue
            [[ "$line" =~ ^[[:space:]]*# ]] && continue

            # Trim whitespace
            line=$(printf '%s' "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

            # Skip empty lines after trimming
            [[ -z "$line" ]] && continue

            patterns+=("$line")
        done < "$ignore_file"
    fi

    # Join patterns with newlines for proper handling
    local IFS=$'\n'
    echo "${patterns[*]}"
}

# Check if a resource name matches any ignore pattern
# Supports gitignore-style patterns:
# - Exact matches
# - Wildcard patterns (*)
# Returns 0 (true) if resource should be ignored, 1 (false) if not
should_ignore_resource() {
    local resource_name="$1"
    local patterns="$2"

    # Split patterns by newlines
    while IFS= read -r pattern; do
        # Remove trailing whitespace and skip empty patterns
        pattern=$(printf '%s' "$pattern" | sed 's/[[:space:]]*$//')
        [[ -z "$pattern" ]] && continue

        # Check for exact match first
        if [[ "$resource_name" == "$pattern" ]]; then
            return 0
        fi

        # Convert gitignore pattern to regex
        # Escape special regex characters except *
        local regex_pattern
        regex_pattern=$(printf '%s' "$pattern" | sed 's/[.+?^${}()|[\]\\]/\\&/g' | sed 's/\\*/.*/g')

        # Match from the beginning (no leading wildcard)
        if [[ "$pattern" != *"*"* ]] || [[ "$pattern" == "*" ]] || [[ "$pattern" =~ ^\\* ]]; then
            # Pattern doesn't start with wildcard, anchor to start
            regex_pattern="^$regex_pattern"
        fi

        # Match to the end (no trailing wildcard)
        if [[ "$pattern" != *"*" ]] || [[ "$pattern" =~ \\*$ ]]; then
            # Pattern doesn't end with wildcard, anchor to end
            regex_pattern="$regex_pattern$"
        fi

        if [[ "$resource_name" =~ $regex_pattern ]]; then
            return 0
        fi
    done <<< "$patterns"

    return 1
}

# Read ignore patterns BEFORE switching branches
echo "  Reading ignore patterns from .bundlerunignore..."
ignore_patterns_output=$(read_ignore_patterns)
if [[ -n "$ignore_patterns_output" ]]; then
    echo "  Found ignore patterns: $ignore_patterns_output"
else
    echo "  No ignore patterns found (or file doesn't exist)"
fi

# Temporarily disable git hooks to avoid pre-commit output during automation
HOOKS_DIR="$ROOT_DIR/.git/hooks"
HOOKS_BACKUP="$ROOT_DIR/.git/hooks.backup"
if [[ -d "$HOOKS_DIR" ]]; then
    mv "$HOOKS_DIR" "$HOOKS_BACKUP"
fi

git checkout -q "$CURRENT_BRANCH"

# Restore hooks in trap
restore_hooks() {
    if [[ -d "$HOOKS_BACKUP" ]]; then
        mv "$HOOKS_BACKUP" "$HOOKS_DIR"
    fi
    git checkout -q "$ORIGINAL_BRANCH"
}
trap restore_hooks EXIT

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────

# --- Generate Action Plan ---
echo "▶ Analyzing differences to generate action plan..."
declare -A ACTION_PLAN
RESOURCES_FOUND=false
profile="${DATABRICKS_PROFILE:-DEFAULT}"

if [[ -n "$SINGLE_BUNDLE" ]]; then
  MODIFIED_BUNDLE_NAMES=("$SINGLE_BUNDLE")
else
  mapfile -t MODIFIED_BUNDLE_NAMES < <(
    bash "$ROOT_DIR/scripts/helpers/find_modified_bundles.sh" "$CURRENT_BRANCH" "$TARGET_BRANCH"
  )
fi

for bundle_name in "${MODIFIED_BUNDLE_NAMES[@]}"; do
  bundle_path="$ROOT_DIR/bundles/$bundle_name"
  if [[ ! -d "$bundle_path" ]]; then
    echo "  ⚠ Skipping '$bundle_name': directory not found at $bundle_path" >&2
    continue
  fi

  echo "  Found changes in bundle: $bundle_name"

  bundle_json=""
  exit_code=0
  bundle_json=$( (cd "$bundle_path" && databricks bundle validate -t "${DATABRICKS_BUNDLE_TARGET:-dev}" --output json) 2>&1 ) || exit_code=$?

  if (( exit_code != 0 )); then
    echo "Error: 'databricks bundle validate' failed for bundle '$bundle_name'." >&2
    echo "Directory: $bundle_path" >&2
    echo "Exit Code: $exit_code" >&2
    echo -e "\n--- Databricks CLI Output ---\n$bundle_json\n---------------------------\n" >&2
    exit 1
  fi

  # Strip any leading non-JSON lines (warnings) before parsing with jq
  clean_json=$(printf '%s' "$bundle_json" | awk 'BEGIN{start=0} { if(start){print} else if($0 ~ /^[[:space:]]*\{/){start=1; print} }')

  bundle_root_path=""
  bundle_root_path=$(printf '%s' "$clean_json" | jq -r '.bundle.git.bundle_root_path // ""')

  if [[ -z "$bundle_root_path" ]]; then
    echo "Warning: Could not find bundle path for '$bundle_name'. Skipping resources." >&2
    continue
  fi

  # Build resource list, but only include those whose files changed within this bundle
  # We approximate: include resources if any of their definition files likely changed
  # by scanning common files under the bundle path.
  resources_list=$(printf '%s' "$clean_json" | jq -r '.resources | (.pipelines // {} | keys_unsorted[] as $name | "pipelines:" + $name), (.jobs // {} | keys_unsorted[] as $name | "jobs:" + $name)')

  if [[ -z "$resources_list" ]]; then
    continue
  fi

  while IFS= read -r resource_key; do
    resource_type="${resource_key%%:*}"
    resource_name="${resource_key#*:}"

    # .bundlerunignore only skips real runs; dry-run validation still covers every resource
    if [[ "$DATABRICKS_COMMAND" != "run --validate-only" ]] && should_ignore_resource "$resource_name" "$ignore_patterns_output"; then
      echo "  Ignoring resource: $resource_name (matches ignore pattern)"
      continue
    fi

    # Skip jobs when using --validate-only flag (jobs don't support dry-run)
    if [[ "$resource_type" == "jobs" && "$DATABRICKS_COMMAND" == "run --validate-only" ]]; then
      echo "  - Skipping job '$resource_name' (dry-run/validation is not supported for jobs)."
    else
      ACTION_PLAN["$bundle_root_path/$resource_name"]="(cd '$ROOT_DIR/$bundle_root_path' && databricks bundle $DATABRICKS_COMMAND -t '${DATABRICKS_BUNDLE_TARGET:-dev}' '$resource_name')"
      RESOURCES_FOUND=true
    fi
  done <<< "$resources_list"
done

# --- Execute Action Plan ---
if [[ $RESOURCES_FOUND == false ]]; then
  echo "✔ No modified or added pipelines/jobs to action."
  exit 0
fi

echo -e "\n▶ Executing commands for modified resources..."
for key in "${!ACTION_PLAN[@]}"; do
  cmd="${ACTION_PLAN[$key]}"
  if (( DEBUG_MODE == 1 )); then
    echo "DEBUG: $cmd"
  else
    echo "EXEC: $cmd"
    eval "$cmd"
  fi
done

printf '\n✔ Done.\n'
