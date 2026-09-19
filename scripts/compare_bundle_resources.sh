#!/usr/bin/env bash
#
# Compare Databricks bundle resources between two specified branches.
#
# The output is a colour-coded list:
#   + added   (green)
#   - removed (red)
#   ~ changed (yellow – definition modified in place)
#
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Error: Two branch names are required." >&2
  echo "Usage: $0 <CURRENT_BRANCH> <target_branch>" >&2
  exit 1
fi

CURRENT_BRANCH="$1"
TARGET_BRANCH="$2"
ROOT_DIR="$(git rev-parse --show-toplevel)"
BUNDLES_DIR="$ROOT_DIR/bundles"


# ──────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────────────────────────────────────────

colour() {
  case $1 in
    add) printf '\033[32m+ %s\033[0m\n' "$2";;
    del) printf '\033[31m- %s\033[0m\n' "$2";;
    mod) printf '\033[33m~ %s\033[0m\n' "$2";;
  esac
}

# Compute a stable hash for all relevant code files in a bundle directory.
# Relevant = .yml, .yaml, .py, .sql, .ipynb
compute_bundle_code_hash() {
  local bundle_dir="$1"
  local -a files=()
  # Collect files deterministically
  while IFS= read -r -d '' f; do
    files+=("$f")
  done < <(find "$bundle_dir" -type f \
            \( -name '*.yml' -o -name '*.yaml' -o -name '*.py' -o -name '*.sql' -o -name '*.ipynb' \) \
            -print0 | LC_ALL=C sort -z)

  if (( ${#files[@]} == 0 )); then
    printf '%s' "no_code_files"
    return 0
  fi

  # Hash each file then hash the list of hashes for stability
  local tmp_hashes
  tmp_hashes=$(mktemp)
  trap 'rm -f "$tmp_hashes"' RETURN
  for f in "${files[@]}"; do
    sha1sum "$f" | awk '{print $1"  "FILENAME}' FILENAME="$f"
  done | LC_ALL=C sort > "$tmp_hashes"

  sha1sum "$tmp_hashes" | awk '{print $1}'
}

# This function encapsulates the logic for validating and hashing resources.
collect_bundle_data() {
  local branch_name="$1"
  local json_map_name="$2" # Name of the JSON map variable
  local list_map_name="$3" # Name of the list map variable
  shift 3
  local profile="${DATABRICKS_PROFILE:-DEFAULT}"

  echo "▶ Collecting resource data on branch '$branch_name'..."

  if [[ $# == 0 ]]; then
    mapfile -t _all_paths < <(bash "$ROOT_DIR/scripts/helpers/find_bundles.sh" "$BUNDLES_DIR")
    for bundle_path in "${_all_paths[@]}"; do
      local bundle_name="${bundle_path#$BUNDLES_DIR/}"
      process_bundle "$bundle_name" "$bundle_path" "$json_map_name" "$list_map_name" "$profile" "$branch_name"
    done
  else
    for bundle_name in "$@"; do
      local bundle_path="$BUNDLES_DIR/$bundle_name"
      [[ -d "$bundle_path" && -f "$bundle_path/databricks.yml" ]] || { echo "⚠️ Warning: Bundle '$bundle_name' does not exist on branch '$branch_name'. Skipping." >&2; continue; }
      process_bundle "$bundle_name" "$bundle_path" "$json_map_name" "$list_map_name" "$profile" "$branch_name"
    done
  fi
}

process_bundle() {
  local bundle_name="$1"
  local bundle_path="$2"
  local json_map_name="$3"
  local list_map_name="$4"
  local profile="$5"
  local branch_name="$6"

  local branch_safe_name="${branch_name//\//_}"
  local bundle_safe_name="${bundle_name//\//_}"

  local json_file="$TMP/${bundle_safe_name}_${branch_safe_name}.json"
  local list_file="$TMP/${bundle_safe_name}_${branch_safe_name}.list"

  # Safe assignment using eval
  local bundle_esc json_esc list_esc
  bundle_esc=$(printf '%q' "$bundle_name")
  json_esc=$(printf '%q' "$json_file")
  list_esc=$(printf '%q' "$list_file")
  eval "${json_map_name}[${bundle_esc}]=${json_esc}"
  eval "${list_map_name}[${bundle_esc}]=${list_esc}"

  local bundle_json exit_code=0
  local target="${DATABRICKS_BUNDLE_TARGET:-dev}"
  echo "▶ Running 'databricks bundle validate' (bundle='$bundle_name', branch='$branch_name', profile='$profile', target='$target')" >&2
  bundle_json=$( (cd "$bundle_path" && databricks bundle validate -t "$target" --output json) 2>&1 ) || exit_code=$?

  if (( exit_code != 0 )); then
    echo "❌ Error: 'databricks bundle validate' failed for bundle '$bundle_name' on branch '$branch_name'." >&2
    echo -e "\n--- Databricks CLI Output ---\n$bundle_json\n---------------------------\n" >&2
    exit 1
  fi

  # Persist raw validate output for debugging jq parse errors
  local raw_file="$TMP/${bundle_safe_name}_${branch_safe_name}.validate.raw"
  printf '%s' "$bundle_json" > "$raw_file"

  # Echo any leading warnings prior to JSON body (if present)
  local warn_file="$TMP/${bundle_safe_name}_${branch_safe_name}.validate.warn"
  awk 'BEGIN{start=0} { if(start){next} else if($0 ~ /^[[:space:]]*\{/){start=1; next} else { print } }' "$raw_file" > "$warn_file"
  if [[ -s "$warn_file" ]]; then
    echo "--- Databricks CLI messages before JSON (bundle='$bundle_name', branch='$branch_name') ---" >&2
    cat "$warn_file" >&2 || true
    echo "--- end messages ---" >&2
  fi

  # Strip any leading warnings prior to JSON body
  local json_body_file="$TMP/${bundle_safe_name}_${branch_safe_name}.validate.json"
  awk 'BEGIN{start=0} { if(start){print} else if($0 ~ /^[[:space:]]*\{/){start=1; print} }' "$raw_file" > "$json_body_file"

  echo "▶ Parsing validate output with jq (bundle='$bundle_name', branch='$branch_name')" >&2
  set +e
  jq -S '.resources' "$json_body_file" > "$json_file" 2>"$raw_file.jq.err"
  local jq_status=$?
  set -e

  # helpful logs to debug jq parse errors
  if (( jq_status != 0 )); then
    echo "ERROR: jq parse error for bundle='$bundle_name' branch='$branch_name'" >&2
    echo "   Raw output:        $raw_file" >&2
    echo "   Extracted JSON at: $json_body_file" >&2
    echo "   jq error:   $raw_file.jq.err" >&2
    echo "--- jq error (first 10 lines) ---" >&2
    sed -n '1,10p' "$raw_file.jq.err" >&2 || true
    echo "--- raw output (first 5 lines) ---" >&2
    sed -n '1,5p' "$raw_file" >&2 || true
    echo "--- extracted json (first 5 lines) ---" >&2
    sed -n '1,5p' "$json_body_file" >&2 || true
    echo "----------------------------------" >&2
    exit 1
  fi

  # Compute a bundle-wide code hash of relevant files
  local code_hash
  code_hash=$(compute_bundle_code_hash "$bundle_path")

  # Build a sorted list of "resource_key<TAB>hash" for comparison
  jq -r '
    . | to_entries[]
    | .key as $type
    | .value | to_entries[]
    | "\($type):\(.key)|\(.value | (if type == "object" then to_entries | sort_by(.key) | from_entries else . end) | tojson)"
  ' "$json_file" | while IFS='|' read -r key raw_json; do
    # Combine resource JSON with the bundle-wide code hash to capture code-only changes
    hash_input="$raw_json$code_hash"
    hash=$(printf '%s' "$hash_input" | sha1sum | cut -d' ' -f1)
    printf '%s\t%s\n' "$key" "$hash"
  done | sort > "$list_file"
}

# ──────────────────────────────────────────────────────────────────────────────
# Pre-flight Checks
# ──────────────────────────────────────────────────────────────────────────────

if [[ -n $(git diff --no-ext-diff --ignore-submodules --ignore-space-at-eol -M -G.) ]]; then
  echo "❌ Error: Working tree has modified files. Commit or stash before running." >&2
  exit 1
fi
for branch in "$CURRENT_BRANCH" "$TARGET_BRANCH"; do
  if ! git rev-parse --verify "$branch" >/dev/null 2>&1; then
    echo "❌ Error: Branch '$branch' does not exist." >&2
    exit 1
  fi
done

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────

ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
TMP="$(mktemp -d)"
# The trap ensures we always clean up and return to the original branch
trap 'rm -rf "$TMP"; git checkout -q "$ORIGINAL_BRANCH"' EXIT

# Identify bundles in each branch without checkout
echo "▶ Identifying bundles in branches..."
# `|| true`: a branch with no bundles yet (e.g. a fresh dev) makes grep exit 1 under pipefail
list_branch_bundles() {
  git ls-tree -r --name-only "$1" -- bundles/ | { grep '/databricks.yml$' || true; } \
    | sed 's|^bundles/||; s|/databricks.yml$||' | sort
}
source_bundles_str=$(list_branch_bundles "$CURRENT_BRANCH")
target_bundles_str=$(list_branch_bundles "$TARGET_BRANCH")

declare -A source_bundles=() target_bundles=() all_bundles=()
for b in $source_bundles_str; do source_bundles["$b"]=1; all_bundles["$b"]=1; done
for b in $target_bundles_str; do target_bundles["$b"]=1; all_bundles["$b"]=1; done

declare -a added=() removed=() modified=()
for b in $source_bundles_str; do
  if [[ -z ${target_bundles[$b]:-} ]]; then
    added+=("$b")
  else
    # Consider any change to ensure we collect bundle data; we'll filter by relevant extensions later
    if ! git diff --quiet "$TARGET_BRANCH" "$CURRENT_BRANCH" -- "bundles/$b"; then
      modified+=("$b")
    fi
  fi
done
for b in $target_bundles_str; do
  if [[ -z ${source_bundles[$b]:-} ]]; then
    removed+=("$b")
  fi
done

# Collect data only for affected bundles
declare -A SOURCE_JSON SOURCE_LISTS
declare -A TARGET_JSON TARGET_LISTS

if [[ ${#removed[@]} -gt 0 || ${#modified[@]} -gt 0 ]]; then
  git checkout -q "$TARGET_BRANCH"
  collect_bundle_data "$TARGET_BRANCH" TARGET_JSON TARGET_LISTS "${removed[@]}" "${modified[@]}"
fi

if [[ ${#added[@]} -gt 0 || ${#modified[@]} -gt 0 ]]; then
  git checkout -q "$CURRENT_BRANCH"
  collect_bundle_data "$CURRENT_BRANCH" SOURCE_JSON SOURCE_LISTS "${added[@]}" "${modified[@]}"
fi

# --- Diffing Logic ---

for bundle in $(printf '%s\n' "${!all_bundles[@]}" | sort); do
  echo
  echo "===== [$bundle] ($CURRENT_BRANCH ⇄ $TARGET_BRANCH) ====="

  if [[ -z ${source_bundles[$bundle]:-} ]]; then
    # Removed bundle
    target_file="${TARGET_LISTS[$bundle]:-}"
    echo "Bundle only exists in $TARGET_BRANCH (all removed from source perspective)"
    if [[ -n "$target_file" ]]; then
      cut -f1 "$target_file" | sort | while read -r key; do colour del "$key"; done
    fi
  elif [[ -z ${target_bundles[$bundle]:-} ]]; then
    # Added bundle
    source_file="${SOURCE_LISTS[$bundle]:-}"
    echo "Bundle only exists in $CURRENT_BRANCH (all added)"
    if [[ -n "$source_file" ]]; then
      cut -f1 "$source_file" | sort | while read -r key; do colour add "$key"; done
    fi
  else
    # Common bundle
    if ! git diff --name-only "$TARGET_BRANCH" "$CURRENT_BRANCH" -- "bundles/$bundle" | grep -E '\.(yml|yaml|py|sql|ipynb)$' >/dev/null; then
      echo "(no changes)"
    else
      # Modified bundle
      source_file="${SOURCE_LISTS[$bundle]:-}"
      target_file="${TARGET_LISTS[$bundle]:-}"
      if [[ -z "$source_file" || -z "$target_file" ]]; then
        echo "(no changes)"
        continue
      fi
      # Collect relevant changed files for this bundle (for display under modified resources)
      mapfile -t __changed_files < <(git diff --name-only "$TARGET_BRANCH" "$CURRENT_BRANCH" -- "bundles/$bundle" | grep -E '\.(yml|yaml|py|sql|ipynb)$')
      output=$(join -t $'\t' -a 1 -a 2 -o '0,1.2,2.2' "$source_file" "$target_file" |
        awk -F '[\t,]' '{
          key=$1; source_hash=$2; target_hash=$3;
          if (source_hash && !target_hash)      { print "add", key }
          else if (!source_hash && target_hash) { print "del", key }
          else if (source_hash != target_hash)  { print "mod", key }
        }')
      if [[ -n "$output" ]]; then
        while read -r action key; do
          colour "$action" "$key"
        done <<< "$output"
        # Print changed files per bundle
        if [[ ${#__changed_files[@]} -gt 0 ]]; then
          __max=10
          __total=${#__changed_files[@]}
          __limit=$(( __total < __max ? __total : __max ))
          for (( i=0; i<__limit; i++ )); do
            printf '      · %s\n' "${__changed_files[$i]}"
          done
          if (( __total > __max )); then
            printf '      (+%d more)\n' "$(( __total - __max ))"
          fi
        fi
      else
        echo "(no changes)"
      fi
    fi
  fi
  echo "=============================================="
done

printf '\n✔ Done.\n'
