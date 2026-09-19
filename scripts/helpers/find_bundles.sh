#!/usr/bin/env bash
# Recursively find bundle directories (those containing databricks.yml).
# Stops recursing into a directory once databricks.yml is found.
# Usage: find_bundles.sh <bundles_root_dir>
set -euo pipefail

_find_bundles() {
  local dir="$1"
  for entry in "$dir"/*/; do
    [[ -d "$entry" ]] || continue
    entry="${entry%/}"
    if [[ -f "$entry/databricks.yml" ]]; then
      echo "$entry"
    else
      _find_bundles "$entry"
    fi
  done
}

BUNDLES_ROOT="${1:?Usage: $0 <bundles_root_dir>}"
_find_bundles "$BUNDLES_ROOT"
