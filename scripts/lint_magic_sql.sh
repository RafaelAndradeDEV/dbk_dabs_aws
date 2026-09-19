#!/bin/bash
# Script to fix and lint SQL within # MAGIC %sql blocks in Databricks .py files using sqlfluff

status=0
mkdir -p ./tmp
temp_sql="./tmp/temp_sql_$$.sql"
temp_py="./tmp/temp_py_$$.py"

for file in "$@"; do
  # Extract SQL from # MAGIC %sql blocks
  sql=$(awk '/# MAGIC[[:space:]]*%sql\r?/{f=1;next} f&&/# COMMAND\r?/{f=0} f{sub(/^# MAGIC /,"");print}' "$file")
  if [ -n "$sql" ]; then
    echo "sqlfluff-magic: $file"
    # Save SQL to temp file for fixing
    printf "%s\n" "$sql" > "$temp_sql"

    # Run sqlfluff fix (suppress noisy stdout)
    python -m sqlfluff fix --config .sqlfluff --disable-progress-bar "$temp_sql" >/dev/null || status=1

    # Read fixed SQL
    fixed_sql=$(cat "$temp_sql")

    # Reinsert fixed SQL into the .py file, preserving # MAGIC and aligning indentation
    awk -v fixed="$fixed_sql" '
      BEGIN { split(fixed, lines, "\n"); line_idx=1 }
      /# MAGIC[[:space:]]*%sql\r?/{print; f=1; next}
      f&&/# COMMAND\r?/{f=0; print; next}
      f {if (line_idx <= length(lines) && lines[line_idx] != "") {print "# MAGIC " lines[line_idx++]} else {line_idx++}}
      !f {print}
    ' "$file" > "$temp_py"

    # Update original file if awk produced output
    if [ -s "$temp_py" ]; then
      mv "$temp_py" "$file" || status=1
    else
      status=1
    fi

    # Lint the fixed SQL to confirm; only print on failure and include filename
    lint_output=$(printf "%s\n" "$fixed_sql" | python -m sqlfluff lint - --config .sqlfluff --disable-progress-bar --stdin-filename "$file" 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then
      echo "sqlfluff-magic FAILED: $file"
      printf "%s\n" "$lint_output"
      status=1
    fi
  fi
done

rm -f "$temp_sql" "$temp_py"
exit $status
