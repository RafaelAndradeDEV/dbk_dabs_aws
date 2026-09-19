"""Module: ddl_compiler.py

Compiles YAML metadata into Databricks-compatible DDL schema strings.
"""

from shared_utils.yaml_parser import TableMetadata


def compile_ddl_schema(table_metadata: TableMetadata, catalog: str, schema: str) -> str:
    """Compiles table metadata into a DDL schema string for Spark Declarative Pipelines.

    Args:
        table_metadata: Parsed table configuration.
        catalog: Target catalog name.
        schema: Target schema name.

    Returns:
        A DDL string containing column definitions, masks, and valid constraints.
    """
    ddl_parts = []

    for col in table_metadata.columns:
        col_ddl = f"{col.name} {col.type}"

        # 1. Nullability
        nullable = col.nullable
        other_constraints = []
        if col.constraints:
            for c in col.constraints:
                if c.upper() == "NOT NULL":
                    nullable = False
                else:
                    other_constraints.append(c)

        if not nullable:
            col_ddl += " NOT NULL"

        # 2. Generated Column
        if col.generated:
            col_ddl += f" GENERATED ALWAYS AS ({col.generated})"

        # 3. MASK
        mask_def = col.masked or col.mask
        if mask_def:
            mask_func_name = f"mask__{table_metadata.name}__{col.name}"
            mask_udf = f"{catalog}.{schema}.{mask_func_name}"
            col_ddl += f" MASK {mask_udf}"

        # 4. COMMENT
        desc = col.description or col.comment
        if desc:
            desc_escaped = desc.replace("'", "''")
            col_ddl += f" COMMENT '{desc_escaped}'"

        # 5. Inline Column Constraints
        if other_constraints:
            valid_constraints = [c for c in other_constraints if not any(k in c.upper() for k in ["CHECK", "UNIQUE"])]
            if valid_constraints:
                col_ddl += " " + " ".join(valid_constraints)

        ddl_parts.append(col_ddl)

    # 6. Table-level constraints
    if table_metadata.constraints:
        for constraint in table_metadata.constraints:
            if not any(k in constraint.upper() for k in ["CHECK", "UNIQUE"]):
                ddl_parts.append(f"CONSTRAINT {constraint}")

    return ",\n    ".join(ddl_parts)


def compile_expectations(table_metadata: TableMetadata) -> dict[str, dict[str, str]]:
    """Compiles data quality expectations from table metadata.

    Converts standard DLT expectations and legacy SQL CHECK constraints into
    quality gate dictionaries grouped by action.

    Args:
        table_metadata: Parsed table configuration.

    Returns:
        Mapping of actions (expect, expect_or_drop, expect_or_fail) to rule dictionaries.
    """
    expectations: dict[str, dict[str, str]] = {
        "expect": {},
        "expect_or_drop": {},
        "expect_or_fail": {},
    }

    if table_metadata.expectations:
        for expectation in table_metadata.expectations:
            action = expectation.action or "expect"
            if action in expectations:
                expectations[action][expectation.name] = expectation.condition

    # Auto-convert CHECK constraints to 'expect_or_fail'
    if table_metadata.constraints:
        for constraint in table_metadata.constraints:
            if "CHECK" in constraint.upper():
                parts = constraint.split("CHECK", 1)
                name = parts[0].strip() or f"check_{hash(constraint)}"
                condition = parts[1].strip().strip("() ")
                expectations["expect_or_fail"][name] = condition

    # Column-level CHECK constraints
    for col in table_metadata.columns:
        if col.constraints:
            for c in col.constraints:
                if "CHECK" in c.upper():
                    parts = c.split("CHECK", 1)
                    name = f"check_{col.name}"
                    condition = parts[1].strip().strip("() ")
                    expectations["expect_or_fail"][name] = condition

    return expectations


__all__ = ["compile_ddl_schema", "compile_expectations"]
