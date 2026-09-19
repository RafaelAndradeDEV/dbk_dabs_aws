"""Module: udf_generator.py

Generates Unity Catalog UDFs for row filters and column masks.
"""

import logging

from pyspark.sql import SparkSession

from shared_utils.yaml_parser import TableMetadata

logger = logging.getLogger(__name__)


def generate_row_filter_udf(
    model_name: str,
    filter_name: str,
    access_group: str,
    non_member_filter: str,
    columns: list[str],
) -> tuple[str, str]:
    """Generate row filter UDF name and definition.

    Args:
        model_name: Name of the target table/model.
        filter_name: Unique identifier for the filter.
        access_group: Databricks group granted bypass access.
        non_member_filter: SQL condition applied to non-group members.
        columns: List of columns used in the filter logic.

    Returns:
        A tuple of (function_name, sql_definition).
    """
    func_name = f"rf__{model_name}__{filter_name}"
    col_params = ", ".join(f"{col} STRING" for col in columns)

    func_def = f"""
    ({col_params})
    RETURNS BOOLEAN
    RETURN
        CASE
            WHEN is_account_group_member('{access_group}') THEN true
            ELSE ({non_member_filter})
        END
    """

    return func_name, func_def


def generate_column_mask_udf(
    model_name: str,
    column_name: str,
    access_group: str,
) -> tuple[str, str]:
    """Generates the SQL definition for a column mask UDF.

    Args:
        model_name: Name of the target table/model.
        column_name: Name of the column being masked.
        access_group: Databricks group granted clear-text access.

    Returns:
        A tuple of (function_name, sql_definition).
    """
    func_name = f"mask__{model_name}__{column_name}"

    func_def = f"""
    ({column_name} STRING)
    RETURNS STRING
    RETURN
        CASE
            WHEN is_account_group_member('{access_group}') THEN {column_name}
            ELSE 'REDACTED'
        END
    """

    return func_name, func_def


def deploy_udfs(
    table_metadata: TableMetadata,
    catalog: str,
    schema: str,
    spark: SparkSession,
) -> dict[str, str]:
    """Deploys governance UDFs (masks/filters) defined in table metadata.

    Args:
        table_metadata: Parsed table configuration.
        catalog: Target catalog name.
        schema: Target schema name.
        spark: Active Spark session.

    Returns:
        Mapping of UDF types to their fully qualified names.
    """
    deployed_udfs = {}

    if table_metadata.row_filter:
        rf = table_metadata.row_filter
        columns = rf.columns or []

        if not columns:
            logger.warning(
                "Row filter '%s' for table '%s' has no columns defined. Skipping.",
                rf.name,
                table_metadata.name,
            )
        else:
            func_name, func_def = generate_row_filter_udf(
                table_metadata.name, rf.name, rf.access, rf.non_member_filter, columns
            )
            full_name = f"{catalog}.{schema}.{func_name}"
            spark.sql(f"CREATE OR REPLACE FUNCTION {full_name} {func_def}")
            deployed_udfs["row_filter"] = full_name
            logger.info("Deployed row filter: %s", full_name)

    for col in table_metadata.columns:
        mask_def = col.masked or col.mask
        if mask_def:
            func_name, func_def = generate_column_mask_udf(table_metadata.name, col.name, mask_def.access)
            full_name = f"{catalog}.{schema}.{func_name}"
            spark.sql(f"CREATE OR REPLACE FUNCTION {full_name} {func_def}")
            deployed_udfs[f"mask_{col.name}"] = full_name
            logger.info("Deployed column mask: %s", full_name)

    return deployed_udfs


__all__ = [
    "deploy_udfs",
    "generate_column_mask_udf",
    "generate_row_filter_udf",
]
