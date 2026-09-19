"""Module: sdp_builder.py

Builds SDP decorator parameters from YAML metadata.
"""

from pathlib import Path
from typing import Any

from shared_utils.ddl_compiler import compile_ddl_schema, compile_expectations
from shared_utils.yaml_parser import load_yaml_model


def build_sdp_decorator_args(
    yaml_path: str | Path,
    catalog: str,
    schema: str,
) -> dict[str, Any]:
    """Build all arguments for @dp decorator from YAML metadata.

    This function generates decorator arguments (schema, table properties, etc.)
    from YAML. The transformation logic remains in the Python function body.

    Args:
        yaml_path: Path to YAML metadata file.
        catalog: Target catalog name.
        schema: Target schema name.

    Returns:
        Dictionary that can be unpacked into @dp args.
        Includes 'expectations' key to be handled by decorators.
    """
    table_metadata = load_yaml_model(yaml_path)

    args = {
        "name": f"{schema}.{table_metadata.name}",
    }

    args["schema"] = compile_ddl_schema(table_metadata, catalog, schema)
    args["expectations"] = compile_expectations(table_metadata)

    if table_metadata.table_properties:
        args["table_properties"] = table_metadata.table_properties

    if table_metadata.cluster_by:
        args["cluster_by"] = table_metadata.cluster_by

    if table_metadata.partition_cols:
        args["partition_cols"] = table_metadata.partition_cols

    if table_metadata.row_filter:
        rf = table_metadata.row_filter
        func_name = f"rf__{table_metadata.name}__{rf.name}"
        func_full = f"{catalog}.{schema}.{func_name}"
        cols_str = ", ".join(f"`{c}`" for c in rf.columns or [])
        if cols_str:
            args["row_filter"] = f"ROW FILTER {func_full} ON ({cols_str})"

    return args


__all__ = ["build_sdp_decorator_args"]
