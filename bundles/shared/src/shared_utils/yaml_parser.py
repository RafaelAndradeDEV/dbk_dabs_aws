"""Module: yaml_parser.py

Provides Pydantic models and parsing functions for YAML-based table metadata.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class ColumnMask(BaseModel):
    """Column mask definition for Unity Catalog column-level security.

    Attributes:
        access: Group name with access to clear text values.
        function: Optional UDF name. Auto-generated if not provided.
        definition: Optional UDF definition. Auto-generated if not provided.
        using: Optional list of columns to pass to the mask function.
    """

    access: str
    function: str | None = None
    definition: str | None = None
    using: list[str] | None = None


class RowFilter(BaseModel):
    """Row filter definition for Unity Catalog row-level security.

    Attributes:
        name: Filter name identifier.
        access: Group name with full table access.
        non_member_filter: SQL condition applied to non-members.
        columns: Columns to filter on. Required for row filter application.
        function: Optional UDF name. Auto-generated if not provided.
        definition: Optional UDF definition. Auto-generated if not provided.
    """

    name: str
    access: str
    non_member_filter: str
    columns: list[str] | None = None
    function: str | None = None
    definition: str | None = None


class ColumnMetadata(BaseModel):
    """Column definition with metadata and governance.

    Attributes:
        name: Column name.
        type: DDL type (e.g., "STRING", "INT", "DECIMAL(10,2)").
        nullable: Whether column allows NULL values.
        description: Column description for documentation.
        comment: Alias for description.
        tags: Key-value tags for Unity Catalog.
        masked: Column mask definition for PII/sensitive data.
        mask: Alias for masked (for compatibility with existing YAML).
        constraints: List of constraints (e.g., ["NOT NULL", "PRIMARY KEY"]).
        generated: GENERATED ALWAYS AS expression.
        dqx_tests: Data quality tests to apply.
    """

    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    comment: str | None = None
    tags: dict[str, Any] | None = None
    masked: ColumnMask | None = None
    mask: ColumnMask | None = None
    constraints: list[str] | None = None
    generated: str | None = None
    dqx_tests: list[Any] | None = None


class Expectation(BaseModel):
    """SDP/DLT expectation for data quality gates.

    Attributes:
        name: Expectation name.
        condition: SQL condition for the expectation.
        action: Action on violation (expect, expect_or_drop, expect_or_fail).
    """

    name: str
    condition: str
    action: str = "expect"


class TableMetadata(BaseModel):
    """Table metadata from YAML.

    Attributes:
        name: Table name.
        description: Table description for documentation.
        tags: Key-value tags for Unity Catalog.
        columns: List of column definitions.
        row_filter: Row-level security filter.
        partition_cols: List of partition columns.
        cluster_by: List of clustering columns.
        table_properties: Delta table properties.
        configs: Additional configuration options.
        materialization: Table materialization type (for compatibility).
        expectations: List of SDP/DLT expectations.
        constraints: List of table-level constraints.
    """

    name: str
    description: str | None = None
    tags: dict[str, Any] | None = None
    columns: list[ColumnMetadata]
    row_filter: RowFilter | None = None
    partition_cols: list[str] | None = None
    cluster_by: list[str] | None = None
    table_properties: dict[str, str] | None = None
    configs: dict[str, Any] | None = None
    materialization: str | None = None
    expectations: list[Expectation] | None = None
    constraints: list[str] | None = None


class ModelFile(BaseModel):
    """Root YAML structure.

    Attributes:
        models: List of table metadata definitions.
    """

    models: list[TableMetadata]


def load_yaml_model(yaml_path: str | Path) -> TableMetadata:
    """Load and validate a YAML model file.

    Args:
        yaml_path: Path to the .yml file.

    Returns:
        A validated TableMetadata instance.

    Raises:
        FileNotFoundError: If YAML file does not exist.
        ValueError: If YAML is invalid or contains multiple models.
        ValidationError: If YAML does not match expected schema.
    """
    yaml_path = Path(yaml_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {yaml_path}")

    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    model_file = ModelFile(**data)

    if len(model_file.models) != 1:
        raise ValueError(f"Expected exactly 1 model, found {len(model_file.models)}")

    return model_file.models[0]


def validate_yaml_schema(yaml_path: str | Path) -> None:
    """Validate YAML against expected schema.

    Args:
        yaml_path: Path to YAML file.

    Raises:
        FileNotFoundError: If YAML file does not exist.
        ValidationError: If YAML does not match expected schema.
    """
    load_yaml_model(yaml_path)


__all__ = [
    "ColumnMask",
    "ColumnMetadata",
    "ModelFile",
    "RowFilter",
    "TableMetadata",
    "load_yaml_model",
    "validate_yaml_schema",
]
