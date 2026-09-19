# typings/dlt.pyi
# Type stubs for Databricks Delta Live Tables (DLT) API
# Overrides the databricks-dlt package for local development with added methods
from collections.abc import Callable, Collection
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StructType

# Existing functions from databricks-dlt, converted to stubs

def append_flow(
    name: str | None = None,
    target: str | None = None,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
    once: bool = False,
) -> Callable[[Callable[[], DataFrame]], None]: ...
def table(
    query_function: Callable[..., DataFrame] | None = None,
    name: str | None = None,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
    table_properties: dict[str, str] | None = None,
    partition_cols: list[str] | None = None,
    path: str | None = None,
    schema: StructType | str | None = None,
    temporary: bool = False,
    cluster_by: list[str] | None = None,
    row_filter: str | None = None,
) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def view(
    query_function: Callable[..., DataFrame] | None = None,
    name: str | None = None,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def on_event_hook(
    user_event_hook_fn: Callable | None = None,
    max_allowable_consecutive_failures: int | None = None,
) -> Callable[[Callable], Callable]: ...
def expect_all(
    expectations: dict[str, str | Column],
) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def expect_all_or_fail(
    expectations: dict[str, str | Column],
) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def expect_all_or_drop(
    expectations: dict[str, str | Column],
) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def expect(name: str, inv: str | Column) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def expect_or_fail(name: str, inv: str | Column) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def expect_or_drop(name: str, inv: str | Column) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]: ...
def read(name: str) -> DataFrame: ...
def read_stream(name: str) -> DataFrame: ...
def create_streaming_table(
    name: str,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
    table_properties: dict[str, str] | None = None,
    partition_cols: Collection[str] | None = None,
    path: str | None = None,
    schema: StructType | str | None = None,
    expect_all: dict[str, str | Column] | None = None,
    expect_all_or_drop: dict[str, str | Column] | None = None,
    expect_all_or_fail: dict[str, str | Column] | None = None,
    cluster_by: list[str] | None = None,
    row_filter: str | None = None,
) -> None: ...
def apply_changes(
    target: str,
    source: str,
    keys: list[str] | list[Column],
    sequence_by: str | Column,
    where: str | Column | None = None,
    ignore_null_updates: bool | None = None,
    apply_as_deletes: str | Column | None = None,
    apply_as_truncates: str | Column | None = None,
    column_list: list[str] | list[Column] | None = None,
    except_column_list: list[str] | list[Column] | None = None,
    stored_as_scd_type: str | int = "1",
    track_history_column_list: list[str] | list[Column] | None = None,
    track_history_except_column_list: list[str] | list[Column] | None = None,
    flow_name: str | None = None,
    once: bool = False,
    ignore_null_updates_column_list: list[str] | list[Column] | None = None,
    ignore_null_updates_except_column_list: list[str] | list[Column] | None = None,
    columns_to_update: str | Column | None = None,
) -> None: ...
def apply_changes_from_snapshot(
    target: str,
    source: str | None = None,
    keys: list[str] | list[Column] | None = None,
    stored_as_scd_type: str | int | None = None,
    snapshot_and_version: Callable | None = None,
    track_history_column_list: list[str] | list[Column] | None = None,
    track_history_except_column_list: list[str] | list[Column] | None = None,
) -> None: ...

# New method stubs (not in the latest databricks-dlt package - 0.3.0 yet)
def create_auto_cdc_flow(
    target: str,
    source: Any,
    keys: list[str | Column],
    sequence_by: str | Column,
    ignore_null_updates: bool = False,
    apply_as_deletes: str | Column | None = None,
    apply_as_truncates: str | Column | None = None,
    column_list: list[str | Column] | None = None,
    except_column_list: list[str | Column] | None = None,
    stored_as_scd_type: str | int = "1",
    track_history_column_list: list[str | Column] | None = None,
    track_history_except_column_list: list[str | Column] | None = None,
    name: str | None = None,
    once: bool = False,
) -> None: ...
def create_auto_cdc_from_snapshot_flow(
    target: str,
    source: Any,
    keys: list[str | Column],
    stored_as_scd_type: str | int = "1",
    track_history_column_list: list[str | Column] | None = None,
    track_history_except_column_list: list[str | Column] | None = None,
) -> None: ...
def create_sink(
    name: str,
    format: str,  # noqa: A002
    options: dict[str, Any],
) -> None: ...
