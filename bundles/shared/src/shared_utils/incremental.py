"""JDBC Incremental Batch Ingestion with Flexible Modes.

This script ingests data from a JDBC source into a Delta table on Databricks.
It supports five ingestion strategies, configurable via the `--mode` argument:

1. full-load
   - Reads the entire source table and overwrites the target Delta table.

2. append-only
   - Requires a watermark column.
   - Queries the source for rows where `watermark > last_watermark_in_target`.
   - Appends new rows only (suitable for immutable event data).

3. delete-append
   - Requires a watermark column.
   - Deletes rows in the target table where `watermark = last_watermark_in_target`.
   - Re-appends from the source for `watermark >= last_watermark_in_target`.

4. scd-type-1
   - Requires a primary key column(s) and optionally a watermark column.
   - Supports a single primary key or a list of columns (comma-separated).
   - Creates a synthetic column `mergekey` (PK or concatenation of multiple PKs).
   - Performs incremental merge/delete into the target table using `mergekey`.

5. scd-type-2
   - Requires a primary key column(s) and optionally a watermark column.
   - Maintains historical versions of records using Slowly Changing Dimension Type 2.
   - Supports single or multiple primary keys.
   - Adds metadata columns: 'current', 'effective_date', 'end_date'.
   - Performs incremental merge with change detection for historical versioning.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_date,
    current_timestamp,
    date_sub,
    from_utc_timestamp,
    lit,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException


def get_spark_session() -> SparkSession:
    """Initialize and return a SparkSession.

    Returns:
        SparkSession: Active Spark session for running the job.
    """
    return SparkSession.builder.appName("JDBC_Incremental_Batch_Ingestion").getOrCreate()


# Helper functions
def add_extracted_at(df: DataFrame) -> DataFrame:
    """Add '__extracted_at__' timestamp column (UTC-3).

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with the new '__extracted_at__' column added.
    """
    return df.withColumn("__extracted_at__", from_utc_timestamp(current_timestamp(), "UTC-3"))


def get_last_watermark_value(spark: SparkSession, target_table: str, watermark_col: str | None) -> str | None:
    """Return the last watermark value from a Delta table.

    Args:
        spark (SparkSession): Active Spark session.
        target_table (str): Fully qualified name of the Delta table.
        watermark_col (str | None): Column used as watermark.

    Returns:
        str | None: The latest watermark value or None if unavailable.
    """
    if not watermark_col:
        return None
    try:
        target_df = spark.read.table(target_table)
        return target_df.select(spark_max(col(watermark_col))).first()[0]
    except AnalysisException:
        return None


def filter_by_watermark(
    df: DataFrame, watermark_col: str | None = None, last_value: str | None = None, delete_append: bool = False
) -> DataFrame:
    """Filter DataFrame based on watermark logic.

    Args:
        df (DataFrame): Input DataFrame.
        watermark_col (str | None): Column to use as watermark.
        last_value (str | None): Last known watermark value.
        delete_append (bool): Whether to include equality condition for filtering.

    Returns:
        DataFrame: Filtered DataFrame according to watermark conditions.
    """
    if watermark_col and last_value:
        if delete_append:
            print(f"Applying filter on source: {watermark_col} >= {last_value}")
            return df.filter(col(watermark_col) >= last_value)
        print(f"Applying filter on source: {watermark_col} > {last_value}")
        return df.filter(col(watermark_col) > last_value)
    return df


def handle_initial_load(
    source_df: DataFrame,
    target_table: str,
    mode: str,
    pk_col: str | None,
    watermark_col: str | None,
):
    """Perform initial load when target table does not exist.

    Args:
        source_df (DataFrame): Source data from JDBC.
        target_table (str): Target Delta table name.
        mode (str): Ingestion mode.
        pk_col (str | None): Primary key column(s).
        watermark_col (str | None): Watermark column if applicable.
    """
    if mode == "full-load":
        processed_df = add_extracted_at(source_df)
        (
            processed_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
    elif mode == "append-only":
        processed_df = add_extracted_at(source_df)
        processed_df.write.format("delta").mode("append").saveAsTable(target_table)
    elif mode == "delete-append":
        processed_df = add_extracted_at(source_df)
        (
            processed_df.write.format("delta")
            .mode("append")
            .option("delta.enableChangeDataFeed", True)
            .option("delta.enableRowTracking", True)
            .option("delta.enableDeletionVectors", True)
            .saveAsTable(target_table)
        )
    elif mode == "scd-type-1":
        if not pk_col:
            raise ValueError("scd-type-1 mode requires a primary key column for initial load.")
        pk_cols = [c.strip() for c in pk_col.split(",")] if "," in pk_col else [pk_col]
        if len(pk_cols) > 1:
            processed_df = source_df.withColumn("mergekey", concat_ws("||", *[col(c) for c in pk_cols]))
        else:
            processed_df = source_df.withColumn("mergekey", col(pk_cols[0]))
        processed_df = add_extracted_at(processed_df)
        (
            processed_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.enableChangeDataFeed", True)
            .option("delta.enableRowTracking", True)
            .option("delta.enableDeletionVectors", True)
            .saveAsTable(target_table)
        )
    elif mode == "scd-type-2":
        processed_df = add_extracted_at(source_df)
        date_col = watermark_col if watermark_col else "__extracted_at__"
        processed_df = (
            processed_df.withColumn("current", lit(True))
            .withColumn("effective_date", col(date_col))
            .withColumn("end_date", lit(None).cast(TimestampType()))
        )
        processed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    else:
        raise ValueError(f"Mode '{mode}' not supported for initial load.")

    rows_written = processed_df.count()
    print(f"Initial load completed. {rows_written} rows written.")


# Mode-specific ingestion functions
def run_full_load(source_df: DataFrame, target_table: str):
    """Execute full-load mode by overwriting the target table.

    Args:
        source_df (DataFrame): Source data from JDBC.
        target_table (str): Target Delta table name.
    """
    processed_df = add_extracted_at(source_df)
    processed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"Full-load completed. Rows written: {processed_df.count()}")


def run_append_only(source_df: DataFrame, target_table: str):
    """Execute append-only mode.

    Args:
        source_df (DataFrame): Source data from JDBC.
        target_table (str): Target Delta table name.
    """
    processed_df = add_extracted_at(source_df)
    processed_df.write.format("delta").mode("append").saveAsTable(target_table)
    print(f"Append-only completed. Rows written: {processed_df.count()}")


def run_delete_append(spark: SparkSession, source_df: DataFrame, target_table: str, watermark_col: str | None):
    """Execute delete-append mode.

    Deletes records in the target table with the last watermark, then appends new data.

    Args:
        spark (SparkSession): Active Spark session.
        source_df (DataFrame): Source data.
        target_table (str): Target Delta table name.
        watermark_col (str | None): Watermark column used for filtering.
    """
    last_value = get_last_watermark_value(spark, target_table, watermark_col)
    if last_value:
        target_dt = DeltaTable.forName(spark, target_table)
        target_dt.delete(col(watermark_col) == last_value)
    processed_df = add_extracted_at(source_df)
    processed_df.write.format("delta").mode("append").saveAsTable(target_table)
    print(f"Delete-append completed. Rows written: {processed_df.count()}")


def run_scd_type_1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    pk_col: str | None,
    watermark_col: str | None,
    time_window: int | None,
):
    """Execute Slowly Changing Dimension Type 1 (SCD1) ingestion.

    Updates existing rows when data changes.

    Args:
        spark (SparkSession): Active Spark session.
        source_df (DataFrame): Source data.
        target_table (str): Target Delta table name.
        pk_col (str | None): Primary key column(s).
        watermark_col (str | None): Watermark column.
        time_window (int | None): Optional lookback window (days).
    """
    processed_df = add_extracted_at(source_df)

    # Add mergekey
    pk_cols = [c.strip() for c in pk_col.split(",")] if pk_col and "," in pk_col else [pk_col]
    if len(pk_cols) > 1:
        processed_df = processed_df.withColumn("mergekey", concat_ws("||", *[col(c) for c in pk_cols]))
    else:
        processed_df = processed_df.withColumn("mergekey", col(pk_cols[0]))

    target_dt = DeltaTable.forName(spark, target_table)

    # Columns to check for changes
    exclude_cols = ["mergekey", "__extracted_at__"]
    business_cols = [c for c in processed_df.columns if c not in exclude_cols]

    change_condition = lit(False)
    for c in business_cols:
        change_condition = change_condition | (col(f"target.{c}") != col(f"source.{c}"))

    delete_condition = (
        f"target.{watermark_col} >= date_sub(current_date(), {time_window})" if time_window and watermark_col else None
    )

    (
        target_dt.alias("target")
        .merge(processed_df.alias("source"), "target.mergekey = source.mergekey")
        .whenMatchedUpdate(
            condition=change_condition, set={c: f"source.{c}" for c in [*business_cols, "__extracted_at__"]}
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=delete_condition)
        .execute()
    )

    history = spark.sql(f"DESCRIBE HISTORY {target_table}")
    latest_operation = history.limit(1).collect()[0]

    print("Operation metrics:")
    for key, value in latest_operation.operationMetrics.items():
        print(f"\t{key}: {value}")


def run_scd_type_2(
    spark: SparkSession, source_df: DataFrame, target_table: str, pk_col: str | None, watermark_col: str | None
):
    """Slowly Changing Dimension Type 2 (SCD2) Implementation:

    This mode maintains historical versions of records by creating new rows for changes while expiring old ones.
    Key features:
    - Requires a primary key (single or multiple columns) to identify unique records.
    - Optional watermark column for incremental filtering (e.g., last_modified_timestamp); falls back to
      __extracted_at__ if not provided.
    - Adds metadata columns: 'current' (boolean flag for active version), 'effective_date' (start of version),
      'end_date' (end of version, NULL for current).

    Logic Flow:
    1. Normalize primary keys and check if target table exists.
    2. If table doesn't exist, perform initial load: insert all source records with current=true, effective_date
       set, end_date=NULL.
    3. Filter source data incrementally using watermark if provided.
    4. Identify business columns (non-PK, non-watermark/extracted) to detect changes.
    5. Prepare staged updates using the 'NULL merge key' pattern:
       - Part 1: All source records with merge_key = PK (for new records or unchanged matches).
       - Part 2: Changed existing records with merge_key = NULL (to force insertion as new versions).
    6. Perform Delta merge:
       - On match (by PK): If current and changed, expire by setting current=false and end_date to new
         version's date.
       - On no match: Insert new version with current=true, effective_date set, end_date=NULL.

    This ensures:
    - New records are inserted as current versions.
    - Unchanged records are ignored (matched but no update if not changed).
    - Changed records: Old version expired, new version inserted.
    - Historical versions preserved for auditing/time-travel.

    Note: Assumes source provides complete snapshots or incremental changes; for full snapshots without watermark,
    it will reprocess all each time.
    """
    print(
        f"Running SCD TYPE 2 mode with PK '{pk_col}'" + (f" and watermark '{watermark_col}'." if watermark_col else ".")
    )

    # Normalize PKs into a list (supports single or comma-separated multiple keys)
    pk_cols = [c.strip() for c in pk_col.split(",")] if pk_col and "," in pk_col else [pk_col]

    target_dt = None
    target_df = None
    last_watermark_value = None
    try:
        target_dt = DeltaTable.forName(spark, target_table)
        target_df = target_dt.toDF()
        if watermark_col:
            last_watermark_value = target_df.select(spark_max(col(watermark_col))).first()[0]
    except AnalysisException:
        pass

    # If table exists but lacks SCD2 columns (e.g., created via full-load), add and backfill them
    if target_dt is not None:
        existing_cols = set(target_df.columns)
        required_scd_cols = {"current", "effective_date", "end_date"}
        if not required_scd_cols.issubset(existing_cols):
            base_date_col_on_target = (
                watermark_col if watermark_col and watermark_col in existing_cols else "__extracted_at__"
            )
            migrated_df = target_df
            if "current" not in existing_cols:
                migrated_df = migrated_df.withColumn("current", lit(True))
            if "effective_date" not in existing_cols:
                if base_date_col_on_target in existing_cols:
                    migrated_df = migrated_df.withColumn("effective_date", col(base_date_col_on_target))
                else:
                    migrated_df = migrated_df.withColumn("effective_date", current_timestamp())
            if "end_date" not in existing_cols:
                migrated_df = migrated_df.withColumn("end_date", lit(None))
            migrated_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                target_table
            )
            target_dt = DeltaTable.forName(spark, target_table)
            target_df = target_dt.toDF()

    updates_df = add_extracted_at(source_df)

    # Use watermark_col if provided, else fallback to __extracted_at__ for effective/end dates
    date_col = watermark_col if watermark_col else "__extracted_at__"

    if last_watermark_value is not None:
        updates_df = updates_df.filter(col(watermark_col) > last_watermark_value)

    # Identify business columns (excluding PK, watermark, and extracted_at) for detecting changes
    exclude_list = [*pk_cols, "__extracted_at__"]
    if watermark_col:
        exclude_list.append(watermark_col)
    business_cols = [c for c in updates_df.columns if c not in exclude_list]

    # Build condition to detect if any business column has changed
    change_condition = lit(False)
    for c in business_cols:
        change_condition = change_condition | (col(f"updates.{c}") != col(f"target.{c}"))

    # Create composite PK expressions if multiple keys
    if len(pk_cols) > 1:
        updates_pk = concat_ws("||", *[col(f"updates.{c}") for c in pk_cols])
        target_pk = concat_ws("||", *[col(f"target.{c}") for c in pk_cols])
    else:
        updates_pk = col(f"updates.{pk_cols[0]}")
        target_pk = col(f"target.{pk_cols[0]}")

    current_col = col("target.current")

    # Part 1: Potential new records or updates (with merge_key = PK for matching)
    if len(pk_cols) > 1:
        updates_part1 = updates_df.withColumn("merge_key", concat_ws("||", *[col(c) for c in pk_cols]))
    else:
        updates_part1 = updates_df.withColumn("merge_key", col(pk_cols[0]))

    # Part 2: Changed existing records (set merge_key=NULL to force insert as new version)
    updates_part2 = (
        updates_df.alias("updates")
        .join(
            target_df.alias("target"),
            on=(updates_pk == target_pk) & (current_col == lit(True)) & change_condition,
            how="inner",
        )
        .select("updates.*")
        .withColumn("merge_key", lit(None))
    )

    # Combine into staged updates: changed records will be inserted as new versions
    staged_updates = updates_part1.unionByName(updates_part2, allowMissingColumns=True)

    # Build merge condition using PK (composite if multiple)
    if len(pk_cols) > 1:
        pk_terms = ", ".join(f"target.{c}" for c in pk_cols)
        merge_condition = f"concat_ws('||', {pk_terms}) = source.merge_key"
    else:
        merge_condition = f"target.{pk_cols[0]} = source.merge_key"

    # Build condition for updating existing records (only if changed and current)
    change_condition_str = " OR ".join(f"target.{c} <> source.{c}" for c in business_cols) if business_cols else "false"
    update_condition = f"target.current = true AND ({change_condition_str})"

    # Perform SCD2 merge: Expire old versions if changed, insert new versions
    target_dt.alias("target").merge(source=staged_updates.alias("source"), condition=merge_condition).whenMatchedUpdate(
        condition=update_condition, set={"current": "false", "end_date": f"source.{date_col}"}
    ).whenNotMatchedInsert(
        values={c: f"source.{c}" for c in updates_df.columns}
        | {"current": "true", "effective_date": f"source.{date_col}", "end_date": "NULL"}
    ).execute()

    history = spark.sql(f"DESCRIBE HISTORY {target_table}")
    latest_operation = history.limit(1).collect()[0]

    print("Operation metrics:")
    for key, value in latest_operation.operationMetrics.items():
        print(f"\t{key}: {value}")


def run_incremental_ingestion(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    mode: str,
    pk_col: str | None = None,
    watermark_col: str | None = None,
    time_window: int | None = None,
    full_refresh: bool = False,
) -> None:
    """Main orchestration for incremental ingestion.

    Determines whether to perform an initial load or an incremental update,
    and calls the appropriate mode-specific ingestion function.

    Args:
        spark (SparkSession): Active Spark session.
        source_df (DataFrame): Source data from JDBC.
        target_table (str): Target Delta table.
        mode (str): Ingestion mode.
        pk_col (str | None): Primary key column(s).
        watermark_col (str | None): Watermark column name.
        time_window (int | None): Lookback window (in days).
        full_refresh (bool): If True, force full reload.
    """
    print(f"Starting ingestion for target table: {target_table} (mode={mode})")

    # Check if table exists
    try:
        spark.read.table(target_table).limit(1).collect()
        table_exists = True
    except AnalysisException:
        table_exists = False

    # Full refresh
    if table_exists and full_refresh:
        print(f"Full refresh requested for {target_table}. Dropping table...")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        handle_initial_load(source_df, target_table, mode, pk_col, watermark_col)
        return

    # Initial load
    if not table_exists:
        print("Target table not found. Performing initial load...")
        handle_initial_load(source_df, target_table, mode, pk_col, watermark_col)
        return

    # Apply watermark filtering for incremental modes
    last_watermark_value = (
        get_last_watermark_value(spark, target_table, watermark_col) if watermark_col and mode != "full-load" else None
    )

    filtered_df = source_df
    if watermark_col and last_watermark_value:
        if mode == "delete-append":
            filtered_df = filter_by_watermark(
                df=source_df, watermark_col=watermark_col, last_value=last_watermark_value, delete_append=True
            )
        elif mode == "scd-type-1" and time_window:
            filtered_df = source_df.filter(col(watermark_col) >= date_sub(current_date(), time_window))
        elif mode in ("append-only", "delete-append", "scd-type-2"):
            filtered_df = filter_by_watermark(
                df=source_df, watermark_col=watermark_col, last_value=last_watermark_value, delete_append=False
            )
        print(f"Rows after watermark filter: {filtered_df.count()}")

    # Run the appropriate ingestion mode
    if mode == "full-load":
        run_full_load(filtered_df, target_table)
    elif mode == "append-only":
        run_append_only(filtered_df, target_table)
    elif mode == "delete-append":
        run_delete_append(spark, filtered_df, target_table, watermark_col)
    elif mode == "scd-type-1":
        run_scd_type_1(spark, filtered_df, target_table, pk_col, watermark_col, time_window)
    elif mode == "scd-type-2":
        run_scd_type_2(spark, filtered_df, target_table, pk_col, watermark_col)
    else:
        raise ValueError(f"Invalid mode: {mode}")

    print(f"Ingestion completed for {target_table}\n")
