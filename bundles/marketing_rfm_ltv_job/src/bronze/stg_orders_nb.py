# Databricks notebook source
"""Bronze job notebook: stg_orders (PySpark).

Reads raw order-header parquet from S3 (batch), casts/renames and drops rows
with a null key. Mirrors the DLT `stg_orders`.
"""

# COMMAND ----------
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("table_suffix", "")
dbutils.widgets.text("stg_schema", "")
dbutils.widgets.text("marketing_source_path", "")
dbutils.widgets.text("env_target", "")

target_catalog = dbutils.widgets.get("target_catalog")
table_suffix = dbutils.widgets.get("table_suffix")
stg_schema = dbutils.widgets.get("stg_schema")
source_path = dbutils.widgets.get("marketing_source_path")
env_target = dbutils.widgets.get("env_target")

# COMMAND ----------
from pyspark.sql.functions import col, lower

orders_path = f"{source_path}/{env_target}/marketing/orders/"

df = (
    spark.read.format("parquet")
    .load(orders_path)
    .select(
        col("OrderId").cast("int").alias("order_id"),
        col("CustomerId").cast("int").alias("customer_id"),
        col("OrderTimestamp").cast("timestamp").alias("order_ts"),
        col("OrderTimestamp").cast("date").alias("order_date"),
        col("Channel").cast("string").alias("channel"),
        lower(col("Status").cast("string")).alias("status"),
        col("ModifiedDate").cast("timestamp").alias("updated_at"),
        col("__extracted_at__").cast("timestamp").alias("extracted_at"),
    )
    .filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
)

# COMMAND ----------
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{stg_schema}.stg_orders{table_suffix}"
)
