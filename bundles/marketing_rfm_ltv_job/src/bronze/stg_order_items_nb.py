# Databricks notebook source
"""Bronze job notebook: stg_order_items (PySpark).

Reads raw order-line parquet from S3 (batch), deduplicates to the latest record
per line and casts/renames. Mirrors the DLT `stg_order_items`.
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
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

items_path = f"{source_path}/{env_target}/marketing/order_items/"
dedup_window = Window.partitionBy("OrderItemId").orderBy(col("ModifiedDate").desc())

df = (
    spark.read.format("parquet")
    .load(items_path)
    .withColumn("__rn__", row_number().over(dedup_window))
    .filter(col("__rn__") == 1)
    .select(
        col("OrderItemId").cast("int").alias("order_item_id"),
        col("OrderId").cast("int").alias("order_id"),
        col("ProductId").cast("int").alias("product_id"),
        col("Category").cast("string").alias("category"),
        col("Quantity").cast("int").alias("quantity"),
        col("UnitPrice").cast("decimal(18,4)").alias("unit_price"),
        col("Discount").cast("decimal(9,4)").alias("discount"),
        col("ModifiedDate").cast("timestamp").alias("updated_at"),
        col("__extracted_at__").cast("timestamp").alias("extracted_at"),
    )
    .filter(col("order_item_id").isNotNull())
)

# COMMAND ----------
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{stg_schema}.stg_order_items{table_suffix}"
)
