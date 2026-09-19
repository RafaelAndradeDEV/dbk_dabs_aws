# Databricks notebook source
"""Bronze job notebook: stg_customers (PySpark).

Reads raw customer parquet from S3 (batch), deduplicates to the latest record
per customer and writes a managed Delta table. Mirrors the DLT `stg_customers`.
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
from pyspark.sql.functions import col, lit, row_number

customers_path = f"{source_path}/{env_target}/marketing/customers/"
dedup_window = Window.partitionBy("CustomerId").orderBy(col("ModifiedDate").desc())

df = (
    spark.read.format("parquet")
    .load(customers_path)
    .withColumn("__rn__", row_number().over(dedup_window))
    .filter(col("__rn__") == 1)
    .select(
        col("CustomerId").cast("int").alias("customer_id"),
        col("FullName").cast("string").alias("full_name"),
        col("Email").cast("string").alias("email"),
        col("Country").cast("string").alias("country"),
        col("SignupDate").cast("date").alias("signup_date"),
        col("AcquisitionChannel").cast("string").alias("acquisition_channel"),
        col("ModifiedDate").cast("timestamp").alias("updated_at"),
        lit("ecommerce_app").alias("system_name"),
        col("__extracted_at__").cast("timestamp").alias("extracted_at"),
    )
    .filter(col("customer_id").isNotNull())
)

# COMMAND ----------
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{stg_schema}.stg_customers{table_suffix}"
)
