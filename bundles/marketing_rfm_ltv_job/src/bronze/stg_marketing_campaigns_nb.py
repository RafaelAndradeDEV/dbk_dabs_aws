# Databricks notebook source
"""Bronze job notebook: stg_marketing_campaigns (PySpark).

Reads raw campaign parquet from S3 (batch) and casts/renames. Campaign spend
per acquisition channel feeds CAC and channel ROI. Mirrors the DLT model.
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
from pyspark.sql.functions import col

campaigns_path = f"{source_path}/{env_target}/marketing/marketing_campaigns/"

df = (
    spark.read.format("parquet")
    .load(campaigns_path)
    .select(
        col("CampaignId").cast("int").alias("campaign_id"),
        col("Channel").cast("string").alias("channel"),
        col("CampaignName").cast("string").alias("campaign_name"),
        col("StartDate").cast("date").alias("start_date"),
        col("EndDate").cast("date").alias("end_date"),
        col("Cost").cast("decimal(18,2)").alias("cost"),
        col("Impressions").cast("bigint").alias("impressions"),
        col("Clicks").cast("bigint").alias("clicks"),
        col("ModifiedDate").cast("timestamp").alias("updated_at"),
        col("__extracted_at__").cast("timestamp").alias("extracted_at"),
    )
    .filter(col("campaign_id").isNotNull())
)

# COMMAND ----------
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{stg_schema}.stg_marketing_campaigns{table_suffix}"
)
