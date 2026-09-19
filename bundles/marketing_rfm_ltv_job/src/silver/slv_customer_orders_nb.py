# Databricks notebook source
"""Silver job notebook: slv_customer_orders (PySpark).

Joins the order fact with the customer master and keeps completed orders only,
carrying acquisition attributes downstream. Mirrors the DLT model.
"""

# COMMAND ----------
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("table_suffix", "")
dbutils.widgets.text("stg_schema", "")
dbutils.widgets.text("int_schema", "")

target_catalog = dbutils.widgets.get("target_catalog")
table_suffix = dbutils.widgets.get("table_suffix")
stg_schema = dbutils.widgets.get("stg_schema")
int_schema = dbutils.widgets.get("int_schema")

# COMMAND ----------
from pyspark.sql.functions import col

orders = spark.read.table(f"{target_catalog}.{int_schema}.slv_order_facts{table_suffix}").filter(
    col("status") == "completed"
)
customers = spark.read.table(f"{target_catalog}.{stg_schema}.stg_customers{table_suffix}")

result = (
    orders.alias("o")
    .join(customers.alias("c"), on="customer_id", how="inner")
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.acquisition_channel"),
        col("c.country"),
        col("c.signup_date"),
        col("o.order_ts"),
        col("o.order_date"),
        col("o.channel").alias("order_channel"),
        col("o.num_line_items"),
        col("o.total_quantity"),
        col("o.gross_revenue"),
        col("o.total_discount"),
        col("o.net_revenue"),
    )
)

# COMMAND ----------
result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{int_schema}.slv_customer_orders{table_suffix}"
)
