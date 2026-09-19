# Databricks notebook source
"""Gold job notebook: gold_customer_rfm (PySpark).

Computes RFM metrics per customer, scores each dimension into quintiles with
NTILE (5 = best) and assigns a marketing segment. Mirrors the DLT model; the
`assign_rfm_segment` ladder is identical to
`marketing_rfm_ltv_dlt/src/gold/rfm_segments.py` (single canonical logic).
"""

# COMMAND ----------
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("table_suffix", "")
dbutils.widgets.text("int_schema", "")
dbutils.widgets.text("mart_schema", "")

target_catalog = dbutils.widgets.get("target_catalog")
table_suffix = dbutils.widgets.get("table_suffix")
int_schema = dbutils.widgets.get("int_schema")
mart_schema = dbutils.widgets.get("mart_schema")

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import col, concat_ws, countDistinct, datediff, lit, ntile, udf
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StringType


def assign_rfm_segment(r, f, m=0):  # noqa: ARG001 - m kept for a stable (r, f, m) signature
    """Map R/F/M scores (1-5, 5 = best) to a marketing segment label."""
    if r >= 4 and f >= 4:
        return "Champions"
    if r >= 3 and f >= 4:
        return "Loyal Customers"
    if r >= 4 and f == 3:
        return "Potential Loyalist"
    if r == 5 and f <= 2:
        return "New Customers"
    if r == 4 and f <= 2:
        return "Promising"
    if r == 3 and f == 3:
        return "Needs Attention"
    if r <= 2 and f >= 4:
        return "Cannot Lose Them"
    if r <= 2 and f == 3:
        return "At Risk"
    if r == 3 and f <= 2:
        return "About to Sleep"
    if r <= 2 and f == 2:
        return "Hibernating"
    return "Lost"


segment_udf = udf(assign_rfm_segment, StringType())

# COMMAND ----------
orders = spark.read.table(f"{target_catalog}.{int_schema}.slv_customer_orders{table_suffix}")
snapshot_date = orders.agg(spark_max("order_date").alias("d")).collect()[0]["d"]

per_customer = (
    orders.groupBy("customer_id", "acquisition_channel")
    .agg(
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date"),
        countDistinct("order_id").alias("frequency"),
        spark_sum("net_revenue").alias("monetary"),
    )
    .withColumn("recency_days", datediff(lit(snapshot_date), col("last_order_date")))
)

# customer_id breaks ties so NTILE buckets are deterministic across runs.
r_window = Window.orderBy(col("recency_days").desc(), col("customer_id"))
f_window = Window.orderBy(col("frequency").asc(), col("customer_id"))
m_window = Window.orderBy(col("monetary").asc(), col("customer_id"))

scored = (
    per_customer.withColumn("r_score", ntile(5).over(r_window))
    .withColumn("f_score", ntile(5).over(f_window))
    .withColumn("m_score", ntile(5).over(m_window))
)

result = scored.select(
    col("customer_id"),
    col("acquisition_channel"),
    col("first_order_date"),
    col("last_order_date"),
    col("recency_days"),
    col("frequency"),
    col("monetary"),
    col("r_score"),
    col("f_score"),
    col("m_score"),
    concat_ws("", col("r_score"), col("f_score"), col("m_score")).alias("rfm_cell"),
    segment_udf(col("r_score"), col("f_score"), col("m_score")).alias("rfm_segment"),
    lit(snapshot_date).alias("snapshot_date"),
)

# COMMAND ----------
result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_catalog}.{mart_schema}.gold_customer_rfm{table_suffix}"
)
