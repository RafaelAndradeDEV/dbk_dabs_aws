"""Gold mart: customer RFM segmentation (PySpark).

Computes Recency / Frequency / Monetary metrics per customer from the silver
customer-order fact, scores each dimension into quintiles (1-5, 5 = best) with
NTILE, then maps the scores to actionable marketing segments.

Recency is measured against the dataset's latest order date (snapshot date) so
the result is reproducible regardless of when the pipeline runs.
"""

from databricks.connect import DatabricksSession
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat_ws, countDistinct, datediff, lit, ntile, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum

catalog_name = spark.conf.get("catalog_name")
silver_schema_name = spark.conf.get("silver_schema_name")
gold_schema_name = spark.conf.get("gold_schema_name")


@dp.materialized_view(
    name=f"{catalog_name}.{gold_schema_name}.gold_customer_rfm",
    comment="Gold: per-customer RFM scores (1-5) and marketing segment.",
    cluster_by=["rfm_segment"],
    table_properties={"quality": "gold", "layer": "marketing_mart"},
)
@dp.expect_or_fail("valid_customer", "customer_id IS NOT NULL")
def gold_customer_rfm(spark: SparkSession = None):
    """Build the RFM segmentation mart from silver customer orders."""
    spark = spark or DatabricksSession.builder.getOrCreate()

    orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.slv_customer_orders")

    global_window = Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    per_customer = (
        orders.groupBy("customer_id", "acquisition_channel")
        .agg(
            spark_max("order_date").alias("last_order_date"),
            spark_min("order_date").alias("first_order_date"),
            countDistinct("order_id").alias("frequency"),
            spark_sum("net_revenue").alias("monetary"),
        )
        .withColumn("snapshot_date", spark_max("last_order_date").over(global_window))
        .withColumn("recency_days", datediff(col("snapshot_date"), col("last_order_date")))
    )

    # Quintile scoring (5 = best). Recency is inverted: fewer days = higher score.
    # customer_id breaks ties so NTILE buckets are deterministic across runs.
    r_window = Window.orderBy(col("recency_days").desc(), col("customer_id"))
    f_window = Window.orderBy(col("frequency").asc(), col("customer_id"))
    m_window = Window.orderBy(col("monetary").asc(), col("customer_id"))

    scored = (
        per_customer.withColumn("r_score", ntile(5).over(r_window))
        .withColumn("f_score", ntile(5).over(f_window))
        .withColumn("m_score", ntile(5).over(m_window))
    )

    return scored.select(
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
        when((col("r_score") >= 4) & (col("f_score") >= 4), "Champions")
        .when((col("r_score") >= 3) & (col("f_score") >= 4), "Loyal Customers")
        .when((col("r_score") >= 4) & (col("f_score") == 3), "Potential Loyalist")
        .when((col("r_score") == 5) & (col("f_score") <= 2), "New Customers")
        .when((col("r_score") == 4) & (col("f_score") <= 2), "Promising")
        .when((col("r_score") == 3) & (col("f_score") == 3), "Needs Attention")
        .when((col("r_score") <= 2) & (col("f_score") >= 4), "Cannot Lose Them")
        .when((col("r_score") <= 2) & (col("f_score") == 3), "At Risk")
        .when((col("r_score") == 3) & (col("f_score") <= 2), "About to Sleep")
        .when((col("r_score") <= 2) & (col("f_score") == 2), "Hibernating")
        .otherwise("Lost")
        .alias("rfm_segment"),
        col("snapshot_date"),
    )
