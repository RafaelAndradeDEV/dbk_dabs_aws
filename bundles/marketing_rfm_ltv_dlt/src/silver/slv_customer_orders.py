"""Silver model: customer-enriched order fact (PySpark).

Joins the conformed order fact with the customer master so every recognized
(completed) order carries the acquisition channel, signup date and geography
needed by the gold RFM / CLV / channel-performance marts.
"""

from databricks.connect import DatabricksSession
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

catalog_name = spark.conf.get("catalog_name")
bronze_schema_name = spark.conf.get("bronze_schema_name")
silver_schema_name = spark.conf.get("silver_schema_name")


@dp.materialized_view(
    name=f"{catalog_name}.{silver_schema_name}.slv_customer_orders",
    comment="Silver: completed orders enriched with customer acquisition attributes.",
    cluster_by=["customer_id"],
    table_properties={"quality": "silver", "layer": "intermediate"},
)
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect("recognized_revenue", "net_revenue >= 0")
def slv_customer_orders(spark: SparkSession = None):
    """Join order facts with the customer master; keep completed orders only."""
    spark = spark or DatabricksSession.builder.getOrCreate()

    orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.slv_order_facts").filter(
        col("status") == "completed"
    )
    customers = spark.read.table(f"{catalog_name}.{bronze_schema_name}.stg_customers")

    return (
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
