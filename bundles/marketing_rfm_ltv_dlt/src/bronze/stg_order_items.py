"""Bronze staging model for the `order_items` source (PySpark).

Reads raw order-line parquet from the S3 landing zone in BATCH mode and
applies cast / rename / dedup only. Line-level economics (gross / net) are
intentionally deferred to the silver layer per medallion rules.
"""

from databricks.connect import DatabricksSession
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

catalog_name = spark.conf.get("catalog_name")
bronze_schema_name = spark.conf.get("bronze_schema_name")
source_path = spark.conf.get("marketing_source_path")
env_target = spark.conf.get("env_target")

order_items_path = f"{source_path}/{env_target}/marketing/order_items/"

schema = StructType(
    [
        StructField("order_item_id", IntegerType(), False, metadata={"comment": "Primary key: order line id."}),
        StructField("order_id", IntegerType(), False, metadata={"comment": "Foreign key to orders."}),
        StructField("product_id", IntegerType(), True, metadata={"comment": "Product identifier."}),
        StructField("category", StringType(), True, metadata={"comment": "Product category."}),
        StructField("quantity", IntegerType(), True, metadata={"comment": "Units ordered on the line."}),
        StructField("unit_price", DecimalType(18, 4), True, metadata={"comment": "Unit list price."}),
        StructField("discount", DecimalType(9, 4), True, metadata={"comment": "Line discount fraction (0-1)."}),
        StructField("updated_at", TimestampType(), True, metadata={"comment": "Source last-modified timestamp."}),
        StructField("extracted_at", TimestampType(), True, metadata={"comment": "Ingestion (extraction) timestamp."}),
    ]
)


@dp.materialized_view(
    name=f"{catalog_name}.{bronze_schema_name}.stg_order_items",
    comment="Bronze staging: cleaned order line items (one row per order_item_id).",
    schema=schema,
    cluster_by=["order_id"],
    table_properties={"quality": "bronze", "layer": "staging"},
)
def stg_order_items(spark: SparkSession = None):
    """Read raw order-item parquet from S3 and deduplicate to latest record."""
    spark = spark or DatabricksSession.builder.getOrCreate()

    raw = spark.read.format("parquet").load(order_items_path)

    dedup_window = Window.partitionBy("OrderItemId").orderBy(col("ModifiedDate").desc())

    return (
        raw.withColumn("__rn__", row_number().over(dedup_window))
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
    )
