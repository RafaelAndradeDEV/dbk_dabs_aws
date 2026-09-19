"""Bronze staging model for the `customers` source (PySpark).

Reads raw customer parquet files from the S3 landing zone in BATCH mode
(materialized view, no streaming) and applies cast / rename / dedup only,
following the medallion bronze rules. One row per `customer_id` is kept,
using the latest `ModifiedDate`.
"""

from databricks.connect import DatabricksSession
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import (
    DateType,
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

# Batch source: s3://<base>/<env>/marketing/customers/
customers_path = f"{source_path}/{env_target}/marketing/customers/"

schema = StructType(
    [
        StructField("customer_id", IntegerType(), False, metadata={"comment": "Primary key: unique customer id."}),
        StructField("full_name", StringType(), True, metadata={"comment": "Customer full name."}),
        StructField("email", StringType(), True, metadata={"comment": "Customer email address."}),
        StructField("country", StringType(), True, metadata={"comment": "Customer country (ISO name)."}),
        StructField("signup_date", DateType(), True, metadata={"comment": "Date the customer registered."}),
        StructField(
            "acquisition_channel",
            StringType(),
            True,
            metadata={"comment": "Marketing channel that acquired the customer."},
        ),
        StructField("updated_at", TimestampType(), True, metadata={"comment": "Source last-modified timestamp."}),
        StructField("system_name", StringType(), False, metadata={"comment": "Source system identifier."}),
        StructField("extracted_at", TimestampType(), True, metadata={"comment": "Ingestion (extraction) timestamp."}),
    ]
)


@dp.materialized_view(
    name=f"{catalog_name}.{bronze_schema_name}.stg_customers",
    comment="Bronze staging: cleaned customer master (one row per customer_id).",
    schema=schema,
    cluster_by=["customer_id"],
    table_properties={"quality": "bronze", "layer": "staging"},
)
def stg_customers(spark: SparkSession = None):
    """Read raw customer parquet from S3 and deduplicate to latest record."""
    spark = spark or DatabricksSession.builder.getOrCreate()

    raw = spark.read.format("parquet").load(customers_path)

    dedup_window = Window.partitionBy("CustomerId").orderBy(col("ModifiedDate").desc())

    return (
        raw.withColumn("__rn__", row_number().over(dedup_window))
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
    )
