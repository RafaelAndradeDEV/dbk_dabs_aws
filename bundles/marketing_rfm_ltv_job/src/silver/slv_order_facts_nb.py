# Databricks notebook source
"""Silver job notebook: slv_order_facts (SQL).

Joins order headers with their line items and rolls them up to one row per
order with gross / net revenue. Mirrors the DLT `slv_order_facts`.
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
spark.sql(
    f"""
    create or replace table {target_catalog}.{int_schema}.slv_order_facts{table_suffix}
    using delta
    cluster by (customer_id)
    as
    with lines as (
        select
            order_id
            , count(order_item_id) as num_line_items
            , sum(quantity) as total_quantity
            , cast(sum(unit_price * quantity) as decimal(18, 4)) as gross_revenue
            , cast(sum(unit_price * quantity * discount) as decimal(18, 4)) as total_discount
            , cast(sum(unit_price * quantity * (1 - discount)) as decimal(18, 4)) as net_revenue
        from {target_catalog}.{stg_schema}.stg_order_items{table_suffix}
        group by order_id
    )
    select
        o.order_id
        , o.customer_id
        , o.order_ts
        , o.order_date
        , o.channel
        , o.status
        , cast(l.num_line_items as int) as num_line_items
        , cast(l.total_quantity as bigint) as total_quantity
        , l.gross_revenue
        , l.total_discount
        , l.net_revenue
    from {target_catalog}.{stg_schema}.stg_orders{table_suffix} as o
    inner join lines as l on o.order_id = l.order_id
    where l.net_revenue >= 0
    """
)
