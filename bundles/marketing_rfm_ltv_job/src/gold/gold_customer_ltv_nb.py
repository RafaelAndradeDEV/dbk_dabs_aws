# Databricks notebook source
"""Gold job notebook: gold_customer_ltv (SQL).

Historical + predictive CLV, CAC and LTV:CAC per customer. CLV assumptions
(gross margin, expected lifespan) arrive as job parameters. Mirrors the DLT
`gold_customer_ltv`.
"""

# COMMAND ----------
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("table_suffix", "")
dbutils.widgets.text("stg_schema", "")
dbutils.widgets.text("int_schema", "")
dbutils.widgets.text("mart_schema", "")
dbutils.widgets.text("clv_gross_margin", "0.30")
dbutils.widgets.text("clv_expected_lifespan_years", "3")

target_catalog = dbutils.widgets.get("target_catalog")
table_suffix = dbutils.widgets.get("table_suffix")
stg_schema = dbutils.widgets.get("stg_schema")
int_schema = dbutils.widgets.get("int_schema")
mart_schema = dbutils.widgets.get("mart_schema")
margin = dbutils.widgets.get("clv_gross_margin")
lifespan = dbutils.widgets.get("clv_expected_lifespan_years")

# COMMAND ----------
spark.sql(
    f"""
    create or replace table {target_catalog}.{mart_schema}.gold_customer_ltv{table_suffix}
    using delta
    cluster by (acquisition_channel)
    as
    with snapshot as (
        select max(order_date) as ref_date
        from {target_catalog}.{int_schema}.slv_customer_orders{table_suffix}
    ),
    cust as (
        select
            customer_id
            , max(acquisition_channel) as acquisition_channel
            , min(order_date) as first_order_date
            , max(order_date) as last_order_date
            , count(distinct order_id) as total_orders
            , sum(net_revenue) as total_net_revenue
        from {target_catalog}.{int_schema}.slv_customer_orders{table_suffix}
        group by customer_id
    ),
    channel_cac as (
        select
            c.acquisition_channel
            , coalesce(camp.total_cost, 0) / nullif(count(distinct c.customer_id), 0) as cac
        from {target_catalog}.{stg_schema}.stg_customers{table_suffix} as c
        left join (
            select channel, sum(cost) as total_cost
            from {target_catalog}.{stg_schema}.stg_marketing_campaigns{table_suffix}
            group by channel
        ) as camp on c.acquisition_channel = camp.channel
        group by c.acquisition_channel, camp.total_cost
    ),
    metrics as (
        select
            cu.*
            , s.ref_date
            , cu.total_net_revenue / cu.total_orders as aov
            , cu.total_orders / (greatest(datediff(s.ref_date, cu.first_order_date), 1) / 365.0) as annual_freq
        from cust as cu
        cross join snapshot as s
    )
    select
        m.customer_id
        , m.acquisition_channel
        , m.first_order_date
        , m.last_order_date
        , m.total_orders
        , cast(m.total_net_revenue as decimal(18, 2)) as historical_clv
        , cast(m.aov as decimal(18, 2)) as avg_order_value
        , datediff(m.ref_date, m.first_order_date) as tenure_days
        , round(m.annual_freq, 4) as annual_order_frequency
        , cast(m.aov * m.annual_freq * {lifespan} * {margin} as decimal(18, 2)) as predictive_clv
        , cast(cc.cac as decimal(18, 2)) as customer_acquisition_cost
        , round((m.aov * m.annual_freq * {lifespan} * {margin}) / nullif(cc.cac, 0), 2) as ltv_to_cac_ratio
    from metrics as m
    left join channel_cac as cc on m.acquisition_channel = cc.acquisition_channel
    """
)
