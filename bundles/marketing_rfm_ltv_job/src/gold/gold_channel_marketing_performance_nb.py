# Databricks notebook source
"""Gold job notebook: gold_channel_marketing_performance (SQL).

Channel marketing scorecard: revenue, CAC, ROI, LTV:CAC and CTR per acquisition
channel. Mirrors the DLT `gold_channel_marketing_performance`.
"""

# COMMAND ----------
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("table_suffix", "")
dbutils.widgets.text("stg_schema", "")
dbutils.widgets.text("mart_schema", "")

target_catalog = dbutils.widgets.get("target_catalog")
table_suffix = dbutils.widgets.get("table_suffix")
stg_schema = dbutils.widgets.get("stg_schema")
mart_schema = dbutils.widgets.get("mart_schema")

# COMMAND ----------
spark.sql(
    f"""
    create or replace table {target_catalog}.{mart_schema}.gold_channel_marketing_performance{table_suffix}
    using delta
    cluster by (acquisition_channel)
    as
    with campaign_agg as (
        select
            channel
            , sum(cost) as total_cost
            , sum(impressions) as impressions
            , sum(clicks) as clicks
        from {target_catalog}.{stg_schema}.stg_marketing_campaigns{table_suffix}
        group by channel
    )
    select
        ltv.acquisition_channel
        , count(distinct ltv.customer_id) as customers
        , sum(ltv.total_orders) as total_orders
        , cast(sum(ltv.historical_clv) as decimal(18, 2)) as total_revenue
        , cast(avg(ltv.predictive_clv) as decimal(18, 2)) as avg_predictive_clv
        , cast(coalesce(c.total_cost, 0) as decimal(18, 2)) as total_campaign_cost
        , cast(coalesce(c.total_cost, 0) / nullif(count(distinct ltv.customer_id), 0) as decimal(18, 2)) as cac
        , round((sum(ltv.historical_clv) - coalesce(c.total_cost, 0)) / nullif(c.total_cost, 0), 2) as marketing_roi
        , round(
            avg(ltv.predictive_clv)
            / nullif(coalesce(c.total_cost, 0) / nullif(count(distinct ltv.customer_id), 0), 0), 2
        ) as ltv_to_cac_ratio
        , c.impressions
        , c.clicks
        , round(c.clicks / nullif(c.impressions, 0), 4) as click_through_rate
    from {target_catalog}.{mart_schema}.gold_customer_ltv{table_suffix} as ltv
    left join campaign_agg as c on ltv.acquisition_channel = c.channel
    group by ltv.acquisition_channel, c.total_cost, c.impressions, c.clicks
    """
)
