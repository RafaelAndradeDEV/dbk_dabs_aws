-- Gold mart: Acquisition-channel marketing performance (SQL).
--
-- Rolls customer lifetime value up to the acquisition channel and joins it to
-- campaign spend / delivery to produce the executive marketing scorecard:
-- revenue, CAC, marketing ROI, LTV:CAC and click-through rate per channel.

create or refresh materialized view ${catalog_name}.${gold_schema_name}.gold_channel_marketing_performance (
    acquisition_channel string comment "Marketing acquisition channel."
    , customers bigint not null comment "Customers acquired through the channel."
    , total_orders bigint comment "Lifetime orders from channel customers."
    , total_revenue decimal(18, 2) comment "Lifetime net revenue from channel customers."
    , avg_predictive_clv decimal(18, 2) comment "Average modeled CLV per channel customer."
    , total_campaign_cost decimal(18, 2) comment "Total campaign spend on the channel."
    , cac decimal(18, 2) comment "Customer acquisition cost (cost / customers)."
    , marketing_roi double comment "(Revenue - cost) / cost."
    , ltv_to_cac_ratio double comment "Avg predictive CLV / CAC."
    , impressions bigint comment "Total impressions delivered."
    , clicks bigint comment "Total clicks generated."
    , click_through_rate double comment "Clicks / impressions."
    , constraint valid_channel expect (acquisition_channel is not null) on violation drop row
)
comment "Gold: marketing scorecard per acquisition channel."
cluster by (acquisition_channel)
as
with campaign_agg as (
    select
        channel
        , sum(cost) as total_cost
        , sum(impressions) as impressions
        , sum(clicks) as clicks
    from ${catalog_name}.${bronze_schema_name}.stg_marketing_campaigns
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
    , round(cast((sum(ltv.historical_clv) - coalesce(c.total_cost, 0)) / nullif(c.total_cost, 0) as double), 2) as marketing_roi
    , round(cast(
        avg(ltv.predictive_clv)
        / nullif(coalesce(c.total_cost, 0) / nullif(count(distinct ltv.customer_id), 0), 0) as double), 2
    ) as ltv_to_cac_ratio
    , c.impressions
    , c.clicks
    , cast(round(c.clicks / nullif(c.impressions, 0), 4) as double) as click_through_rate
from ${catalog_name}.${gold_schema_name}.gold_customer_ltv as ltv
left join campaign_agg as c on ltv.acquisition_channel = c.channel
group by ltv.acquisition_channel, c.total_cost, c.impressions, c.clicks;
