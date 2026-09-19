-- Gold mart: Customer Lifetime Value (SQL).
--
-- Computes, per customer:
--   * historical CLV  = total recognized net revenue to date
--   * predictive CLV  = AOV x annual order frequency x expected lifespan x margin
--   * CAC             = channel campaign spend / customers acquired in that channel
--   * LTV:CAC ratio   = predictive CLV / CAC  (a >3 ratio is a healthy unit economic)
--
-- Business assumptions (gross margin, expected lifespan) are injected from the
-- pipeline configuration so they can be tuned per environment.

create or refresh materialized view ${catalog_name}.${gold_schema_name}.gold_customer_ltv (
    customer_id int comment "Customer identifier."
    , acquisition_channel string comment "Channel that acquired the customer."
    , first_order_date date comment "Date of first order."
    , last_order_date date comment "Date of most recent order."
    , total_orders bigint not null comment "Lifetime number of orders."
    , historical_clv decimal(18, 2) comment "Lifetime recognized net revenue."
    , avg_order_value decimal(18, 2) comment "Average net revenue per order."
    , tenure_days int comment "Days from first order to snapshot date."
    , annual_order_frequency double comment "Annualized purchase frequency."
    , predictive_clv decimal(18, 2) comment "Modeled forward-looking lifetime value."
    , customer_acquisition_cost decimal(18, 2) comment "Allocated acquisition cost (CAC)."
    , ltv_to_cac_ratio double comment "Predictive CLV divided by CAC."
    , constraint valid_customer expect (customer_id is not null) on violation drop row
)
comment "Gold: per-customer historical and predictive lifetime value with CAC."
cluster by (acquisition_channel)
as
with snapshot as (
    select max(order_date) as ref_date
    from ${catalog_name}.${silver_schema_name}.slv_customer_orders
),

cust as (
    select
        customer_id
        , max(acquisition_channel) as acquisition_channel
        , min(order_date) as first_order_date
        , max(order_date) as last_order_date
        , count(distinct order_id) as total_orders
        , sum(net_revenue) as total_net_revenue
    from ${catalog_name}.${silver_schema_name}.slv_customer_orders
    group by customer_id
),

channel_cac as (
    select
        c.acquisition_channel
        , coalesce(camp.total_cost, 0) / nullif(count(distinct c.customer_id), 0) as cac
    from ${catalog_name}.${bronze_schema_name}.stg_customers as c
    left join (
        select
            channel
            , sum(cost) as total_cost
        from ${catalog_name}.${bronze_schema_name}.stg_marketing_campaigns
        group by channel
    ) as camp on c.acquisition_channel = camp.channel
    group by c.acquisition_channel, camp.total_cost
),

metrics as (
    select
        cu.customer_id
        , cu.acquisition_channel
        , cu.first_order_date
        , cu.last_order_date
        , cu.total_orders
        , cu.total_net_revenue
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
    , round(cast(m.annual_freq as double), 4) as annual_order_frequency
    , cast(m.aov * m.annual_freq * ${clv_expected_lifespan_years} * ${clv_gross_margin} as decimal(18, 2))
        as predictive_clv
    , cast(cc.cac as decimal(18, 2)) as customer_acquisition_cost
    , round(cast(
        (m.aov * m.annual_freq * ${clv_expected_lifespan_years} * ${clv_gross_margin}) / nullif(cc.cac, 0) as double), 2
    ) as ltv_to_cac_ratio
from metrics as m
left join channel_cac as cc on m.acquisition_channel = cc.acquisition_channel;
