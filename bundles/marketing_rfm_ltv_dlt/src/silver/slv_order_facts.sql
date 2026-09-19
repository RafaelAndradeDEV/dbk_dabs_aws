-- Silver model: order-grain fact (SQL).
--
-- Joins order headers with their line items and rolls the lines up to one row
-- per order, computing gross and net (post-discount) revenue. This is the
-- conformed transactional fact the gold marketing marts aggregate from.

create or refresh materialized view ${catalog_name}.${silver_schema_name}.slv_order_facts (
    order_id int comment 'Primary key: order id.'
    , customer_id int comment 'Foreign key to customers.'
    , order_ts timestamp comment 'Order timestamp.'
    , order_date date comment 'Order calendar date.'
    , channel string comment 'Order channel.'
    , status string comment 'Order status.'
    , num_line_items int comment 'Distinct line items on the order.'
    , total_quantity bigint comment 'Total units across all lines.'
    , gross_revenue decimal(18, 4) comment 'Revenue before discounts.'
    , total_discount decimal(18, 4) comment 'Total discount amount.'
    , net_revenue decimal(18, 4) comment 'Revenue after discounts (recognized).'
    , constraint valid_order expect (order_id is not null) on violation drop row
    , constraint non_negative_revenue expect (net_revenue >= 0)
)
comment 'Silver: one conformed row per order with revenue economics.'
cluster by (customer_id)
as
with
    lines as (
        select
            i.order_id
            , count(i.order_item_id) as num_line_items
            , sum(i.quantity) as total_quantity
            , sum(i.unit_price * i.quantity) as gross_revenue
            , sum(i.unit_price * i.quantity * i.discount) as total_discount
            , sum(i.unit_price * i.quantity * (1 - i.discount)) as net_revenue
        from ${catalog_name}.${bronze_schema_name}.stg_order_items as i
        group by i.order_id
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
    , cast(l.gross_revenue as decimal(18, 4)) as gross_revenue
    , cast(l.total_discount as decimal(18, 4)) as total_discount
    , cast(l.net_revenue as decimal(18, 4)) as net_revenue
from ${catalog_name}.${bronze_schema_name}.stg_orders as o
inner join lines as l on o.order_id = l.order_id;
