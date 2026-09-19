-- Bronze staging model for the `orders` source (SQL).
--
-- Reads raw order-header parquet from the S3 landing zone in BATCH mode
-- (materialized view, no streaming) via read_files() and applies cast /
-- rename only. Quality is enforced with an EXPECT constraint on the key.

create or refresh materialized view ${catalog_name}.${bronze_schema_name}.stg_orders (
    order_id int comment 'Primary key: unique order id.'
    , customer_id int comment 'Foreign key to customers.'
    , order_ts timestamp comment 'Timestamp the order was placed.'
    , order_date date comment 'Calendar date the order was placed.'
    , channel string comment 'Order channel (web / app / store).'
    , status string comment 'Order status (completed / cancelled / returned).'
    , updated_at timestamp comment 'Source last-modified timestamp.'
    , extracted_at timestamp comment 'Ingestion (extraction) timestamp.'
    , constraint valid_order_id expect (order_id is not null) on violation drop row
    , constraint valid_customer_id expect (customer_id is not null) on violation drop row
)
comment 'Bronze staging: cleaned order headers.'
cluster by (customer_id)
as select
    cast(OrderId as int) as order_id
    , cast(CustomerId as int) as customer_id
    , cast(OrderTimestamp as timestamp) as order_ts
    , cast(OrderTimestamp as date) as order_date
    , cast(Channel as string) as channel
    , lower(cast(Status as string)) as status
    , cast(ModifiedDate as timestamp) as updated_at
    , cast(__extracted_at__ as timestamp) as extracted_at
from read_files(
    '${marketing_source_path}/${env_target}/marketing/orders/'
    , format => 'parquet'
);
