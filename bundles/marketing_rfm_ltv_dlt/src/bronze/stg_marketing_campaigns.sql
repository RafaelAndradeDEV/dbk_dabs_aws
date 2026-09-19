-- Bronze staging model for the `marketing_campaigns` source (SQL).
--
-- Reads raw campaign parquet from the S3 landing zone in BATCH mode and
-- applies cast / rename only. Campaign spend per acquisition channel feeds
-- the CAC and channel-ROI calculations downstream.

create or refresh materialized view ${catalog_name}.${bronze_schema_name}.stg_marketing_campaigns (
    campaign_id int comment "Primary key: unique campaign id."
    , channel string comment "Acquisition channel funded by the campaign."
    , campaign_name string comment "Human-readable campaign name."
    , start_date date comment "Campaign start date."
    , end_date date comment "Campaign end date."
    , cost decimal(18, 2) comment "Total campaign spend."
    , impressions bigint comment "Impressions delivered."
    , clicks bigint comment "Clicks generated."
    , updated_at timestamp comment "Source last-modified timestamp."
    , extracted_at timestamp comment "Ingestion (extraction) timestamp."
    , constraint valid_campaign_id expect (campaign_id is not null) on violation drop row
    , constraint non_negative_cost expect (cost >= 0)
)
comment "Bronze staging: cleaned marketing campaign spend."
cluster by (channel)
as select
    cast(CampaignId as int) as campaign_id
    , cast(Channel as string) as channel
    , cast(CampaignName as string) as campaign_name
    , cast(StartDate as date) as start_date
    , cast(EndDate as date) as end_date
    , cast(Cost as decimal(18, 2)) as cost
    , cast(Impressions as bigint) as impressions
    , cast(Clicks as bigint) as clicks
    , cast(ModifiedDate as timestamp) as updated_at
    , cast(__extracted_at__ as timestamp) as extracted_at
from read_files(
    '${marketing_source_path}/${env_target}/marketing/marketing_campaigns/'
    , format => 'parquet'
);
