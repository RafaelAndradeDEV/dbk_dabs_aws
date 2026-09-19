# marketing_rfm_ltv_dlt

Marketing **RFM + Customer Lifetime Value** case, implemented as a **Lakeflow
Declarative Pipeline** (DLT). Batch only (no streaming); bronze reads parquet
from the S3 landing zone. PySpark **and** SQL models.

> A second, equivalent implementation of the same case using **Lakeflow Jobs**
> lives in [`../marketing_rfm_ltv_job`](../marketing_rfm_ltv_job). See
> [`feature-docs/marketing_rfm_ltv.md`](../../feature-docs/marketing_rfm_ltv.md)
> for the full architecture, data model and methodology.

## Layers

| Layer | Model | Lang | Purpose |
|-------|-------|------|---------|
| Bronze | `stg_customers` | PySpark | Clean customer master (dedup latest) |
| Bronze | `stg_order_items` | PySpark | Clean order lines |
| Bronze | `stg_orders` | SQL | Clean order headers (`read_files`) |
| Bronze | `stg_marketing_campaigns` | SQL | Clean campaign spend (`read_files`) |
| Silver | `slv_order_facts` | SQL | Order-grain revenue economics |
| Silver | `slv_customer_orders` | PySpark | Completed orders + customer attrs |
| Gold | `gold_customer_rfm` | PySpark | RFM scores (NTILE) + segment |
| Gold | `gold_customer_ltv` | SQL | Historical + predictive CLV, CAC, LTV:CAC |
| Gold | `gold_channel_marketing_performance` | SQL | Channel ROI scorecard |

## Run it

```bash
# 1. Land sample parquet in S3 (run from a Databricks notebook/job, or locally)
python src/_setup/generate_marketing_data.py --env dev --num-customers 2000
#    local smoke test:  --local-path ./_sample_data

# 2. Deploy + run the pipeline
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run marketing_rfm_ltv_pipeline --target dev
```

## Key config (`resources/dlt_pipeline.yml`)

- `marketing_source_path` / `env_target` — S3 bronze landing zone
- `clv_gross_margin` (0.30) / `clv_expected_lifespan_years` (3) — CLV assumptions
- `bronze_/silver_/gold_schema_name` — medallion target schemas
