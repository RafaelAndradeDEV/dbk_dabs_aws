# marketing_rfm_ltv_job

Marketing **RFM + Customer Lifetime Value** case, implemented as a **Lakeflow
Job** — a notebook-task DAG over the bronze/silver/gold medallion. This is the
job-based twin of the DLT pipeline in
[`../marketing_rfm_ltv_dlt`](../marketing_rfm_ltv_dlt); same data model, same
business logic, different orchestration primitive.

See [`feature-docs/marketing_rfm_ltv.md`](../../feature-docs/marketing_rfm_ltv.md)
for the full architecture and methodology.

## Task DAG (`resources/marketing_job.yml`)

```
stg_customers ─┐
stg_orders ────┼─▶ slv_order_facts ─▶ slv_customer_orders ─┬─▶ gold_customer_rfm
stg_order_items┘                                           │
stg_marketing_campaigns ───────────────────────────────────┴─▶ gold_customer_ltv ─▶ gold_channel_marketing_performance
```

| Layer | Notebook | Lang |
|-------|----------|------|
| Bronze | `bronze/stg_*_nb.py` (×4) | PySpark |
| Silver | `silver/slv_order_facts_nb.py` | SQL (`spark.sql`) |
| Silver | `silver/slv_customer_orders_nb.py` | PySpark |
| Gold | `gold/gold_customer_rfm_nb.py` | PySpark (NTILE + UDF) |
| Gold | `gold/gold_customer_ltv_nb.py` | SQL |
| Gold | `gold/gold_channel_marketing_performance_nb.py` | SQL |

## Run it

```bash
# Reuse the generator from the DLT bundle to land sample parquet in S3:
python ../marketing_rfm_ltv_dlt/src/_setup/generate_marketing_data.py --env dev

databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run marketing_rfm_ltv_job --target dev
```

Job parameters (catalog, schemas, `marketing_source_path`, `env_target`,
`clv_gross_margin`, `clv_expected_lifespan_years`) are defined at job level and
read inside each notebook via `dbutils.widgets`.
