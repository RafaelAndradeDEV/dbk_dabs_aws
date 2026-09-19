# marketing_rfm_ltv_dashboard

Serving layer for the marketing case: a native **AI/BI (Lakeview) dashboard**
plus a paired **Genie space**, deployed declaratively with Databricks Asset
Bundles. Reads the gold marts produced by
[`../marketing_rfm_ltv_dlt`](../marketing_rfm_ltv_dlt) (or the equivalent
[`../marketing_rfm_ltv_job`](../marketing_rfm_ltv_job)).

## What it answers

| Page / space | Source mart | Business questions |
|--------------|-------------|--------------------|
| Dashboard — **Channel ROI & CAC** | `gold_channel_marketing_performance` | Revenue, CAC, marketing ROI, LTV:CAC (healthy > 3), CTR per acquisition channel |
| Dashboard — **RFM segmentation** | `gold_customer_rfm` | Customers & revenue per RFM segment, segment distribution, segment mix by channel |
| **Genie space** | both marts | Natural-language Q&A over the same metrics |

## Requirements

- **DIRECT deployment engine.** The `genie_spaces` resource is only supported
  with `bundle.engine: direct` (set in `databricks.yml`); this needs a recent
  Databricks CLI. If `direct` is unavailable, remove `resources/genie_space.yml`
  and drop `engine: direct` to deploy the dashboard alone on the classic engine.
- A **SQL warehouse**. No id is committed — supply `dashboard_warehouse_id` per
  environment.

## Deploy

```bash
cd bundles/marketing_rfm_ltv_dashboard

# Validate (engine + resource schema)
databricks bundle validate --target dev

# Deploy with a warehouse id
databricks bundle deploy --target dev \
  --var dashboard_warehouse_id=<your_sql_warehouse_id>
# (or set BUNDLE_VAR_dashboard_warehouse_id / a default in targets.yml)
```

After deploy, open the dashboard and Genie space from the workspace
**Dashboards** / **Genie** lists.

## Per-environment resolution

- **Dashboard** (`src/marketing_rfm_ltv.lvdash.json`): datasets use *unqualified*
  table names; `dataset_catalog` / `dataset_schema` in `resources/dashboard.yml`
  resolve them per target. (Bundle `${var.*}` are **not** interpolated inside the
  `.lvdash.json` file.)
- **Genie space**: defined *inline* via `serialized_space` in
  `resources/genie_space.yml` so `${var.target_catalog}.${var.mart_schema}`
  interpolate the fully-qualified table identifiers per target.

Target → catalog / mart schema mapping mirrors `marketing_rfm_ltv_dlt`:

| Target | Catalog | Mart schema |
|--------|---------|-------------|
| dev | `dev_catalog_name` | `dev_schema_name` (per-user) |
| ci / qa / prod | `<env>_catalog_name` | `mart_schema_name` (`mart`) |

## Editing the dashboard in the UI

```bash
# Pull UI edits back into the local .lvdash.json
databricks bundle generate dashboard --existing-id <dashboard_id> --target dev
```
