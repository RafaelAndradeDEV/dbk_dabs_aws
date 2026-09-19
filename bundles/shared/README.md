# What is this module?

`/shared/common_variables.yaml` sets a group of variables intended to be shared among different bundle projects, following a structure recommended by databricks in the [documentation](https://docs.databricks.com/gcp/en/dev-tools/bundles/sharing).

## DQX helpers (`shared_utils.metadata`)

`generate_dqx_rules` reads a YAML (columns with `dqx_tests`) and returns a plan (`DqxPlan`) with the rules, serialized payload, and a method to apply them.

Notebook snippet:

```python
from shared_utils.metadata import generate_dqx_rules

plan = generate_dqx_rules(
    "stg_sales_salesorderheader_nb.yml",
    default_criticality="warn",  # optional fallback when YAML omits it
)

# Optional: inspect rules per column
print(plan.describe())

# Apply rules
df = spark.table(f"{target_catalog}.{stg_schema}.stg_sales_salesorderheader_nb")
annotated_df = plan.apply_rules(df, verbose=False)
# display(annotated_df)

# Strict mode: raises on errors, logs warnings
# annotated_df = plan.evaluate(df, verbose=True)

# With callbacks (warnings/errors summaries + full annotated DF)
# def handle_warn(summary, annotated_df):
#     print("WARNINGS", summary)
# def handle_err(summary, annotated_df):
#     print("ERRORS", summary)
#     # e.g., write annotated_df to a table/location for inspection
# annotated_df = plan.evaluate(
#     df,
#     verbose=True,
#     onwarning=handle_warn,
#     onerror=handle_err,
# )
```

Useful attributes/methods:

- `plan.rules`: list of `DQRowRule`.
- `plan.serialized`: payload already serialized for `DQEngine`.
- `plan.columns`: covered columns.
- `plan.apply_rules(df, workspace_client=None, dq_engine=None, verbose=False)`: applies rules and returns annotated DF (reuse client/engine if needed).
- `plan.evaluate(df, workspace_client=None, dq_engine=None, verbose=False, onwarning=None, onerror=None)`: applies, logs per-rule warning summary, raises if any error; optional callbacks receive (`summary_list`, annotated_df).
- `plan.describe()`: mapping column → rules (name, criticality, arguments).
- `default_criticality`: fallback when neither `configs.dqx_tests` nor the test define criticality (default `"warn"`).

Validation: if the YAML references a `rule_name` missing in `databricks.labs.dqx.check_funcs`, an explicit error is raised.
