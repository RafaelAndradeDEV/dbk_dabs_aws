"""Module: metadata_dqx.py

Provides helper APIs:
  - apply_metadata_from_yaml(yaml_path, catalog=None, schema=None, ...)
  - _generate_metadata_statements(...), _execute_sql(...), _generate_dqx_rules(...) as internals

Reads a YAML file describing models, columns, descriptions, tags, and dqx_tests.
Outputs COMMENT ON / SET TAG statements and DQX rules.

Notes:
  - dqx_tests become a column-level tag named "dqx_tests" whose value is a JSON string.
  - Uses SQLGlot for SQL generation.
"""

import json
import logging
from collections.abc import Callable
from typing import Any

import yaml
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_serializer import serialize_checks
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from sqlglot import exp

logger = logging.getLogger(__name__)


def _load_yaml(path: str) -> dict[str, Any]:
    """Load and parse a YAML file.

    Parameters
    ----------
    path : str
        File path to the YAML file.

    Returns:
    -------
    dict[str, Any]
        Parsed YAML content.
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _full_name(model_name: str, catalog: str | None, schema: str | None) -> str:
    """Construct a fully-qualified object name using catalog, schema, and model name.

    Parameters
    ----------
    model_name : str
        Base object name.
    catalog : str | None
        Catalog name, if provided.
    schema : str | None
        Schema name, if provided.

    Returns:
    -------
    str
        Fully qualified name (catalog.schema.model_name).
    """
    parts = []
    if catalog:
        parts.append(catalog)
    if schema:
        parts.append(schema)
    parts.append(model_name)
    return ".".join(parts)


def _comment_on_table(fullname: str, desc: str | None) -> list[str]:
    """Generate a COMMENT ON TABLE statement if description exists.

    Parameters
    ----------
    fullname : str
        Fully qualified table name.
    desc : str | None
        Table description.

    Returns:
    -------
    list[str]
        SQL statements for table comment (zero or one element).
    """
    if not desc:
        return []
    catalog_name, schema_name, table_name = fullname.split(".")
    stmt = exp.Comment(
        this=exp.Table(
            this=exp.Identifier(this=table_name),
            db=exp.Identifier(this=schema_name),
            catalog=exp.Identifier(this=catalog_name),
        ),
        kind="TABLE",
        expression=exp.Literal.string(desc),
    ).sql()
    return [stmt]


def _comment_on_column(fullname: str, column: str, desc: str | None) -> list[str]:
    """Generate a COMMENT ON COLUMN statement if description exists.

    Parameters
    ----------
    fullname : str
        Fully qualified table name.
    column : str
        Column name.
    desc : str | None
        Column description.

    Returns:
    -------
    list[str]
        SQL statements for column comment (zero or one element).
    """
    if not desc:
        return []

    catalog_name, schema_name, table_name = fullname.split(".")
    stmt = exp.Comment(
        this=exp.Column(
            this=exp.Identifier(this=column),
            table=exp.Identifier(this=table_name),
            db=exp.Identifier(this=schema_name),
            catalog=exp.Identifier(this=catalog_name),
        ),
        kind="COLUMN",
        expression=exp.Literal.string(desc),
    ).sql()
    return [stmt]


def _set_tags(fullname: str, tags: dict[str, Any], asset_type: str, column: str | None = None) -> list[str]:
    """Generate SET TAGS for an asset.

    Parameters
    ----------
    fullname : str
        Fully qualified asset name.
    tags : dict[str, Any]
        Mapping of tag keys to values. Values may be None or JSON-serializable.
    asset_type : str
        "TABLE" or "COLUMN".
    column : str | None
        Column name, if asset_type is "COLUMN".

    Returns:
    -------
    list[str]
        SQL statements to unset and set tags.
    """
    out = []
    if not tags:
        return out
    tag_list = []
    for k, v in tags.items():
        if v is None:
            tag_list.append(f"'{k}' = ''")
        else:
            val = v if isinstance(v, str) else json.dumps(v, separators=(",", ":"))
            tag_list.append(f"'{k}' = '{val}'")
    if asset_type == "TABLE":
        out.append(f"ALTER TABLE {fullname} SET TAGS ({', '.join(tag_list)})")
    elif asset_type == "COLUMN" and column:
        out.append(f"ALTER TABLE {fullname} ALTER COLUMN `{column}` SET TAGS ({', '.join(tag_list)})")
    else:
        raise ValueError(f"Invalid asset type: {asset_type} and column: {column}")
    return out


def _table_metadata_cleanup_statements(
    spark: SparkSession,
    table_identifier: str,
    catalog: str,
    schema: str,
    name: str,
) -> list[str]:
    """Return SQL statements that remove existing table comment and tags if present."""
    catalog = (catalog or "").strip()
    schema = (schema or "").strip()
    name = (name or "").strip()

    if not (catalog and schema and name):
        return []

    statements: list[str] = []
    try:
        tables_view = f"`{catalog}`.information_schema.tables"
        table_comment_df = spark.sql(
            f"""
            SELECT comment
            FROM {tables_view}
            WHERE table_catalog = '{catalog}'
              AND table_schema  = '{schema}'
              AND table_name    = '{name}'
            """
        )

        comments = [r["comment"] for r in table_comment_df.collect() if r["comment"]]
        if comments:
            statements.append(f"COMMENT ON TABLE {table_identifier} IS NULL")

        table_tags_view = f"`{catalog}`.information_schema.table_tags"
        table_tags_df = spark.sql(
            f"""
            SELECT tag_name
            FROM {table_tags_view}
            WHERE catalog_name = '{catalog}'
              AND schema_name  = '{schema}'
              AND table_name   = '{name}'
            """
        )

        tag_names = {r["tag_name"] for r in table_tags_df.collect()}
        if tag_names:
            escaped = [t.replace("'", "''") for t in tag_names]
            inner = ", ".join(f"'{t}'" for t in escaped)
            statements.append(f"ALTER TABLE {table_identifier} UNSET TAGS ({inner})")

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Skipping table metadata cleanup for %s due to error: %s",
            table_identifier,
            exc,
            exc_info=True,
        )
        return []

    if statements:
        logger.info("Prepared table metadata cleanup statements for %s", table_identifier)
    return statements


def _column_metadata_cleanup_statements(
    spark: SparkSession,
    table_identifier: str,
    catalog: str,
    schema: str,
    name: str,
) -> list[str]:
    """Return SQL statements that remove existing column comments and tags if present."""
    catalog = (catalog or "").strip()
    schema = (schema or "").strip()
    name = (name or "").strip()

    if not (catalog and schema and name):
        return []

    statements: list[str] = []
    try:
        columns_view = f"`{catalog}`.information_schema.columns"
        col_comments_df = spark.sql(
            f"""
            SELECT column_name, comment
            FROM {columns_view}
            WHERE table_catalog = '{catalog}'
              AND table_schema  = '{schema}'
              AND table_name    = '{name}'
              AND comment IS NOT NULL
            """
        )

        col_comments = [r["column_name"] for r in col_comments_df.collect() if r["comment"]]

        for col in col_comments:
            statements.append(f"COMMENT ON COLUMN {table_identifier}.`{col}` IS NULL")

        column_tags_view = f"`{catalog}`.information_schema.column_tags"
        col_tags_df = spark.sql(
            f"""
            SELECT column_name, tag_name
            FROM {column_tags_view}
            WHERE catalog_name = '{catalog}'
              AND schema_name  = '{schema}'
              AND table_name   = '{name}'
            """
        )

        tags_by_column: dict[str, set[str]] = {}
        for row in col_tags_df.collect():
            col = row["column_name"]
            tag = row["tag_name"]
            tags_by_column.setdefault(col, set()).add(tag)

        for col, tags in tags_by_column.items():
            escaped = [t.replace("'", "''") for t in tags]
            inner = ", ".join(f"'{t}'" for t in escaped)
            statements.append(f"ALTER TABLE {table_identifier} ALTER COLUMN `{col}` UNSET TAGS ({inner})")

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Skipping column metadata cleanup for %s due to error: %s",
            table_identifier,
            exc,
            exc_info=True,
        )
        return []

    if statements:
        logger.info("Prepared column metadata cleanup statements for %s", table_identifier)
    return statements


def _clear_existing_metadata(
    spark: SparkSession,
    table_identifier: str,
    catalog: str,
    schema: str,
    name: str,
) -> list[str]:
    """Return SQL statements to clear existing metadata before reapplying."""
    statements: list[str] = []
    try:
        statements.extend(_table_metadata_cleanup_statements(spark, table_identifier, catalog, schema, name))
        statements.extend(_column_metadata_cleanup_statements(spark, table_identifier, catalog, schema, name))
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Unable to clear existing metadata for %s due to error: %s",
            table_identifier,
            exc,
            exc_info=True,
        )
        return []
    return statements


def _generate_metadata_statements_from_models(
    models: list[dict[str, Any]], catalog: str | None, schema: str | None, prefix: str | None
) -> list[str]:
    outputs: list[str] = []

    for model in models:
        name = f"{prefix or ''}{model['name']}"
        fullname = _full_name(name, catalog, schema)
        outputs.extend(_model_metadata_statements(model, fullname))

    return outputs


def _model_metadata_statements(model: dict[str, Any], fullname: str) -> list[str]:
    outputs: list[str] = []

    table_desc = model.get("description")
    outputs.extend(_comment_on_table(fullname, table_desc.rstrip() if isinstance(table_desc, str) else table_desc))

    outputs.extend(_set_tags(fullname, model.get("tags", {}), "TABLE"))

    for col in model.get("columns", []):
        colname = col["name"]
        col_desc = col.get("description")
        outputs.extend(
            _comment_on_column(fullname, colname, col_desc.rstrip() if isinstance(col_desc, str) else col_desc)
        )

        col_tags = dict(col.get("tags", {}))

        if "dqx_tests" in col:
            col_tags["dqx_tests"] = []
            for test in col["dqx_tests"]:
                match test:
                    case str():
                        test_name = test
                    case {**object} if len(object.keys()) == 1:
                        test_name = next(iter(object.keys()))
                    case _:
                        raise ValueError(f"Unfornseen case: {test}")

                col_tags["dqx_tests"].append(test_name)

        if col_tags:
            outputs.extend(_set_tags(fullname, col_tags, "COLUMN", colname))

    return outputs


def _generate_metadata_statements(
    yaml_path: str, catalog: str | None = None, schema: str | None = None, prefix: str | None = ""
) -> list[str]:
    """Generate COMMENT ON statements and tag statements for tables and columns.

    Reads the YAML model specification and produces:
      - COMMENT ON TABLE / COLUMN
      - UNSET TAG / SET TAG statements
      - Column tag "dqx_tests" containing JSON list of rule names

    Parameters
    ----------
    yaml_path : str
        Path to the YAML file.
    catalog : str | None
        Catalog prefix for generated names.
    schema : str | None
        Schema prefix for generated names.
    prefix : str | None
        Optional prefix applied to model names.

    Returns:
    -------
    list[str]
        All generated SQL statements in execution order.
    """
    data = _load_yaml(yaml_path)
    models = data.get("models", [])
    return _generate_metadata_statements_from_models(models, catalog, schema, prefix)


def _execute_sql(spark, stmts):
    """Execute SQL statements against a Spark session.

    Accepts a string or iterable of strings. Failures on UNSET TAG statements
    are ignored because tags may not exist and since Databricks does not offer
    a `if [not] exists` option for UNSET/SET tag SQL API, exception handling is
    necessary.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    stmts : str | iterable[str]
        SQL statement or list of statements.

    Raises:
    ------
    TypeError
        If input is not a string or iterable.
    RuntimeError
        If a non-tag SQL statement fails.
    """
    if isinstance(stmts, str):
        stmts = [stmts]
    elif isinstance(stmts, list | tuple):
        pass
    else:
        raise TypeError(
            f"SQL statements `stmts` is expected to be a string or an iterable (list, tuple) not {type(stmts)}"
        )

    for stmt in stmts:
        try:
            spark.sql(stmt)
        except PySparkException as e:
            if "unset tag" in stmt.lower():
                pass
            else:
                raise RuntimeError(f"SQL statement `{stmt}` failed to run") from e


class DqxPlan:
    """Plan with DQX checks and a runner.

    Attributes:
    ----------
    rules : list[DQRowRule]
        Lista de regras geradas a partir do YAML.
    serialized : Any
        Payload serializado via `serialize_checks` pronto para o DQEngine.
    columns : set[str]
        Conjunto das colunas cobertas pelas regras.
    apply_rules(data_frame, workspace_client=None, dq_engine=None, verbose=False) -> DataFrame
        Aplica as regras no DataFrame e retorna o dataframe anotado.
    describe() -> dict[str, list[dict[str, Any]]]
        Mapeia cada coluna para uma lista de regras (gnome, criticidade, argumentos).
    evaluate(
        data_frame,
        workspace_client=None,
        dq_engine=None,
        verbose=False,
        onwarning=None,
        onerror=None,
    ) -> DataFrame
        Aplica as regras; loga warnings, executa callbacks opcionais e lança error se houver criticality error.
    """

    def __init__(self, rules: list[DQRowRule]):
        self.rules = rules
        self.serialized = serialize_checks(rules)
        self.columns = {r.column for r in rules}

    def apply_rules(
        self,
        data_frame: DataFrame,
        workspace_client: WorkspaceClient | None = None,
        dq_engine: DQEngine | None = None,
        verbose: bool = False,
    ) -> DataFrame:
        ws = workspace_client or WorkspaceClient()
        engine = dq_engine or DQEngine(ws)
        if verbose:
            logger.info("Applying %d DQX checks across columns %s", len(self.rules), sorted(self.columns))
        return engine.apply_checks_by_metadata(data_frame, self.serialized)

    def describe(self) -> dict[str, list[dict[str, Any]]]:
        """Return a readable mapping of columns to their rules."""
        summary: dict[str, list[dict[str, Any]]] = {}
        for rule in self.rules:
            rule_suffix = rule.name.removeprefix(f"{rule.column}_")
            summary.setdefault(rule.column, []).append(
                {
                    "rule": rule_suffix,
                    "criticality": rule.criticality,
                    "arguments": rule.check_func_kwargs,
                }
            )
        for col in summary:
            summary[col] = sorted(summary[col], key=lambda r: r["rule"])
        return dict(sorted(summary.items()))

    def evaluate(
        self,
        data_frame: DataFrame,
        workspace_client: WorkspaceClient | None = None,
        dq_engine: DQEngine | None = None,
        verbose: bool = False,
        onwarning: Callable[[list[dict[str, Any]], DataFrame | None], None] | None = None,
        onerror: Callable[[list[dict[str, Any]], DataFrame | None], None] | None = None,
    ) -> DataFrame:
        """Apply rules, log warnings, and raise on errors."""
        ws = workspace_client or WorkspaceClient()
        engine = dq_engine or DQEngine(ws)
        annotated = engine.apply_checks_by_metadata(data_frame, self.serialized)

        before_cols = {field.name for field in data_frame.schema}

        def _infer_check_columns(annotated_df: DataFrame) -> tuple[str | None, str | None]:
            """Infer warning/error columns using get_valid/get_invalid semantics.

            Rules:
              - Candidates: columns added by apply_checks (schema diff).
              - Warnings: column has entries in BOTH get_valid and get_invalid (intersection non-empty).
              - Errors: column has entries ONLY in get_invalid (empty in get_valid).
              - If still ambiguous with 2 candidates: first->errors, second->warnings.
              - Fallback: `_errors`/`_error` and `_warnings`/`_warning`.
            """
            candidates = [field.name for field in annotated_df.schema if field.name not in before_cols]

            valid_df = engine.get_valid(annotated_df)
            invalid_df = engine.get_invalid(annotated_df)

            def _has_entries(df: DataFrame, col: str) -> bool:
                return df.filter(f.size(f.col(col)) > 0).limit(1).count() > 0

            warnings_col = None
            errors_col = None

            for col in candidates:
                in_valid = _has_entries(valid_df, col)
                in_invalid = _has_entries(invalid_df, col)

                if in_valid and in_invalid and warnings_col is None:
                    warnings_col = col
                elif in_invalid and not in_valid and errors_col is None:
                    errors_col = col

            if (warnings_col is None or errors_col is None) and len(candidates) >= 2:
                errors_col = errors_col or candidates[0]
                warnings_col = warnings_col or candidates[1]

            cols_after = [field.name for field in annotated_df.schema]
            warnings_col = warnings_col or next((c for c in ["_warnings", "_warning"] if c in cols_after), None)
            errors_col = errors_col or next((c for c in ["_errors", "_error"] if c in cols_after), None)

            return warnings_col, errors_col

        warnings_col, errors_col = _infer_check_columns(annotated)

        if verbose:
            logger.info("DQX inferred columns warnings=%s errors=%s", warnings_col, errors_col)

        def _explode_checks(df: DataFrame, colname: str | None, alias: str) -> DataFrame:
            if colname:
                return df.select(f.explode_outer(colname).alias(alias)).where(f.col(alias).isNotNull())
            return df.select(f.lit(None).alias(alias)).where(f.lit(False))

        warnings_df = _explode_checks(annotated, warnings_col, "w")
        errors_df = _explode_checks(annotated, errors_col, "e")

        warning_count = warnings_df.count()
        error_count = errors_df.count()

        if warning_count:
            warning_summary = (
                warnings_df.withColumn("rule", f.col("w").getField("name"))
                .withColumn("message", f.col("w").getField("message"))
                .groupBy("rule", "message")
                .agg(f.count("*").alias("count"))
                .orderBy(f.col("count").desc(), f.col("rule"))
                .collect()
            )
            by_rule_str = ", ".join(
                f"{row['rule']}: {row['count']}" for row in warning_summary if row["rule"] is not None
            )
            warning_msg = "\n".join(
                f"{row['rule']}: {row['message']} (x{row['count']})"
                if row["message"] is not None
                else f"{row['rule']} (x{row['count']})"
                for row in warning_summary
            )
            if onwarning:
                warning_payload: list[dict[str, Any]] = []
                for row in warning_summary:
                    warning_payload.append({"rule": row["rule"], "message": row["message"], "count": row["count"]})
                onwarning(warning_payload, annotated)
            if warning_msg:
                logger.warning(
                    f"DQX warnings encountered (total={warning_count}, by_rule={{ {by_rule_str} }}):\n{warning_msg}"
                )
            else:
                logger.warning("DQX warnings encountered")

        if error_count:
            error_summary = (
                errors_df.withColumn("rule", f.col("e").getField("name"))
                .withColumn("message", f.col("e").getField("message"))
                .groupBy("rule", "message")
                .agg(f.count("*").alias("count"))
                .orderBy(f.col("count").desc(), f.col("rule"))
                .collect()
            )
            by_rule_str = ", ".join(
                f"{row['rule']}: {row['count']}" for row in error_summary if row["rule"] is not None
            )
            summary_str = "\n".join(
                f"{row['rule']}: {row['message']} (x{row['count']})"
                if row["message"] is not None
                else f"{row['rule']} (x{row['count']})"
                for row in error_summary
            )
            if onerror:
                error_payload: list[dict[str, Any]] = []
                for row in error_summary:
                    error_payload.append({"rule": row["rule"], "message": row["message"], "count": row["count"]})
                onerror(error_payload, annotated)
            raise RuntimeError(
                f"DQX found {error_count} errors; inspect annotated DataFrame for details. "
                f"By rule={{ {by_rule_str} }}:\n{summary_str}"
            )

        return annotated


def generate_dqx_rules(yaml_path: str, default_criticality: str = "warn") -> DqxPlan:
    """Convert YAML dqx_tests into a plan with rules and an applier.

    Parameters
    ----------
    yaml_path : str
        Path to YAML file.
    default_criticality : str, optional
        Fallback criticality when not specified in the YAML. Defaults to "warn".

    Returns:
    -------
    DqxPlan
        Plan that contains rules, serialized payload, and an `apply_rules` method.
    """
    data = _load_yaml(yaml_path)
    rules: list[DQRowRule] = []

    for model in data.get("models", []):
        configs = model.get("configs", {})
        dqx_configs = configs.get("dqx_tests", {})
        base_criticality = dqx_configs.get("criticality", default_criticality)

        for col in model.get("columns", []):
            colname = col["name"]
            tests = col.get("dqx_tests", [])
            for test in tests:
                check_func_kwargs = {}
                current_criticality = base_criticality
                # uses match-case to check for type and auto-expand into variables
                match test:
                    case str():
                        rule_name = test
                    case {**object} if len(object.keys()) == 1:
                        rule_name = next(iter(object.keys()))
                        rule = object[rule_name]
                        current_criticality = rule.get("criticality", base_criticality)
                        check_func_kwargs = rule.get("arguments", check_func_kwargs)
                    case _:
                        raise ValueError(f"Unforeseen case: {test}")

                if not hasattr(check_funcs, rule_name):
                    raise ValueError(f"DQX check function `{rule_name}` not found in check_funcs")

                rules.append(
                    DQRowRule(
                        name=f"{colname}_{rule_name}",
                        criticality=current_criticality,
                        check_func=getattr(check_funcs, rule_name),
                        column=colname,
                        check_func_kwargs=check_func_kwargs,
                    )
                )

    return DqxPlan(rules)


def apply_metadata_from_yaml(
    yaml_path: str,
    catalog: str | None = None,
    schema: str | None = None,
    prefix: str | None = "",
    clear_metadata: bool = True,
) -> bool:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session. Unable to apply metadata.")

    data = _load_yaml(yaml_path)
    models = data.get("models", [])

    cleanup_statements: list[str] = []
    if clear_metadata and catalog and schema:
        for model in models:
            name = f"{prefix or ''}{model['name']}"
            table_identifier = f"`{catalog}`.`{schema}`.`{name}`"
            cleanup_statements.extend(_clear_existing_metadata(spark, table_identifier, catalog, schema, name))
    elif clear_metadata and (not catalog or not schema):
        logger.warning(
            "Requested metadata cleanup but catalog (%s) or schema (%s) is missing; skipping.",
            catalog,
            schema,
        )

    metadata_statements = _generate_metadata_statements_from_models(models, catalog, schema, prefix)
    statements = cleanup_statements + metadata_statements

    for stmt in statements:
        _execute_sql(spark, stmt)

    return True


__all__ = ["apply_metadata_from_yaml", "generate_dqx_rules"]
