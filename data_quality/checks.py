"""Eight essential quality rules and one append-only results table."""

from datetime import datetime, timezone
from functools import reduce
import json
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils.config import load_config
from utils.metadata import apply_table_descriptions


RESULTS_TABLE = "workspace.beverage_sales_quality.validation_tables"
RESULTS_DESCRIPTION = "Data quality check outcomes for loaded beverage tables."
COLUMN_DESCRIPTIONS = {
    "run_id": "Job run identifier or generated identifier for a manual validation.",
    "validated_at": "Date and time when the quality check finished.",
    "table_name": "Fully qualified name of the table checked.",
    "check_name": "Identifier of the quality rule declared in the table configuration.",
    "status": "Check outcome: PASS, FAIL for a data issue, or ERROR when the check could not run.",
    "message": "Explanation of a failed check or execution error; null for approved checks.",
    "rules": "Plain-language explanation of what the quality rule checks.",
    "description_table": "Business purpose of the checked table, captured from its configuration.",
}
RESULT_SCHEMA = T.StructType([
    T.StructField(name, T.TimestampType() if name == "validated_at" else T.StringType(),
                  True, {"comment": description})
    for name, description in COLUMN_DESCRIPTIONS.items()
])


def should_validate(mode, changes):
    """Always validate manually; automatic runs use upstream write indicators.

    Missing or unresolved task values are errors, never evidence of no changes.
    No table or checkpoint is read to make this decision.
    """
    if mode == "Manual":
        return True
    if mode != "Automatic":
        raise ValueError("Execution Mode must be Manual or Automatic.")
    values = json.loads(changes)
    if not isinstance(values, dict) or not values:
        raise ValueError("Automatic validation requires upstream task results.")
    flags = []
    for name, value in values.items():
        if value["state"] == "excluded":
            continue
        if value["state"] != "success":
            raise ValueError(f"Upstream task did not succeed: {name}")
        normalized = str(value["changed"]).lower()
        if normalized not in ("true", "false"):
            raise ValueError(f"Missing or invalid write indicator for {name}: {value['changed']}")
        flags.append(normalized == "true")
    return any(flags)


def require_empty(frame, message):
    """Fail a rule if its query finds an invalid record."""
    if frame.limit(1).count():
        raise AssertionError(message)


def check_required(frame, columns):
    """Reject null fields and blank strings among the specified columns."""
    types = dict(frame.dtypes)
    predicates = []
    for name in columns:
        invalid = F.col(name).isNull()
        if types[name] == "string":
            invalid = invalid | (F.length(F.trim(F.col(name))) == 0)
        predicates.append(invalid)
    require_empty(frame.filter(reduce(lambda a, b: a | b, predicates)),
                  "Missing required value: " + ", ".join(columns))


def check_keys(frame, columns):
    """Check that a complete key is populated and occurs only once."""
    check_required(frame, columns)
    require_empty(frame.groupBy(*columns).count().filter("count > 1"),
                  "Duplicate key: " + ", ".join(columns))


def evaluate_rule(run_id, table, name, operation, description="", table_description=""):
    """Return PASS, a business FAIL, or a technical ERROR without stopping peers."""
    status, message = "PASS", None
    try:
        operation()
    except AssertionError as error:
        status, message = "FAIL", str(error)
    except Exception as error:
        status, message = "ERROR", f"{type(error).__name__}: {error}"[:4000]
    return (run_id, datetime.now(timezone.utc), table, name, status, message,
            description or name, table_description)


def check_references(spark, frame, references, contracts):
    """Require configured keys to exist in the referenced configured tables."""
    for reference in references:
        columns = reference["columns"]
        remote_columns = reference.get("reference_columns", columns)
        if not columns or len(columns) != len(remote_columns):
            raise ValueError("Reference columns must have matching, non-empty lists.")
        target = contracts[reference["table"]]["target"]
        lookup = spark.table(target).select(*[
            F.col(remote).alias(local) for local, remote in zip(columns, remote_columns)
        ])
        require_empty(frame.join(lookup, columns, "left_anti"),
                      f"Missing reference in {target}: {', '.join(columns)}")


def check_aggregate(spark, actual, rule, contracts):
    """Compare grouped totals with the configured source and dimension joins."""
    source = spark.table(contracts[rule["source"]]["target"])
    for join in rule.get("joins", []):
        source = source.join(spark.table(contracts[join["table"]]["target"]),
                             join["columns"])
    keys, measure = rule["group_by"], rule["measure"]
    columns = keys + [measure]
    expected = source.groupBy(*keys).agg(F.sum(measure).alias(measure)).select(*columns)
    actual = actual.select(*columns)
    check_required(actual, [measure])
    require_empty(actual.exceptAll(expected), "Stored totals differ from the configured source.")
    require_empty(expected.exceptAll(actual), "Missing totals or groups from the configured source.")


def apply_rule(spark, table, rule, contracts):
    """Execute a supported rule using only its JSON settings, without table names."""
    frame = spark.table(table)
    kind = rule["type"]
    if kind == "null":
        invalid = reduce(lambda a, b: a | b,
                         [F.col(name).isNotNull() for name in rule["columns"]])
        require_empty(frame.filter(invalid), "Expected null values: " + ", ".join(rule["columns"]))
    elif kind == "required":
        check_required(frame, rule["columns"])
    elif kind == "unique":
        check_keys(frame, rule["columns"])
    elif kind == "references":
        if not rule["references"]:
            raise ValueError("A references rule requires at least one reference.")
        check_references(spark, frame, rule["references"], contracts)
    elif kind == "aggregate":
        check_aggregate(spark, frame, rule, contracts)
    else:
        raise ValueError(f"Unsupported quality rule type: {kind}")


def collect_results(spark, project_root, run_id):
    """Discover table contracts and execute each declared quality rule.

    Reference names use layer/filename, for example gold/dim_product. Tables
    without a quality block have no rules to execute. Rule names are preserved
    in the results, so their meaning is defined by the table's JSON.
    """
    root = Path(project_root) / "configs"
    contracts = {
        f"{layer}/{path.stem}": load_config(path)
        for layer in ("bronze", "silver", "gold")
        for path in sorted((root / layer).glob("*.json"))
    }
    results = []
    for contract in contracts.values():
        table = contract["target"]
        for name, rule in contract.get("quality", {}).items():
            results.append(evaluate_rule(
                run_id, table, name, lambda: apply_rule(spark, table, rule, contracts),
                description=rule.get("description", name),
                table_description=contract["description"]
            ))
    if not results:
        raise ValueError("No quality rules configured in the table JSONs.")
    return results


def run_quality(spark, project_root, run_id="", results_table=RESULTS_TABLE):
    """Persist all rule outcomes once and return them to the notebook.

    Manual executions receive a UUID. Jobs supply their own run identifier.
    The caller must fail the task when any persisted outcome is FAIL or ERROR.
    """
    run_id = str(run_id or uuid4())
    results = collect_results(spark, project_root, run_id)
    spark.createDataFrame(results, RESULT_SCHEMA).write.format("delta").mode(
        "append"
    ).option("mergeSchema", "true").saveAsTable(results_table)
    apply_table_descriptions(spark, results_table, RESULTS_DESCRIPTION, COLUMN_DESCRIPTIONS)
    return results
