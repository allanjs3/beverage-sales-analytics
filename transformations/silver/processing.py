"""Process each Silver table with one incremental stream and one checkpoint."""

from pathlib import Path
from time import perf_counter
from uuid import uuid4
import json

from pyspark.sql import functions as F

from transformations.silver.key_validation import RULES_VERSION, ensure_registry, make_batch_writer
from transformations.silver.rules import BRONZE_METADATA_TYPES, METADATA_DESCRIPTIONS
from transformations.silver.tables import TABLES, sales
from utils.execution import to_json
from utils.metadata import apply_table_descriptions, create_described_table
from utils.streaming import checkpoint_has_metadata, latest_version


TRANSFORMATIONS = {name: rule["transform"] for name, rule in TABLES.items()}


def expected_input_types(source):
    """Return the expected Bronze input types for the selected transformation."""
    columns = TABLES[source["name"]]["input_columns"]
    return {**{name: "string" for name in columns}, **BRONZE_METADATA_TYPES}


def column_descriptions(source):
    """Combine business descriptions with the three Silver lineage fields."""
    return {**{c["target"]: c["description"] for c in source["columns"]}, **METADATA_DESCRIPTIONS}


def prepare_source(spark, dbutils, source):
    """Initialize table metadata and bind its checkpoint to an idempotent writer.

    An empty table created before a stream starts can resume initialization.
    A lost checkpoint after data writes, a lost target or a replaced source
    requires an explicit rebuild; input is never silently skipped or duplicated.
    Return the persisted writer properties after these checks.
    """

    raw = spark.table(source["source_table"])
    if dict(raw.dtypes) != expected_input_types(source):
        raise ValueError(f"Unexpected Bronze schema for {source['source_table']}")
    source_id = spark.sql(f"DESCRIBE DETAIL {source['source_table']}").first()["id"]
    target = source["target"]
    exists = spark.catalog.tableExists(target)
    checkpoint_exists = checkpoint_has_metadata(dbutils, source["checkpoint"])
    if not exists:
        if checkpoint_exists or spark.catalog.tableExists(source["validation_table"]):
            raise RuntimeError(f"Target is missing while processing state remains: {target}")
        typed = (TRANSFORMATIONS[source["name"]](raw).drop("_quality_errors")
                 .withColumn("_silver_run_id", F.lit(""))
                 .withColumn("_processed_at", F.current_timestamp()))
        create_described_table(
            spark, target, {f.name: f.dataType.simpleString() for f in typed.schema.fields},
            source["description"], column_descriptions(source),
            {"beverage.sourceTableId": source_id, "beverage.rulesVersion": RULES_VERSION,
             "beverage.appId": str(uuid4())},
        )
    elif not checkpoint_exists:
        last = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()
        if last["version"] != 0 or last["operation"] != "CREATE TABLE":
            raise RuntimeError(f"Checkpoint is missing for an already processed table: {target}")
    detail = spark.sql(f"DESCRIBE DETAIL {target}").first()
    properties = detail["properties"]
    if (properties.get("beverage.sourceTableId") != source_id
            or properties.get("beverage.rulesVersion") != RULES_VERSION
            or not properties.get("beverage.appId")):
        raise RuntimeError(f"Source or rules changed for {target}; revalidate before resuming.")
    ensure_registry(spark, source, detail["id"], checkpoint_exists)
    return properties


def summarize_batch_commits(commits, app_id, run_id):
    """Count actual Delta appends tagged by this writer and execution."""
    rows, batches = 0, 0
    for commit in commits:
        if commit["operation"] in {"OPTIMIZE", "SET TBLPROPERTIES"}:
            continue
        metadata = json.loads(commit.get("userMetadata") or "{}")
        if (commit["operation"] != "WRITE" or metadata.get("app_id") != app_id
                or metadata.get("run_id") != run_id):
            raise RuntimeError("An unexpected writer modified the Silver target during this run.")
        value = (commit.get("operationMetrics") or {}).get("numOutputRows")
        if value is None or int(value) < 0:
            raise RuntimeError("Missing or invalid committed row count.")
        rows += int(value)
        batches += 1
    return {"rows_added": rows, "batches_committed": batches}


def process_source(spark, source, properties, run_id, channel_table=None):
    """Skip a consumed source version or finish its pending AvailableNow stream.

    Record the source version observed before starting, only after the stream
    and commit checks succeed. Checkpoints remain authoritative for retries.
    A concurrent source append may cause an extra run, never skipped input.
    """

    started = perf_counter()
    source_version = latest_version(spark, source["source_table"])
    before = latest_version(spark, source["target"])
    if properties.get("beverage.sourceVersion") == str(source_version):
        return {
            "source": source["name"], "source_table": source["source_table"],
            "target": source["target"], "query_id": None,
            "version_before": before, "version_after": before,
            "rows_added": 0, "batches_committed": 0, "status": "NO_NEW_DATA",
            "duration_seconds": round(perf_counter() - started, 3),
        }
    app_id = properties["beverage.appId"]
    incoming = (spark.readStream.format("delta").option("maxBytesPerTrigger", "256m")
                .table(source["source_table"]))
    prepared = TRANSFORMATIONS[source["name"]](incoming)
    if source["name"] == "sales":
        channel_version = latest_version(spark, channel_table)
        channel_reference = spark.sql(f"SELECT * FROM {channel_table} VERSION AS OF {channel_version}")
        prepared = sales.check_sales_channels(prepared, channel_reference, unique_keys=True)
    query = None
    try:
        query = (prepared.writeStream.foreachBatch(make_batch_writer(source, app_id, run_id))
                 .option("checkpointLocation", source["checkpoint"])
                 .queryName(f"beverage_silver_{source['name']}")
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        query_id = str(query.id)
    finally:
        if query is not None and query.isActive:
            query.stop()
    after = latest_version(spark, source["target"])
    history = spark.sql(f"DESCRIBE HISTORY {source['target']}").filter(
        (F.col("version") > before) & (F.col("version") <= after)
    ).select("operation", "operationMetrics", "userMetadata")
    metrics = summarize_batch_commits(
        (r.asDict(recursive=True) for r in history.toLocalIterator()), app_id, run_id
    )
    spark.sql(
        f"ALTER TABLE {source['target']} SET TBLPROPERTIES "
        f"('beverage.sourceVersion' = '{source_version}')"
    )
    after = latest_version(spark, source["target"])
    return {
        "source": source["name"], "source_table": source["source_table"],
        "target": source["target"], "query_id": query_id,
        "version_before": before, "version_after": after, **metrics,
        "duration_seconds": round(perf_counter() - started, 3),
        "status": "PROCESSED" if metrics["rows_added"] else "NO_NEW_DATA",
    }


def summarize_run(execution, results):
    """Summarize appended records independently from catalog metadata changes."""
    changed = [r["source"] for r in results if r["rows_added"] > 0]
    return {**execution, "status": "SILVER_UPDATED" if changed else "NO_NEW_DATA",
            "has_new_data": bool(changed), "changed_sources": changed, "sources": results}


def process_sources(spark, dbutils, config, execution):
    """Process selected tables in dependency order; channels precede sales.

    Selecting sales alone uses already approved channel mappings. New channels
    must be processed first, or by selecting All tables. No unselected data
    table is written. Real validation and service errors remain visible.
    """
    results = []
    try:
        state_root = Path("/Volumes/" + config["state_volume"].replace(".", "/"))
        if not state_root.is_dir():
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['state_volume']}")
        selected = {s["name"] for s in config["sources"]}
        if "sales" in selected and "channel_group" not in selected:
            reference = config["contracts"]["channel_group"]["validation_table"]
            if not spark.catalog.tableExists(reference):
                raise ValueError("Process channel_group first, or select All tables.")
        for source in config["sources"]:
            properties = prepare_source(spark, dbutils, source)
            channel_table = (config["contracts"]["channel_group"]["validation_table"]
                             if source["name"] == "sales" else None)
            result = process_source(spark, source, properties, execution["run_id"], channel_table)
            results.append(result)
            result["metadata_changes"] = apply_table_descriptions(
                spark, source["target"], source["description"], column_descriptions(source)
            )
    except Exception as exc:
        print(to_json({**execution, "status": "FAILED", "completed_sources": results,
                       "error_type": type(exc).__name__}))
        raise
    return summarize_run(execution, results)


def inspect_new_rows(spark, config, result, show=None):
    """Summarize source paths and amounts for newly processed records.

    Args:
        spark: Active Spark session.
        config: Selected Silver contracts.
        result: Successful processing result containing run_id and sources.
        show: Optional display function for source file counts.

    Returns:
        dict: Run result augmented with per-source warning counts.
    """

    output = {**result, "quality_warnings": {}}
    for item in result["sources"]:
        if not item["rows_added"]:
            continue
        rows = spark.table(item["target"]).filter(F.col("_silver_run_id") == result["run_id"])
        output["quality_warnings"][item["source"]] = (
            sales.summarize_sales_amounts(rows) if item["source"] == "sales" else {}
        )
        if show:
            show(rows.groupBy("_source_file_path").count())
    return output
