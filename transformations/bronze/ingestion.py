"""Incremental CSV ingestion shared by configured Bronze sources.

Spark and Databricks utilities are explicit dependencies. Importing this module
performs no reads, writes, configuration loading or stream startup.
"""

from copy import deepcopy
from pathlib import Path
import fnmatch
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils.config import load_configs
from utils.execution import to_json
from utils.metadata import apply_table_descriptions, validate_description
from utils.selection import select_tables
from utils.streaming import (
    validate_checkpoint_pair, checkpoint_has_metadata, summarize_commits,
)
from utils.validation import require_keys, require_unique, validate_identifier


TECHNICAL_COLUMN_DESCRIPTIONS = {
    "_rescued_data": "JSON containing fields rescued by Auto Loader during parsing; null when no fields were rescued.",
    "_source_file_path": "Full path of the source file from which the record was read.",
    "_source_file_name": "Name of the source file from which the record was read.",
    "_source_file_size": "Size of the source file in bytes at ingestion.",
    "_source_file_modified_at": "Last modification timestamp of the source file reported at ingestion.",
    "_ingested_at": "Timestamp assigned to the record during Bronze ingestion.",
    "_ingestion_run_id": "Identifier of the pipeline execution that ingested the record.",
}


def load_bronze_config(directory, table_name=""):
    """Assemble the Bronze configuration from one JSON file per target table.

    Args:
        directory: Directory containing the table-specific JSON files.
        table_name: One name, comma-separated names or a list. Empty string
            and All tables select every configured target.

    Returns:
        dict: Shared settings and validated sources in selection order.

    Raises:
        ValueError: A filename differs from its target table, shared settings
            conflict, the directory is empty, a selected table is unknown,
            or a source definition is invalid.
        OSError: A configuration file cannot be read.
    """
    available = load_configs(directory)
    documents = {name: available[name] for name in select_tables(table_name, available)}
    config = None
    for filename, document in documents.items():
        require_keys(document, {"schema_version", "state_volume", "target"}, filename)
        validate_identifier(document["target"], "target table", parts=3)
        if filename != document["target"].split(".")[-1]:
            raise ValueError(f"Configuration filename must match its target table: {filename}")
        common = {key: document[key] for key in ("schema_version", "state_volume")}
        if config is None:
            config = {**common, "sources": []}
        elif any(config[key] != value for key, value in common.items()):
            raise ValueError("Bronze configurations must share schema_version and state_volume.")
        config["sources"].append({key: value for key, value in document.items() if key not in common})
    validate_sources(config)
    return config


def validate_sources(config):
    """Check the source contract before creating state or starting ingestion.

    Args:
        config: Combined configuration with schema_version, state_volume and
            a non-empty sources list.

    Raises:
        ValueError: Definitions contain unsupported settings, invalid names,
            duplicate identities, conflicting columns or overlapping patterns.

    This check inspects configuration only; it does not access workspace data.
    """
    require_keys(config, {"schema_version", "state_volume", "sources"}, "Bronze config")
    if config["schema_version"] != 1:
        raise ValueError("Unsupported Bronze configuration version.")
    validate_identifier(config["state_volume"], "state volume", parts=3)
    sources = config["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("At least one source is required.")
    for source in sources:
        require_keys(source, {"name", "source_path", "file_pattern", "format", "target", "reader_options", "columns"}, "source")
        validate_identifier(source["name"], "source name")
        validate_identifier(source["target"], "target table", parts=3)
        validate_description(source.get("description"), source["target"])
        if source["format"] != "csv":
            raise ValueError("This ingestion implementation supports CSV sources only.")
        path = source["source_path"]
        if not isinstance(path, str) or not path.startswith("/Volumes/") or len(path.split("/")) < 5 or ".." in path.split("/"):
            raise ValueError("Source paths must be absolute Unity Catalog volume paths.")
        pattern = source["file_pattern"]
        if not isinstance(pattern, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+\*\.csv", pattern):
            raise ValueError("File patterns must contain a literal prefix followed by *.csv.")
        options = source["reader_options"]
        require_keys(options, {"encoding", "sep", "multiLine"}, "reader_options")
        if set(options) != {"encoding", "sep", "multiLine"}:
            raise ValueError("Unsupported reader option; ingestion controls cannot be overridden.")
        if not isinstance(options["encoding"], str) or not options["encoding"]:
            raise ValueError("encoding must be a non-empty string.")
        if not isinstance(options["sep"], str) or len(options["sep"]) != 1:
            raise ValueError("sep must contain one character.")
        if not isinstance(options["multiLine"], bool):
            raise ValueError("multiLine must be a JSON boolean.")
        columns = source["columns"]
        if not isinstance(columns, list) or not columns:
            raise ValueError("Every source requires column mappings.")
        for column in columns:
            require_keys(column, {"source", "target"}, "column mapping")
            if not isinstance(column["source"], str) or not column["source"] or column["source"].lower() in {"_metadata", "_rescued_data"}:
                raise ValueError("Invalid or reserved source column name.")
            validate_identifier(column["target"], "target column")
            validate_description(column.get("description"), column["target"])
            if column["target"].startswith("_"):
                raise ValueError("Underscore-prefixed target names are reserved for ingestion metadata.")
        require_unique((column["source"].lower() for column in columns), "source columns")
        require_unique((column["target"].lower() for column in columns), "target columns")
        references = source.get("reference_counts", {})
        if not isinstance(references, dict):
            raise ValueError("reference_counts must be an object.")
        for filename, count in references.items():
            if "/" in filename or "\\" in filename or not fnmatch.fnmatchcase(filename, pattern):
                raise ValueError("Reference filenames must match their source pattern.")
            if type(count) is not int or count < 0:
                raise ValueError("Reference counts must be non-negative integers.")
    require_unique((source["name"].lower() for source in sources), "source names and checkpoints")
    require_unique((source["target"].lower() for source in sources), "target tables")
    for index, left in enumerate(sources):
        for right in sources[index + 1:]:
            left_path = left["source_path"].rstrip("/")
            right_path = right["source_path"].rstrip("/")
            paths_overlap = (left_path == right_path or left_path.startswith(right_path + "/") or right_path.startswith(left_path + "/"))
            a, b = left["file_pattern"].split("*")[0], right["file_pattern"].split("*")[0]
            if paths_overlap and (a.startswith(b) or b.startswith(a)):
                raise ValueError("Source file patterns overlap in the same input directory tree.")


def prepare_sources(config):
    """Validate sources and resolve their independent state directories.

    Args:
        config: Combined Bronze configuration.

    Returns:
        list: Deep-copied source definitions with checkpoint and schema_location.

    Raises:
        ValueError: The configuration fails source-contract validation.

    Paths depend on state_volume and source name, so repeated executions reuse
    the same state. No directory is created by this function.
    """
    validate_sources(config)
    state_root = "/Volumes/" + config["state_volume"].replace(".", "/")
    sources = deepcopy(config["sources"])
    for source in sources:
        source["checkpoint"] = f"{state_root}/{source['name']}/checkpoint"
        source["schema_location"] = f"{state_root}/{source['name']}/schema"
    return sources


def ingest_sources(spark, dbutils, config, execution):
    """Process all configured sources and summarize their committed records.

    Args:
        spark: Active Spark session used for catalog checks and Delta writes.
        dbutils: Databricks utilities passed to the shared checkpoint interface.
        config: Combined Bronze configuration.
        execution: Execution context containing run_id and started_at.

    Returns:
        dict: Run summary with per-source metrics and a has_new_data indicator.

    Raises:
        ValueError: Source definitions are invalid.
        RuntimeError: Target/checkpoint consistency or commit checks fail.

    Creates the state volume if needed and starts one stream per source. Service
    errors propagate. A later failure does not undo earlier committed sources;
    the failure diagnostic records sources already completed.
    """
    sources = prepare_sources(config)
    source_results = []
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['state_volume']}")
        # Check every pair before starting any source.
        for source in sources:
            source["target_exists"] = spark.catalog.tableExists(source["target"])
            validate_checkpoint_pair(
                source["name"], source["target_exists"],
                checkpoint_has_metadata(dbutils, source["checkpoint"]),
            )
        for source in sources:
            result = ingest_source(spark, source, execution["run_id"])
            source_results.append(result)
            result["metadata_changes"] = apply_table_descriptions(
                spark, source["target"], source["description"],
                {**{column["target"]: column["description"] for column in source["columns"]},
                 **TECHNICAL_COLUMN_DESCRIPTIONS},
            )
    except Exception as exc:
        print(to_json({
            **execution, "status": "FAILED", "completed_sources": source_results,
            "error_type": type(exc).__name__,
        }))
        raise
    return {**execution, **build_run_summary(execution["run_id"], source_results)}


def build_run_summary(run_id, results):
    """Describe whether any source added records during the execution.

    Args:
        run_id: Identifier shared by the source ingestion operations.
        results: Source result dictionaries containing source and rows_added.

    Returns:
        dict: Status, changed source names, has_new_data and the supplied results.
        Zero added rows produce NO_NEW_DATA, including header-only inputs.
    """
    changed = [result["source"] for result in results if result["rows_added"] > 0]
    return {
        "run_id": run_id,
        "status": "BRONZE_UPDATED" if changed else "NO_NEW_DATA",
        "has_new_data": bool(changed),
        "changed_sources": changed,
        "sources": results,
    }


def ingest_source(spark, source, run_id):
    """Append newly discovered CSV records using Auto Loader and a Delta sink.

    Args:
        spark: Active Spark session supporting Auto Loader and Delta streaming.
        source: Validated source with state paths and preflight target_exists.
        run_id: Identifier added to every record ingested in this execution.

    Returns:
        dict: Source identity, query ID, table versions and committed row counts.

    Raises:
        RuntimeError: Delta commit validation detects inconsistent metrics or
            another writer. Spark and filesystem errors also propagate.

    Source values remain strings. AvailableNow processes available files and
    terminates; the checkpoint and native Delta sink coordinate retry progress.
    Writes are committed independently from subsequent diagnostic checks.
    """

    before_version = (
        int(spark.sql(f"DESCRIBE HISTORY {source['target']} LIMIT 1").first()["version"])
        if source["target_exists"] else -1
    )
    schema = StructType([
        StructField(column["source"], StringType(), True) for column in source["columns"]
    ] + [StructField("_rescued_data", StringType(), True)])
    incoming = (
        spark.readStream.format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", source["format"])
        .option("cloudFiles.schemaLocation", source["schema_location"])
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.allowOverwrites", False)
        .option("cloudFiles.useNotifications", False)
        .option("cloudFiles.useManagedFileEvents", False)
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .option("pathGlobFilter", source["file_pattern"])
        .option("header", True)
        .option("enforceSchema", False)
        .options(**source["reader_options"])
        .option("ignoreLeadingWhiteSpace", False)
        .option("ignoreTrailingWhiteSpace", False)
        .option("mode", "FAILFAST")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source["source_path"])
    )
    prepared = incoming.select(
        *[F.col("`" + column["source"].replace("`", "``") + "`").alias(column["target"])
          for column in source["columns"]],
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file_path"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_ingestion_run_id"),
    )
    query = None
    try:
        query = (
            prepared.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", source["checkpoint"])
            .queryName(f"beverage_bronze_{source['name']}")
            .trigger(availableNow=True)
            .toTable(source["target"])
        )
        query.awaitTermination()
        query_id = str(query.id)
    finally:
        if query is not None and query.isActive:
            query.stop()

    after_version = int(spark.sql(
        f"DESCRIBE HISTORY {source['target']} LIMIT 1"
    ).first()["version"])
    history = spark.sql(f"DESCRIBE HISTORY {source['target']}").filter(
        (F.col("version") > before_version) & (F.col("version") <= after_version)
    ).select("operation", "operationParameters", "operationMetrics")
    # Read transaction metadata, not the full Bronze table or a bounded progress buffer.
    metrics = summarize_commits(
        (row.asDict(recursive=True) for row in history.toLocalIterator()), query_id
    )
    return {
        "source": source["name"],
        "target": source["target"],
        "query_id": query_id,
        "version_before": before_version,
        "version_after": after_version,
        **metrics,
        "status": "INGESTED" if metrics["rows_added"] else "NO_NEW_DATA",
    }


def inspect_new_rows(spark, config, result, show=None):
    """Check rescued fields and reference counts for records added by this run.

    Args:
        spark: Active Spark session with read access to the target tables.
        config: Combined configuration containing optional reference_counts.
        result: Ingestion result with run_id and per-source row counts.
        show: Optional callable that displays the materialized load summary.

    Returns:
        dict: Deep-copied result with file checks, rescued counts and a review
        flag. Flagged data produces BRONZE_UPDATED_REVIEW_REQUIRED.

    Sources with zero new rows are skipped. These checks run after commits and
    do not roll back records; read or display errors propagate to the caller.
    """

    run_summary = deepcopy(result)
    references = {source["name"]: source.get("reference_counts", {}) for source in config["sources"]}
    review_required = False
    for result in run_summary["sources"]:
        reference_counts = references[result["source"]]
        if result["rows_added"] == 0:
            continue
        new_rows = spark.table(result["target"]).filter(F.col("_ingestion_run_id") == run_summary["run_id"])
        file_summary = new_rows.groupBy("_source_file_path", "_source_file_name").agg(
            F.count("*").alias("row_count"),
            F.count(F.when(F.col("_rescued_data").isNotNull(), 1)).alias("rescued_rows"),
        )
        # Return totals and only configured reference files to the driver.
        # Display reuses this result instead of reading Bronze a second time.
        summary_frame = file_summary.agg(
            F.count("*").alias("file_count"),
            F.coalesce(F.sum("row_count"), F.lit(0)).alias("row_count"),
            F.coalesce(F.sum("rescued_rows"), F.lit(0)).alias("rescued_rows"),
            F.collect_list(F.when(
                F.col("_source_file_name").isin(list(reference_counts)),
                F.struct("_source_file_path", "_source_file_name", "row_count", "rescued_rows"),
            )).alias("reference_files"),
        )
        summary = summary_frame.first()
        if show is not None:
            show(spark.createDataFrame([summary], summary_frame.schema))
        rescued = summary["rescued_rows"]
        result["rescued_rows"] = int(rescued)
        reference_results = []
        for reference in summary["reference_files"]:
            matches = reference["row_count"] == reference_counts[reference["_source_file_name"]]
            reference_results.append({
                "file": reference["_source_file_name"],
                "expected_rows": reference_counts[reference["_source_file_name"]],
                "actual_rows": reference["row_count"],
                "matches": matches,
            })
            review_required = review_required or not matches
        result["reference_checks"] = reference_results
        review_required = review_required or rescued > 0

    run_summary["review_required"] = review_required
    if review_required:
        run_summary["status"] = "BRONZE_UPDATED_REVIEW_REQUIRED"
    return run_summary
