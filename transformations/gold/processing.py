"""Maintain the dimensional model and independently resumable sales summaries."""

import json
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from transformations.gold.configuration import table_module
from utils.metadata import apply_table_descriptions, create_described_table
from utils.selection import order_tables
from utils.streaming import checkpoint_has_metadata, latest_version


MODEL_VERSION = "1"


def ensure_table(spark, contract, properties=None):
    """Create descriptions in one DDL statement or apply only changed comments."""
    target = contract["target"]
    columns = contract["columns"]
    comments = {c["name"]: c["description"] for c in columns}
    types = {c["name"]: c["type"] for c in columns}
    if not spark.catalog.tableExists(target):
        create_described_table(spark, target, types, contract["description"], comments,
                               properties or {"beverage.modelVersion": MODEL_VERSION})
    else:
        if dict(spark.table(target).dtypes) != types:
            raise ValueError(f"Unexpected Gold schema: {target}")
        apply_table_descriptions(spark, target, contract["description"], comments)


def merge_dimension(spark, contract, records, key):
    """Insert unseen dimension members; approved Silver attributes are immutable."""
    (DeltaTable.forName(spark, contract["target"]).alias("target")
     .merge(records.alias("source"), f"target.{key} = source.{key}")
     .whenNotMatchedInsertAll().execute())


def make_table_writer(tables, name, app_id, run_id):
    """Write one selected table; never update its prerequisites implicitly.

    Dimension inserts are idempotent MERGEs. Fact appends use the checkpoint
    batch ID and a stable application ID to preserve every sales occurrence.
    """
    contract = tables[name]

    def write_batch(batch, batch_id):
        """Transform pending rows and check fact references before appending."""
        if batch.isEmpty():
            return
        spark = batch.sparkSession
        output = table_module(name).transform(batch)
        if contract["kind"] == "dimension":
            merge_dimension(spark, contract, output, contract["key"])
            return
        missing = None
        for dependency in contract["depends_on"]:
            dimension = tables[dependency]
            if dimension["kind"] != "dimension":
                continue
            key = dimension["key"]
            absent = (output.select(key).distinct()
                      .join(spark.table(dimension["target"]).select(key), key, "left_anti")
                      .select(F.lit(dependency).alias("dimension"), F.col(key).cast("string").alias("key")))
            missing = absent if missing is None else missing.unionByName(absent)
        if missing is not None and missing.limit(1).collect():
            raise ValueError("Missing dimension members. Load the required dimensions before the fact.")
        metadata = json.dumps({"app_id": app_id, "run_id": run_id, "batch_id": batch_id})
        (output.write.format("delta").mode("append")
         .option("txnAppId", app_id).option("txnVersion", batch_id)
         .option("userMetadata", metadata).saveAsTable(contract["target"]))

    return write_batch


def process_table(spark, dbutils, tables, name, execution):
    """Consume one Silver source with an independent checkpoint per target.

    The source version is a conservative no-change shortcut, recorded only
    after a successful stream. Checkpoints remain authoritative for retries.
    A concurrent source append can cause one extra check, never skipped data.
    """
    started = perf_counter()
    contract = tables[name]
    target = contract["target"]
    exists = spark.catalog.tableExists(target)
    checkpoint_exists = checkpoint_has_metadata(dbutils, contract["checkpoint"])
    if checkpoint_exists and not exists:
        raise RuntimeError(f"Checkpoint exists without its target: {name}")
    if exists and not checkpoint_exists:
        last = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()
        if last["version"] != 0 or last["operation"] != "CREATE TABLE":
            raise RuntimeError(f"Loaded target has no checkpoint: {name}; review both together.")
    source_id = spark.sql(f"DESCRIBE DETAIL {contract['source_table']}").first()["id"]
    source_version = latest_version(spark, contract["source_table"])
    initial = {"beverage.appId": str(uuid4()), "beverage.sourceTableId": source_id,
               "beverage.modelVersion": MODEL_VERSION}
    if contract["kind"] == "fact":
        initial["delta.enableChangeDataFeed"] = "true"
    ensure_table(spark, contract, initial)
    properties = spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"]
    if (properties.get("beverage.sourceTableId") != source_id
            or properties.get("beverage.modelVersion") != MODEL_VERSION):
        raise RuntimeError(f"Source identity or model version changed: {name}")
    if contract["kind"] == "fact" and properties.get("delta.enableChangeDataFeed") != "true":
        raise RuntimeError("Fact change data feed must remain enabled.")
    result = {"table": name, "rows": 0, "measure": "Registros adicionados", "status": "NO_NEW_DATA"}
    if checkpoint_exists and properties.get("beverage.sourceVersion") == str(source_version):
        return {**result, "duration_seconds": round(perf_counter() - started, 3)}
    volume = contract["state_volume"]
    if not Path("/Volumes/" + volume.replace(".", "/")).is_dir():
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")
    before = latest_version(spark, target)
    query = None
    try:
        incoming = (spark.readStream.format("delta").option("maxBytesPerTrigger", "256m")
                    .table(contract["source_table"]))
        query = (incoming.writeStream.foreachBatch(
                    make_table_writer(tables, name, properties["beverage.appId"], execution["run_id"]))
                 .option("checkpointLocation", contract["checkpoint"])
                 .trigger(availableNow=True).start())
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
    history = (spark.sql(f"DESCRIBE HISTORY {target}").filter(F.col("version") > before)
               .filter(F.col("operation").isin("WRITE", "MERGE"))
               .select("operation", "operationMetrics", "userMetadata").collect())
    for row in history:
        if row["operation"] == "WRITE":
            metadata = json.loads(row["userMetadata"] or "{}")
            if metadata.get("run_id") != execution["run_id"]:
                raise RuntimeError(f"Another writer modified {name} during this execution.")
            result["rows"] += int(row["operationMetrics"]["numOutputRows"])
        else:
            result["rows"] += int(row["operationMetrics"]["numTargetRowsInserted"])
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('beverage.sourceVersion' = '{source_version}')")
    result["status"] = "UPDATED" if result["rows"] else "NO_NEW_DATA"
    return {**result, "duration_seconds": round(perf_counter() - started, 3)}


def summary_cursor(spark, target):
    """Read the fact version committed atomically with the last summary write."""
    writes = (spark.sql(f"DESCRIBE HISTORY {target}")
              .filter(F.col("operation").isin(
                  "WRITE", "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT"
              )).orderBy(F.desc("version")).limit(1).collect())
    if not writes:
        return None
    metadata = json.loads(writes[0]["userMetadata"] or "{}")
    if "fact_version" not in metadata or metadata.get("model_version") != MODEL_VERSION:
        raise RuntimeError(f"Missing or incompatible summary watermark: {target}")
    return metadata


def fact_data_version(spark, target):
    """Ignore metadata/compaction commits when checking for pending fact data.

    Unexpected data-changing operations remain visible to the CDF validation.
    Compaction and comment changes alone do not require rewriting summaries.
    """
    maintenance = ["OPTIMIZE", "SET TBLPROPERTIES", "CHANGE COLUMN", "SET TABLE COMMENT",
                   "ADD COLUMNS", "COMPUTE STATS"]
    return int(spark.sql(f"DESCRIBE HISTORY {target}")
               .filter(~F.col("operation").isin(maintenance))
               .agg(F.max("version")).first()[0])


def process_summary(spark, tables, name, execution):
    """Add new fact amounts to a compact summary with an atomic version marker.

    The initial build reads the fact once. Subsequent builds aggregate only
    inserted Change Data Feed rows and combine them with existing totals.
    Replacing the small summary and storing its cursor in the same Delta commit
    prevents double counting after a failure. No fact history is rescanned.
    A single writer per target and append-only source contracts are required.
    """
    started = perf_counter()
    contract = tables[name]
    fact = tables[contract["source_name"]]["target"]
    if not spark.catalog.tableExists(fact):
        raise ValueError(f"Process {contract['source_name']} before building {name}.")
    target = contract["target"]
    ensure_table(spark, contract)
    cursor = summary_cursor(spark, target)
    detail = spark.sql(f"DESCRIBE DETAIL {fact}").first()
    version = fact_data_version(spark, fact)
    if cursor and cursor["fact_id"] != detail["id"]:
        raise RuntimeError("The fact was replaced; rebuild its dependent summaries explicitly.")
    if cursor and cursor["fact_version"] == version:
        return {"table": name, "rows": 0, "measure": "Grupos gravados", "status": "NO_NEW_DATA",
                "duration_seconds": round(perf_counter() - started, 3)}
    if cursor and cursor["fact_version"] > version:
        raise RuntimeError("The summary cursor is newer than the fact.")
    if cursor is None:
        incoming = spark.read.option("versionAsOf", version).table(fact)
    else:
        incoming = (spark.read.format("delta").option("readChangeFeed", "true")
                    .option("startingVersion", cursor["fact_version"] + 1)
                    .option("endingVersion", version).table(fact))
        if incoming.filter(F.col("_change_type") != "insert").limit(1).count():
            raise RuntimeError("Gold summaries require append-only facts; mutations need a rebuild.")
    dimensions = {n: spark.table(tables[n]["target"]) for n in contract["depends_on"]
                  if tables[n]["kind"] == "dimension"}
    rule = table_module(name)
    totals = rule.transform(incoming, dimensions)
    if cursor is not None:
        # Read the current target snapshot before replacing it atomically.
        current = spark.read.option("versionAsOf", latest_version(spark, target)).table(target)
        totals = totals.unionByName(current).groupBy(*rule.KEYS).agg(
            F.sum("sales_amount").alias("sales_amount"))
    metadata = {"fact_id": detail["id"], "fact_version": version,
                "model_version": MODEL_VERSION, "run_id": execution["run_id"]}
    (totals.write.format("delta").mode("overwrite")
     .option("userMetadata", json.dumps(metadata)).saveAsTable(target))
    last = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()
    return {"table": name, "rows": int(last["operationMetrics"]["numOutputRows"]),
            "measure": "Grupos gravados", "status": "UPDATED",
            "duration_seconds": round(perf_counter() - started, 3)}


def process_sources(spark, dbutils, config, execution):
    """Order selected tables and require unselected dependencies to exist.

    A Job supplies fresh prerequisites through task dependencies. A manual
    subset uses the already loaded prerequisites without refreshing them.
    """
    tables = config["tables"]
    selected = order_tables(config["selected"], {n: t["depends_on"] for n, t in tables.items()})
    for name in selected:
        for dependency in tables[name]["depends_on"]:
            if dependency not in selected and not spark.catalog.tableExists(tables[dependency]["target"]):
                raise ValueError(f"Load {dependency} before {name}, or include it in the selection.")
    results = []
    for name in selected:
        if tables[name]["kind"] == "summary":
            results.append(process_summary(spark, tables, name, execution))
        else:
            results.append(process_table(spark, dbutils, tables, name, execution))
    return {**execution, "sources": results,
            "has_new_data": any(r["status"] == "UPDATED" for r in results)}
