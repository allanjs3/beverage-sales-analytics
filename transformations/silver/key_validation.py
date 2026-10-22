"""Validate each incoming batch against compact, approved key mappings."""

from functools import reduce
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from transformations.silver.tables import TABLES
from utils.metadata import create_described_table


# Change only when the meaning of approved mappings changes.
RULES_VERSION = "3"
KEY_RULES = {name: (rule["keys"], rule["attributes"]) for name, rule in TABLES.items()}


def ensure_registry(spark, source, target_id, checkpoint_exists):
    """Create the key registry or verify that it belongs to the current target."""
    table = source["validation_table"]
    keys, attributes = KEY_RULES[source["name"]]
    if not spark.catalog.tableExists(table):
        if checkpoint_exists:
            raise RuntimeError(f"Validation registry is missing for an existing checkpoint: {table}")
        descriptions = {c["target"]: c["description"] for c in source["columns"]}
        create_described_table(
            spark, table, {name: "STRING" for name in (*keys, *attributes)},
            source.get("validation_description", f"Approved keys and attributes for Silver {source['name']}."),
            descriptions, {"beverage.targetTableId": target_id},
        )
    else:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
        if detail["properties"].get("beverage.targetTableId") != target_id:
            raise RuntimeError(f"Validation registry belongs to another target: {table}")
        if dict(spark.table(table).dtypes) != {name: "string" for name in (*keys, *attributes)}:
            raise RuntimeError(f"Unexpected validation registry schema: {table}")


def make_validation_writer(table, keys, attributes):
    """Build a serializable, idempotent foreachBatch callback.

    Only the incoming batch is grouped. Historical comparisons and the MERGE
    use a compact key registry, never the Bronze or Silver transaction history.
    The callback captures only strings/tuples and uses the batch Spark session
    so it also works with Spark Connect and serverless compute.
    """
    keys, attributes = tuple(keys), tuple(attributes)

    def validate_batch(batch, batch_id):
        """Reject invalid/conflicting keys before inserting new approved mappings."""

        if batch.isEmpty():
            return False
        invalid = batch.filter(F.size("_quality_errors") > 0).select(
            "_source_file_path", "_quality_errors"
        ).limit(5).collect()
        if invalid:
            raise ValueError(f"Invalid Silver input in validation batch {batch_id}: {invalid}")
        mappings = batch.select(*keys, *attributes).distinct()
        conflicts = mappings.groupBy(*keys).count().filter("count > 1").limit(5).collect()
        if conflicts:
            raise ValueError(f"Conflicting attributes within incoming batch: {conflicts}")
        approved = batch.sparkSession.table(table).alias("approved")
        incoming = mappings.alias("incoming")
        on = reduce(lambda a, b: a & b, [
            F.col(f"approved.{key}") == F.col(f"incoming.{key}") for key in keys
        ])
        different = reduce(lambda a, b: a | b, [
            ~F.col(f"approved.{name}").eqNullSafe(F.col(f"incoming.{name}"))
            for name in attributes
        ])
        conflicts = incoming.join(approved, on, "inner").filter(different).select(
            *[F.col(f"incoming.{key}").alias(key) for key in keys]
        ).limit(5).collect()
        if conflicts:
            raise ValueError(f"Conflicting attributes against approved keys: {conflicts}")
        condition = " AND ".join(f"approved.`{key}` = incoming.`{key}`" for key in keys)
        # Replaying a batch after a committed MERGE only matches existing keys.
        (DeltaTable.forName(batch.sparkSession, table).alias("approved")
         .merge(mappings.alias("incoming"), condition)
         .whenNotMatchedInsertAll().execute())
        return True

    return validate_batch



def make_batch_writer(source, app_id, run_id):
    """Validate and write one batch, preserving retries with Delta transaction IDs.

    Registry inserts are idempotent. The target write uses a stable application
    ID and the streaming batch ID, so a retry after a committed write adds no rows.
    The closure captures only plain values and a serializable validation callback.
    """
    keys, attributes = KEY_RULES[source["name"]]
    validate = make_validation_writer(source["validation_table"], keys, attributes)
    target = source["target"]

    def write_batch(batch, batch_id):
        """Approve the complete batch before appending its original occurrences."""

        if not validate(batch, batch_id):
            return
        output = (batch.drop("_quality_errors")
                  .withColumn("_silver_run_id", F.lit(run_id))
                  .withColumn("_processed_at", F.current_timestamp()))
        metadata = json.dumps({"app_id": app_id, "run_id": run_id, "batch_id": batch_id})
        (output.write.format("delta").mode("append")
         .option("txnAppId", app_id).option("txnVersion", batch_id)
         .option("userMetadata", metadata).saveAsTable(target))

    return write_batch
