"""Shared checkpoint and Delta commit controls for incremental pipelines."""

from pathlib import Path
from stat import S_ISREG


def validate_checkpoint_pair(source_name, table_exists, checkpoint_exists):
    """Require a target and its checkpoint to exist together or both be absent.

    Args:
        source_name: Source label used in the error message.
        table_exists: Whether the configured target table exists.
        checkpoint_exists: Whether checkpoint metadata exists.

    Raises:
        RuntimeError: Exactly one member of the pair exists.

    This existence check does not establish checkpoint integrity or repair it.
    """
    if table_exists != checkpoint_exists:
        raise RuntimeError(
            f"Inconsistent ingestion state for {source_name}: "
            f"table_exists={table_exists}, checkpoint_exists={checkpoint_exists}. "
            "Review the target and checkpoint together. Do not delete either to force a reload."
        )

def checkpoint_has_metadata(dbutils, checkpoint_path):
    """Inspect a checkpoint file without submitting a failing filesystem command.

    Args:
        dbutils: Retained for compatibility with the shared pipeline interface.
        checkpoint_path: Absolute path to a source checkpoint directory.

    Returns:
        bool: True for a metadata file, False when it does not exist.
        Permission and other unexpected filesystem errors propagate.
    """
    try:
        return S_ISREG((Path(checkpoint_path) / "metadata").stat().st_mode)
    except FileNotFoundError:
        return False

def latest_version(spark, table_name):
    """Read the most recent Delta version without scanning table records.

    Args:
        spark: Active Spark session with catalog and SQL access.
        table_name: Previously validated, fully qualified Delta table name.

    Returns:
        int: Latest committed version, or -1 if the table does not exist.
    """
    if not spark.catalog.tableExists(table_name):
        return -1
    return int(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()["version"])

def summarize_commits(commits, query_id):
    """Count rows committed by the expected streaming query.

    Args:
        commits: Iterable of Delta history dictionaries for the run's versions.
        query_id: Streaming query identifier allowed to write during this run.

    Returns:
        dict: Total rows_added and number of streaming_commits.

    Raises:
        RuntimeError: An unexpected operation or writer is found, or required
            metrics are missing or negative.
        ValueError: A row-count metric cannot be converted to an integer.

    Recognized metadata and optimization operations do not add source records.
    Missing metrics must not be interpreted as a successful empty run.
    """
    rows_added = 0
    streaming_commits = 0
    for commit in commits:
        operation = commit.get("operation")
        if operation in {"CREATE TABLE", "OPTIMIZE", "SET TBLPROPERTIES", "ADD COLUMNS"}:
            continue
        if operation != "STREAMING UPDATE":
            raise RuntimeError(f"Unexpected target operation during ingestion: {operation}")
        parameters = commit.get("operationParameters") or {}
        if parameters.get("queryId") != query_id:
            raise RuntimeError("Another streaming writer modified the target during this run.")
        metric = (commit.get("operationMetrics") or {}).get("numOutputRows")
        if metric is None:
            raise RuntimeError("Missing committed row count; refusing to report NO_NEW_DATA.")
        value = int(metric)
        if value < 0:
            raise RuntimeError("Invalid committed row count.")
        rows_added += value
        streaming_commits += 1
    return {"rows_added": rows_added, "streaming_commits": streaming_commits}
