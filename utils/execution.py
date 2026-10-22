"""Execution identity and serializable results, independent of processing layer."""

from datetime import datetime, timezone
import json
import uuid


def utc_now():
    """Return the current UTC timestamp as a timezone-aware ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def start_execution():
    """Return a new execution context with a UUID run_id and UTC started_at."""
    return {"run_id": str(uuid.uuid4()), "started_at": utc_now()}


def finish_execution(result):
    """Add a completion timestamp to a copy of an execution result.

    Args:
        result: Dictionary containing the execution context and outcomes.

    Returns:
        dict: Shallow copy with finished_at set to the current UTC timestamp.
    """
    return {**result, "finished_at": utc_now()}


def to_json(value):
    """Serialize an execution value with deterministic dictionary key ordering.

    Args:
        value: JSON-compatible execution context or result.

    Returns:
        str: JSON representation suitable for logs or notebook output.

    Raises:
        TypeError: A value cannot be serialized by the JSON encoder.
    """
    return json.dumps(value, sort_keys=True)
