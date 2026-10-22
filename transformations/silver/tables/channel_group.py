"""Standardize sales channel classifications and validate their keys."""

from pyspark.sql import functions as F

from transformations.silver.rules import (
    build_records, clean_text, validate_key_attributes,
)


INPUT_COLUMNS = (
    "trade_channel_description", "trade_group_description",
    "trade_type_description",
)
OUTPUT_COLUMNS = (*INPUT_COLUMNS, "trade_channel_key")


def transform_channel_group(frame):
    """Trim channel labels and add a normalized relationship key.

    Args:
        frame: Batch or streaming Bronze channel DataFrame.

    Returns:
        DataFrame: Standardized classifications, lineage and quality errors.
    """

    values = {name: clean_text(name) for name in INPUT_COLUMNS}
    values["trade_channel_key"] = F.upper(clean_text("trade_channel_description"))
    return build_records(frame, values)


def validate_channels(frame):
    """Reject channel keys associated with conflicting groups or types."""
    validate_key_attributes(
        frame,
        keys=["trade_channel_key"],
        attributes=["trade_group_description", "trade_type_description"],
        rule_name="consistent_channel_classification",
    )
