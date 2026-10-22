"""Project channel classifications from the approved Silver source."""

from pyspark.sql import functions as F


def transform(frame):
    """Retain all classified channels, including channels without sales."""
    return frame.select(
        F.sha2("trade_channel_key", 256).alias("channel_key"), "trade_channel_key",
        "trade_channel_description", "trade_group_description", "trade_type_description",
    ).distinct()
