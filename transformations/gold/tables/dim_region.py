"""Project sales regions."""

from pyspark.sql import functions as F


def transform(frame):
    """Return one deterministic key per region name."""
    return frame.select(F.sha2("region_name", 256).alias("region_key"), "region_name").distinct()
