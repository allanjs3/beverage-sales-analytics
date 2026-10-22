"""Summarize monetary sales by region and trade group."""

from pyspark.sql import functions as F


KEYS = ["region_name", "trade_group_description"]


def transform(facts, dimensions):
    """Join unique dimensions and aggregate the supplied fact records."""
    return (facts.join(dimensions["dim_region"], "region_key")
            .join(dimensions["dim_channel"], "channel_key")
            .groupBy(*KEYS).agg(F.sum("sales_amount").alias("sales_amount")))
