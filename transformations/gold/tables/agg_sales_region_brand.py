"""Summarize observed brand sales by region without filling missing sales."""

from pyspark.sql import functions as F


KEYS = ["region_name", "brand_name"]


def transform(facts, dimensions):
    """Preserve negative and zero totals for observed combinations."""
    return (facts.join(dimensions["dim_region"], "region_key")
            .join(dimensions["dim_product"], "product_key")
            .groupBy(*KEYS).agg(F.sum("sales_amount").alias("sales_amount")))
