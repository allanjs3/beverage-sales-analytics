"""Summarize monetary sales by brand and calendar month."""

from pyspark.sql import functions as F


KEYS = ["brand_name", "year", "month"]


def transform(facts, dimensions):
    """Keep years separate when grouping monthly brand sales."""
    return (facts.join(dimensions["dim_product"], "product_key")
            .join(dimensions["dim_calendar"], "date_key")
            .groupBy(*KEYS).agg(F.sum("sales_amount").alias("sales_amount")))
