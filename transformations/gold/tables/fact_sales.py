"""Preserve every sales occurrence at its original reporting grain."""

from pyspark.sql import functions as F

from transformations.gold.tables.dim_product import product_key


def transform(frame):
    """Replace descriptive attributes with dimensional keys; never deduplicate."""
    return frame.select(
        F.date_format("sale_date", "yyyyMMdd").cast("int").alias("date_key"),
        product_key().alias("product_key"),
        F.sha2("region_name", 256).alias("region_key"),
        F.sha2("trade_channel_key", 256).alias("channel_key"),
        "channel_group", "period", "sales_amount", "_source_file_path", "_silver_run_id",
    )
