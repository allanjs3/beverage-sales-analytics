"""Identify beverage presentations independently of arrival order."""

from pyspark.sql import functions as F


def product_key():
    """Hash a structured business key without delimiter ambiguity."""
    return F.sha2(F.to_json(F.struct(
        "brand_flavor_code", "package_category_code", "package_name"
    )), 256)


def transform(frame):
    """Return distinct product presentations from the incoming sales batch."""
    return frame.select(
        product_key().alias("product_key"), "brand_flavor_code", "brand_name",
        "package_category_code", "package_category_description", "package_name",
    ).distinct()
