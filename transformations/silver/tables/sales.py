"""Standardize beverage sales and validate their business relationships."""

from pyspark.sql import functions as F

from transformations.silver.rules import (
    build_records, clean_text, validate_key_attributes,
)


INPUT_COLUMNS = (
    "sale_date", "brand_flavor_code", "brand_name", "region_name",
    "channel_group", "trade_channel_description", "package_category_code",
    "package_category_description", "package_name", "sales_amount",
    "year", "month", "period",
)
OUTPUT_COLUMNS = (*INPUT_COLUMNS, "trade_channel_key")


def normalize_package_name(name="package_name"):
    """Standardize presentation labels without merging volume/unit variants.

    Uppercase the label, remove trailing asterisks and standalone S/F tokens,
    and write a leading decimal as 0.x. A decimal comma at the start is also
    represented with a point. Other suffixes and internal letters remain unchanged.
    The suffix removal is an agreed case assumption, not a source definition.
    """
    value = F.upper(clean_text(name))
    value = F.trim(F.regexp_replace(value, r"(?:\s+[SF]|\s*\*)+$", ""))
    value = F.regexp_replace(value, r"^[.,](?=\d)", "0.")
    return F.when(F.length(value) > 0, value)


def transform_sales(frame):
    """Type sales fields and flag invalid values without removing records.

    Args:
        frame: Batch or streaming Bronze sales DataFrame.

    Returns:
        DataFrame: Standardized fields, lineage and quality observations.
    """

    sale_date = F.try_to_timestamp(
        clean_text("sale_date"), F.lit("M/d/yyyy")
    ).cast("date")
    # Reject extra fractional digits before casting to avoid silent rounding.
    amount = F.when(
        clean_text("sales_amount").rlike(r"^[+-]?[0-9]+(\.[0-9]{1,2})?$"),
        F.expr("try_cast(trim(sales_amount) AS decimal(28,2))"),
    )
    period = F.when(
        clean_text("period").rlike(r"^[+-]?[0-9]+$"),
        F.expr("try_cast(trim(period) AS int)"),
    )
    values = {name: clean_text(name) for name in INPUT_COLUMNS}
    values.update(
        sale_date=sale_date, sales_amount=amount, period=period,
        year=F.year(sale_date), month=F.month(sale_date),
        trade_channel_key=F.upper(clean_text("trade_channel_description")),
        package_name=normalize_package_name(),
    )
    errors = []
    for part in ("year", "month"):
        received = F.expr(f"try_cast(trim({part}) AS int)")
        matches = clean_text(part).rlike(r"^[0-9]+$") & (
            received == values[part]
        )
        errors.append(F.when(
            ~F.coalesce(matches, F.lit(False)),
            F.lit(f"source_{part}_matches_date"),
        ))
    return build_records(frame, values, errors)


def summarize_sales_amounts(frame):
    """Count negative and zero amounts without storing derived flags.

    Args:
        frame: Typed sales DataFrame for the records being inspected.

    Returns:
        dict: Negative and zero record counts, including zero for empty inputs.
    """

    row = frame.agg(
        F.count(F.when(F.col("sales_amount") < 0, 1)).alias("negative_sales_amount"),
        F.count(F.when(F.col("sales_amount") == 0, 1)).alias("zero_sales_amount"),
    ).first()
    return row.asDict()



def validate_products(frame):
    """Reject conflicting brand or package attributes for a product key."""
    validate_key_attributes(
        frame,
        keys=["brand_flavor_code", "package_category_code", "package_name"],
        attributes=["brand_name", "package_category_description"],
        rule_name="consistent_product_attributes",
    )


def check_sales_channels(frame, channels, unique_keys=False):
    """Flag sales without a channel classification, preserving row counts.

    Args:
        frame: Typed sales DataFrame, batch or streaming.
        channels: Validated static channel DataFrame.
        unique_keys: True only for the registry maintained by the key validator.

    Returns:
        DataFrame: Sales with a blocking error for unmatched channel keys.
    """

    keys = channels.select("trade_channel_key")
    if not unique_keys:
        keys = keys.distinct()
    keys = keys.withColumn("_channel_found", F.lit(True))
    return frame.join(keys, "trade_channel_key", "left").withColumn(
        "_quality_errors",
        F.when(
            F.col("_channel_found").isNull(),
            F.concat(
                F.col("_quality_errors"),
                F.array(F.lit("channel_classification_exists")),
            ),
        ).otherwise(F.col("_quality_errors")),
    ).drop("_channel_found")
