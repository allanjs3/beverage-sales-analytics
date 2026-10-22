"""Generate a daily calendar covering complete observed calendar years."""

from pyspark.sql import functions as F


def transform(frame):
    """Expand observed years into days, including dates without sales."""
    bounds = frame.agg(
        F.min("sale_date").alias("first_date"), F.max("sale_date").alias("last_date")
    )
    dates = bounds.select(F.explode(F.sequence(
        F.make_date(F.year("first_date"), F.lit(1), F.lit(1)),
        F.make_date(F.year("last_date"), F.lit(12), F.lit(31)),
    )).alias("calendar_date"))
    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date", F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
    )
