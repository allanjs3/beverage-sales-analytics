
from pyspark.sql import functions as F
"""Shared quality checks for explicit Silver transformations."""


BRONZE_METADATA_TYPES = {
    "_rescued_data": "string",
    "_source_file_path": "string",
    "_source_file_name": "string",
    "_source_file_size": "bigint",
    "_source_file_modified_at": "timestamp",
    "_ingested_at": "timestamp",
    "_ingestion_run_id": "string",
}

METADATA_DESCRIPTIONS = {
    "_source_file_path": "Full path of the file from which the record originated.",
    "_silver_run_id": "Identifier of the Silver execution that processed the record.",
    "_processed_at": "Timestamp when the record was processed into Silver.",
}


def clean_text(name):
    """Trim the borders of a text column and represent blank values as null."""

    value = F.trim(F.col(name))
    return F.when(F.length(value) > 0, value)


def build_records(frame, values, errors=()):
    """Project required business fields, lineage and row quality observations.

    Args:
        frame: Original Bronze DataFrame, batch or streaming.
        values: Mapping of output names to explicit Spark expressions.
        errors: Additional blocking business error expressions.

    Returns:
        DataFrame: All input occurrences, source path and transient errors.
    """

    checks = list(errors)
    checks.extend(
        F.when(value.isNull(), F.lit(f"required_or_invalid_{name}"))
        for name, value in values.items()
    )
    checks.append(F.when(
        F.col("_rescued_data").isNotNull(), F.lit("rescued_source_data")
    ))
    for name, datatype in BRONZE_METADATA_TYPES.items():
        if name == "_rescued_data":
            continue
        missing = F.col(name).isNull()
        if datatype == "string":
            missing = missing | (F.length(F.trim(F.col(name))) == 0)
        checks.append(F.when(missing, F.lit(f"missing{name}")))
    return frame.select(
        *[value.alias(name) for name, value in values.items()],
        F.col("_source_file_path"),
        F.filter(
            F.array(*checks).cast("array<string>"), lambda item: item.isNotNull()
        ).alias("_quality_errors"),
    )


def validate_key_attributes(frame, keys, attributes, rule_name):
    """Reject business keys with conflicting attribute combinations.

    Args:
        frame: Complete typed source snapshot with row quality results.
        keys: Columns identifying the business entity.
        attributes: Columns that must be consistent for each key.
        rule_name: Business rule included in a failure message.

    Raises:
        ValueError: One valid key has multiple attribute combinations.

    Identical repeated records are accepted and remain in the output.
    """

    conflicts = frame.filter(F.size("_quality_errors") == 0).groupBy(*keys).agg(
        F.countDistinct(F.struct(*attributes)).alias("attribute_variants")
    ).filter(F.col("attribute_variants") > 1)
    sample = conflicts.limit(5).collect()
    if sample:
        raise ValueError(f"{rule_name} failed; sample={sample}")


def enforce_quality(frame, run_id):
    """Fail a write containing invalid rows and retain minimal lineage.

    Blocking failures leave the original values in Bronze. Delta commits no
    invalid batch; earlier successful microbatches remain committed. The native
    checkpoint coordinates a retry without duplicating those committed batches.
    """

    return frame.withColumn(
        "_silver_run_id",
        F.when(
            F.size("_quality_errors") > 0,
            F.raise_error(F.concat(
                F.lit("Silver quality checks failed: "),
                F.concat_ws(", ", F.col("_quality_errors")),
                F.lit("; source file="), F.coalesce(F.col("_source_file_path"), F.lit("unknown")),
            )),
        ).otherwise(F.lit(run_id)),
    ).withColumn("_processed_at", F.current_timestamp()).drop("_quality_errors")
