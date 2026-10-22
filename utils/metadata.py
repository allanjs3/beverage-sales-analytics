"""Synchronize configured table and column descriptions with catalog metadata."""

from utils.validation import validate_identifier


def validate_description(value, context):
    """Require a non-empty description before any catalog changes.

    Args:
        value: Description supplied by the configuration.
        context: Object label included in validation errors.

    Raises:
        ValueError: The description is missing, blank or contains a null byte.
    """
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{context} requires a non-empty description without null bytes.")


def _comment_literal(value):
    """Encode a SQL string using Spark's default backslash escape rules."""
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


def apply_table_descriptions(spark, table_name, description, columns):
    """Update only table and column comments that differ from configuration.

    Args:
        spark: Active Spark session with catalog metadata access.
        table_name: Fully qualified existing table name.
        description: Desired table comment.
        columns: Mapping of column names to their desired comments.

    Returns:
        dict: Whether the table comment changed and names of changed columns.

    Raises:
        ValueError: An identifier or description is invalid, or a column is absent.

    Reads catalog metadata without scanning records. Run after the table's
    stream terminates. DDL errors propagate; a retry compares metadata again
    and applies only remaining changes. Unconfigured columns are left intact.
    """
    validate_identifier(table_name, "metadata table", parts=3)
    validate_description(description, table_name)
    for name, comment in columns.items():
        validate_identifier(name, "metadata column")
        validate_description(comment, name)

    current_description = spark.catalog.getTable(table_name).description
    current_columns = {
        field.name: field.metadata.get("comment")
        for field in spark.table(table_name).schema.fields
    }
    missing = set(columns) - set(current_columns)
    if missing:
        raise ValueError(f"Columns absent from {table_name}: {sorted(missing)}")
    changed_columns = [
        name for name, comment in columns.items()
        if current_columns[name] != comment
    ]
    table_changed = current_description != description
    if table_changed or changed_columns:
        quoted_table = ".".join(f"`{part}`" for part in table_name.split("."))
        if table_changed:
            spark.sql(f"COMMENT ON TABLE {quoted_table} IS {_comment_literal(description)}")
        if changed_columns:
            changes = ", ".join(
                f"`{name}` COMMENT {_comment_literal(columns[name])}"
                for name in changed_columns
            )
            spark.sql(f"ALTER TABLE {quoted_table} ALTER COLUMN {changes}")
    return {"table_comment_updated": table_changed, "columns_updated": changed_columns}


def create_described_table(spark, table_name, types, description, comments, properties):
    """Create a Delta table with all descriptions and properties in one statement."""
    validate_identifier(table_name, "metadata table", parts=3)
    validate_description(description, table_name)
    definitions = []
    for name, data_type in types.items():
        validate_identifier(name, "metadata column")
        validate_description(comments[name], name)
        definitions.append(f"`{name}` {data_type} COMMENT {_comment_literal(comments[name])}")
    settings = ", ".join(
        f"{_comment_literal(key)} = {_comment_literal(value)}"
        for key, value in properties.items()
    )
    spark.sql(
        f"CREATE TABLE {table_name} ({', '.join(definitions)}) USING DELTA "
        f"COMMENT {_comment_literal(description)} TBLPROPERTIES ({settings})"
    )
