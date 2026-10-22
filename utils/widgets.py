"""Prepare table multi-table choices independently of Databricks widget rendering."""

from utils.config import list_config_paths
from utils.selection import select_tables
from utils.validation import validate_identifier

ALL_TABLES = "All tables"
SELECT_TABLE = "Select a table"


def build_table_dropdown(directory, previous_value=None, reset=False):
    """Build table choices from configuration filenames.

    Args:
        directory: Directory containing one JSON file per target table.
        previous_value: Previous widget value, if one exists.
        reset: Whether a project change requires a fresh selection.

    Returns:
        dict: Ordered choices and default_value for a multiselect widget.

    Raises:
        ValueError: No configurations exist or a filename is not a valid
            unqualified table identifier.

    No JSON payload or table data is read. A removed or invalid previous choice
    becomes the selection prompt, never an implicit request for every table.
    """
    tables = [path.stem for path in list_config_paths(directory)]
    for table in tables:
        validate_identifier(table, "configuration filename")
    choices = [SELECT_TABLE, ALL_TABLES, *tables]
    default = SELECT_TABLE
    if not reset and previous_value:
        try:
            default = resolve_table_selection(previous_value, choices) or ALL_TABLES
        except ValueError:
            pass
    return {"choices": choices, "default_value": default}


def resolve_table_selection(value, choices, label="Bronze Tables"):
    """Translate a selected widget values into the ingestion table parameter.

    Args:
        value: Current value of the table selector.
        choices: Choices discovered for the current project.
        label: Widget label included in selection errors.

    Returns:
        str: Comma-separated table names, or an empty string for an explicit all-tables
        selection.

    Raises:
        ValueError: Selection is missing, stale or not present in the list.
    """
    if not isinstance(value, str) or not value.strip() or value == SELECT_TABLE:
        raise ValueError(f"Select a table in {label} before starting processing.")
    names = select_tables(value, [n for n in choices if n not in (ALL_TABLES, SELECT_TABLE)])
    return "" if value.strip() == ALL_TABLES else ",".join(names)
