"""Load Silver table locations and catalog descriptions."""

from pathlib import Path

from transformations.silver.tables import TABLES
from utils.config import load_config, list_config_paths
from utils.metadata import validate_description
from utils.selection import order_tables, select_tables
from utils.validation import require_keys, require_unique, validate_identifier


TABLE_COLUMNS = {name: rule["columns"] for name, rule in TABLES.items()}


def validate_source(source, filename):
    """Validate table identity, checkpoint isolation and catalog descriptions.

    Args:
        source: Parsed table configuration.
        filename: Configuration filename without extension.

    Raises:
        ValueError: A location, table selection or description is invalid.
    """
    keys = {
        "schema_version", "name", "source_table", "target", "state_volume",
        "description", "columns", "validation_table",
    }
    require_keys(source, keys, filename)
    if set(source) - keys - {"checkpoint", "quality", "validation_description"}:
        raise ValueError("Unsupported Silver settings; business rules belong in Python.")
    if source["schema_version"] != 1:
        raise ValueError("Unsupported Silver configuration version.")
    if filename not in TABLE_COLUMNS or source["name"] != filename:
        raise ValueError(f"No Silver transformation registered for: {filename}")
    for key in ("source_table", "target", "state_volume", "validation_table"):
        validate_identifier(source[key], key, parts=3)
    if filename != source["target"].split(".")[-1]:
        raise ValueError("Silver filename, name and target table must match.")
    if source["source_table"].lower() == source["target"].lower():
        raise ValueError("Source and target must be different tables.")
    if source["validation_table"].lower() in {
        source["source_table"].lower(), source["target"].lower(),
    }:
        raise ValueError("Validation registry must be separate from data tables.")
    if (source["state_volume"].lower().split(".")[:2]
            != source["target"].lower().split(".")[:2]):
        raise ValueError("The state volume must belong to the target catalog and schema.")
    validate_description(source["description"], filename)
    if "validation_description" in source:
        validate_description(source["validation_description"], source["validation_table"])
    columns = source["columns"]
    if not isinstance(columns, list) or not columns:
        raise ValueError("A non-empty columns list is required.")
    for column in columns:
        require_keys(column, {"target", "description"}, "column")
        if set(column) != {"target", "description"}:
            raise ValueError("Column settings contain only target and description.")
        validate_identifier(column["target"], "column")
        validate_description(column["description"], column["target"])
    require_unique((c["target"] for c in columns), "Silver output columns")
    if {c["target"] for c in columns} != set(TABLE_COLUMNS[filename]):
        raise ValueError("Descriptions must match the transformation output columns.")


def load_silver_config(directory, table_name=""):
    """Load selected tables and the approved channel reference used by sales.

    Args:
        directory: Directory containing one JSON per Silver table.
        table_name: One name, comma-separated names or a list. Empty string
            and All tables select every configured target.

    Returns:
        dict: Selected sources, input configurations and shared state volume.

    Selecting sales also loads the channel reference location. It does not
    process new channel input or write the channel Silver table.
    """
    directory = Path(directory)
    names = select_tables(table_name, [path.stem for path in list_config_paths(directory)])
    if any(name not in TABLE_COLUMNS for name in names):
        raise ValueError("Selected table has no registered Silver transformation.")
    dependencies = {name: rule["depends_on"] for name, rule in TABLES.items()}
    names = order_tables(names, dependencies)
    needed = set(names)
    for name in names:
        needed.update(dependencies[name])
    contracts = {}
    for name in sorted(needed):
        source = load_config(directory / f"{name}.json")
        validate_source(source, name)
        source["checkpoint"] = (
            "/Volumes/" + source["state_volume"].replace(".", "/")
            + f"/{name}/checkpoint"
        )
        contracts[name] = source
    volumes = {source["state_volume"] for source in contracts.values()}
    if len(volumes) != 1:
        raise ValueError("Silver tables must share one state volume.")
    targets = {source["target"].lower() for source in contracts.values()}
    registries = [source["validation_table"].lower() for source in contracts.values()]
    require_unique(registries, "Validation tables")
    data_tables = targets | {source["source_table"].lower() for source in contracts.values()}
    if set(registries) & data_tables:
        raise ValueError("Validation registries cannot overlap data tables.")
    if any(s["source_table"].lower() in targets for s in contracts.values()):
        raise ValueError("Silver inputs must not be targets of this processing stage.")
    return {
        "sources": [contracts[name] for name in names],
        "contracts": contracts,
        "state_volume": next(iter(volumes)),
    }
