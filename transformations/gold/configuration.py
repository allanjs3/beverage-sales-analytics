"""Load table contracts and order only explicitly selected Gold outputs."""

from importlib import import_module

from utils.config import load_configs
from utils.metadata import validate_description
from utils.selection import order_tables, select_tables
from utils.validation import validate_identifier


def table_module(name):
    """Locate a table transformation by its validated configuration filename."""
    validate_identifier(name, "Gold table")
    return import_module(f"transformations.gold.tables.{name}")


def load_gold_config(directory, selection=""):
    """Validate contracts without implicitly selecting upstream tables."""
    tables = load_configs(directory)
    for name, table in tables.items():
        validate_identifier(name, "Gold table")
        if table["name"] != name:
            raise ValueError(f"Configuration name differs from filename: {name}")
        if table["kind"] not in {"dimension", "fact", "summary"}:
            raise ValueError(f"Unknown Gold table kind: {name}")
        validate_identifier(table["target"], "Gold target", parts=3)
        validate_description(table["description"], name)
        columns = table["columns"]
        if len({c["name"] for c in columns}) != len(columns):
            raise ValueError(f"Repeated columns: {name}")
        for column in columns:
            validate_identifier(column["name"], "Gold column")
            validate_description(column["description"], column["name"])
            if column["type"] not in {"string", "int", "date", "decimal(28,2)", "decimal(38,2)"}:
                raise ValueError(f"Unsupported type in {name}: {column['type']}")
        if not callable(table_module(name).transform):
            raise ValueError(f"Missing transformation: {name}")
        if table["kind"] == "summary":
            source = table["source_name"]
            if source not in table["depends_on"] or source not in tables or tables[source]["kind"] != "fact":
                raise ValueError(f"Summary must depend on its fact: {name}")
        else:
            for key in ("source_table", "state_volume"):
                validate_identifier(table[key], key, parts=3)
            table["checkpoint"] = (
                "/Volumes/" + table["state_volume"].replace(".", "/") + f"/{name}/checkpoint"
            )
        if table["kind"] == "dimension" and table["key"] not in {c["name"] for c in columns}:
            raise ValueError(f"Dimension key is missing: {name}")
    targets = [table["target"] for table in tables.values()]
    if len(targets) != len(set(targets)):
        raise ValueError("Gold targets must be distinct.")
    dependencies = {name: table["depends_on"] for name, table in tables.items()}
    order_tables(list(tables), dependencies)
    selected = order_tables(select_tables(selection, tables), dependencies)
    return {"tables": tables, "selected": selected}
