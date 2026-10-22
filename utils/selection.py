"""Select explicit table names and order selected dependencies without side effects."""

from utils.validation import validate_identifier


def select_tables(value, available):
    """Accept one name, comma-separated names, a list, or an explicit all selection.

    Empty string retains the loader API's all-tables default. Widgets reject
    empty input separately. Unknown names, empty items and mixed All selections
    fail before any data processing.
    """
    available = list(available)
    if isinstance(value, str):
        if value.strip() in ("", "All tables"):
            return available
        names = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        names = list(value)
    else:
        raise ValueError("Table selection must be a string or a list of names.")
    if not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Select at least one table; empty items are not allowed.")
    for name in names:
        validate_identifier(name, "selected table")
    unknown = set(names) - set(available)
    if unknown:
        raise ValueError(f"Unknown tables: {sorted(unknown)}")
    return list(dict.fromkeys(names))


def order_tables(selected, dependencies):
    """Order selected tables, validating the graph without adding dependencies."""
    ordered, visiting, visited = [], set(), set()

    def visit(name):
        if name not in dependencies:
            raise ValueError(f"Unknown table dependency: {name}")
        if name in visiting:
            raise ValueError(f"Cyclic table dependency: {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in dependencies[name]:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        if name in selected:
            ordered.append(name)

    for name in selected:
        visit(name)
    return ordered
