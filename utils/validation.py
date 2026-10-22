"""Small validation primitives shared by processing modules."""

import re


def require_keys(value, required, context):
    """Require named keys in a dictionary without changing its content.

    Args:
        value: Object to validate.
        required: Iterable of mandatory key names.
        context: Label included in validation errors.

    Raises:
        ValueError: The object is not a dictionary or mandatory keys are absent.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object.")
    missing = set(required) - value.keys()
    if missing:
        raise ValueError(f"Missing keys in {context}: {sorted(missing)}")


def require_unique(values, context):
    """Reject duplicate values in an iterable.

    Args:
        values: Iterable of hashable values, consumed during validation.
        context: Label included in validation errors.

    Raises:
        ValueError: At least one value is repeated.
    """
    values = list(values)
    if len(values) != len(set(values)):
        raise ValueError(f"Duplicate values in {context}.")


def validate_identifier(value, context, parts=1):
    """Validate a simple identifier or a qualified Unity Catalog name.

    Args:
        value: Name whose components contain letters, digits or underscores.
        context: Label included in validation errors.
        parts: Required number of dot-separated components; defaults to one.

    Raises:
        ValueError: The name has an invalid type, component count or character.

    Components cannot start with a digit. Restricting this syntax allows callers
    to interpolate validated identifiers into SQL statements.
    """
    if not isinstance(value, str):
        raise ValueError(f"{context} must be a string.")
    tokens = value.split(".")
    if len(tokens) != parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token) for token in tokens):
        raise ValueError(f"Invalid {context}: {value!r}")
