"""Load declarative JSON configuration without evaluating executable content."""

from pathlib import Path
import json


def _unique_object(pairs):
    """Build a dictionary from JSON key-value pairs, rejecting repeated keys.

    Args:
        pairs: Ordered key-value pairs supplied by the JSON decoder.

    Returns:
        dict: Decoded object with unique keys.

    Raises:
        ValueError: A key appears more than once in the same JSON object.
    """
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate configuration key: {key}")
        result[key] = value
    return result


def load_config(path):
    """Load a declarative configuration from a UTF-8 JSON file.

    Args:
        path: Filesystem path to the configuration file.

    Returns:
        dict: Parsed configuration, without evaluating executable content.

    Raises:
        ValueError: JSON is invalid, has duplicate keys, or is not an object.
        OSError: The file cannot be opened or read.
    """
    with Path(path).open(encoding="utf-8") as stream:
        config = json.load(stream, object_pairs_hook=_unique_object)
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a JSON object.")
    return config


def list_config_paths(directory):
    """Discover JSON configuration paths without parsing their contents.

    Args:
        directory: Directory containing files with the .json extension.

    Returns:
        list: File paths in alphabetical order. Subdirectories are not searched.

    Raises:
        ValueError: The directory is missing or empty, filenames conflict
            ignoring case.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"Configuration directory does not exist: {directory}")
    paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    if not paths:
        raise ValueError(f"No JSON configurations found in {directory}")
    names = [path.stem.lower() for path in paths]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate configuration filenames ignoring case.")
    return paths


def load_configs(directory):
    """Load the JSON files directly inside a configuration directory.

    Args:
        directory: Directory containing files with the .json extension.

    Returns:
        dict: Parsed configurations keyed by filename without its extension,
        in alphabetical order.

    Raises:
        ValueError: Discovery fails or a configuration is invalid.
        OSError: A configuration file cannot be read.
    """
    return {path.stem: load_config(path) for path in list_config_paths(directory)}
