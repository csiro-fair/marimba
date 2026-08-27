"""
Marimba Configuration Utilities.

This module provides functions for loading and saving YAML configuration files. It includes utilities for handling
file paths and converting YAML data to Python dictionaries.

"""

import json
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    """
    Load a YAML config file.

    Args:
        config_path: The path to the config file.

    Returns:
        The config data as a dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.scanner.ScannerError: If the config file is not valid YAML.
        TypeError: If the configuration data is not a dictionary.
    """
    config_path = Path(config_path)

    with Path.open(config_path, encoding="utf-8") as file:
        data = yaml.safe_load(file)

        if not isinstance(data, dict):
            msg = "Configuration data must be a dictionary"
            raise TypeError(msg)

    return data


def save_config(config_path: str | Path, config_data: dict[Any, Any]) -> None:
    """
    Save a YAML config file.

    Args:
        config_path: The path to the config file.
        config_data: The config data as a dictionary.
    """
    config_path = Path(config_path)

    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config_data, f)


def _is_existing_config_file(config: str) -> bool:
    """Return True if ``config`` names an existing file.

    ``Path.is_file()`` raises ``OSError`` (e.g. ``ENAMETOOLONG``) for long inline
    JSON on some Python/platform combinations instead of returning False.
    """
    try:
        return Path(config).is_file()
    except OSError:
        return False


def _parse_inline_json_object(config: str) -> dict[str, Any]:
    """Parse an inline JSON object string."""
    try:
        data = json.loads(config)
    except json.JSONDecodeError as exc:
        msg = (
            "Could not parse --config as JSON, and it is not an existing file. "
            "Pass a YAML/JSON file path or a JSON object string."
        )
        raise ValueError(msg) from exc
    if not isinstance(data, dict):
        msg = "Configuration data must be a dictionary"
        raise TypeError(msg)
    return data


def parse_cli_config(config: str | None) -> dict[str, Any]:
    """
    Parse a ``--config`` value as a YAML/JSON file path, or an inline JSON object.

    Existing files are loaded with ``yaml.safe_load`` (JSON is valid YAML). Inline
    strings still use ``json.loads``. Python is not executed.

    Args:
        config: A filesystem path, an inline JSON object string, or None/empty.

    Returns:
        The configuration dictionary. Empty input yields ``{}``.

    Raises:
        ValueError: If the value is not an existing file and not valid JSON, or if a
            file exists but is not valid YAML/JSON.
        TypeError: If parsed data is not a dictionary.
    """
    if not config:
        return {}
    # Inline JSON objects/arrays must not be passed to Path.is_file(): a typical
    # nested object is longer than NAME_MAX (255), and that raises OSError on
    # Python 3.12/3.13 instead of returning False.
    stripped = config.lstrip()
    if stripped.startswith(("{", "[")):
        return _parse_inline_json_object(config)
    path = Path(config)
    if _is_existing_config_file(config):
        try:
            return load_config(path)
        except yaml.YAMLError as exc:
            msg = f"Invalid YAML/JSON in config file {path}: {exc}"
            raise ValueError(msg) from exc
    return _parse_inline_json_object(config)
