"""
Marimba Configuration Utilities.

This module provides functions for loading and saving YAML configuration files. It includes utilities for handling
file paths and converting YAML data to Python dictionaries.

"""

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
