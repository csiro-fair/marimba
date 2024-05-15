"""
Marimba configuration utilities.
"""
from pathlib import Path
from typing import Any, Dict, Union

import yaml


def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load a YAML config file.

    Args:
        config_path: The path to the config file.

    Returns:
        The config data as a dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.scanner.ScannerError: If the config file is not valid YAML.
    """
    config_path = Path(config_path)

    with open(config_path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

        if not isinstance(data, dict):
            raise TypeError("Configuration data must be a dictionary")

    return data


def save_config(config_path: Union[str, Path], config_data: Dict[Any, Any]) -> None:
    """
    Save a YAML config file.

    Args:
        config_path: The path to the config file.
        config_data: The config data as a dictionary.
    """
    config_path = Path(config_path)

    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config_data, f)
