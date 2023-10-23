"""
MarImBA configuration utilities.
"""
from pathlib import Path
from typing import Union

import yaml


def load_config(config_path: Union[str, Path]) -> dict:
    """
    Load a YAML config file.

    Args:
        config_path: The path to the config file.

    Returns:
        The config data as a dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.parser.ParserError: If the config file is not valid YAML.
    """
    config_path = Path(config_path)

    with config_path.open() as f:
        config_data = yaml.safe_load(f)

    return config_data


def save_config(config_path: Union[str, Path], config_data: dict):
    """
    Save a YAML config file.

    Args:
        config_path: The path to the config file.
        config_data: The config data as a dictionary.
    """
    config_path = Path(config_path)

    with config_path.open("w") as f:
        yaml.safe_dump(config_data, f)
