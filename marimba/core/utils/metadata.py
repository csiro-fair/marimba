"""
Metadata saver.

This module provides saver types and function to save metadata to different file types.

Classes:
    MetadataSaverTypes: Enum for available file types to save the metadata into.

Functions:
    json_saver: Saves the metadata as a JSON file.
    yaml_saver: Saves the metadata as a YAML file.
    get_saver: Returns a save function based on the given saver type.
"""

import json
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any

import yaml


class MetadataSaverTypes(str, Enum):
    """
    Types available as metadata output types.
    """

    json = "json"
    yaml = "yaml"


def json_saver(path: Path, file_name: str, data: dict[str, Any]) -> None:
    """
    Saves data as a JSON file.

    Args:
        path: Path to the dictionary where the data is to be saved.
        file_name: File name of the JSON file without the extension.
        data: JSON serializable dictionary.

    Returns:
        None
    """
    with Path.open(path / f"{file_name}.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def yaml_saver(path: Path, file_name: str, data: dict[str, Any]) -> None:
    """
    Saves data as a YAML file.

    Args:
        path: Path to the dictionary where the data is to be saved.
        file_name: File name of the YAML file without the extension.
        data: YAML serializable dictionary.

    Returns:
        None
    """
    with Path.open(path / f"{file_name}.yml", "w", encoding="utf-8") as file:
        yaml.safe_dump(data, file)


def get_saver(saver_name: MetadataSaverTypes) -> Callable[[Path, str, dict[str, Any]], None]:
    """
    Returns a save function based on the given saver type name.

    Args:
        saver_name: Saver type name.

    Returns:
        Save function which takes a path, filename without extension and data as json serializable dict.
    """
    match saver_name:
        case MetadataSaverTypes.json:
            return json_saver
        case MetadataSaverTypes.yaml:
            return yaml_saver
        case _:
            raise ValueError(f"Unknown saver: {saver_name}")
