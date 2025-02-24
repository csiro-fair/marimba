import json
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import yaml

class MetadataSaverTypes(str, Enum):
    json = "json"
    yaml = "yaml"


def json_saver(path: Path, file_name: str, data: dict[str, Any]) -> None:
    with open(path / f"{file_name}.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def yaml_saver(path: Path, file_name: str, data: dict[str, Any]) -> None:
    with open(path / f"{file_name}.yaml", "w", encoding="utf-8") as file:
        yaml.safe_dump(data, file)


def get_saver(saver_name: MetadataSaverTypes) -> Callable:
    match saver_name:
        case MetadataSaverTypes.json:
            return json_saver
        case MetadataSaverTypes.yaml:
            return yaml_saver
        case _:
            raise ValueError(f"Unknown saver: {saver_name}")