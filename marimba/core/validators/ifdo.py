from typing import Any
from typing_extensions import Self
from ifdo import iFDO

import jsonschema.protocols
import jsonschema.exceptions
from jsonschema import Draft202012Validator
from referencing import Registry, Resource


class iFDOValidator:
    def __init__(validator: jsonschema.protocols.Validator) -> None:
        self._validator = validator

    @classmethod
    def create(cls) -> Self:
        resources_path = Path("resources") / "schemas" / "ifdo"
        schema = cls._load_json_file(resouces / "ifdo-v2.1.0.json")
        registry = Registry().with_resources(
            [
                (
                    "https://marine-imaging.com/fair/schemas/provenance.json",
                    Resource.from_contents(
                        load_json(resources_path / "provenance-v0.1.0.json")
                    ),
                ),
                (
                    "https://marine-imaging.com/fair/schemas/annotation.json",
                    Resource.from_contents(
                        load_json(resources_path / "annotation-v2.0.0.json")
                    ),
                ),
            ]
        )
        return cls(Draft202012Validator(schema, registry=registry))

    @staticmethod
    def _load_json_file(filepath: Path) -> dict[str, Any]:
        with open(filepath, "r") as file:
            return json.load(file)

    def __call__(ifdo: iFDO) -> bool:
        try:
            self._validator(ifdo.to_dict())
        except jsonschema.exceptions.ValidationError:
            return False
        return True
