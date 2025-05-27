"""
iFDO validator module.

This module provides an class for validating iFDOs based on the iFDO json schema.
"""

import json
from pathlib import Path
from typing import Any

import jsonschema.exceptions
import jsonschema.protocols
from ifdo import iFDO
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from typing_extensions import Self


class iFDOValidator:  # noqa: N801
    """iFDO validator based on the jsonschema validator."""

    def __init__(self, validator: jsonschema.protocols.Validator) -> None:
        """
        Initializes a new ifdo validator instance.

        Args:
            validator: JSON schema validator
        """
        self._validator = validator

    @classmethod
    def create(cls) -> Self:
        """
        Creates a new ifdo schema validator.
        """
        resources_path = Path("resources") / "schemas" / "ifdo"
        schema = cls._load_json_file(resources_path / "ifdo-v2.1.0.json")
        registry = Registry().with_resources(
            [
                (
                    "https://marine-imaging.com/fair/schemas/provenance.json",
                    Resource.from_contents(
                        cls._load_json_file(resources_path / "provenance-v0.1.0.json"),
                    ),
                ),
                (
                    "https://marine-imaging.com/fair/schemas/annotation.json",
                    Resource.from_contents(
                        cls._load_json_file(resources_path / "annotation-v2.0.0.json"),
                    ),
                ),
            ],
        )
        return cls(Draft202012Validator(schema, registry=registry))

    @staticmethod
    def _load_json_file(filepath: Path) -> dict[Any, Any]:
        with filepath.open("r") as file:
            data = json.load(file)
            if not isinstance(data, dict):
                raise TypeError("Invalid schema!")
            return data

    def __call__(self, ifdo: iFDO) -> bool:
        """
        Validates an ifdo based on the ifdo json schema.

        Args:
            ifdo: iFDO to validate.

        Returns:
            Wether the ifdo is valid.
        """
        try:
            self._validator.validate(ifdo.to_dict())
        except jsonschema.exceptions.ValidationError:
            return False
        return True
