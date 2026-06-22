"""
iFDO completeness checking.

ifdo-py's Pydantic models already enforce iFDO field types and structure when the iFDO is constructed, so this
module deliberately does not re-validate those. Its sole job is the one signal the Pydantic layer does not
provide: which fields the official iFDO JSON schema marks as required are left unpopulated for a packaged
dataset - a FAIR-R1 completeness signal.

A field counts as populated if it is set in the image-set header (a set-wide default) or on every item record,
matching iFDO semantics; the result is therefore independent of how Marimba deduplicates fields between the
header and the items. It never raises; callers decide how to report the result.
"""

import functools
import json
from pathlib import Path
from typing import Any

import marimba

_SCHEMA_PATH = Path(marimba.__path__[0]) / "resources" / "schemas" / "ifdo" / "ifdo-v2.2.1.json"


class iFDOValidator:  # noqa: N801
    """Reports which schema-required iFDO fields a document leaves unpopulated."""

    def __init__(self, required_fields: list[str]) -> None:
        """Store the official iFDO schema's list of required fields."""
        self._required_fields = required_fields

    @classmethod
    def create(cls) -> "iFDOValidator":
        """Build a validator from the bundled official iFDO schema's required-field list."""
        with _SCHEMA_PATH.open(encoding="utf-8") as file:
            schema = json.load(file)
        required_fields = list(schema.get("properties", {}).get("image-set-header", {}).get("required", []))
        return cls(required_fields)

    @staticmethod
    def _is_populated(field: str, ifdo_dict: dict[str, Any]) -> bool:
        """Return whether a field is set for every image, via the header default or each item record."""
        header = ifdo_dict.get("image-set-header")
        if isinstance(header, dict) and field in header:
            return True
        items = ifdo_dict.get("image-set-items")
        if not isinstance(items, dict):
            return False
        records = [record for item in items.values() for record in (item if isinstance(item, list) else [item])]
        return bool(records) and all(isinstance(record, dict) and field in record for record in records)

    def unpopulated_required_fields(self, ifdo_dict: dict[str, Any]) -> list[str]:
        """
        Return the schema-required fields that are not populated for every image.

        Args:
            ifdo_dict: The iFDO as the dict that will be written (alias keys, ``None`` fields excluded).

        Returns:
            The schema-required fields absent from both the header and at least one item record. Header defaults
            and per-item values both count, so the result reflects genuine completeness rather than whether a
            field happens to live in the header or the items.
        """
        return [field for field in self._required_fields if not self._is_populated(field, ifdo_dict)]


@functools.lru_cache(maxsize=1)
def get_ifdo_validator() -> iFDOValidator:
    """Return a process-wide cached iFDO validator (the schema is read once)."""
    return iFDOValidator.create()
