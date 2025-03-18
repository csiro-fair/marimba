"""
Marimba Generic Metadata Implementation.

This module provides a simple, dictionary-based implementation of the BaseMetadata interface for storing and managing
metadata about files. It handles basic metadata attributes like datetime, geolocation, licensing, and file hashes
without the complexity of specialized metadata schemas.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Union, cast

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.metadata import json_saver


class GenericMetadata(BaseMetadata):
    """
    A simple, dictionary-based metadata implementation.

    This class provides a straightforward way to store and manage metadata without the
    complexity of specialized metadata schemas. It implements the BaseMetadata interface
    using a dictionary to store values, with properties for access and validation.

    Attributes:
        DEFAULT_METADATA_NAME (str): Default filename for metadata files.
    """

    DEFAULT_METADATA_NAME = "metadata"

    def __init__(
        self,
        datetime_: datetime | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        altitude: float | None = None,
        context: str | None = None,
        license_: str | None = None,
        creators: list[str] | None = None,
        hash_sha256_: bytes | str | None = None,
    ) -> None:
        """Initialize a new GenericMetadata instance."""
        # Handle string hash conversion more safely
        processed_hash: bytes | None = None
        if hash_sha256_ is not None:
            if isinstance(hash_sha256_, str):
                try:
                    processed_hash = bytes.fromhex(hash_sha256_)
                except ValueError:
                    processed_hash = hash_sha256_.encode("utf-8")
            else:
                processed_hash = hash_sha256_

        self._data = {
            "datetime": datetime_,
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude,
            "context": context,
            "license": license_,
            "creators": creators or [],
            "hash_sha256": processed_hash,
        }

    def strftime(self, format_string: str) -> str:
        """Format the datetime using strftime."""
        if self.datetime is None:
            raise ValueError("Cannot format datetime: datetime is None")
        return self.datetime.strftime(format_string)

    def isoformat(self) -> str:
        """Format the datetime in ISO format."""
        if self.datetime is None:
            raise ValueError("Cannot format datetime: datetime is None")
        return self.datetime.isoformat()

    def __lt__(self, other: Union["GenericMetadata", datetime]) -> bool:
        if isinstance(other, GenericMetadata | datetime):
            if self.datetime is None:
                return True
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            if other_dt is None:
                return False
            return self.datetime < other_dt
        return NotImplemented

    def __gt__(self, other: Union["GenericMetadata", datetime]) -> bool:
        if isinstance(other, GenericMetadata | datetime):
            if self.datetime is None:
                return False
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            if other_dt is None:
                return True
            return self.datetime > other_dt
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GenericMetadata | datetime):
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            return self.datetime == other_dt
        return NotImplemented

    def __le__(self, other: Union["GenericMetadata", datetime]) -> bool:
        return self < other or self == other

    def __ge__(self, other: Union["GenericMetadata", datetime]) -> bool:
        return self > other or self == other

    def __hash__(self) -> int:
        """Enable use in sets and as dictionary keys."""
        return hash((self.datetime,))

    @property
    def datetime(self) -> datetime | None:
        """When the data was captured/created."""
        value = self._data.get("datetime")
        return cast(datetime | None, value)

    @property
    def latitude(self) -> float | None:
        """Geographic latitude in decimal degrees."""
        value = self._data.get("latitude")
        return cast(float | None, value)

    @property
    def longitude(self) -> float | None:
        """Geographic longitude in decimal degrees."""
        value = self._data.get("longitude")
        return cast(float | None, value)

    @property
    def altitude(self) -> float | None:
        """Altitude in meters."""
        value = self._data.get("altitude")
        return cast(float | None, value)

    @property
    def context(self) -> str | None:
        """Contextual information about the data."""
        value = self._data.get("context")
        return cast(str | None, value)

    @property
    def license(self) -> str | None:
        """License information."""
        value = self._data.get("license")
        return cast(str | None, value)

    @property
    def creators(self) -> list[str]:
        """List of creator names."""
        value = self._data.get("creators", [])
        return cast(list[str], value)

    @property
    def hash_sha256(self) -> str | None:
        """SHA256 hash of the associated file as a hexadecimal string."""
        value = self._data.get("hash_sha256")
        return cast(str | None, value)

    @hash_sha256.setter
    def hash_sha256(self, value: str | None) -> None:
        """Set the SHA256 hash of the associated file."""
        self._data["hash_sha256"] = value

    def format_hash(self) -> str | None:
        """Format the hash value as a hexadecimal string."""
        if self.hash_sha256 is None:
            return None
        return self.hash_sha256

    @classmethod
    def create_dataset_metadata(
        cls,
        dataset_name: str,
        root_dir: Path,
        items: dict[str, list["BaseMetadata"]],
        metadata_name: str | None = None,
        *,
        dry_run: bool = False,
        saver_overwrite: Callable[[Path, str, dict[str, Any]], None] | None = None,
    ) -> None:
        """Create dataset-level metadata by combining all items into a JSON file."""
        saver = json_saver if saver_overwrite is None else saver_overwrite

        dataset_metadata = {
            "dataset_name": dataset_name,
            "items": {
                path: [
                    {
                        "datetime": item.datetime.isoformat() if item.datetime else None,
                        "latitude": item.latitude,
                        "longitude": item.longitude,
                        "altitude": item.altitude,
                        "context": item.context,
                        "license": item.license,
                        "creators": item.creators,
                        "hash_sha256": item.format_hash() if hasattr(item, "format_hash") else None,
                    }
                    for item in metadata_items
                ]
                for path, metadata_items in items.items()
            },
        }

        output_name = metadata_name or cls.DEFAULT_METADATA_NAME
        if not dry_run:
            saver(root_dir, output_name, dataset_metadata)

    @classmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list["BaseMetadata"], dict[str, Any] | None]],
        max_workers: int | None = None,
        logger: logging.Logger | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """Process files according to the metadata type's requirements."""
