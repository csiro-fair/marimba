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
from typing import TYPE_CHECKING, Any, Union, cast

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.metadata import yaml_saver

if TYPE_CHECKING:
    from marimba.core.pipeline import BasePipeline


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
            msg = "Cannot format datetime: datetime is None"
            raise ValueError(msg)
        return self.datetime.strftime(format_string)

    def isoformat(self) -> str:
        """Format the datetime in ISO format."""
        if self.datetime is None:
            msg = "Cannot format datetime: datetime is None"
            raise ValueError(msg)
        return self.datetime.isoformat()

    def __lt__(self, other: Union["GenericMetadata", datetime]) -> bool:
        """Compare if this metadata is older than another metadata or datetime."""
        if isinstance(other, GenericMetadata | datetime):
            if self.datetime is None:
                return True
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            if other_dt is None:
                return False
            return self.datetime < other_dt
        return NotImplemented

    def __gt__(self, other: Union["GenericMetadata", datetime]) -> bool:
        """Compare if this metadata is newer than another metadata or datetime."""
        if isinstance(other, GenericMetadata | datetime):
            if self.datetime is None:
                return False
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            if other_dt is None:
                return True
            return self.datetime > other_dt
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        """Compare if this metadata has the same datetime as another metadata or datetime."""
        if isinstance(other, GenericMetadata | datetime):
            other_dt = other.datetime if isinstance(other, GenericMetadata) else other
            return self.datetime == other_dt
        return NotImplemented

    def __le__(self, other: Union["GenericMetadata", datetime]) -> bool:
        """Compare if this metadata is older than or equal to another metadata or datetime."""
        if isinstance(other, GenericMetadata | datetime):
            return self < other or self == other
        return NotImplemented

    def __ge__(self, other: Union["GenericMetadata", datetime]) -> bool:
        """Compare if this metadata is newer than or equal to another metadata or datetime."""
        if isinstance(other, GenericMetadata | datetime):
            return self > other or self == other
        return NotImplemented

    def __hash__(self) -> int:
        """Enable use in sets and as dictionary keys."""
        return hash((self.datetime,))

    @property
    def datetime(self) -> datetime | None:
        """When the data was captured/created."""
        value = self._data.get("datetime")
        return cast("datetime | None", value)

    @property
    def latitude(self) -> float | None:
        """Geographic latitude in decimal degrees."""
        value = self._data.get("latitude")
        return cast("float | None", value)

    @property
    def longitude(self) -> float | None:
        """Geographic longitude in decimal degrees."""
        value = self._data.get("longitude")
        return cast("float | None", value)

    @property
    def altitude(self) -> float | None:
        """Altitude in meters."""
        value = self._data.get("altitude")
        return cast("float | None", value)

    @property
    def context(self) -> str | None:
        """Contextual information about the data."""
        value = self._data.get("context")
        return cast("str | None", value)

    @property
    def license(self) -> str | None:
        """License information."""
        value = self._data.get("license")
        return cast("str | None", value)

    @property
    def creators(self) -> list[str]:
        """List of creator names."""
        value = self._data.get("creators", [])
        return cast("list[str]", value)

    @property
    def hash_sha256(self) -> str | None:
        """SHA256 hash of the associated file as a hexadecimal string."""
        value = self._data.get("hash_sha256")
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.hex()
        return cast("str", value)

    @hash_sha256.setter
    def hash_sha256(self, value: str | None) -> None:
        """Set the SHA256 hash of the associated file."""
        if value is None:
            self._data["hash_sha256"] = None
        else:
            try:
                self._data["hash_sha256"] = bytes.fromhex(value)
            except ValueError:
                self._data["hash_sha256"] = value.encode("utf-8")

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
        pipeline_instance: "BasePipeline | None" = None,
        context: str = "dataset",
        collection_config: dict[str, Any] | None = None,
    ) -> None:
        """Create dataset-level metadata by combining all items into a YAML file."""
        saver = yaml_saver if saver_overwrite is None else saver_overwrite

        # Get user metadata from pipeline if available
        user_metadata = {}
        if pipeline_instance:
            user_metadata = pipeline_instance.get_metadata_header(
                context=context,
                collection_config=collection_config,
            )

        # Extract common fields from all items for deduplication
        common_fields = cls._extract_common_fields(items)

        # Log deduplication statistics
        if common_fields:
            logging.getLogger(__name__).debug(
                f"Deduplicated {len(common_fields)} common field(s) to header: {', '.join(common_fields.keys())}",
            )
        else:
            logging.getLogger(__name__).debug("No common fields found for deduplication")

        # Build header from user metadata and common fields
        header = cls._build_header(
            dataset_name=dataset_name,
            metadata_name=metadata_name,
            user_metadata=user_metadata,
            common_fields=common_fields,
            context=context,
        )

        # Remove common fields from items to avoid duplication
        deduplicated_items = cls._deduplicate_items(items, common_fields)

        # Construct final metadata structure
        dataset_metadata = {
            "header": header,
            "items": deduplicated_items,
        }

        output_name = metadata_name or cls.DEFAULT_METADATA_NAME
        if not dry_run:
            saver(root_dir, output_name, dataset_metadata)

    @classmethod
    def _extract_common_fields(
        cls,
        items: dict[str, list["BaseMetadata"]],
    ) -> dict[str, Any]:
        """
        Extract fields that are identical across all metadata items.

        Args:
            items: Mapping of file paths to metadata items

        Returns:
            Dictionary of field names to values that are common across all items
        """
        if not items:
            return {}

        # Flatten all metadata items
        all_items = [item for metadata_list in items.values() for item in metadata_list]
        if not all_items:
            return {}

        # Fields to check for commonality (exclude datetime as it usually varies)
        fields_to_check = ["latitude", "longitude", "altitude", "context", "license", "creators"]

        common_fields = {}
        for field in fields_to_check:
            # Get all non-None values for this field
            values = [getattr(item, field) for item in all_items if getattr(item, field) is not None]

            # If all items have the same value, it's common
            if values and all(v == values[0] for v in values):
                common_fields[field] = values[0]

        return common_fields

    @classmethod
    def _build_header(
        cls,
        dataset_name: str,
        metadata_name: str | None,
        user_metadata: dict[str, Any],
        common_fields: dict[str, Any],
        context: str,
    ) -> dict[str, Any]:
        """
        Build header from user metadata and common fields.

        Priority order:
        1. User-specified metadata (from pipeline.get_metadata_header())
        2. Smart defaults based on context
        3. Auto-deduplicated common fields

        Args:
            dataset_name: Name of the dataset
            metadata_name: Optional metadata file name (format: "collection.pipeline" or "pipeline")
            user_metadata: Generic metadata from pipeline.get_metadata_header()
            common_fields: Fields extracted as common across all items
            context: One of 'dataset', 'pipeline', or 'collection'

        Returns:
            Header dictionary
        """
        header = {}

        # Extract pipeline and collection names from metadata_name
        pipeline_name = None
        collection_name = None
        if metadata_name:
            parts = metadata_name.split(".")
            min_parts_for_collection = 2
            if len(parts) == min_parts_for_collection:
                collection_name, pipeline_name = parts
            elif len(parts) == 1:
                pipeline_name = parts[0]

        # Add user-specified name (highest priority)
        if "name" in user_metadata:
            header["name"] = user_metadata["name"]
        # Smart defaults based on context
        elif context == "collection" and pipeline_name and collection_name:
            header["name"] = f"{dataset_name} - {pipeline_name} - {collection_name}"
        elif context == "pipeline" and pipeline_name:
            header["name"] = f"{dataset_name} - {pipeline_name}"
        else:  # dataset level
            header["name"] = dataset_name

        # Add other user metadata
        header.update({key: value for key, value in user_metadata.items() if key != "name" and value is not None})

        # Add common fields (only if not already set by user)
        for field_name, field_value in common_fields.items():
            if field_name not in header:
                header[field_name] = field_value

        return header

    @classmethod
    def _deduplicate_items(
        cls,
        items: dict[str, list["BaseMetadata"]],
        common_fields: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Remove common fields from items since they're in the header.

        Args:
            items: Original items mapping
            common_fields: Fields that are in the header

        Returns:
            Deduplicated items as dictionaries
        """
        deduplicated: dict[str, list[dict[str, Any]]] = {}

        for path, metadata_items in items.items():
            deduplicated[path] = []
            for item in metadata_items:
                item_dict = {
                    "datetime": item.datetime.isoformat() if item.datetime else None,
                    "latitude": item.latitude if "latitude" not in common_fields else None,
                    "longitude": item.longitude if "longitude" not in common_fields else None,
                    "altitude": item.altitude if "altitude" not in common_fields else None,
                    "context": item.context if "context" not in common_fields else None,
                    "license": item.license if "license" not in common_fields else None,
                    "creators": item.creators if "creators" not in common_fields else None,
                    "hash_sha256": item.format_hash() if hasattr(item, "format_hash") else None,
                }
                # Remove None values to keep output clean
                item_dict = {k: v for k, v in item_dict.items() if v is not None}
                deduplicated[path].append(item_dict)

        return deduplicated

    @classmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list["BaseMetadata"], dict[str, Any] | None]],
        max_workers: int | None = None,
        logger: logging.Logger | None = None,
        *,
        dry_run: bool = False,
        chunk_size: int | None = None,
    ) -> None:
        """Process files according to the metadata type's requirements."""
