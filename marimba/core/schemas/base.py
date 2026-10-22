"""
Marimba Metadata Abstract Base Class.

This module defines the base interfaces for handling different metadata schemas in the Marimba library.
The BaseMetadata class provides a standard interface that all metadata implementations must follow.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any


class BaseMetadata(ABC):
    """
    Base metadata class. All metadata classes should inherit from this class.
    """

    @property
    @abstractmethod
    def datetime(self) -> datetime | None:
        """When image was captured."""
        raise NotImplementedError

    @property
    @abstractmethod
    def latitude(self) -> float | None:
        """Capture latitude."""
        raise NotImplementedError

    @property
    @abstractmethod
    def longitude(self) -> float | None:
        """Capture longitude."""
        raise NotImplementedError

    @property
    @abstractmethod
    def altitude(self) -> float | None:
        """Capture altitude in meters."""
        raise NotImplementedError

    @property
    @abstractmethod
    def context(self) -> str | None:
        """Context of the image data."""
        raise NotImplementedError

    @property
    @abstractmethod
    def license(self) -> str | None:
        """License information."""
        raise NotImplementedError

    @property
    @abstractmethod
    def creators(self) -> list[str]:
        """List of creator names."""
        raise NotImplementedError

    @property
    @abstractmethod
    def hash_sha256(self) -> str | None:
        """SHA256 hash of the associated file."""
        raise NotImplementedError

    @hash_sha256.setter
    @abstractmethod
    def hash_sha256(self, value: str) -> None:
        """Set the SHA256 hash of the associated file."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def create_dataset_metadata(
        cls,
        dataset_name: str,
        root_dir: Path,
        items: dict[str, list["BaseMetadata"]],
        logger: logging.Logger,
        metadata_name: str | None = None,
        *,
        dry_run: bool = False,
        saver_overwrite: Callable[[Path, str, dict[str, Any]], None] | None = None,
    ) -> None:
        """Create dataset-level metadata from a collection of items."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list["BaseMetadata"], dict[str, Any] | None]],
        max_workers: int | None = None,
        logger: logging.Logger | None = None,
        *,
        dry_run: bool = False,
        chunk_size: int | None = None,
        image_set_uuid: str | None = None,
    ) -> None:
        """
        Process files according to the metadata type's requirements.

        An implementation that processes files in batches may also accept a keyword-only
        ``on_files_done: Callable[[list[Path]], None] | None = None`` and call it with each batch's paths once the batch
        is written, so an interrupted packaging run resumes after the last completed batch. Packaging passes it only to
        implementations whose signature declares it; others resume from the start of the stage.
        """
        raise NotImplementedError

    def get_derived_state(self) -> dict[str, Any]:
        """
        Return state that :meth:`process_files` derived from the file and stored on this item.

        Resumable packaging journals this for each processed file and restores it with :meth:`apply_derived_state`
        instead of reprocessing the file. The default is an empty dictionary, for types that derive nothing.

        Returns:
            A JSON-serialisable dictionary.
        """
        return {}

    def apply_derived_state(self, state: dict[str, Any]) -> None:  # noqa: ARG002
        """
        Restore state previously returned by :meth:`get_derived_state`.

        Args:
            state: The dictionary returned by :meth:`get_derived_state`.
        """
        return
