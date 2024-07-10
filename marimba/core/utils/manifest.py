"""
Marimba Manifest Utilities.

This module provides functionality to create and validate manifests for datasets. It allows for the creation of a
manifest from a directory, validation of a directory against a manifest, and saving/loading manifests to/from files.
The module uses SHA-256 hashing to ensure data integrity and supports multithreaded processing for improved performance.

Imports:
    - hashlib: Provides cryptographic hash functions.
    - dataclasses: Offers decorator and functions for automatically adding generated special methods to classes.
    - pathlib: Offers classes representing filesystem paths.
    - typing: Provides support for type hints.
    - distlib.util: Utilities for distribution-related operations.
    - rich.progress: Offers rich text and beautiful formatting in the terminal.
    - marimba.lib.decorators: Custom decorators for the marimba library.

Classes:
    - Manifest: Represents a dataset manifest for validation and integrity checking.
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

from distlib.util import Progress
from ifdo.models import ImageData
from rich.progress import TaskID

from marimba.lib.decorators import multithreaded


@dataclass
class Manifest:
    """
    Dataset manifest. Used to validate datasets to check if the underlying data has been corrupted or modified.
    """

    hashes: Dict[Path, bytes]

    @staticmethod
    def compute_hash(path: Path) -> bytes:
        """
        Compute the hash of a path.

        Args:
            path: The path.

        Returns:
            The hash of the path contents.
        """
        # SHA-256 hash
        file_hash = hashlib.sha256()

        if path.is_file():
            # Hash the file contents
            with path.open("rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)

        # Hash the path
        file_hash.update(str(path.as_posix()).encode("utf-8"))

        return file_hash.digest()

    @classmethod
    def from_dir(
        cls,
        directory: Path,
        exclude_paths: Optional[Iterable[Path]] = None,
        image_set_items: Optional[Dict[str, ImageData]] = None,
        progress: Optional[Progress] = None,
        task: Optional[TaskID] = None,
    ) -> "Manifest":
        """
        Create a manifest from a directory.

        Args:
            directory: The directory.
            exclude_paths: Paths to exclude from the manifest.
            progress: A progress bar to monitor the manifest generation

        Returns:
            A manifest.
        """
        hashes: Dict[Path, bytes] = {}
        exclude_paths = set(exclude_paths) if exclude_paths is not None else set()
        globbed_files = list(directory.glob("**/*"))

        @multithreaded()
        def process_file(
            self: None,
            thread_num: str,
            item: Path,
            directory: Path,
            exclude_paths: Optional[Iterable[Path]],
            hashes: Dict[Path, bytes],
            image_set_items: Optional[Dict[str, ImageData]] = None,
            progress: Optional[Progress] = None,
            task: Optional[TaskID] = None,
        ) -> None:
            if progress and task is not None:
                progress.advance(task)
            if exclude_paths and item in exclude_paths:
                return

            rel_path = item.resolve().relative_to(directory)

            # Check if the relative path as a string exists in image_set_items and return the hash if it does
            if image_set_items is not None and str(rel_path) in image_set_items:
                hashes[rel_path] = image_set_items[str(rel_path)].image_hash_sha256
                return  # Return if hash exists in image_set_items to avoid re-computation

            # Compute the hash if it does not exist in image_set_items
            hashes[rel_path] = Manifest.compute_hash(item)

        process_file(
            self=None,
            items=globbed_files,
            directory=directory,
            exclude_paths=exclude_paths,
            hashes=hashes,
            image_set_items=image_set_items,
            progress=progress,
            task=task,
        )  # type: ignore

        return cls(dict(sorted(hashes.items(), key=lambda item: item[0])))

    def validate(
        self,
        directory: Path,
        exclude_paths: Optional[Iterable[Path]] = None,
        progress: Optional[Progress] = None,
        task: Optional[TaskID] = None,
    ) -> bool:
        """
        Validate a directory against the manifest.

        Args:
            directory: The directory.
            exclude_paths: Paths to exclude from the manifest.

        Returns:
            True if the directory is valid, False otherwise.
        """
        # Create a manifest from the directory
        manifest = Manifest.from_dir(
            directory,
            exclude_paths=exclude_paths,
            progress=progress,
            task=task,
        )

        return self == manifest

    def __eq__(self, other: object) -> bool:
        """
        Check if two manifests are equal.

        Args:
            other: The other manifest.

        Returns:
            True if the manifests are equal, False otherwise.
        """
        if not isinstance(other, Manifest):
            return NotImplemented

        if len(self.hashes) != len(other.hashes):
            return False

        for path, file_hash in self.hashes.items():
            if file_hash != other.hashes.get(path):
                return False

        return True

    def save(self, path: Path) -> None:
        """
        Save the manifest to a file.

        Args:
            path: The path to the file.
        """
        with path.open("w") as f:
            for file_path, file_hash in self.hashes.items():
                f.write(f"{file_path.as_posix()}:{file_hash.hex()}\n")

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """
        Load a manifest from a file.

        Args:
            path: The path to the file.

        Returns:
            A manifest.
        """
        hashes = {}
        with path.open("r") as f:
            for line in f:
                if line:
                    path_str, hash_str = line.split(":")
                    hashes[Path(path_str)] = bytes.fromhex(hash_str)
        return cls(hashes)
