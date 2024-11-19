"""
Marimba Manifest Utilities.

This module provides functionality to create and validate manifests for datasets. It allows for the creation of a
manifest from a directory, validation of a directory against a manifest, and saving/loading manifests to/from files.
The module uses SHA-256 hashing to ensure data integrity and supports multithreaded processing for improved performance.

Imports:
    hashlib: Provides cryptographic hash functions.
    dataclasses: Offers decorator and functions for automatically adding generated special methods to classes.
    pathlib: Offers classes representing filesystem paths.
    typing: Provides support for type hints.
    distlib.util: Utilities for distribution-related operations.
    rich.progress: Offers rich text and beautiful formatting in the terminal.
    marimba.lib.decorators: Custom decorators for the marimba library.

Classes:
    Manifest: Represents a dataset manifest for validation and integrity checking.
"""

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from distlib.util import Progress
from ifdo.models import ImageData
from rich.progress import TaskID

from marimba.lib.decorators import multithreaded


@dataclass
class Manifest:
    """
    Dataset manifest. Used to validate datasets to check if the underlying data has been corrupted or modified.
    """

    hashes: dict[Path, bytes]

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
        exclude_paths: Iterable[Path] | None = None,
        image_set_items: dict[str, ImageData] | None = None,
        progress: Progress | None = None,
        task: TaskID | None = None,
    ) -> "Manifest":
        """
        Create a manifest from a directory.

        This class method generates a manifest by processing files in the specified directory. It computes hashes for
        each file, excluding specified paths and utilizing pre-computed hashes for image set items if provided. The
        method supports multithreaded processing and can display progress using a progress bar.

        Args:
            directory (Path): The root directory to create the manifest from.
            exclude_paths (Iterable[Path] | None): An iterable of paths to exclude from the manifest. Defaults to None.
            image_set_items (dict[str, ImageData] | None): A dictionary of pre-computed image data. Defaults to None.
            progress (Progress | None): A progress bar object to monitor the manifest generation. Defaults to None.
            task (TaskID | None): A task ID associated with the progress bar. Defaults to None.

        Returns:
            Manifest: A new Manifest object containing the processed files and their hashes.

        Raises:
            OSError: If there are issues accessing files or directories.
            ValueError: If the provided directory is invalid or doesn't exist.
        """
        hashes: dict[Path, bytes] = {}
        exclude_paths = set(exclude_paths) if exclude_paths is not None else set()
        globbed_files = list(directory.glob("**/*"))

        @multithreaded()
        def process_file(
            self: None,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            directory: Path,
            exclude_paths: Iterable[Path] | None,
            hashes: dict[Path, bytes],
            image_set_items: dict[str, ImageData] | None = None,
            progress: Progress | None = None,
            task: TaskID | None = None,
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
        )  # type: ignore[call-arg]

        return cls(dict(sorted(hashes.items(), key=lambda item: item[0])))

    def validate(
        self,
        directory: Path,
        exclude_paths: Iterable[Path] | None = None,
        progress: Progress | None = None,
        task: TaskID | None = None,
    ) -> bool:
        """
        Validate a directory against the manifest.

        This function compares the contents of a given directory with the current manifest. It creates a new manifest
        from the specified directory and compares it with the existing manifest to determine if the directory is valid.

        Args:
            directory: A Path object representing the directory to validate.
            exclude_paths: An optional iterable of Path objects representing paths to exclude from the manifest.
            progress: An optional Progress object for tracking the validation process.
            task: An optional TaskID object for associating the validation with a specific task.

        Returns:
            A boolean value indicating whether the directory is valid (True) or not (False).

        Raises:
            FileNotFoundError: If the specified directory does not exist.
            PermissionError: If there are insufficient permissions to access the directory or its contents.
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

        return all(file_hash == other.hashes.get(path) for path, file_hash in self.hashes.items())

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
