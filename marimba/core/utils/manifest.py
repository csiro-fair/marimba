"""
Marimba Manifest Utilities.

This module provides functionality to create and validate manifests for datasets. It allows for the creation of a
manifest from a directory, validation of a directory against a manifest, and saving/loading manifests to/from files.
The module uses SHA-256 hashing to ensure data integrity and supports multithreaded processing for improved performance.

"""

import logging
from collections.abc import Iterable
from copy import copy
from dataclasses import dataclass
from pathlib import Path

from rich.progress import Progress, TaskID

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.hash import compute_hash
from marimba.lib.decorators import multithreaded


@dataclass
class Manifest:
    """
    Dataset manifest. Used to validate datasets to check if the underlying data has been corrupted or modified.

    ``hashes`` maps the *relative-posix path string* of each tracked file to its SHA-256 digest.
    Stored as ``dict[str, str]`` rather than ``dict[Path, str]`` because ``Path`` objects carry an
    internal parsed-parts cache that costs ~3x the memory of the equivalent string at million-file
    scale; the manifest is the single largest in-memory accumulator during packaging.
    """

    hashes: dict[str, str]
    logger: logging.Logger | None = None

    def __hash__(self) -> int:
        """Enable use in sets and as dictionary keys."""
        return hash(tuple(sorted(self.hashes.items())))

    @staticmethod
    def _validate_directory(directory: Path) -> None:
        """
        Validate that the directory exists and is a directory.

        Args:
            directory: The directory to validate.

        Raises:
            ValueError: If the directory doesn't exist or isn't a directory
        """
        if not directory.exists():
            msg = f"Directory does not exist: {directory}"
            raise ValueError(msg)
        if not directory.is_dir():
            msg = f"Path is not a directory: {directory}"
            raise ValueError(msg)

    @staticmethod
    def _get_files_from_directory(
        directory: Path,
        logger: logging.Logger | None,
    ) -> list[Path]:
        """
        Get all files from a directory.

        Args:
            directory: The directory to scan.
            logger: Logger for recording information.

        Returns:
            List of paths.

        Raises:
            OSError: If there are issues scanning the directory
        """
        try:
            return list(directory.glob("**/*"))
        except Exception as e:
            if logger:
                logger.exception(f"Failed to glob directory {directory}")
            msg = f"Failed to scan directory {directory}: {e!s}"
            raise OSError(msg) from e

    @staticmethod
    def _get_hash_from_metadata(
        rel_path_str: str,
        dataset_items: dict[str, list[BaseMetadata]] | None,
    ) -> str | None:
        """
        Get hash from metadata if available.

        Args:
            rel_path_str: String representation of relative path.
            dataset_items: Dictionary of metadata items.

        Returns:
            Hash if available, None otherwise.
        """
        if dataset_items is not None and rel_path_str in dataset_items:
            metadata_list = dataset_items[rel_path_str]
            if metadata_list and metadata_list[0].hash_sha256 is not None:
                return metadata_list[0].hash_sha256
        return None

    @classmethod
    def _process_single_file(
        cls,
        item: Path,
        directory: Path,
        dataset_items: dict[str, list[BaseMetadata]] | None,
        logger: logging.Logger | None,
    ) -> tuple[str, str] | None:
        """
        Process a single file and return its relative-posix path string and hash.

        Args:
            item: The file to process.
            directory: The root directory.
            dataset_items: Pre-computed dataset items.
            logger: Logger for recording information.

        Returns:
            Tuple of ``(relative_posix_string, hash)`` if successful, None if file should be skipped.

        Raises:
            OSError: If there are issues computing the hash.
        """
        try:
            # Get relative path without the data/ prefix
            rel_path = item.resolve().relative_to(directory.resolve())
            rel_path_str = rel_path.as_posix()
            metadata_path = (
                rel_path.relative_to("data").as_posix() if rel_path_str.startswith("data/") else rel_path_str
            )

            # Try to get hash from metadata first
            existing_hash = cls._get_hash_from_metadata(metadata_path, dataset_items)
            if existing_hash is not None:
                return rel_path_str, existing_hash

            # Compute new hash if needed
            return rel_path_str, compute_hash(item, directory)
        except (OSError, PermissionError) as e:
            if logger:
                logger.exception(f"Failed to process file {item}")
            msg = f"Failed to process file {item}: {e!s}"
            raise OSError(msg) from e

    @classmethod
    def _process_files_with_progress(
        cls,
        files: list[Path],
        directory: Path,
        exclude_paths: set[Path],
        dataset_items: dict[str, list[BaseMetadata]] | None,
        progress: Progress | None,
        task: TaskID | None,
        logger: logging.Logger | None,
        max_workers: int | None,
    ) -> dict[str, str]:
        """
        Process multiple files with progress tracking and threading.

        Args:
            files: List of files to process.
            directory: Root directory.
            exclude_paths: Set of paths to exclude.
            dataset_items: Pre-computed dataset items.
            progress: Progress bar object.
            task: Task ID for progress bar.
            logger: Logger for recording information.
            max_workers: Maximum number of worker processes.

        Returns:
            Dictionary of ``{relative_posix_string: hash}`` for the processed files.

        Raises:
            RuntimeError: If file processing fails.
        """
        hashes: dict[str, str] = {}
        manifest_instance = cls({}, logger=logger)

        @multithreaded(max_workers=max_workers)
        def process_file(
            self: Manifest,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            directory: Path,
            exclude_paths: set[Path],
            hashes: dict[str, str],
            dataset_items: dict[str, list[BaseMetadata]] | None = None,
            logger: logging.Logger | None = None,
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            if progress and task is not None:
                progress.advance(task)

            if item in exclude_paths:
                return

            try:
                result = cls._process_single_file(
                    item,
                    directory,
                    dataset_items,
                    logger,
                )
                if result:
                    rel_path_str, file_hash = result
                    hashes[rel_path_str] = file_hash
            except Exception as e:
                if logger:
                    logger.exception(f"Error processing file {item}")
                msg = f"Failed to process file {item}: {e!s}"
                raise RuntimeError(msg) from e

        process_file(
            self=manifest_instance,
            items=files,
            directory=directory,
            exclude_paths=exclude_paths,
            hashes=hashes,
            dataset_items=dataset_items,
            logger=logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

        # Sort by Path so the on-disk manifest preserves byte-for-byte compatibility with the
        # pre-string-keyed implementation; storage is dict[str, str] but ordering is Path-aware.
        return dict(sorted(hashes.items(), key=lambda item: Path(item[0])))

    @classmethod
    def from_dir(
        cls,
        directory: Path,
        exclude_paths: Iterable[Path] | None = None,
        dataset_items: dict[str, list[BaseMetadata]] | None = None,
        progress: Progress | None = None,
        task: TaskID | None = None,
        logger: logging.Logger | None = None,
        max_workers: int | None = None,
        files: list[Path] | None = None,
    ) -> "Manifest":
        """
        Create a manifest from a directory.

        Args:
            directory: The root directory to create the manifest from.
            exclude_paths: An iterable of paths to exclude from the manifest.
            dataset_items: A dictionary of pre-computed data.
            progress: A progress bar object to monitor the manifest generation.
            task: A task ID associated with the progress bar.
            logger: A logger object for logging information.
            max_workers: Maximum number of worker processes to use.
            files: Optional pre-walked file list (skips the internal ``glob("**/*")``).
                Callers that have already walked ``directory`` can pass the result
                to avoid a second walk.

        Returns:
            Manifest: A new Manifest object containing the processed files and their hashes.

        Raises:
            ValueError: If the directory doesn't exist or isn't a directory
            RuntimeError: If manifest creation fails
        """
        try:
            cls._validate_directory(directory)
            walked_files = files if files is not None else cls._get_files_from_directory(directory, logger)
            exclude_set = set(exclude_paths) if exclude_paths is not None else set()

            hashes = cls._process_files_with_progress(
                files=walked_files,
                directory=directory,
                exclude_paths=exclude_set,
                dataset_items=dataset_items,
                progress=progress,
                task=task,
                logger=logger,
                max_workers=max_workers,
            )

            return cls(hashes, logger=logger)

        except Exception as e:
            if logger:
                logger.exception("Failed to create manifest")
            msg = "Failed to create manifest completely"
            raise RuntimeError(msg) from e

    def validate(
        self,
        directory: Path,
        exclude_paths: Iterable[Path] | None = None,
        progress: Progress | None = None,
        task: TaskID | None = None,
        logger: logging.Logger | None = None,
        files: list[Path] | None = None,
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
            logger (logging.Logger | None, optional): A Logger object for logging validation progress and results.
            files: Optional pre-walked file list, forwarded to :meth:`from_dir` so callers can
                avoid a second ``rglob`` when they have already walked ``directory``.

        Returns:
            A boolean value indicating whether the directory is valid (True) or not (False).

        Raises:
            FileNotFoundError: If the specified directory does not exist.
            PermissionError: If there are insufficient permissions to access the directory or its contents.
        """
        try:
            # Create a manifest from the directory
            manifest = Manifest.from_dir(
                directory,
                exclude_paths=exclude_paths,
                progress=progress,
                task=task,
                logger=logger,
                files=files,
            )
        except Exception as e:
            if logger:
                logger.exception("Failed to create comparison manifest")
            msg = "Failed to validate directory"
            raise RuntimeError(msg) from e

        return self == manifest

    @staticmethod
    def _get_sub_directories(files: set[Path], base_directory: Path) -> set[Path]:
        """
        Gets the subdirectories of the files contained by the base directory.

        Args:
            files: Files to get the subdirectories for
            base_directory: Base directory to get the subdirectories for

        Returns:
            Subdirectories between the files and the base directory.
        """
        sub_directories = set()
        todo = copy(files)
        while len(todo) != 0:
            entry = todo.pop()
            parent = entry.parent
            if parent == base_directory or parent in sub_directories:
                continue

            todo.add(parent)
            sub_directories.add(parent)

        return sub_directories

    def update(
        self,
        files: set[Path],
        directory: Path,
        exclude_paths: set[Path],
        logger: logging.Logger | None = None,
        max_workers: int | None = None,
    ) -> None:
        """
        Updates the entries of the given files.

        Args:
            files: Files to update.
            directory: Root directory.
            exclude_paths: Set of paths to exclude.
            logger: Logger for recording information.
            max_workers: Maximum number of worker processes.

        """
        subdirectories = self._get_sub_directories(files, directory)
        files.update(subdirectories)

        deleted_files = {path for path in files if not path.exists()}
        resolved_directory = directory.resolve()
        relative_deleted_strs = {path.resolve().relative_to(resolved_directory).as_posix() for path in deleted_files}
        self.hashes = {path: value for path, value in self.hashes.items() if path not in relative_deleted_strs}

        changed_files = files - deleted_files

        new_hashes = self._process_files_with_progress(
            files=list(changed_files),
            directory=directory,
            exclude_paths=exclude_paths,
            dataset_items=None,
            progress=None,
            task=None,
            logger=logger,
            max_workers=max_workers,
        )
        self.hashes.update(new_hashes)

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

    def save(self, path: Path, logger: logging.Logger | None = None) -> None:
        """
        Save the manifest to a file.

        Args:
            path: The path to the file.
            logger (logging.Logger | None, optional): A Logger object for logging validation progress and results.
        """
        try:
            with path.open("w") as f:
                for file_path_str, file_hash in self.hashes.items():
                    f.write(f"{file_path_str}:{file_hash}\n")
        except OSError as e:
            if logger:
                logger.exception(f"Failed to save manifest to {path}")
            msg = f"Failed to save manifest to {path}: {e!s}"
            raise OSError(msg) from e

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """
        Load a manifest from a file.

        Args:
            path: The path to the file.

        Returns:
            A manifest.
        """
        try:
            hashes: dict[str, str] = {}
            with path.open("r") as f:
                for line in f:
                    if line:
                        try:
                            path_str, hash_str = line.strip().split(":")
                            hashes[path_str] = hash_str
                        except ValueError as e:
                            msg = f"Invalid manifest file format at line: {line.strip()}"
                            raise ValueError(
                                msg,
                            ) from e
            return cls(hashes)
        except OSError as e:
            msg = f"Failed to load manifest from {path}: {e!s}"
            raise OSError(msg) from e
