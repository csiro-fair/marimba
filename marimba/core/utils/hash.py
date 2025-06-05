"""
Marimba Hash Utilities.

This module provides functionality to compute SHA-256 hashes for files and paths. It can handle both files and
directories, hashing file contents and path strings for files, and just path strings for directories or other
types of paths. The module is designed to be efficient, using a large buffer size for reading large files.

Imports:
    hashlib: Provides hash algorithms, including SHA-256.
    pathlib.Path: Offers object-oriented filesystem paths.

Functions:
    compute_hash: Computes the SHA-256 hash of a path's contents and path string.
"""

import hashlib
from pathlib import Path


def compute_hash(path: Path, root_dir: Path | None = None) -> str:
    """Compute the SHA-256 hash of a path or its contents.

    For files, this function hashes only the file contents.
    For non-files (directories, etc), it hashes the path string.

    Args:
        path (Path): The path to hash. Can be a file, directory, or any other valid path.
        root_dir (Path | None): Optional root directory to make paths relative to.
            Only used when hashing non-file paths.
            If None, the absolute path will be used for non-file paths.

    Returns:
        str: The hexadecimal digest of the computed SHA-256 hash.

    Raises:
        OSError: If there are issues reading the file.
        ValueError: If root_dir is provided and path is not within it.
    """
    file_hash = hashlib.sha256()

    if path.is_file():
        # For files, hash only the contents
        try:
            with path.open("rb") as f:
                while chunk := f.read(1_048_576):  # 1MB chunks
                    file_hash.update(chunk)
        except OSError as e:
            raise OSError(f"Failed to read file {path}: {e!s}") from e
    else:
        # For non-files, hash the path string
        if root_dir is not None:
            try:
                relative_path = path.resolve().relative_to(root_dir.resolve())
                path_to_hash = relative_path
            except ValueError as e:
                raise ValueError(
                    f"Path {path} is not within root directory {root_dir}",
                ) from e
        else:
            path_to_hash = path

        file_hash.update(str(path_to_hash.as_posix()).encode())

    return file_hash.hexdigest()
