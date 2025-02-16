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


def compute_hash(path: Path) -> str:
    """Compute the SHA-256 hash of a path's contents and path string.

    This function calculates a SHA-256 hash for the given path. For files, it hashes both the file contents and the path
    string. For directories or other types of paths, it only hashes the path string. The function uses a buffer to read
    large files in chunks, making it memory-efficient.

    Args:
        path (Path): The path to hash. Can be a file, directory, or any other valid path.

    Returns:
        str: The hexadecimal digest of the computed SHA-256 hash.

    Raises:
        OSError: If there are issues reading the file or encoding the path string.
    """
    file_hash = hashlib.sha256()

    # Only read file contents if it's a file
    if path.is_file():
        try:
            # Use a larger buffer size (1MB) for faster reading of large files
            with path.open("rb") as f:
                while chunk := f.read(1_048_576):  # 1MB chunks
                    file_hash.update(chunk)
        except OSError as e:
            raise OSError(f"Failed to read file {path}: {e!s}") from e

    # Always hash the path string
    file_hash.update(str(path.as_posix()).encode())

    return file_hash.hexdigest()
