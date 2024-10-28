"""
Marimba iFDO Utilities.

This module provides functions for loading and saving iFDO files. It simplifies the process of working with iFDO
objects by abstracting the file operations.

Imports:
    - pathlib: Provides the Path class for handling file system paths.
    - typing: Provides type hinting support.
    - ifdo: Contains the iFDO class for working with iFDO objects.

Functions:
    - load_ifdo: Loads an iFDO file from a given path.
    - save_ifdo: Saves an iFDO object to a specified path.
"""

from pathlib import Path

from ifdo import iFDO


def load_ifdo(path: str | Path) -> iFDO:
    """
    Load an iFDO file from a path.

    Args:
        path: The path to the iFDO file.

    Returns:
        The parsed iFDO object.
    """
    return iFDO.load(path)


def save_ifdo(ifdo: iFDO, path: str | Path) -> None:
    """
    Save an iFDO file to a path.

    Args:
        ifdo: The iFDO object to save.
        path: The path to save the iFDO file to. Should end in .yaml.
    """
    ifdo.save(path)
