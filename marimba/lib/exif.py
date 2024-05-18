"""
Marimba Standard Library - EXIF.

It uses the piexif library to load and parse the EXIF data from a given file path. If the file does not contain
valid EXIF data, it returns None instead.

Imports:
    - pathlib.Path: Provides an object-oriented interface for working with file paths.
    - typing.Any: Specifies that a variable can be of any type.
    - typing.Union: Specifies that a variable can be one of several types.
    - piexif: A library for reading and writing EXIF data from image files.

Functions:
    - get_dict(path: Union[str, Path]) -> Any: Retrieves the EXIF data from the specified file path as a dictionary,
    or returns None if no valid EXIF data is found.

"""

from pathlib import Path
from typing import Any, Union

import piexif


def get_dict(path: Union[str, Path]) -> Any:
    """
    Get the EXIF data from a path.

    Args:
        path: The path to get the EXIF data from.

    Returns:
        The EXIF data from the image, or None if there is no EXIF data.
    """
    try:
        exif_dict = piexif.load(str(path))
        return exif_dict
    except piexif.InvalidImageDataError:
        return None
