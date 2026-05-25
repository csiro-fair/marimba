"""
Marimba Standard Library - EXIF.

It uses the exiftool library to load and parse the EXIF data from a given file path. If the file does not contain
valid EXIF data, it returns None instead.

"""

from pathlib import Path
from typing import Any

import exiftool
from exiftool.exceptions import ExifToolException

from marimba.core.utils.dependencies import ToolDependency, show_dependency_error_and_exit


def get_dict(path: str | Path) -> Any:  # noqa: ANN401
    """
    Get the EXIF data from a path.

    Args:
        path: The path to get the EXIF data from.

    Returns:
        The EXIF data from the image, or None if there is no EXIF data.
    """
    try:
        with exiftool.ExifToolHelper() as et:
            metadata = et.get_metadata(str(path))
            if metadata:
                return metadata[0]  # exiftool returns a list
            return None
    except FileNotFoundError as e:
        if "exiftool" in str(e).lower():
            show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
        return None
    except ExifToolException:
        return None
