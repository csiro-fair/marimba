from pathlib import Path
from typing import Union, Any

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
