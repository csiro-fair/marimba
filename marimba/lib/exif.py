from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import piexif


def get_dict(path: Union[str, Path]) -> Optional[dict]:
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


def get_datetime(path: Optional[Union[str, Path]] = None, data: Optional[dict] = None) -> Optional[datetime]:
    """
    Get the EXIF datetime from a path or EXIF dict.

    Args:
        path: The path to get the EXIF datetime from.

    Returns:
        The EXIF datetime from the image, or None if there is no EXIF datetime.
    """
    exif_dict = get_dict(path) if data is None else data
    if exif_dict is None:
        return None

    try:
        datetime_str = exif_dict["0th"][piexif.ExifIFD.DateTimeOriginal].decode("utf-8")
        subsecond_str = exif_dict["Exif"][piexif.ExifIFD.SubSecTimeOriginal].decode("utf-8")

        datetime_str = datetime_str + "." + subsecond_str

        dt = datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S.%f")

        return dt
    except Exception:
        return None


def copy_exif(source: Union[str, Path], destination: Union[str, Path]) -> bool:
    """
    Copy EXIF metadata from a source path to a destination path, if possible.

    Args:
        source: The path to copy the EXIF metadata from.
        destination: The path to copy the EXIF metadata to.

    Returns:
        True if the EXIF metadata was copied, False otherwise.
    """
    source = Path(source)
    destination = Path(destination)

    if not source.exists() or not destination.exists():
        return False

    exif_dict = get_dict(source)
    if exif_dict is None:
        return False

    exif_bytes = piexif.dump(exif_dict)
    piexif.insert(exif_bytes, str(destination))

    return True
