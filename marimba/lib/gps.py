"""
GPS functions.
"""

from pathlib import Path

import exiftool

from marimba.core.utils.dependencies import ToolDependency, show_dependency_error_and_exit


def convert_gps_coordinate_to_degrees(
    value: tuple[tuple[int, int], tuple[int, int], tuple[int, int]] | list[tuple[int, int]],
) -> float:
    """
    Convert a GPS coordinate value to decimal degrees.

    Args:
        value: The GPS coordinate value.

    Returns:
        The GPS coordinate value in decimal degrees.
    """
    degrees = value[0][0] / value[0][1]
    minutes = value[1][0] / value[1][1] / 60
    seconds = value[2][0] / value[2][1] / 3600
    return degrees + minutes + seconds


def convert_degrees_to_gps_coordinate(degrees: float) -> tuple[int, int, int]:
    """
    Convert GPS coordinates from decimal degrees format to degrees, minutes, and seconds (DMS) format.

    Note:
        Negative values will result in positive degrees, minutes, and seconds.
        Use the appropriate hemisphere letter to indicate N/S or E/W when writing EXIF.

    Args:
        degrees: The GPS coordinate in decimal degrees format.

    Returns:
        A tuple containing the degrees, minutes, and seconds.
    """
    degrees = abs(degrees)
    d = int(degrees)
    m = int((degrees - d) * 60)
    s = int((degrees - d - m / 60) * 3600 * 1000)
    return d, m, s


def read_exif_location(path: str | Path) -> tuple[float | None, float | None]:
    """
    Read the latitude and longitude from a file EXIF metadata.

    Units are decimal degrees, with negative values for south and west.

    Args:
        path: The path to the file.

    Returns:
        A tuple containing the latitude and longitude, or (None, None) if the location could not be found.
    """
    path = Path(path)

    try:
        with exiftool.ExifToolHelper() as et:
            metadata = et.get_metadata(str(path.absolute()))
            if not metadata:
                return None, None

            data = metadata[0]

            # ExifTool returns GPS coordinates in decimal degrees directly
            # Try Composite tags first as they handle coordinate conversion automatically
            latitude = data.get("Composite:GPSLatitude") or data.get("EXIF:GPSLatitude")
            longitude = data.get("Composite:GPSLongitude") or data.get("EXIF:GPSLongitude")

            if latitude is not None and longitude is not None:
                return float(latitude), float(longitude)

            return None, None

    except FileNotFoundError as e:
        if "exiftool" in str(e).lower():
            show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
        return None, None
    except (KeyError, ValueError, TypeError, Exception):
        # KeyError: Missing expected EXIF data structure
        # ValueError: Invalid EXIF data format
        # TypeError: Unexpected data type in EXIF fields
        # Exception: Any exiftool errors
        return None, None
