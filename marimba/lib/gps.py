"""
GPS functions.

For batched workflows, prefer reading EXIF via :func:`marimba.lib.exif.session`
and parsing locations with :func:`parse_location_from_metadata` — that path
shares one ExifTool subprocess across many files. :func:`read_exif_location`
remains as a one-shot convenience but pays the full Perl-startup cost on every
call.
"""

from pathlib import Path
from typing import Any

from marimba.lib import exif


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


def parse_location_from_metadata(metadata: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """
    Extract (latitude, longitude) from an ExifTool metadata dict.

    Units are decimal degrees, with negative values for south and west. Returns
    ``(None, None)`` when the metadata does not carry a usable GPS pair. Cheap;
    does not spawn any subprocess.

    Args:
        metadata: An ExifTool metadata dict (as returned by
            :meth:`marimba.lib.exif.ExifSession.get_dict`). May be ``None``.

    Returns:
        A tuple ``(latitude, longitude)``, or ``(None, None)`` when missing.
    """
    if not metadata:
        return None, None

    try:
        latitude = metadata.get("Composite:GPSLatitude") or metadata.get("EXIF:GPSLatitude")
        longitude = metadata.get("Composite:GPSLongitude") or metadata.get("EXIF:GPSLongitude")
        if latitude is not None and longitude is not None:
            return float(latitude), float(longitude)
    except (KeyError, ValueError, TypeError, AttributeError, IndexError):
        return None, None

    return None, None


def read_exif_location(path: str | Path) -> tuple[float | None, float | None]:
    """
    Read the latitude and longitude from a file EXIF metadata.

    One-shot path: spawns a fresh ExifTool subprocess (~100-300 ms of Perl startup)
    per call. For batch reads prefer::

        with exif.session() as et:
            for path in paths:
                lat, lon = parse_location_from_metadata(et.get_dict(path))

    Args:
        path: The path to the file.

    Returns:
        A tuple containing the latitude and longitude, or ``(None, None)`` if
        the location could not be found.
    """
    with exif.session() as et:
        return parse_location_from_metadata(et.get_dict(Path(path).absolute()))
