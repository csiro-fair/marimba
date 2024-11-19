"""
GPS functions.
"""

from pathlib import Path

import piexif


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
        # Load the EXIF metadata
        exif_data = piexif.load(str(path.absolute()))

        # Extract the GPS information
        gps_data = exif_data["GPS"]
        gps_latitude = gps_data.get(piexif.GPSIFD.GPSLatitude)
        gps_latitude_ref = gps_data.get(piexif.GPSIFD.GPSLatitudeRef)
        gps_longitude = gps_data.get(piexif.GPSIFD.GPSLongitude)
        gps_longitude_ref = gps_data.get(piexif.GPSIFD.GPSLongitudeRef)

        # Parse the GPS information into degrees
        if gps_latitude and gps_latitude_ref and gps_longitude and gps_longitude_ref:
            latitude = convert_gps_coordinate_to_degrees(gps_latitude)
            if gps_latitude_ref == b"S":
                latitude = 0 - latitude
            longitude = convert_gps_coordinate_to_degrees(gps_longitude)
            if gps_longitude_ref == b"W":
                longitude = 0 - longitude
            return latitude, longitude  # success!

    except (KeyError, ValueError, piexif.InvalidImageDataError, TypeError):
        # KeyError: Missing expected EXIF data structure
        # ValueError: Invalid EXIF data format
        # InvalidImageDataError: File doesn't contain valid EXIF data
        # TypeError: Unexpected data type in EXIF fields
        return None, None
    else:
        # no GPS data
        return None, None
