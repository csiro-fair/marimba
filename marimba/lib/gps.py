"""
GPS functions.
"""

from pathlib import Path
from typing import Union

import piexif


def convert_gps_coordinate_to_degrees(value):
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


def read_exif_location(path: Union[str, Path]):
    """
    Read the latitude and longitude from a file EXIF metadata.

    Units are decimal degrees, with negative values for south and west.

    Args:
        path: The path to the file.

    Returns:
        A tuple containing the latitude and longitude, or (None, None) if the location could not be found.
    """
    try:
        # Load the EXIF metadata
        exif_data = piexif.load(path)

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
            return (latitude, longitude)  # success!
        else:  # no GPS data
            return (None, None)
    except Exception:  # no/bad EXIF data
        return (None, None)
