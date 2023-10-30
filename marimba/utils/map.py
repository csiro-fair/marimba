"""
Map generation utilities.
"""


from typing import Iterable, Tuple

import piexif
from PIL import Image
from staticmap import CircleMarker, StaticMap


def make_summary_map(geolocations: Iterable[Tuple[float, float]]) -> Image.Image:
    """
    Make a summary map of the given geolocations.

    Args:
        geolocations: Iterable of (latitude, longitude) tuples.

    Returns:
        PIL image of the map.
    """
    m = StaticMap(800, 600)

    for lat, lon in geolocations:
        marker = CircleMarker((lon, lat), "red", 10)
        marker.opacity = 128
        marker.radius = 5
        marker.color = "red"
        marker.fill_color = "red"
        m.add_marker(marker)

    return m.render()


def get_image_location(image_path):
    """
    Get the latitude and longitude from an image file.

    Args:
        image_path: The path to the image file.

    Returns:
        A tuple containing the latitude and longitude, or (None, None) if the location could not be found.
    """
    try:
        exif_data = piexif.load(image_path)
        gps_data = exif_data["GPS"]
        gps_latitude = gps_data.get(piexif.GPSIFD.GPSLatitude)
        gps_latitude_ref = gps_data.get(piexif.GPSIFD.GPSLatitudeRef)
        gps_longitude = gps_data.get(piexif.GPSIFD.GPSLongitude)
        gps_longitude_ref = gps_data.get(piexif.GPSIFD.GPSLongitudeRef)
        if gps_latitude and gps_latitude_ref and gps_longitude and gps_longitude_ref:
            latitude = convert_to_degrees(gps_latitude)
            if gps_latitude_ref == b"S":
                latitude = 0 - latitude
            longitude = convert_to_degrees(gps_longitude)
            if gps_longitude_ref == b"W":
                longitude = 0 - longitude
            return (latitude, longitude)
        else:
            return (None, None)
    except Exception:
        return (None, None)


def convert_to_degrees(value):
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
