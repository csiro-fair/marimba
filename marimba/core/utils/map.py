"""
Map generation utilities.
"""

from typing import Iterable, Optional, Tuple

from PIL import Image
from staticmap import CircleMarker, StaticMap


def make_summary_map(geolocations: Iterable[Tuple[float, float]]) -> Optional[Image.Image]:
    """
    Make a summary map of the given geolocations.

    Args:
        geolocations: Iterable of (latitude, longitude) tuples.

    Returns:
        PIL image of the map, or None if no geolocations were given.
    """
    m = StaticMap(800, 600)

    for lat, lon in geolocations:
        marker = CircleMarker((lon, lat), "red", 10)
        marker.opacity = 128
        marker.radius = 5
        marker.color = "red"
        marker.fill_color = "red"
        m.add_marker(marker)

    if not m.markers:
        return None

    return m.render()
