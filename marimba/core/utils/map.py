"""
Marimba Map Utilities.
"""

from collections.abc import Iterable
from typing import cast

import requests
from PIL import Image, ImageDraw, ImageFont
from staticmap import CircleMarker, StaticMap


class NetworkConnectionError(Exception):
    """
    Raised when there is a network connection error.
    """


def add_axes(
    draw: ImageDraw.ImageDraw,
    width: int,
    height: int,
    num_x_lines: int,
    num_y_lines: int,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> None:
    """
    Add latitude and longitude axes to the map.

    Args:
        draw: ImageDraw object to draw on the map.
        width: Width of the map.
        height: Height of the map.
        num_x_lines: Number of dashed lines on the x-axis.
        num_y_lines: Number of dashed lines on the y-axis.
        min_lat: Minimum latitude of the geolocations.
        max_lat: Maximum latitude of the geolocations.
        min_lon: Minimum longitude of the geolocations.
        max_lon: Maximum longitude of the geolocations.
    """
    margin = 20
    margin_x = 60  # Margin to prevent grid lines from running into the labels on x-axis
    margin_y = 40  # Margin to prevent grid lines from running into the labels on y-axis

    # Drawing lat/lon grid lines
    def draw_dashed_line(start: tuple[int, int], end: tuple[int, int], dash_length: int = 5) -> None:
        x1, y1 = start
        x2, y2 = end
        total_length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        dashes = int(total_length // dash_length // 2)
        for i in range(dashes):
            draw.line(
                (
                    x1 + (x2 - x1) * (2 * i) / (2 * dashes),
                    y1 + (y2 - y1) * (2 * i) / (2 * dashes),
                    x1 + (x2 - x1) * (2 * i + 1) / (2 * dashes),
                    y1 + (y2 - y1) * (2 * i + 1) / (2 * dashes),
                ),
                fill="grey",
                width=2,
            )

    # Draw x-axis lines
    x_interval = width // (num_x_lines + 1)
    for i in range(1, num_x_lines + 1):
        x = x_interval * i
        draw_dashed_line((x, margin), (x, height - margin_y))

    # Draw y-axis lines
    y_interval = height // (num_y_lines + 1)
    for i in range(1, num_y_lines + 1):
        y = y_interval * i
        draw_dashed_line((margin_x, y), (width - margin, y))

    # Adding lat/lon labels
    font = ImageFont.load_default(size=16)  # Using the default font
    for i in range(1, num_x_lines + 1):
        # Longitude labels (bottom)
        lon_label = f"{min_lon + (max_lon - min_lon) * i / (num_x_lines + 1):.2f}°"
        x = x_interval * i
        draw.text((x, height - margin_y + 20), lon_label, fill="black", font=font, anchor="mm")

    for i in range(1, num_y_lines + 1):
        # Latitude labels (left)
        lat_label = f"{max_lat - (max_lat - min_lat) * i / (num_y_lines + 1):.2f}°"
        y = y_interval * i
        draw.text((margin_x - 30, y), lat_label, fill="black", font=font, anchor="mm")


def make_summary_map(
    geolocations: Iterable[tuple[float, float]],
    width: int = 1920,
    height: int = 1080,
    marker_color: str = "red",
    marker_size: int = 10,
    num_x_lines: int = 5,
    num_y_lines: int = 5,
    zoom: int | None = None,
) -> Image.Image | None:
    """
    Make a summary map of the given geolocations.

    Args:
        geolocations: Iterable of (latitude, longitude) tuples.
        width: Width of the map.
        height: Height of the map.
        marker_color: Color of the markers.
        marker_size: Size of the markers.
        num_x_lines: Number of dashed lines on the x-axis.
        num_y_lines: Number of dashed lines on the y-axis.
        zoom: Zoom level for the map (lower values zoom out, higher values zoom in). If None, the map will use the
            default zoom level based on geolocations.


    Returns:
        PIL image of the map, or None if no geolocations were given.
    """
    geolocations = list(geolocations)
    if not geolocations:
        return None

    min_lat = min(lat for lat, lon in geolocations)
    max_lat = max(lat for lat, lon in geolocations)
    min_lon = min(lon for lat, lon in geolocations)
    max_lon = max(lon for lat, lon in geolocations)

    try:
        m = StaticMap(width, height, url_template="http://a.tile.osm.org/{z}/{x}/{y}.png")

        for lat, lon in geolocations:
            marker = CircleMarker((lon, lat), marker_color, marker_size)
            m.add_marker(marker)

        image = cast(Image.Image, m.render(zoom=zoom))

    except requests.exceptions.ConnectionError:
        raise NetworkConnectionError("Unable to render the map.") from None

    # Add coordinate axes
    draw = ImageDraw.Draw(image)
    add_axes(draw, width, height, num_x_lines, num_y_lines, min_lat, max_lat, min_lon, max_lon)

    return image
