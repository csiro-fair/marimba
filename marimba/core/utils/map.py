"""
Marimba Map Utilities.
"""

from collections.abc import Iterable
from typing import TypeVar, cast

import requests
from PIL import Image, ImageDraw, ImageFont
from staticmap import CircleMarker, StaticMap

# Type variable for comparable types
T = TypeVar("T", bound="SupportsRichComparison")
SupportsRichComparison = object

# Constants for coordinate calculations
MIN_COORDINATE_RANGE = 1e-10  # Minimum valid coordinate range
DEFAULT_SMALL_RANGE = 0.0001  # About 10m, used when range is too small

# Constants for interval decimal places
TINY_INTERVAL = 0.0001
SMALL_INTERVAL = 0.001
MEDIUM_INTERVAL = 0.01

# Constants for margin calculations
MIN_DECIMAL_PLACES = 2
MARGIN_PER_DECIMAL = 12


class NetworkConnectionError(Exception):
    """
    Raised when there is a network connection error.
    """


def calculate_grid_intervals(min_val: float, max_val: float, num_lines: int) -> tuple[list[float], int]:
    """
    Calculate appropriate grid line intervals based on coordinate range.

    Args:
        min_val: Minimum coordinate value
        max_val: Maximum coordinate value
        num_lines: Number of visible grid lines desired

    Returns:
        Tuple of (grid_positions, decimal_places)
    """
    coordinate_range = abs(max_val - min_val)

    # For very small ranges, ensure we don't divide by zero
    if coordinate_range < MIN_COORDINATE_RANGE:
        coordinate_range = DEFAULT_SMALL_RANGE
        center = (min_val + max_val) / 2
        min_val = center - coordinate_range / 2
        max_val = center + coordinate_range / 2

    # Calculate interval to get exactly num_lines + 2 lines
    interval = coordinate_range / (num_lines + 1)  # +1 because we're excluding first and last

    # Calculate positions
    positions = []
    for i in range(num_lines + 2):  # +2 for first and last lines
        pos = min_val + (i * interval)
        positions.append(pos)

    # Determine decimal places based on interval size
    if interval < TINY_INTERVAL:
        decimal_places = 5
    elif interval < SMALL_INTERVAL:
        decimal_places = 4
    elif interval < MEDIUM_INTERVAL:
        decimal_places = 3
    else:
        decimal_places = 2

    return positions, decimal_places


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
    """
    margin = 20
    margin_x = 60
    margin_y = 40

    # Calculate grid intervals and positions
    lon_positions, lon_decimals = calculate_grid_intervals(min_lon, max_lon, num_x_lines)
    lat_positions, lat_decimals = calculate_grid_intervals(min_lat, max_lat, num_y_lines)

    # Adjust left margin based on latitude decimal places
    if lat_decimals > MIN_DECIMAL_PLACES:
        margin_x += (lat_decimals - MIN_DECIMAL_PLACES) * MARGIN_PER_DECIMAL

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

    # Get ranges for positioning (use full range including hidden lines)
    lon_range = lon_positions[-1] - lon_positions[0]
    lat_range = lat_positions[-1] - lat_positions[0]

    # Skip first and last positions when drawing
    visible_lon_positions = lon_positions[1:-1]
    visible_lat_positions = lat_positions[1:-1]

    # Draw longitude lines and labels
    for lon in visible_lon_positions:
        # Convert longitude to x position using full range
        x = margin_x + (width - margin_x - margin) * (lon - lon_positions[0]) / lon_range
        if margin_x <= x <= width - margin:
            draw_dashed_line((int(x), margin), (int(x), height - margin_y))
            lon_label = f"{lon:.{lon_decimals}f}°"
            draw.text(
                (int(x), height - margin_y + 20),
                lon_label,
                fill="black",
                font=ImageFont.load_default(size=16),
                anchor="mm",
            )

    # Draw latitude lines and labels
    for lat in visible_lat_positions:
        # Convert latitude to y position using full range
        y = margin + (height - margin - margin_y) * (lat_positions[-1] - lat) / lat_range
        if margin <= y <= height - margin_y:
            draw_dashed_line((margin_x, int(y)), (width - margin, int(y)))
            lat_label = f"{lat:.{lat_decimals}f}°"
            draw.text(
                (margin_x - 40, int(y)),
                lat_label,
                fill="black",
                font=ImageFont.load_default(size=16),
                anchor="mm",
            )


def make_summary_map(
    geolocations: Iterable[tuple[float | None, float | None]],
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
        zoom: Zoom level for the map (lower values zoom out, higher values zoom in).
            If None, the map will use the default zoom level based on geolocations.

    Returns:
        PIL image of the map, or None if no geolocations were given.

    Raises:
        NetworkConnectionError: If unable to render the map due to network issues.
    """
    geolocations_list = list(geolocations)
    if not geolocations_list:
        return None

    # Filter out None values and get valid coordinates
    valid_coords = [(lat, lon) for lat, lon in geolocations_list if lat is not None and lon is not None]
    if not valid_coords:
        return None

    # Extract latitude and longitude lists
    lats, lons = zip(*valid_coords, strict=False)

    # Compute bounds
    min_lat = min(lats)
    max_lat = max(lats)
    min_lon = min(lons)
    max_lon = max(lons)

    try:
        m = StaticMap(width, height, url_template="http://a.tile.osm.org/{z}/{x}/{y}.png")

        for lat, lon in valid_coords:
            marker = CircleMarker((lon, lat), marker_color, marker_size)
            m.add_marker(marker)

        image = cast(Image.Image, m.render(zoom=zoom))

    except requests.exceptions.ConnectionError:
        raise NetworkConnectionError("Unable to render the map") from None

    # Add coordinate axes
    draw = ImageDraw.Draw(image)
    add_axes(draw, width, height, num_x_lines, num_y_lines, min_lat, max_lat, min_lon, max_lon)

    return image
