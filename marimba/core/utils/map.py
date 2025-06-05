"""
Marimba Map Utilities.
"""

import math
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

# Constants for margin and padding calculations
BASE_LEFT_MARGIN = 5  # Base margin before label text starts
LABEL_WIDTH = 10  # Fixed width allocated for latitude labels
BASE_DASH_PADDING = 60  # Base padding between label and dash
PADDING_PER_DECIMAL = 8  # Additional padding per decimal place
MIN_DECIMAL_PLACES = 2  # Minimum number of decimal places


class NetworkConnectionError(Exception):
    """
    Raised when there is a network connection error.
    """


def lat_to_y(lat: float, zoom: int) -> float:
    """Convert latitude to y tile coordinate."""
    lat_rad = math.radians(lat)
    n = 2.0**zoom
    return (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n


def y_to_lat(y: float, zoom: int) -> float:
    """Convert y tile coordinate to latitude."""
    n = 2.0**zoom
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    return math.degrees(lat_rad)


def lon_to_x(lon: float, zoom: int) -> float:
    """Convert longitude to x tile coordinate."""
    n = 2.0**zoom
    return (lon + 180.0) / 360.0 * n


def x_to_lon(x: float, zoom: int) -> float:
    """Convert x tile coordinate to longitude."""
    n = 2.0**zoom
    return (x / n * 360.0) - 180.0


def calculate_grid_intervals(
    min_val: float,
    max_val: float,
    num_lines: int,
) -> tuple[list[float], int]:
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


def calculate_visible_bounds(
    center_lat: float,
    center_lon: float,
    zoom: int,
    width: int,
    height: int,
) -> tuple[float, float, float, float]:
    """
    Calculate the visible bounds of the map given the center point, zoom level, and dimensions.

    Returns:
        Tuple of (min_lat, max_lat, min_lon, max_lon)
    """
    # Convert center point to tile coordinates
    center_x = lon_to_x(center_lon, zoom)
    center_y = lat_to_y(center_lat, zoom)

    # Calculate pixels per tile
    pixels_per_tile = 256  # Standard tile size

    # Calculate tile range
    x_range = width / (2 * pixels_per_tile)
    y_range = height / (2 * pixels_per_tile)

    # Calculate bounds in tile coordinates
    min_x = center_x - x_range
    max_x = center_x + x_range
    min_y = center_y - y_range
    max_y = center_y + y_range

    # Convert back to lat/lon
    min_lat = y_to_lat(max_y, zoom)  # Note: y is inverted
    max_lat = y_to_lat(min_y, zoom)
    min_lon = x_to_lon(min_x, zoom)
    max_lon = x_to_lon(max_x, zoom)

    return min_lat, max_lat, min_lon, max_lon


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
    zoom: int,
) -> None:
    """
    Add latitude and longitude axes to the map using the actual map bounds.
    """
    # Bottom margin for longitude labels
    margin_y = 40

    # Calculate grid intervals and positions first
    lon_positions, lon_decimals = calculate_grid_intervals(
        min_lon,
        max_lon,
        num_x_lines,
    )
    lat_positions, lat_decimals = calculate_grid_intervals(
        min_lat,
        max_lat,
        num_y_lines,
    )

    # Calculate dynamic dash padding based on decimal places in latitude labels
    extra_padding = max(0, lat_decimals - MIN_DECIMAL_PLACES) * PADDING_PER_DECIMAL
    total_dash_padding = BASE_DASH_PADDING + extra_padding

    # Calculate total left margin based on fixed label width and dynamic padding
    total_left_margin = BASE_LEFT_MARGIN + LABEL_WIDTH + total_dash_padding

    def draw_dashed_line(
        start: tuple[int, int],
        end: tuple[int, int],
        dash_length: int = 5,
    ) -> None:
        """Draw a dashed line between two points."""
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

    # Draw longitude lines and labels
    for lon in lon_positions[1:-1]:  # Skip first and last positions
        # Convert longitude to pixel position using web mercator
        x_tile = lon_to_x(lon, zoom)
        center_x_tile = lon_to_x((min_lon + max_lon) / 2, zoom)
        rel_x = (x_tile - center_x_tile) * 256  # 256 pixels per tile
        x = width / 2 + rel_x

        if total_left_margin <= x <= width - BASE_LEFT_MARGIN:
            draw_dashed_line((int(x), BASE_LEFT_MARGIN), (int(x), height - margin_y))
            lon_label = f"{lon:.{lon_decimals}f}°"
            draw.text(
                (int(x), height - margin_y + 20),
                lon_label,
                fill="black",
                font=ImageFont.load_default(size=16),
                anchor="mm",
            )

    # Draw latitude lines and labels
    for lat in lat_positions[1:-1]:  # Skip first and last positions
        # Convert latitude to pixel position using web mercator
        y_tile = lat_to_y(lat, zoom)
        center_y_tile = lat_to_y((min_lat + max_lat) / 2, zoom)
        rel_y = (y_tile - center_y_tile) * 256  # 256 pixels per tile
        y = height / 2 + rel_y

        if BASE_LEFT_MARGIN <= y <= height - margin_y:
            lat_label = f"{lat:.{lat_decimals}f}°"
            label_x = BASE_LEFT_MARGIN + LABEL_WIDTH
            draw.text(
                (label_x, int(y)),
                lat_label,
                fill="black",
                font=ImageFont.load_default(size=16),
                anchor="lm",
            )
            draw_dashed_line(
                (total_left_margin, int(y)),
                (width - BASE_LEFT_MARGIN, int(y)),
            )


def calculate_zoom_level(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    width: int,
    height: int,
) -> int:
    """
    Calculate an appropriate zoom level based on coordinate bounds and image dimensions.

    Args:
        min_lat: Minimum latitude
        max_lat: Maximum latitude
        min_lon: Minimum longitude
        max_lon: Maximum longitude
        width: Image width in pixels
        height: Image height in pixels

    Returns:
        int: Appropriate zoom level (0-19)
    """
    # Calculate the ranges
    lat_range = abs(max_lat - min_lat)
    lon_range = abs(max_lon - min_lon)

    # Handle single point or very close points by setting a minimum range
    min_range = 0.0001  # About 11 meters at the equator
    lat_range = max(lat_range, min_range)
    lon_range = max(lon_range, min_range)

    # Add padding to prevent points being too close to edges
    padding_factor = 0.2  # 20% padding
    lat_range = lat_range * (1 + padding_factor)
    lon_range = lon_range * (1 + padding_factor)

    # Convert to pixels (assuming 256 pixel tiles)
    pixels_per_tile = 256
    tiles_y = height / pixels_per_tile
    tiles_x = width / pixels_per_tile

    # Calculate zoom level needed for each dimension
    # The 360/170 factor adjusts for the Web Mercator projection distortion
    lat_zoom = math.log2(tiles_y * (360 / 170) / lat_range)
    lon_zoom = math.log2(tiles_x * 360 / lon_range)

    # Use the more conservative (smaller) zoom level and add a boost
    zoom = min(lat_zoom, lon_zoom)
    zoom_boost = 7
    zoom += zoom_boost

    # Round down and clamp between 0 and 19 (typical max zoom for tiles)
    return max(0, min(19, math.floor(zoom)))


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
        width: Width of the map in pixels.
        height: Height of the map in pixels.
        marker_color: Color of the markers.
        marker_size: Size of the markers in pixels.
        num_x_lines: Number of longitude grid lines.
        num_y_lines: Number of latitude grid lines.
        zoom: Zoom level for the map (lower values zoom out, higher values zoom in).
            If None, the map will calculate an appropriate zoom level based on the data.

    Returns:
        PIL image of the map, or None if no valid geolocations were given.

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
        m = StaticMap(
            width,
            height,
            url_template="http://a.tile.osm.org/{z}/{x}/{y}.png",
        )

        # Add markers
        for lat, lon in valid_coords:
            marker = CircleMarker((lon, lat), marker_color, marker_size)
            m.add_marker(marker)

        # Calculate zoom level if not provided
        if zoom is None:
            zoom = calculate_zoom_level(
                min_lat,
                max_lat,
                min_lon,
                max_lon,
                width,
                height,
            )

        # Calculate center point
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2

        # Render the map with calculated zoom and center
        image = cast(Image.Image, m.render(zoom=zoom, center=[center_lon, center_lat]))

        # Calculate the actual visible bounds based on the zoom level
        min_lat, max_lat, min_lon, max_lon = calculate_visible_bounds(
            center_lat,
            center_lon,
            zoom,
            width,
            height,
        )

        # Add coordinate axes with the calculated bounds
        draw = ImageDraw.Draw(image)
        add_axes(
            draw,
            width,
            height,
            num_x_lines,
            num_y_lines,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            zoom,
        )
    except requests.exceptions.ConnectionError:
        raise NetworkConnectionError("Unable to render the map") from None
    else:
        return image
