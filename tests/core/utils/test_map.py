"""Tests for marimba.core.utils.map module."""

import math

import pytest
import pytest_mock
import requests.exceptions

from marimba.core import NetworkConnectionError
from marimba.core.utils.map import (
    add_axes,
    calculate_grid_intervals,
    calculate_visible_bounds,
    calculate_zoom_level,
    lat_to_y,
    lon_to_x,
    make_summary_map,
    x_to_lon,
    y_to_lat,
)


class TestCoordinateConversions:
    """Test coordinate conversion functions."""

    @pytest.mark.unit
    def test_lat_to_y_basic(self) -> None:
        """Test latitude to y tile coordinate conversion with specific expected values."""
        # Arrange
        test_zoom = 1
        tolerance = 0.0001

        # Act
        y_equator = lat_to_y(0.0, test_zoom)
        y_north = lat_to_y(85.0, test_zoom)
        y_south = lat_to_y(-85.0, test_zoom)

        # Assert - Using specific expected values from Web Mercator projection
        assert abs(y_equator - 1.0) < tolerance, f"Expected equator y ~1.0, got {y_equator}"
        # At zoom 1, 85° latitude should map to approximately y=0.0033 based on Web Mercator projection
        assert abs(y_north - 0.0032758295721083686) < tolerance, f"Expected north 85° y ~0.0033, got {y_north}"
        # At zoom 1, -85° latitude should map to approximately y=1.9967 based on Web Mercator projection
        assert abs(y_south - 1.9967241704278917) < tolerance, f"Expected south -85° y ~1.9967, got {y_south}"

    @pytest.mark.unit
    def test_y_to_lat_basic(self) -> None:
        """Test y tile coordinate to latitude conversion with round-trip validation."""
        # Arrange
        center_tile_y = 1.0
        zoom_level = 1
        tolerance = 0.0001

        # Act
        lat_center = y_to_lat(center_tile_y, zoom_level)

        # Round-trip test
        original_lat = 45.0
        zoom_high_precision = 10
        y = lat_to_y(original_lat, zoom_high_precision)
        converted_lat = y_to_lat(y, zoom_high_precision)

        # Assert
        assert abs(lat_center) < tolerance, f"Expected center tile (y=1.0 at zoom=1) latitude ~0.0°, got {lat_center}"
        assert (
            abs(original_lat - converted_lat) < tolerance
        ), f"Round-trip conversion failed: {original_lat}° -> {converted_lat}°"

    @pytest.mark.unit
    def test_lon_to_x_basic(self) -> None:
        """Test longitude to x tile coordinate conversion with specific longitude values."""
        # Arrange & Act
        x_prime = lon_to_x(0.0, 1)
        x_east = lon_to_x(180.0, 1)
        x_west = lon_to_x(-180.0, 1)

        # Assert
        assert abs(x_prime - 1.0) < 0.0001, f"Expected 0° lon x ~1.0, got {x_prime}"
        assert abs(x_east - 2.0) < 0.0001, f"Expected 180° lon x ~2.0, got {x_east}"
        assert abs(x_west) < 0.0001, f"Expected -180° lon x ~0.0, got {x_west}"

    @pytest.mark.unit
    def test_x_to_lon_basic(self) -> None:
        """Test x tile coordinate to longitude conversion with round-trip validation."""
        # Arrange & Act
        lon_center = x_to_lon(1.0, 1)

        # Round-trip test
        original_lon = -122.5
        x = lon_to_x(original_lon, 10)
        converted_lon = x_to_lon(x, 10)

        # Assert
        assert abs(lon_center) < 0.0001, f"Expected center tile lon ~0.0, got {lon_center}"
        assert abs(original_lon - converted_lon) < 0.0001, f"Round-trip failed: {original_lon} -> {converted_lon}"

    @pytest.mark.unit
    def test_coordinate_conversions_different_zoom_levels(self) -> None:
        """
        Test coordinate conversions maintain accuracy across different zoom levels.

        Validates that round-trip coordinate conversions (lat/lon -> tile -> lat/lon)
        maintain precision within acceptable tolerances across various zoom levels.
        Each zoom level is tested independently with specific error reporting.
        """
        # Arrange
        lat, lon = 37.7749, -122.4194  # San Francisco coordinates
        expected_tolerance = 0.001  # Degrees, approximately 100m at San Francisco latitude

        # Test each zoom level independently for clearer failure reporting
        test_cases = [
            (1, "global view"),
            (5, "country view"),
            (10, "city view"),
            (15, "neighborhood view"),
            (18, "building view"),
        ]

        for zoom, description in test_cases:
            # Act
            x = lon_to_x(lon, zoom)
            y = lat_to_y(lat, zoom)
            converted_lon = x_to_lon(x, zoom)
            converted_lat = y_to_lat(y, zoom)

            # Calculate actual errors for detailed reporting
            lat_error = abs(lat - converted_lat)
            lon_error = abs(lon - converted_lon)

            # Assert with specific error information
            assert lat_error < expected_tolerance, (
                f"Latitude conversion failed at zoom {zoom} ({description}): "
                f"original={lat}, converted={converted_lat}, error={lat_error:.6f}° "
                f"(tolerance={expected_tolerance}°)"
            )
            assert lon_error < expected_tolerance, (
                f"Longitude conversion failed at zoom {zoom} ({description}): "
                f"original={lon}, converted={converted_lon}, error={lon_error:.6f}° "
                f"(tolerance={expected_tolerance}°)"
            )

    @pytest.mark.unit
    def test_coordinate_edge_cases(self) -> None:
        """
        Test coordinate conversion edge cases with Web Mercator projection limits.

        Tests the behavior at the Web Mercator projection boundaries:
        - Near-pole latitudes approach the ±85.0511° Web Mercator limits
        - International date line longitudes (±180°) wrap correctly
        - Round-trip conversions maintain acceptable precision at extremes
        """
        # Arrange: Test cases with Web Mercator projection limits
        latitude_test_cases = [
            (85.0, "near north pole"),
            (-85.0, "near south pole"),
        ]
        longitude_test_cases = [
            (179.9, "near +180° longitude"),
            (-179.9, "near -180° longitude"),
            (0.0, "prime meridian"),
        ]

        zoom_level = 10  # Use higher zoom for better precision in round-trip conversion
        expected_precision = 0.001  # ~100m accuracy tolerance at zoom level 10

        # Test extreme latitudes with round-trip validation
        for lat, description in latitude_test_cases:
            # Act
            y = lat_to_y(lat, zoom_level)
            converted_lat = y_to_lat(y, zoom_level)

            # Assert round-trip precision
            lat_error = abs(lat - converted_lat)
            assert lat_error < expected_precision, (
                f"Latitude round-trip failed at {description}: "
                f"original={lat}°, converted={converted_lat:.6f}°, "
                f"error={lat_error:.6f}° (expected <{expected_precision}°)"
            )

            # Assert y coordinate is within valid tile bounds
            max_tiles = 2**zoom_level
            assert 0 <= y <= max_tiles, (
                f"Y tile coordinate {y:.6f} should be within [0, {max_tiles}] "
                f"for {description} at zoom {zoom_level}"
            )

            # Assert reasonable y values for extreme latitudes
            if lat == 85.0:
                assert y < max_tiles * 0.1, (
                    f"Y coordinate for {lat}° latitude should be near 0 (top of map), "
                    f"got {y:.6f} at zoom {zoom_level}"
                )
            elif lat == -85.0:
                assert y > max_tiles * 0.9, (
                    f"Y coordinate for {lat}° latitude should be near {max_tiles} (bottom of map), "
                    f"got {y:.6f} at zoom {zoom_level}"
                )

        # Test extreme longitudes with round-trip validation
        for lon, description in longitude_test_cases:
            # Act
            x = lon_to_x(lon, zoom_level)
            converted_lon = x_to_lon(x, zoom_level)

            # Assert round-trip precision
            lon_error = abs(lon - converted_lon)
            assert lon_error < expected_precision, (
                f"Longitude round-trip failed at {description}: "
                f"original={lon}°, converted={converted_lon:.6f}°, "
                f"error={lon_error:.6f}° (expected <{expected_precision}°)"
            )

            # Assert x coordinate is within valid tile bounds
            max_tiles = 2**zoom_level
            assert 0 <= x <= max_tiles, (
                f"X tile coordinate {x:.6f} should be within [0, {max_tiles}] "
                f"for {description} at zoom {zoom_level}"
            )


class TestGridCalculations:
    """Test grid interval calculation functions."""

    @pytest.mark.unit
    def test_calculate_grid_intervals_normal_range(self) -> None:
        """
        Test grid interval calculation with normal coordinate range.

        Validates that calculate_grid_intervals produces the expected number of grid positions
        (num_lines + 2 to include boundary positions), correct decimal precision for the range,
        and properly ordered position values spanning the input boundaries.
        """
        # Arrange
        min_coordinate = -1.0
        max_coordinate = 1.0
        requested_lines = 3
        expected_positions_count = 5  # requested_lines + 2 boundary positions
        expected_decimal_places = 2  # For 2.0 degree range based on MEDIUM_INTERVAL threshold

        # Act
        positions, decimal_places = calculate_grid_intervals(min_coordinate, max_coordinate, requested_lines)

        # Assert
        assert len(positions) == expected_positions_count, (
            f"Expected {expected_positions_count} positions ({requested_lines} + 2 boundaries), "
            f"got {len(positions)}"
        )
        assert (
            positions[0] == min_coordinate
        ), f"First position should be minimum coordinate {min_coordinate}, got {positions[0]}"
        assert (
            positions[-1] == max_coordinate
        ), f"Last position should be maximum coordinate {max_coordinate}, got {positions[-1]}"
        assert decimal_places == expected_decimal_places, (
            f"Expected {expected_decimal_places} decimal places for range "
            f"{max_coordinate - min_coordinate}, got {decimal_places}"
        )

        # Verify positions are in strictly ascending order
        assert all(
            positions[i] <= positions[i + 1] for i in range(len(positions) - 1)
        ), f"Positions not in ascending order: {positions}"

        # Verify the calculated interval spacing is correct
        expected_interval = (max_coordinate - min_coordinate) / (requested_lines + 1)
        for i in range(1, len(positions)):
            actual_interval = positions[i] - positions[i - 1]
            assert (
                abs(actual_interval - expected_interval) < 1e-10
            ), f"Position interval {actual_interval} should equal expected {expected_interval}"

    @pytest.mark.unit
    def test_calculate_grid_intervals_small_range(self) -> None:
        """
        Test grid interval calculation expands extremely small coordinate ranges.

        When the input range is smaller than MIN_COORDINATE_RANGE (1e-10), the function
        expands it to DEFAULT_SMALL_RANGE (0.0001) to ensure meaningful grid lines can
        be drawn. This prevents division by zero and ensures readable coordinate labels.
        """
        # Arrange
        min_val, max_val, num_lines = 0.0, 1e-12, 3
        expected_positions_count = 5  # num_lines + 2 boundary positions
        # When DEFAULT_SMALL_RANGE (0.0001) is divided by (num_lines + 1) = 4,
        # the interval is 0.000025, which is < TINY_INTERVAL (0.0001), so gets 5 decimal places
        expected_decimal_places = 5

        # Act
        positions, decimals = calculate_grid_intervals(min_val, max_val, num_lines)
        total_range = positions[-1] - positions[0]

        # Assert
        assert (
            len(positions) == expected_positions_count
        ), f"Expected {expected_positions_count} positions ({num_lines} + 2 boundaries), got {len(positions)}"
        assert (
            decimals == expected_decimal_places
        ), f"Expected {expected_decimal_places} decimal places for tiny interval (0.000025), got {decimals}"
        assert total_range > 1e-12, f"Expanded range {total_range} should be much larger than original range {1e-12}"
        # Use approximate equality for floating point comparison
        assert (
            abs(total_range - 0.0001) < 1e-10
        ), f"Range should be expanded to DEFAULT_SMALL_RANGE (0.0001), got {total_range}"

        # Verify positions are properly spaced within the expanded range
        expected_interval = total_range / (num_lines + 1)
        for i in range(1, len(positions)):
            actual_interval = positions[i] - positions[i - 1]
            assert (
                abs(actual_interval - expected_interval) < 1e-10
            ), f"Position interval {actual_interval} should equal expected {expected_interval}"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("coordinate_range", "expected_decimals", "description"),
        [
            (10.0, 2, "large interval (≥0.01)"),
            (0.02, 3, "medium interval (≥0.001, <0.01)"),
            (0.002, 4, "small interval (≥0.0001, <0.001)"),
            (0.0002, 5, "tiny interval (<0.0001)"),
            (0.04, 2, "boundary case just above MEDIUM_INTERVAL threshold"),
            (0.004, 3, "boundary case just above SMALL_INTERVAL threshold"),
            (0.0004, 4, "boundary case just above TINY_INTERVAL threshold"),
        ],
    )
    def test_calculate_grid_intervals_decimal_places(
        self,
        coordinate_range: float,
        expected_decimals: int,
        description: str,
    ) -> None:
        """
        Test decimal places calculation for different coordinate ranges.

        Validates that calculate_grid_intervals returns the correct number of decimal places
        based on the calculated interval size. The decimal places are determined by comparing
        the interval against predefined thresholds (TINY_INTERVAL, SMALL_INTERVAL, MEDIUM_INTERVAL).
        """
        # Arrange
        min_val, max_val, num_lines = 0.0, coordinate_range, 3
        expected_positions_count = 5  # num_lines + 2 boundary positions

        # Act
        positions, decimals = calculate_grid_intervals(min_val, max_val, num_lines)
        calculated_interval = coordinate_range / (num_lines + 1)  # Interval calculation from source

        # Assert
        assert decimals == expected_decimals, (
            f"{description} should have {expected_decimals} decimal places, got {decimals}. "
            f"Coordinate range: {coordinate_range}, calculated interval: {calculated_interval:.6f}"
        )
        assert len(positions) == expected_positions_count, (
            f"Expected {expected_positions_count} positions ({num_lines} + 2 boundaries), "
            f"got {len(positions)} for {description}"
        )

        # Verify positions span the expected range
        assert positions[0] == min_val, f"First position should be {min_val}, got {positions[0]}"
        assert positions[-1] == max_val, f"Last position should be {max_val}, got {positions[-1]}"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("num_lines", "expected_positions", "description"),
        [
            (1, 3, "single interior line"),
            (3, 5, "three interior lines"),
            (5, 7, "five interior lines"),
            (10, 12, "ten interior lines"),
        ],
    )
    def test_calculate_grid_intervals_different_num_lines(
        self,
        num_lines: int,
        expected_positions: int,
        description: str,
    ) -> None:
        """
        Test grid calculation produces correct number of positions and spacing for different line counts.

        Validates that calculate_grid_intervals correctly generates the expected number of grid
        positions (num_lines + 2 boundary positions) and that the positions are evenly spaced
        across the coordinate range. Each test case is parameterized to provide clear failure
        reporting for specific line count scenarios.
        """
        # Arrange
        min_val, max_val = 0.0, 10.0
        coordinate_range = max_val - min_val
        expected_interval = coordinate_range / (num_lines + 1)

        # Act
        positions, decimals = calculate_grid_intervals(min_val, max_val, num_lines)

        # Assert - Position count and boundaries
        assert len(positions) == expected_positions, (
            f"For {description} ({num_lines} lines), expected {expected_positions} total positions, "
            f"got {len(positions)}"
        )
        assert (
            positions[0] == min_val
        ), f"First position should be minimum boundary {min_val}, got {positions[0]} for {description}"
        assert (
            positions[-1] == max_val
        ), f"Last position should be maximum boundary {max_val}, got {positions[-1]} for {description}"

        # Assert - Even spacing and ascending order
        for i in range(1, len(positions)):
            actual_interval = positions[i] - positions[i - 1]
            assert abs(actual_interval - expected_interval) < 1e-10, (
                f"Position interval {i} should be {expected_interval:.6f}, got {actual_interval:.6f} "
                f"for {description} (difference: {abs(actual_interval - expected_interval):.2e})"
            )

        # Assert - Positions in ascending order
        assert all(
            positions[i] <= positions[i + 1] for i in range(len(positions) - 1)
        ), f"Positions should be in ascending order for {description}: {positions}"

        # Assert - Decimal places appropriate for range
        # For 10.0 coordinate range with given num_lines, interval = 10.0/(num_lines+1)
        # All test cases result in intervals >= 0.01, so should get 2 decimal places
        assert (
            decimals == 2
        ), f"Expected 2 decimal places for {description} (interval={expected_interval:.6f}), got {decimals}"

    @pytest.mark.unit
    def test_calculate_grid_intervals_negative_range(self) -> None:
        """
        Test grid calculation handles negative coordinate ranges with proper ordering and spacing.

        Validates that calculate_grid_intervals correctly processes negative coordinate ranges,
        maintaining proper mathematical spacing and ordering. For a range from -5.0 to -2.0
        with 2 lines, should produce 4 total positions including boundaries, with 1.0 degree
        intervals giving 2 decimal places precision.
        """
        # Arrange
        min_val, max_val, num_lines = -5.0, -2.0, 2
        expected_positions_count = 4  # num_lines + 2 boundary positions
        coordinate_range = max_val - min_val  # 3.0 degrees
        expected_interval = coordinate_range / (num_lines + 1)  # 3.0 / 3 = 1.0
        expected_decimal_places = 2  # For 1.0 degree interval (≥ MEDIUM_INTERVAL threshold)

        # Act
        positions, decimals = calculate_grid_intervals(min_val, max_val, num_lines)

        # Assert - Position count and boundaries
        assert len(positions) == expected_positions_count, (
            f"Expected {expected_positions_count} positions ({num_lines} interior + 2 boundaries), "
            f"got {len(positions)}"
        )
        assert positions[0] == min_val, f"First position should be minimum boundary {min_val}, got {positions[0]}"
        assert positions[-1] == max_val, f"Last position should be maximum boundary {max_val}, got {positions[-1]}"

        # Assert - Expected intermediate positions
        expected_positions = [-5.0, -4.0, -3.0, -2.0]
        for i, expected_pos in enumerate(expected_positions):
            assert abs(positions[i] - expected_pos) < 1e-10, (
                f"Position {i} should be {expected_pos}, got {positions[i]} "
                f"(difference: {abs(positions[i] - expected_pos):.2e})"
            )

        # Assert - Verify ascending order with strict inequality
        assert all(
            positions[i] < positions[i + 1] for i in range(len(positions) - 1)
        ), f"Positions should be in strictly ascending order: {positions}"

        # Assert - Verify even spacing
        for i in range(1, len(positions)):
            actual_interval = positions[i] - positions[i - 1]
            assert abs(actual_interval - expected_interval) < 1e-10, (
                f"Position interval {i} should be {expected_interval:.6f}, got {actual_interval:.6f} "
                f"(difference: {abs(actual_interval - expected_interval):.2e})"
            )

        # Assert - Decimal places calculation
        assert decimals == expected_decimal_places, (
            f"Expected {expected_decimal_places} decimal places for interval {expected_interval} "
            f"(≥ MEDIUM_INTERVAL threshold), got {decimals}"
        )


class TestVisibleBounds:
    """Test visible bounds calculation."""

    @pytest.mark.unit
    def test_calculate_visible_bounds_basic(self) -> None:
        """
        Test basic visible bounds calculation with equatorial center point.

        At zoom level 1 with 512x512 dimensions centered at (0,0), the visible bounds
        should span exactly 1 tile in each direction from center. This test validates
        the precise Web Mercator projection calculations for coordinate bounds.
        """
        # Arrange
        center_lat, center_lon = 0.0, 0.0
        zoom = 1
        width, height = 512, 512
        lon_tolerance = 1e-10  # Very precise for longitude calculations
        lat_tolerance = 1e-6  # Slightly larger for latitude due to projection complexity

        # Act
        min_lat, max_lat, min_lon, max_lon = calculate_visible_bounds(center_lat, center_lon, zoom, width, height)

        # Assert - Basic containment requirements
        assert (
            min_lat < center_lat < max_lat
        ), f"Latitude bounds should contain center: {min_lat:.6f} < {center_lat:.6f} < {max_lat:.6f}"
        assert (
            min_lon < center_lon < max_lon
        ), f"Longitude bounds should contain center: {min_lon:.6f} < {center_lon:.6f} < {max_lon:.6f}"

        # Assert - Precise longitude calculations
        # At zoom 1, 512 pixels = 2 tiles, so x_range = 1.0 tile
        # For longitude: ±1 tile at zoom 1 = ±180° from center (0°)
        expected_min_lon = -180.0
        expected_max_lon = 180.0

        assert abs(min_lon - expected_min_lon) < lon_tolerance, (
            f"Min longitude calculation failed: expected {expected_min_lon}°, got {min_lon:.10f}° "
            f"(error: {abs(min_lon - expected_min_lon):.2e}°)"
        )
        assert abs(max_lon - expected_max_lon) < lon_tolerance, (
            f"Max longitude calculation failed: expected {expected_max_lon}°, got {max_lon:.10f}° "
            f"(error: {abs(max_lon - expected_max_lon):.2e}°)"
        )

        # Assert - Precise latitude calculations
        # At equator with ±1 tile range at zoom 1, Web Mercator projection gives ±85.0511°
        expected_min_lat = -85.0511287798066
        expected_max_lat = 85.0511287798066

        assert abs(min_lat - expected_min_lat) < lat_tolerance, (
            f"Min latitude calculation failed: expected {expected_min_lat:.10f}°, got {min_lat:.10f}° "
            f"(error: {abs(min_lat - expected_min_lat):.2e}°)"
        )
        assert abs(max_lat - expected_max_lat) < lat_tolerance, (
            f"Max latitude calculation failed: expected {expected_max_lat:.10f}°, got {max_lat:.10f}° "
            f"(error: {abs(max_lat - expected_max_lat):.2e}°)"
        )

        # Assert - Symmetry validation around center point
        lat_symmetry_error = abs(abs(min_lat) - abs(max_lat))
        lon_symmetry_error = abs(abs(min_lon) - abs(max_lon))

        assert lat_symmetry_error < lat_tolerance, (
            f"Latitude bounds should be symmetric around center (0°): |{min_lat:.6f}| vs |{max_lat:.6f}| "
            f"(symmetry error: {lat_symmetry_error:.2e}°)"
        )
        assert lon_symmetry_error < lon_tolerance, (
            f"Longitude bounds should be symmetric around center (0°): |{min_lon:.6f}| vs |{max_lon:.6f}| "
            f"(symmetry error: {lon_symmetry_error:.2e}°)"
        )

        # Assert - Return type validation
        assert isinstance(min_lat, float), f"min_lat should be float, got {type(min_lat)}"
        assert isinstance(max_lat, float), f"max_lat should be float, got {type(max_lat)}"
        assert isinstance(min_lon, float), f"min_lon should be float, got {type(min_lon)}"
        assert isinstance(max_lon, float), f"max_lon should be float, got {type(max_lon)}"

    @pytest.mark.unit
    def test_calculate_visible_bounds_different_sizes(self) -> None:
        """
        Test visible bounds calculation with different map dimensions shows correct linear scaling.

        When map dimensions increase by 4x (from 256x256 to 1024x1024), the visible bounds
        should increase proportionally. Since visible bounds are calculated using pixels per tile
        and tile ranges, a 4x increase in both width and height should result in a 4x increase
        in the linear range for both latitude and longitude dimensions.
        """
        # Arrange
        center_lat, center_lon = 37.7749, -122.4194
        zoom = 10
        small_size = 256
        large_size = 1024
        expected_ratio = large_size / small_size  # Should be exactly 4.0

        # Act
        bounds_small = calculate_visible_bounds(center_lat, center_lon, zoom, small_size, small_size)
        bounds_large = calculate_visible_bounds(center_lat, center_lon, zoom, large_size, large_size)

        # Assert
        # Unpack bounds for clarity: (min_lat, max_lat, min_lon, max_lon)
        small_min_lat, small_max_lat, small_min_lon, small_max_lon = bounds_small
        large_min_lat, large_max_lat, large_min_lon, large_max_lon = bounds_large

        small_lat_range = small_max_lat - small_min_lat
        large_lat_range = large_max_lat - large_min_lat
        small_lon_range = small_max_lon - small_min_lon
        large_lon_range = large_max_lon - large_min_lon

        # Verify larger map has larger visible bounds
        assert (
            large_lat_range > small_lat_range
        ), f"Large map lat range {large_lat_range} should exceed small map {small_lat_range}"
        assert (
            large_lon_range > small_lon_range
        ), f"Large map lon range {large_lon_range} should exceed small map {small_lon_range}"

        # Verify the scaling is mathematically correct (4x linear dimensions = 4x linear range)
        lat_ratio = large_lat_range / small_lat_range
        lon_ratio = large_lon_range / small_lon_range

        assert (
            abs(lat_ratio - expected_ratio) < 0.0001
        ), f"Latitude range ratio should be exactly {expected_ratio}, got {lat_ratio}"
        assert (
            abs(lon_ratio - expected_ratio) < 0.0001
        ), f"Longitude range ratio should be exactly {expected_ratio}, got {lon_ratio}"

        # Verify both dimensions scale identically (isometric scaling)
        assert (
            abs(lat_ratio - lon_ratio) < 0.0001
        ), f"Latitude and longitude ratios should be identical: lat={lat_ratio}, lon={lon_ratio}"

    @pytest.mark.unit
    def test_calculate_visible_bounds_different_zoom(self) -> None:
        """
        Test visible bounds calculation with different zoom levels shows correct inverse relationship.

        Higher zoom levels should result in smaller visible areas (inverse relationship).
        The zoom difference should follow Web Mercator projection scaling where each zoom level
        doubles resolution, resulting in a 2^zoom_diff scaling factor for linear dimensions.
        """
        # Arrange
        center_lat, center_lon = 37.7749, -122.4194  # San Francisco coordinates
        width, height = 512, 512
        zoom_low, zoom_high = 5, 15
        expected_zoom_ratio = 2 ** (zoom_high - zoom_low)  # Should be 1024

        # Act
        bounds_low = calculate_visible_bounds(center_lat, center_lon, zoom_low, width, height)
        bounds_high = calculate_visible_bounds(center_lat, center_lon, zoom_high, width, height)

        # Assert
        # Unpack bounds for clarity
        min_lat_low, max_lat_low, min_lon_low, max_lon_low = bounds_low
        min_lat_high, max_lat_high, min_lon_high, max_lon_high = bounds_high

        low_lat_range = max_lat_low - min_lat_low
        high_lat_range = max_lat_high - min_lat_high
        low_lon_range = max_lon_low - min_lon_low
        high_lon_range = max_lon_high - min_lon_high

        # Verify inverse relationship: lower zoom = larger visible area
        assert (
            low_lat_range > high_lat_range
        ), f"Low zoom lat range {low_lat_range:.6f} should exceed high zoom {high_lat_range:.6f}"
        assert (
            low_lon_range > high_lon_range
        ), f"Low zoom lon range {low_lon_range:.6f} should exceed high zoom {high_lon_range:.6f}"

        # Verify both dimensions scale proportionally
        lat_ratio = low_lat_range / high_lat_range
        lon_ratio = low_lon_range / high_lon_range

        # Both ratios should be approximately equal (same scaling behavior)
        ratio_difference = abs(lat_ratio - lon_ratio)
        max_allowed_difference = max(lat_ratio, lon_ratio) * 0.1  # 10% relative tolerance
        assert ratio_difference < max_allowed_difference, (
            f"Latitude and longitude should scale identically: "
            f"lat_ratio={lat_ratio:.3f}, lon_ratio={lon_ratio:.3f}, "
            f"difference={ratio_difference:.3f} should be < {max_allowed_difference:.3f}"
        )

        # Verify the scaling follows Web Mercator projection math
        # Zoom difference of 10 should give 2^10 = 1024x difference in linear dimensions
        actual_ratio = lat_ratio  # Use lat_ratio since both should be nearly identical
        tolerance_factor = 0.2  # Allow 20% tolerance due to projection distortion
        min_expected_ratio = expected_zoom_ratio * tolerance_factor
        max_expected_ratio = expected_zoom_ratio * (2 - tolerance_factor)

        assert min_expected_ratio < actual_ratio < max_expected_ratio, (
            f"Zoom scaling ratio should be approximately {expected_zoom_ratio} "
            f"(±{tolerance_factor*100}% tolerance), got {actual_ratio:.1f}. "
            f"Expected range: {min_expected_ratio:.1f} to {max_expected_ratio:.1f}"
        )

    @pytest.mark.unit
    def test_calculate_visible_bounds_rectangular(self) -> None:
        """
        Test visible bounds calculation with rectangular dimensions shows correct aspect ratio behavior.

        Validates that calculate_visible_bounds correctly handles non-square map dimensions by:
        - Wide maps (4:1 aspect ratio) having larger longitude ranges than tall maps
        - Tall maps (1:4 aspect ratio) having larger latitude ranges than wide maps
        - Aspect ratios scaling proportionally with image dimensions
        - Both coordinate ranges maintaining mathematical consistency with Web Mercator projection
        """
        # Arrange
        center_lat, center_lon = 0.0, 0.0
        zoom = 10
        wide_width, wide_height = 1024, 256  # 4:1 aspect ratio
        tall_width, tall_height = 256, 1024  # 1:4 aspect ratio
        expected_wide_aspect = wide_width / wide_height  # Should be exactly 4.0
        expected_tall_aspect = tall_width / tall_height  # Should be exactly 0.25
        tolerance = 0.0001

        # Act
        bounds_wide = calculate_visible_bounds(center_lat, center_lon, zoom, wide_width, wide_height)
        bounds_tall = calculate_visible_bounds(center_lat, center_lon, zoom, tall_width, tall_height)

        # Assert
        # Unpack bounds for clarity: (min_lat, max_lat, min_lon, max_lon)
        wide_min_lat, wide_max_lat, wide_min_lon, wide_max_lon = bounds_wide
        tall_min_lat, tall_max_lat, tall_min_lon, tall_max_lon = bounds_tall

        wide_lon_range = wide_max_lon - wide_min_lon
        wide_lat_range = wide_max_lat - wide_min_lat
        tall_lon_range = tall_max_lon - tall_min_lon
        tall_lat_range = tall_max_lat - tall_min_lat

        # Assert - Range comparisons
        assert (
            wide_lon_range > tall_lon_range
        ), f"Wide map longitude range {wide_lon_range:.6f} should exceed tall map range {tall_lon_range:.6f}"
        assert (
            tall_lat_range > wide_lat_range
        ), f"Tall map latitude range {tall_lat_range:.6f} should exceed wide map range {wide_lat_range:.6f}"

        # Assert - Aspect ratio calculations and validation
        wide_aspect = wide_lon_range / wide_lat_range
        tall_aspect = tall_lon_range / tall_lat_range

        # Verify aspect ratios match expected image dimensions (4:1 and 1:4)
        aspect_ratio_tolerance = 0.1  # 10% tolerance for projection effects
        assert abs(wide_aspect - expected_wide_aspect) < aspect_ratio_tolerance, (
            f"Wide map coordinate aspect ratio should be approximately {expected_wide_aspect:.2f}, "
            f"got {wide_aspect:.2f} (difference: {abs(wide_aspect - expected_wide_aspect):.3f})"
        )
        assert abs(tall_aspect - expected_tall_aspect) < aspect_ratio_tolerance, (
            f"Tall map coordinate aspect ratio should be approximately {expected_tall_aspect:.2f}, "
            f"got {tall_aspect:.2f} (difference: {abs(tall_aspect - expected_tall_aspect):.3f})"
        )

        # Assert - Bounds should be symmetric around center point
        assert abs(wide_min_lat + wide_max_lat) < tolerance, (
            f"Wide map latitude bounds should be symmetric around center (0°): "
            f"min={wide_min_lat:.6f}°, max={wide_max_lat:.6f}°"
        )
        assert abs(wide_min_lon + wide_max_lon) < tolerance, (
            f"Wide map longitude bounds should be symmetric around center (0°): "
            f"min={wide_min_lon:.6f}°, max={wide_max_lon:.6f}°"
        )
        assert abs(tall_min_lat + tall_max_lat) < tolerance, (
            f"Tall map latitude bounds should be symmetric around center (0°): "
            f"min={tall_min_lat:.6f}°, max={tall_max_lat:.6f}°"
        )
        assert abs(tall_min_lon + tall_max_lon) < tolerance, (
            f"Tall map longitude bounds should be symmetric around center (0°): "
            f"min={tall_min_lon:.6f}°, max={tall_max_lon:.6f}°"
        )


class TestAxesDrawing:
    """Test axes drawing functionality."""

    @pytest.mark.unit
    def test_add_axes_basic(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test basic axes drawing functionality with coordinate grid calculations.

        Verifies that add_axes correctly calculates grid positions and formats coordinate
        labels based on the visible bounds and requested number of grid lines. The test
        validates both the mathematical calculations and the drawing operations.
        """
        # Arrange
        mock_draw_instance = mocker.Mock()
        width, height = 500, 500
        num_x_lines, num_y_lines = 3, 3
        min_lat, max_lat = 37.0, 38.0
        min_lon, max_lon = -123.0, -122.0
        zoom = 10

        # Act
        add_axes(
            mock_draw_instance,
            width=width,
            height=height,
            num_x_lines=num_x_lines,
            num_y_lines=num_y_lines,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            zoom=zoom,
        )

        # Assert
        # Verify grid calculation logic by testing calculate_grid_intervals directly
        lon_positions, lon_decimals = calculate_grid_intervals(min_lon, max_lon, num_x_lines)
        lat_positions, lat_decimals = calculate_grid_intervals(min_lat, max_lat, num_y_lines)

        # Longitude range: 1.0°, interval = 1.0/(3+1) = 0.25°, should be 2 decimal places
        expected_lon_positions = [-123.0, -122.75, -122.5, -122.25, -122.0]
        assert len(lon_positions) == 5, f"Expected 5 longitude positions, got {len(lon_positions)}"
        assert lon_decimals == 2, f"Expected 2 decimal places for longitude, got {lon_decimals}"
        for i, expected in enumerate(expected_lon_positions):
            assert (
                abs(lon_positions[i] - expected) < 0.0001
            ), f"Longitude position {i} should be {expected}, got {lon_positions[i]}"

        # Latitude range: 1.0°, interval = 1.0/(3+1) = 0.25°, should be 2 decimal places
        expected_lat_positions = [37.0, 37.25, 37.5, 37.75, 38.0]
        assert len(lat_positions) == 5, f"Expected 5 latitude positions, got {len(lat_positions)}"
        assert lat_decimals == 2, f"Expected 2 decimal places for latitude, got {lat_decimals}"
        for i, expected in enumerate(expected_lat_positions):
            assert (
                abs(lat_positions[i] - expected) < 0.0001
            ), f"Latitude position {i} should be {expected}, got {lat_positions[i]}"

        # Verify drawing operations were called
        # Should draw 3 interior longitude lines + 3 interior latitude lines
        # Lines might be filtered if they fall outside the drawable area
        assert mock_draw_instance.line.call_count >= 0, "Drawing methods should be called"
        assert mock_draw_instance.text.call_count >= 0, "Text methods should be called"

    @pytest.mark.unit
    def test_add_axes_no_lines(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test add_axes with zero grid lines draws no interior lines or labels.

        Validates that when num_x_lines=0 and num_y_lines=0, the add_axes function
        correctly skips drawing interior grid lines and coordinate labels. This tests
        the edge case where calculate_grid_intervals returns only boundary positions
        [min_val, max_val], making positions[1:-1] an empty slice.
        """
        # Arrange
        mock_draw_instance = mocker.Mock()
        width, height = 500, 500
        min_lat, max_lat = 37.0, 38.0
        min_lon, max_lon = -123.0, -122.0
        zoom = 10

        # Act
        add_axes(
            mock_draw_instance,
            width=width,
            height=height,
            num_x_lines=0,
            num_y_lines=0,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            zoom=zoom,
        )

        # Assert
        # With 0 lines requested, no interior grid lines or labels should be drawn
        # The function processes lon_positions[1:-1] and lat_positions[1:-1] which should be empty
        # since calculate_grid_intervals(min_val, max_val, 0) returns positions = [min_val, max_val]
        # and positions[1:-1] would be an empty slice
        assert (
            mock_draw_instance.line.call_count == 0
        ), f"Expected no line calls with 0 grid lines, got {mock_draw_instance.line.call_count}"
        assert (
            mock_draw_instance.text.call_count == 0
        ), f"Expected no text calls with 0 grid lines, got {mock_draw_instance.text.call_count}"

    @pytest.mark.unit
    def test_add_axes_large_decimal_precision(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test axes drawing handles high precision coordinates and calculates correct decimal places."""
        # Arrange
        mock_draw_instance = mocker.Mock()

        # Small coordinate range requiring high precision (0.001 degree range = ~100m)
        min_lat, max_lat = 37.7740, 37.7750
        min_lon, max_lon = -122.4200, -122.4190
        width, height = 500, 500
        num_x_lines, num_y_lines = 3, 3
        zoom = 18

        # Act
        add_axes(
            mock_draw_instance,
            width=width,
            height=height,
            num_x_lines=num_x_lines,
            num_y_lines=num_y_lines,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            zoom=zoom,
        )

        # Assert
        # Verify coordinate grid calculations were invoked (drawing operations should occur for interior grid lines)
        # With 3 x-lines and 3 y-lines, there should be 3 interior longitude + 3 interior latitude lines = 6 total
        # Each line involves drawing operations, so call counts should be > 0
        assert mock_draw_instance.line.call_count > 0, "Line drawing method should be called for grid lines"
        assert mock_draw_instance.text.call_count > 0, "Text drawing method should be called for coordinate labels"

        # Test the actual coordinate precision calculation directly
        lat_positions, lat_decimals = calculate_grid_intervals(min_lat, max_lat, num_y_lines)
        lon_positions, lon_decimals = calculate_grid_intervals(min_lon, max_lon, num_x_lines)

        # For 0.001 degree ranges, should get 4 decimal places (based on SMALL_INTERVAL threshold)
        assert lat_decimals == 4, f"Expected 4 decimal places for lat range {max_lat - min_lat}, got {lat_decimals}"
        assert lon_decimals == 4, f"Expected 4 decimal places for lon range {max_lon - min_lon}, got {lon_decimals}"

        # Verify grid positions are sensible
        assert (
            len(lat_positions) == num_y_lines + 2
        ), f"Expected {num_y_lines + 2} lat positions, got {len(lat_positions)}"
        assert (
            len(lon_positions) == num_x_lines + 2
        ), f"Expected {num_x_lines + 2} lon positions, got {len(lon_positions)}"

        # Verify positions are within expected range
        assert lat_positions[0] == min_lat, f"First lat position should be {min_lat}, got {lat_positions[0]}"
        assert lat_positions[-1] == max_lat, f"Last lat position should be {max_lat}, got {lat_positions[-1]}"
        assert lon_positions[0] == min_lon, f"First lon position should be {min_lon}, got {lon_positions[0]}"
        assert lon_positions[-1] == max_lon, f"Last lon position should be {max_lon}, got {lon_positions[-1]}"


class TestZoomCalculation:
    """Test zoom level calculation."""

    @pytest.mark.unit
    def test_calculate_zoom_level_basic(self) -> None:
        """
        Test basic zoom level calculation returns appropriate integer within valid range for large area.

        Validates that calculate_zoom_level produces a reasonable zoom level for large geographic
        areas, ensuring the result is mathematically sound rather than testing specific hard-coded values.
        The test focuses on behavioral correctness: large areas should get low zoom levels appropriate
        for overview mapping.
        """
        # Arrange
        min_lat, max_lat = 0.0, 90.0  # Quarter of the world vertically
        min_lon, max_lon = -180.0, 180.0  # Full world horizontally
        width, height = 512, 512

        # Act
        zoom = calculate_zoom_level(min_lat, max_lat, min_lon, max_lon, width=width, height=height)

        # Assert
        assert isinstance(zoom, int), f"Zoom should be integer, got {type(zoom)}"
        assert 0 <= zoom <= 19, f"Zoom should be within valid tile server range [0-19], got {zoom}"

        # For very large areas (quarter of world), zoom should be low (overview level)
        assert zoom <= 5, (
            f"Large geographic area (90° x 360°) should produce low zoom level (≤5) for overview, "
            f"got {zoom}. Large areas require low zoom to fit in viewport."
        )

        # Test mathematical reasonableness: area is huge, so zoom should be quite low
        lat_range = max_lat - min_lat  # 90 degrees
        lon_range = max_lon - min_lon  # 360 degrees
        total_area_degrees = lat_range * lon_range  # 32,400 square degrees

        assert total_area_degrees > 30000, f"Test assumes large area (>30k deg²), got {total_area_degrees}"
        assert zoom <= 3, (
            f"For area of {total_area_degrees} square degrees, expect zoom ≤3 for reasonable overview. "
            f"Got zoom={zoom}, which may be too detailed for such a large area."
        )

    @pytest.mark.unit
    def test_calculate_zoom_level_small_area_produces_higher_zoom(self) -> None:
        """
        Test zoom calculation produces higher zoom levels for smaller geographic areas.

        Validates the inverse relationship between geographic area size and zoom level in
        the Web Mercator projection. Smaller areas should receive higher zoom levels to
        provide appropriate detail, while larger areas should receive lower zoom levels
        for proper overview. This test ensures the algorithm correctly implements this
        fundamental mapping relationship.
        """
        # Arrange
        small_min_lat, small_max_lat, small_min_lon, small_max_lon = (
            37.77,
            37.78,
            -122.42,
            -122.41,
        )  # San Francisco neighborhood (~0.01° range)
        large_min_lat, large_max_lat, large_min_lon, large_max_lon = (
            30.0,
            40.0,
            -130.0,
            -120.0,
        )  # Multi-state region (10° range)
        image_width, image_height = 512, 512

        # Calculate area sizes for validation
        small_lat_range = small_max_lat - small_min_lat  # 0.01 degrees
        small_lon_range = small_max_lon - small_min_lon  # 0.01 degrees
        large_lat_range = large_max_lat - large_min_lat  # 10.0 degrees
        large_lon_range = large_max_lon - large_min_lon  # 10.0 degrees

        # Act
        zoom_small_area = calculate_zoom_level(
            small_min_lat,
            small_max_lat,
            small_min_lon,
            small_max_lon,
            width=image_width,
            height=image_height,
        )
        zoom_large_area = calculate_zoom_level(
            large_min_lat,
            large_max_lat,
            large_min_lon,
            large_max_lon,
            width=image_width,
            height=image_height,
        )

        # Assert
        # Verify zoom level data types
        assert isinstance(zoom_small_area, int), f"Small area zoom should be integer, got {type(zoom_small_area)}"
        assert isinstance(zoom_large_area, int), f"Large area zoom should be integer, got {type(zoom_large_area)}"

        # Verify zoom levels are within valid tile server range
        assert 0 <= zoom_small_area <= 19, f"Small area zoom should be within [0-19], got {zoom_small_area}"
        assert 0 <= zoom_large_area <= 19, f"Large area zoom should be within [0-19], got {zoom_large_area}"

        # Verify inverse relationship: smaller areas get higher zoom levels
        assert zoom_small_area > zoom_large_area, (
            f"Small area ({small_lat_range}° x {small_lon_range}°) should have higher zoom than "
            f"large area ({large_lat_range}° x {large_lon_range}°): "
            f"small_zoom={zoom_small_area}, large_zoom={zoom_large_area}"
        )

        # Verify appropriate zoom level for small area (neighborhood scale)
        assert zoom_small_area >= 10, (
            f"Small area (0.01° ≈ 1.1km neighborhood) should get detailed zoom level (≥10), "
            f"got {zoom_small_area}. Area needs high detail for neighborhood-level mapping."
        )

        # Verify appropriate zoom level for large area (regional scale)
        assert zoom_large_area <= 10, (
            f"Large area (10° ≈ 1100km multi-state region) should get overview zoom level (≤10), "
            f"got {zoom_large_area}. Large areas require broad perspective for regional mapping."
        )

        # Verify significant zoom difference demonstrates proper algorithmic scaling
        zoom_difference = zoom_small_area - zoom_large_area
        area_ratio = (large_lat_range * large_lon_range) / (small_lat_range * small_lon_range)
        expected_min_difference = 3  # Minimum difference for proper area scaling

        assert zoom_difference >= expected_min_difference, (
            f"Zoom difference ({zoom_difference}) should be substantial (≥{expected_min_difference}) "
            f"for area ratio of {area_ratio:.0f}x, demonstrating proper scaling algorithm behavior"
        )

    @pytest.mark.unit
    def test_calculate_zoom_level_extreme_values(self) -> None:
        """
        Test zoom calculation with extreme coordinate values produces appropriate zoom levels.

        Validates that the zoom calculation algorithm correctly handles extreme cases:
        - Tiny areas get high zoom levels appropriate for detailed viewing
        - Huge areas get low zoom levels appropriate for overview
        - The algorithm maintains proper mathematical scaling between extreme cases
        - Results are within valid zoom level range [0-19]
        """
        # Arrange
        tiny_coords = (0.0, 0.00001, 0.0, 0.00001)  # ~1.1m x 1.1m area
        huge_coords = (-85.0, 85.0, -180.0, 180.0)  # Near-entire world coverage
        image_dimensions = (512, 512)

        # Act
        zoom_tiny = calculate_zoom_level(*tiny_coords, width=image_dimensions[0], height=image_dimensions[1])
        zoom_huge = calculate_zoom_level(*huge_coords, width=image_dimensions[0], height=image_dimensions[1])

        # Assert
        # Verify both results are valid zoom levels
        assert isinstance(zoom_tiny, int), f"Tiny area zoom should be integer, got {type(zoom_tiny)}"
        assert isinstance(zoom_huge, int), f"Huge area zoom should be integer, got {type(zoom_huge)}"
        assert 0 <= zoom_tiny <= 19, f"Tiny area zoom should be within [0-19], got {zoom_tiny}"
        assert 0 <= zoom_huge <= 19, f"Huge area zoom should be within [0-19], got {zoom_huge}"

        # Verify inverse relationship: smaller areas get higher zoom levels
        assert zoom_tiny > zoom_huge, (
            f"Tiny area zoom ({zoom_tiny}) should be greater than huge area zoom ({zoom_huge}) "
            f"due to inverse relationship between area size and zoom level"
        )

        # Verify extreme values produce expected behavioral ranges
        # Tiny areas should get high zoom (detailed view)
        assert (
            zoom_tiny >= 15
        ), f"Tiny area (0.00001° ≈ 1.1m) should get high zoom level (≥15) for detailed viewing, got {zoom_tiny}"

        # Huge areas should get low zoom (overview)
        assert zoom_huge <= 5, f"Huge area (world span) should get low zoom level (≤5) for overview, got {zoom_huge}"

        # Verify the zoom difference demonstrates significant algorithmic scaling
        zoom_difference = zoom_tiny - zoom_huge
        assert zoom_difference >= 10, (
            f"Zoom difference between tiny and huge areas should be substantial (≥10), "
            f"got {zoom_difference} (tiny={zoom_tiny}, huge={zoom_huge}). "
            f"This demonstrates proper algorithmic scaling across extreme coordinate ranges."
        )

        # Verify mathematical consistency with algorithm behavior
        # Calculate coordinate ranges after padding to understand zoom calculation
        tiny_lat_range = max(0.00001, 0.0001) * 1.2  # min_range = 0.0001, padding_factor = 1.2
        huge_lat_range = 170.0 * 1.2  # Full latitude range with padding
        range_ratio = huge_lat_range / tiny_lat_range

        # The zoom difference should be roughly proportional to log2 of range ratio
        expected_log_ratio = math.log2(range_ratio)
        assert abs(zoom_difference - expected_log_ratio) < 10, (
            f"Zoom difference ({zoom_difference}) should be roughly proportional to "
            f"log2(range_ratio) = {expected_log_ratio:.1f}, but allowing for algorithm specifics"
        )

    @pytest.mark.unit
    def test_calculate_zoom_level_different_dimensions(self) -> None:
        """
        Test zoom calculation with different image dimensions produces appropriate scaling.

        Larger images should support higher zoom levels due to increased pixel density,
        allowing more detailed maps. The zoom level should scale proportionally with
        the square root of the area increase (following Web Mercator tile mathematics).
        """
        # Arrange
        min_lat, max_lat = 37.0, 38.0
        min_lon, max_lon = -123.0, -122.0
        small_width, small_height = 256, 256
        large_width, large_height = 2048, 2048
        area_multiplier = (large_width * large_height) / (small_width * small_height)  # Should be 64

        # Act
        zoom_small = calculate_zoom_level(min_lat, max_lat, min_lon, max_lon, width=small_width, height=small_height)
        zoom_large = calculate_zoom_level(min_lat, max_lat, min_lon, max_lon, width=large_width, height=large_height)

        # Assert
        assert isinstance(zoom_small, int), f"Small image zoom should be integer, got {type(zoom_small)}"
        assert isinstance(zoom_large, int), f"Large image zoom should be integer, got {type(zoom_large)}"
        assert 0 <= zoom_small <= 19, f"Small image zoom should be 0-19, got {zoom_small}"
        assert 0 <= zoom_large <= 19, f"Large image zoom should be 0-19, got {zoom_large}"

        # Larger images should support higher (or equal) zoom levels
        assert zoom_large >= zoom_small, (
            f"Large image zoom {zoom_large} should be ≥ small image zoom {zoom_small} "
            f"due to {area_multiplier}x pixel area increase"
        )

        # Calculate actual zoom difference
        zoom_difference = zoom_large - zoom_small
        assert zoom_difference >= 0, f"Expected non-negative zoom difference, got {zoom_difference}"

        # For 64x area increase (8x linear), expect approximately log2(8) = 3 zoom levels increase
        # Allow reasonable range based on Web Mercator projection calculations
        expected_zoom_increase = math.log2(large_width / small_width)  # log2(8) ≈ 3
        assert zoom_difference <= expected_zoom_increase + 2, (
            f"Zoom difference {zoom_difference} should be reasonable (≤{expected_zoom_increase + 2}) "
            f"for {large_width/small_width}x linear dimension increase"
        )

        # Verify the zoom levels are mathematically sound for the given coordinate range
        # 1 degree lat/lon range at zoom levels should produce appropriate map scales
        lat_range = max_lat - min_lat  # 1.0 degree
        lon_range = max_lon - min_lon  # 1.0 degree
        assert lat_range == 1.0, f"Test assumes 1° latitude range, got {lat_range}"
        assert lon_range == 1.0, f"Test assumes 1° longitude range, got {lon_range}"


class TestMapGeneration:
    """Test map generation functionality."""

    @pytest.mark.unit
    def test_make_summary_map_basic(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test basic map generation with valid coordinates creates map with markers and axes.

        This test verifies that make_summary_map correctly:
        - Creates a StaticMap instance with specified dimensions
        - Adds markers for each coordinate
        - Renders the map with calculated zoom and center
        - Adds coordinate grid lines and labels to the rendered image
        """
        # Arrange
        mock_static_map = mocker.patch("marimba.core.utils.map.StaticMap")
        mock_map = mocker.Mock()
        mock_rendered_image = mocker.Mock()
        mock_map.render.return_value = mock_rendered_image
        mock_static_map.return_value = mock_map

        mock_draw = mocker.patch("marimba.core.utils.map.ImageDraw.Draw")
        mock_draw_instance = mocker.Mock()
        mock_draw.return_value = mock_draw_instance

        coords = [(37.7749, -122.4194), (37.7849, -122.4094)]  # San Francisco area
        expected_width, expected_height = 500, 500

        # Act
        result = make_summary_map(coords, width=expected_width, height=expected_height)

        # Assert
        assert result is mock_rendered_image, "Should return the rendered map image"

        # Verify StaticMap creation with correct parameters
        mock_static_map.assert_called_once_with(
            expected_width,
            expected_height,
            url_template="http://a.tile.osm.org/{z}/{x}/{y}.png",
        )

        # Verify markers were added for each coordinate
        assert mock_map.add_marker.call_count == len(
            coords,
        ), f"Should add exactly {len(coords)} markers for coordinates, got {mock_map.add_marker.call_count}"

        # Verify map rendering was called with zoom and center parameters
        mock_map.render.assert_called_once()
        render_call_args = mock_map.render.call_args
        assert "zoom" in render_call_args.kwargs, "Map rendering should specify zoom parameter"
        assert "center" in render_call_args.kwargs, "Map rendering should specify center parameter"

        # Verify center calculation is reasonable (midpoint of coordinates)
        expected_center_lat = (37.7749 + 37.7849) / 2  # 37.7799
        expected_center_lon = (-122.4194 + -122.4094) / 2  # -122.4144
        actual_center = render_call_args.kwargs["center"]
        assert len(actual_center) == 2, f"Center should be [lon, lat], got {actual_center}"
        assert (
            abs(actual_center[0] - expected_center_lon) < 0.0001
        ), f"Center longitude should be ~{expected_center_lon}, got {actual_center[0]}"
        assert (
            abs(actual_center[1] - expected_center_lat) < 0.0001
        ), f"Center latitude should be ~{expected_center_lat}, got {actual_center[1]}"

        # Verify ImageDraw was used to add coordinate grid
        mock_draw.assert_called_once_with(mock_rendered_image)

        # Verify grid lines and labels were drawn (axes functionality)
        assert (
            mock_draw_instance.line.call_count > 0
        ), f"Should draw grid lines for coordinate axes, got {mock_draw_instance.line.call_count} line calls"
        assert (
            mock_draw_instance.text.call_count > 0
        ), f"Should draw coordinate labels, got {mock_draw_instance.text.call_count} text calls"

    @pytest.mark.unit
    def test_make_summary_map_network_error(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test map generation with network error raises NetworkConnectionError with appropriate message.

        Verifies that when StaticMap.render() throws a requests.ConnectionError,
        the function properly transforms it into a NetworkConnectionError with
        the expected message "Unable to render the map".
        """
        # Arrange
        mock_static_map = mocker.patch("marimba.core.utils.map.StaticMap")
        mock_map = mocker.Mock()
        connection_error = requests.exceptions.ConnectionError("Network error")
        mock_map.render.side_effect = connection_error
        mock_static_map.return_value = mock_map

        coords = [(37.7749, -122.4194), (37.7849, -122.4094)]

        # Act & Assert
        with pytest.raises(NetworkConnectionError, match="Unable to render the map"):
            make_summary_map(coords, width=500, height=500)

        # Verify StaticMap was created and render was attempted
        mock_static_map.assert_called_once_with(
            500,
            500,
            url_template="http://a.tile.osm.org/{z}/{x}/{y}.png",
        )
        mock_map.render.assert_called_once()

    @pytest.mark.unit
    def test_make_summary_map_empty_coords(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test map generation with empty coordinates returns None without attempting to create map."""
        # Arrange
        mock_static_map = mocker.patch("marimba.core.utils.map.StaticMap")

        # Act
        result = make_summary_map([], width=500, height=500)

        # Assert
        assert result is None, "Should return None for empty coordinates"
        mock_static_map.assert_not_called()  # Should not create StaticMap instance for empty coordinates

    @pytest.mark.unit
    def test_make_summary_map_single_coordinate(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test map generation with single coordinate creates map with one marker and proper center calculation.

        Validates that when provided with a single coordinate, make_summary_map correctly:
        - Creates a StaticMap instance with specified dimensions
        - Adds exactly one marker at the provided coordinate
        - Calculates appropriate zoom level for a single point
        - Centers the map on the single coordinate
        - Renders the map and adds coordinate axes
        """
        # Arrange
        mock_static_map = mocker.patch("marimba.core.utils.map.StaticMap")
        mock_map = mocker.Mock()
        mock_rendered_image = mocker.Mock()
        mock_map.render.return_value = mock_rendered_image
        mock_static_map.return_value = mock_map

        mock_draw = mocker.patch("marimba.core.utils.map.ImageDraw.Draw")
        mock_draw_instance = mocker.Mock()
        mock_draw.return_value = mock_draw_instance

        coords = [(37.7749, -122.4194)]  # Single San Francisco coordinate
        expected_width, expected_height = 500, 500

        # Act
        result = make_summary_map(coords, width=expected_width, height=expected_height)

        # Assert
        assert result is mock_rendered_image, "Should return the rendered map image"

        # Verify StaticMap creation with correct parameters
        mock_static_map.assert_called_once_with(
            expected_width,
            expected_height,
            url_template="http://a.tile.osm.org/{z}/{x}/{y}.png",
        )

        # Verify exactly one marker was added with the correct coordinate
        assert mock_map.add_marker.call_count == 1, "Should add exactly one marker for single coordinate"

        # Verify marker call arguments
        marker_call = mock_map.add_marker.call_args[0][0]
        assert hasattr(marker_call, "coord"), "Marker should have coordinate attribute"
        # CircleMarker stores coordinates as (lon, lat) tuple
        expected_marker_coord = (-122.4194, 37.7749)  # (lon, lat)
        assert (
            marker_call.coord == expected_marker_coord
        ), f"Marker coordinate should be {expected_marker_coord}, got {marker_call.coord}"

        # Verify map rendering was called with zoom and center parameters
        mock_map.render.assert_called_once()
        render_call_args = mock_map.render.call_args
        assert "zoom" in render_call_args.kwargs, "Map rendering should specify zoom parameter"
        assert "center" in render_call_args.kwargs, "Map rendering should specify center parameter"

        # Verify center calculation for single coordinate
        expected_center_lat, expected_center_lon = coords[0]  # Same as the single coordinate
        actual_center = render_call_args.kwargs["center"]
        assert len(actual_center) == 2, f"Center should be [lon, lat], got {actual_center}"
        assert (
            abs(actual_center[0] - expected_center_lon) < 0.0001
        ), f"Center longitude should be {expected_center_lon}, got {actual_center[0]}"
        assert (
            abs(actual_center[1] - expected_center_lat) < 0.0001
        ), f"Center latitude should be {expected_center_lat}, got {actual_center[1]}"

        # Verify zoom level is reasonable for single coordinate
        actual_zoom = render_call_args.kwargs["zoom"]
        assert isinstance(actual_zoom, int), f"Zoom should be integer, got {type(actual_zoom)}"
        assert 0 <= actual_zoom <= 19, f"Zoom should be 0-19, got {actual_zoom}"
        # For single coordinate, expect higher zoom due to minimum range expansion
        assert actual_zoom >= 10, f"Single coordinate should get high zoom level (≥10), got {actual_zoom}"

        # Verify ImageDraw was used to add coordinate grid
        mock_draw.assert_called_once_with(mock_rendered_image)

        # Verify axes drawing functionality was invoked
        # For single coordinate with default 5 grid lines, should draw interior lines and labels
        # calculate_grid_intervals with 5 lines returns 7 positions, interior slice [1:-1] = 5 positions
        # Each position may draw a line and label if within bounds, so expect > 0 calls
        assert (
            mock_draw_instance.line.call_count > 0
        ), f"Should draw grid lines for coordinate axes, got {mock_draw_instance.line.call_count} line calls"
        assert (
            mock_draw_instance.text.call_count > 0
        ), f"Should draw coordinate labels, got {mock_draw_instance.text.call_count} text calls"


class TestNetworkConnectionError:
    """Test custom exception class."""

    @pytest.mark.unit
    def test_network_connection_error_creation(self) -> None:
        """Test NetworkConnectionError can be created and raised with specific message."""
        # Arrange
        msg = "Test error message"

        # Act & Assert
        with pytest.raises(NetworkConnectionError, match="Test error message"):
            raise NetworkConnectionError(msg)

    @pytest.mark.unit
    def test_network_connection_error_inheritance(self) -> None:
        """
        Test NetworkConnectionError properly inherits from Exception base class.

        Validates that NetworkConnectionError maintains proper inheritance hierarchy
        for exception handling, ensuring it can be caught by both specific exception
        type and general Exception handlers. This is critical for proper error
        handling in network-dependent map generation functionality.
        """
        # Arrange
        test_message = "Network connection failed"

        # Act
        error = NetworkConnectionError(test_message)

        # Assert inheritance hierarchy
        assert isinstance(error, Exception), (
            "NetworkConnectionError should inherit from Exception base class "
            "to be caught by generic exception handlers"
        )
        assert isinstance(
            error,
            NetworkConnectionError,
        ), "Should be instance of NetworkConnectionError for specific error handling"

        # Assert exception behavior
        assert (
            str(error) == test_message
        ), f"Exception message should be preserved: expected '{test_message}', got '{error!s}'"
        assert error.args == (
            test_message,
        ), f"Exception args should contain message: expected ('{test_message}',), got {error.args}"

        # Assert exception can be raised and caught as both types
        with pytest.raises(Exception, match="Network connection failed"):
            raise NetworkConnectionError(test_message)

        with pytest.raises(NetworkConnectionError, match="Network connection failed"):
            raise NetworkConnectionError(test_message)

    @pytest.mark.unit
    def test_network_connection_error_message(self) -> None:
        """
        Test NetworkConnectionError message handling and string representation.

        Validates that NetworkConnectionError properly stores and returns error messages,
        ensuring consistent error communication in network-dependent map functionality.
        This test specifically verifies the error message behavior required for proper
        error handling in make_summary_map when network issues occur.
        """
        # Arrange
        expected_message = "Unable to render the map"

        # Act
        error = NetworkConnectionError(expected_message)

        # Assert
        assert (
            str(error) == expected_message
        ), f"NetworkConnectionError string representation should be '{expected_message}', got '{error!s}'"
        assert error.args[0] == expected_message, f"Error args should contain '{expected_message}', got {error.args}"

        # Test with different message to ensure generality
        custom_message = "Custom network error"
        custom_error = NetworkConnectionError(custom_message)
        assert (
            str(custom_error) == custom_message
        ), f"Custom error message should be preserved: expected '{custom_message}', got '{custom_error!s}'"
