"""Tests for marimba.lib.gps module."""

from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from marimba.lib.gps import (
    convert_degrees_to_gps_coordinate,
    convert_gps_coordinate_to_degrees,
    read_exif_location,
)


@pytest.fixture
def test_image_path(tmp_path: Path) -> Path:
    """Create a test image path for testing GPS functions."""
    return tmp_path / "test_image.jpg"


class TestGPSUtilities:
    """Test GPS utility functions."""

    @pytest.mark.unit
    def test_read_exif_location_path_handling(self, mocker: MockerFixture) -> None:
        """Test that read_exif_location properly handles both string and Path inputs.

        Verifies that the function correctly converts string paths to Path objects
        and calls exiftool with the absolute path string regardless of input type.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = []  # No GPS data, testing path handling only

        string_path = "/test/path/image.jpg"
        path_object = Path("/test/path/image.jpg")

        # Act & Assert - Test string input
        read_exif_location(string_path)
        mock_et.get_metadata.assert_called_with(str(Path(string_path).absolute()))

        # Reset mock for second test
        mock_et.reset_mock()

        # Act & Assert - Test Path object input
        read_exif_location(path_object)
        mock_et.get_metadata.assert_called_with(str(path_object.absolute()))

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("gps_coordinate", "expected", "description"),
        [
            (
                [(37, 1), (46, 1), (30, 1)],  # Basic format: 37°46'30" = 37.775°
                37 + 46 / 60 + 30 / 3600,
                "basic integer coordinates",
            ),
            (
                [(374, 10), (2760, 100), (0, 1)],  # Fractions: 37.4° + 27.6' = 37.86°
                37.4 + 27.6 / 60 + 0 / 3600,
                "fractional coordinates like real EXIF data",
            ),
            (
                ((37, 1), (46, 1), (30, 1)),  # Tuple format: same as first
                37 + 46 / 60 + 30 / 3600,
                "tuple of tuples format",
            ),
            (
                [(0, 1), (0, 1), (0, 1)],  # Zero coordinates
                0.0,
                "zero coordinates",
            ),
            (
                [(180, 1), (0, 1), (0, 1)],  # Maximum longitude
                180.0,
                "maximum longitude",
            ),
        ],
    )
    def test_convert_gps_coordinate_to_degrees(
        self,
        gps_coordinate: tuple[tuple[int, int], tuple[int, int], tuple[int, int]] | list[tuple[int, int]],
        expected: float,
        description: str,
    ) -> None:
        """Test converting GPS coordinates to degrees with various input formats.

        Tests that the function correctly converts GPS coordinates from the EXIF format
        (degrees, minutes, seconds as fractions) to decimal degrees, supporting both
        list and tuple input formats as found in real EXIF data.
        """
        # Arrange - input parameters provided by parametrize decorator

        # Act
        result = convert_gps_coordinate_to_degrees(gps_coordinate)

        # Assert
        assert result == pytest.approx(
            expected,
            abs=1e-6,
        ), f"GPS coordinate conversion failed for {description}: expected {expected}, got {result}"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("degrees", "expected_d", "expected_m", "expected_s", "description"),
        [
            (37.775, 37, 46, 29999, "positive decimal with seconds"),  # 37°46'30" (with precision)
            (-122.4194, 122, 25, 9839, "negative decimal (uses absolute value)"),  # 122°25'9.839"
            (0.0, 0, 0, 0, "zero degrees"),
            (1.5, 1, 30, 0, "precise half degree (30 minutes)"),
            (180.0, 180, 0, 0, "maximum degrees"),
            (0.0001, 0, 0, 360, "very small decimal"),  # 0.0001 * 3600 * 1000 = 360
            (90.0, 90, 0, 0, "90 degrees (quarter circle)"),
            (0.25, 0, 15, 0, "quarter degree (15 minutes)"),
        ],
    )
    def test_convert_degrees_to_gps_coordinate(
        self,
        degrees: float,
        expected_d: int,
        expected_m: int,
        expected_s: int,
        description: str,
    ) -> None:
        """Test converting decimal degrees to DMS format.

        Tests the conversion from decimal degrees to degrees-minutes-seconds format,
        verifying that negative values are converted to positive (absolute value) and
        that the precision is maintained correctly.
        """
        # Arrange - input already provided by parametrize

        # Act
        d, m, s = convert_degrees_to_gps_coordinate(degrees)

        # Assert
        assert d == expected_d, f"Degrees mismatch for {description}: expected {expected_d}, got {d}"
        assert m == expected_m, f"Minutes mismatch for {description}: expected {expected_m}, got {m}"
        assert s == expected_s, f"Seconds mismatch for {description}: expected {expected_s}, got {s}"

    @pytest.mark.unit
    def test_read_exif_location_with_coordinates(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
    ) -> None:
        """Test reading GPS location from EXIF data with mocked exiftool.

        Tests the core logic of extracting GPS coordinates from metadata,
        using mocks to isolate the function from external dependencies.
        This is a unit test as it tests isolated function behavior with mocked dependencies.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        metadata = {
            "Composite:GPSLatitude": 37.7749,
            "Composite:GPSLongitude": -122.4194,
        }
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat == 37.7749, f"Expected latitude 37.7749, got {lat}"
        assert lon == -122.4194, f"Expected longitude -122.4194, got {lon}"
        mock_et.get_metadata.assert_called_once_with(str(test_image_path.absolute()))

    @pytest.mark.unit
    def test_read_exif_location_exif_fallback(self, mocker: MockerFixture, test_image_path: Path) -> None:
        """Test reading GPS location using EXIF fallback when Composite tags missing.

        Verifies the fallback mechanism that tries EXIF:GPS* tags when
        Composite:GPS* tags are not available in the metadata. This is a
        unit test as it tests isolated function behavior with mocked dependencies.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        metadata = {
            "EXIF:GPSLatitude": 40.7128,
            "EXIF:GPSLongitude": -74.0060,
        }
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat == 40.7128, f"Expected fallback latitude 40.7128, got {lat}"
        assert lon == -74.0060, f"Expected fallback longitude -74.0060, got {lon}"
        mock_et.get_metadata.assert_called_once_with(str(test_image_path.absolute()))

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("metadata", "description"),
        [
            (
                {"Composite:GPSLatitude": 37.7749},
                "missing longitude",
            ),
            (
                {"Composite:GPSLongitude": -122.4194},
                "missing latitude",
            ),
            (
                {"EXIF:GPSLatitude": 40.7128},
                "missing longitude in EXIF fallback",
            ),
            (
                {"EXIF:GPSLongitude": -74.0060},
                "missing latitude in EXIF fallback",
            ),
        ],
    )
    def test_read_exif_location_partial_coordinates(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
        metadata: dict[str, float],
        description: str,
    ) -> None:
        """Test reading GPS location with only partial coordinates.

        Verifies that when only latitude or longitude is present in metadata,
        the function returns None for both coordinates rather than partial data.
        Tests both Composite and EXIF tag fallback scenarios.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, f"Expected None latitude when {description}"
        assert lon is None, f"Expected None longitude when {description}"
        mock_et.get_metadata.assert_called_once_with(str(test_image_path.absolute()))

    @pytest.mark.unit
    def test_read_exif_location_no_metadata(self, mocker: MockerFixture, test_image_path: Path) -> None:
        """Test reading GPS location when no metadata available.

        Tests the case where exiftool returns an empty list, indicating
        no metadata could be extracted from the file.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = []  # No metadata

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, "Expected None latitude when no metadata available"
        assert lon is None, "Expected None longitude when no metadata available"

    @pytest.mark.unit
    def test_read_exif_location_none_metadata(self, mocker: MockerFixture, test_image_path: Path) -> None:
        """Test reading GPS location when metadata is None.

        Tests the case where exiftool returns None instead of a list,
        which should be handled gracefully by returning None coordinates.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = None  # None metadata

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, "Expected None latitude when metadata is None"
        assert lon is None, "Expected None longitude when metadata is None"

    @pytest.mark.unit
    def test_read_exif_location_empty_metadata(self, mocker: MockerFixture, test_image_path: Path) -> None:
        """Test reading GPS location when metadata contains empty dictionary.

        This is the originally mentioned test case that ensures the function
        handles truly empty metadata dictionaries gracefully.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = [{}]  # Empty metadata dict

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, "Expected None latitude for empty metadata dict"
        assert lon is None, "Expected None longitude for empty metadata dict"

    @pytest.mark.unit
    def test_read_exif_location_no_gps_data(self, mocker: MockerFixture, test_image_path: Path) -> None:
        """Test reading GPS location when no GPS data in metadata.

        Tests the case where metadata is returned but contains no GPS data,
        only other file metadata like dimensions and filename.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        metadata = {
            "FileName": "test.jpg",
            "ImageWidth": 800,
            "ImageHeight": 600,
        }
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, "Expected None latitude when no GPS data in metadata"
        assert lon is None, "Expected None longitude when no GPS data in metadata"

    @pytest.mark.unit
    def test_read_exif_location_exiftool_not_found(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
    ) -> None:
        """Test handling when exiftool is not found.

        Verifies that when exiftool is not installed or not found in PATH,
        the dependency error handler is called with the correct dependency and error message,
        and the function raises typer.Exit as expected.
        This is a unit test because it tests isolated function behavior with mocked dependencies.
        """
        # Arrange
        import typer

        from marimba.core.utils.dependencies import ToolDependency

        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_show_error = mocker.patch("marimba.lib.exif.show_dependency_error_and_exit")
        exiftool_error = FileNotFoundError("exiftool not found")
        mock_exiftool_helper.side_effect = exiftool_error
        mock_show_error.side_effect = typer.Exit(1)  # Simulate the actual behavior

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            read_exif_location(test_image_path)

        assert exc_info.value.exit_code == 1, "Expected exit code 1 when exiftool not found"
        mock_show_error.assert_called_once_with(
            ToolDependency.EXIFTOOL,
            "exiftool not found",
        )

    @pytest.mark.unit
    def test_read_exif_location_file_not_found_other(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
    ) -> None:
        """Test handling FileNotFoundError not related to exiftool.

        Tests that when a FileNotFoundError occurs that doesn't mention 'exiftool'
        (e.g., missing image file or other file system error), the function gracefully
        returns None coordinates without calling the dependency error handler.
        This tests the isolated function behavior with mocked dependencies to verify
        correct error differentiation between missing exiftool vs other file errors.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_show_error = mocker.patch("marimba.lib.exif.show_dependency_error_and_exit")
        # Simulate an error that occurs during metadata extraction but doesn't mention exiftool
        image_error = FileNotFoundError("No such file or directory: '/path/to/missing_image.jpg'")
        mock_et.get_metadata.side_effect = image_error

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, "Expected None latitude when image file not found"
        assert lon is None, "Expected None longitude when image file not found"
        mock_show_error.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("exception", "description"),
        [
            (KeyError("Missing GPS data"), "missing GPS key"),
            (ValueError("Invalid coordinate format"), "invalid coordinate format"),
            (TypeError("Unexpected data type"), "unexpected data type"),
            (AttributeError("Missing attribute"), "missing attribute"),
            (IndexError("Index out of range"), "index out of range"),
        ],
    )
    def test_read_exif_location_various_exceptions(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
        exception: Exception,
        description: str,
    ) -> None:
        """Test handling various exceptions during GPS reading.

        Verifies that the function gracefully handles various exceptions that might
        occur during metadata processing and always returns None coordinates.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.side_effect = exception

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat is None, f"Expected None latitude for {description}"
        assert lon is None, f"Expected None longitude for {description}"

    @pytest.mark.unit
    def test_read_exif_location_string_coordinates(
        self,
        mocker: MockerFixture,
        test_image_path: Path,
    ) -> None:
        """Test reading GPS location when coordinates are strings.

        Tests that the function can handle GPS coordinates provided as strings
        and correctly converts them to float values. This verifies the string-to-float
        conversion logic in the read_exif_location function.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        metadata = {
            "Composite:GPSLatitude": "37.7749",  # String values
            "Composite:GPSLongitude": "-122.4194",
        }
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_image_path)

        # Assert
        assert lat == pytest.approx(37.7749, abs=1e-6), f"Expected latitude 37.7749 from string, got {lat}"
        assert lon == pytest.approx(-122.4194, abs=1e-6), f"Expected longitude -122.4194 from string, got {lon}"
        mock_et.get_metadata.assert_called_once_with(str(test_image_path.absolute()))

    @pytest.mark.unit
    def test_read_exif_location_with_nested_path_object(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test reading GPS location with nested Path object and coordinate extraction.

        Tests that deeply nested Path objects are handled correctly, coordinates are
        properly extracted and converted to float values, and the absolute path
        is correctly resolved. This complements the basic path handling test by
        verifying nested directory structures work correctly.
        """
        # Arrange
        test_path = tmp_path / "subfolder" / "nested" / "test.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        metadata = {
            "Composite:GPSLatitude": 51.5074,
            "Composite:GPSLongitude": -0.1278,
        }
        mock_et.get_metadata.return_value = [metadata]

        # Act
        lat, lon = read_exif_location(test_path)

        # Assert
        assert lat == pytest.approx(51.5074, abs=1e-6), f"Expected latitude 51.5074 from nested Path object, got {lat}"
        assert lon == pytest.approx(-0.1278, abs=1e-6), f"Expected longitude -0.1278 from nested Path object, got {lon}"
        mock_et.get_metadata.assert_called_once_with(str(test_path.absolute()))

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("original_degrees", "description"),
        [
            (37.7749, "San Francisco latitude"),
            (-122.4194, "San Francisco longitude (negative)"),
            (0.0, "zero degrees"),
            (90.0, "90 degrees (quarter circle)"),
            (-90.0, "negative 90 degrees"),
            (180.0, "maximum longitude"),
            (-180.0, "minimum longitude"),
            (1.23456, "decimal precision test"),
            (0.0001, "very small decimal"),
            (-0.0001, "very small negative decimal"),
        ],
    )
    def test_coordinate_conversion_round_trip(
        self,
        original_degrees: float,
        description: str,
    ) -> None:
        """Test that coordinate conversion functions work together correctly.

        Tests that convert_degrees_to_gps_coordinate and convert_gps_coordinate_to_degrees
        are inverse operations within acceptable precision limits. This verifies the
        mathematical consistency of the coordinate conversion algorithms by testing
        specific coordinate values individually for easier debugging.
        """
        # Arrange - original_degrees provided by parametrize

        # Act
        d, m, s = convert_degrees_to_gps_coordinate(original_degrees)
        gps_format = [(d, 1), (m, 1), (s, 1000)]
        converted_back = convert_gps_coordinate_to_degrees(gps_format)

        # Assert
        expected = abs(original_degrees)  # convert_degrees_to_gps_coordinate uses abs()
        assert converted_back == pytest.approx(
            expected,
            abs=1e-3,
        ), (
            f"Round-trip conversion failed for {description} ({original_degrees}): "
            f"got {converted_back}, expected {expected}"
        )
