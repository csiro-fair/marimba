"""Tests for marimba.lib.exif module."""

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from exiftool.exceptions import ExifToolException
from pytest_mock import MockerFixture

if TYPE_CHECKING:
    from tests.conftest import TestDataFactory

from marimba.lib.exif import get_dict


class TestExifUtilities:
    """Test EXIF utility functions."""

    @pytest.mark.unit
    def test_get_dict_returns_metadata_when_available(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test getting EXIF data from image with metadata.

        This test verifies that when ExifTool returns metadata in the expected format,
        the function correctly returns the first metadata entry from the list.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        expected_metadata = {"FileName": "test_image.jpg", "ImageWidth": 800}
        mock_et.get_metadata.return_value = [expected_metadata]

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result == expected_metadata, f"Expected metadata {expected_metadata}, but got {result}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_returns_none_when_no_metadata(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test getting EXIF data from image without metadata.

        This test verifies that when ExifTool returns an empty list (no metadata found),
        the function correctly returns None.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = []  # No metadata

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result is None, f"Expected None when no metadata found, but got {result}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_returns_none_when_metadata_is_none(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test getting EXIF data when ExifTool returns None.

        This test verifies that when ExifTool's get_metadata returns None instead of a list,
        the function correctly handles the null response and returns None without errors.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et
        mock_et.get_metadata.return_value = None  # ExifTool returns None

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result is None, f"Expected None when ExifTool returns None, but got {result}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_handles_string_path_input(self, mocker: MockerFixture) -> None:
        """Test get_dict correctly processes string path input.

        This test verifies that when a string path is provided instead of a Path object,
        the function correctly passes the string to ExifTool and returns the expected metadata.
        """
        # Arrange
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        expected_metadata = {"FileName": "test_image.jpg", "FileType": "JPEG"}
        mock_et.get_metadata.return_value = [expected_metadata]
        string_path = "/path/to/test_image.jpg"

        # Act
        result = get_dict(string_path)

        # Assert
        assert result == expected_metadata, f"Expected metadata {expected_metadata} for string path, but got {result}"
        mock_et.get_metadata.assert_called_once_with(string_path)

    @pytest.mark.unit
    def test_get_dict_handles_exiftool_not_found_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test handling when exiftool is not found.

        This test verifies that when get_metadata raises a FileNotFoundError containing
        'exiftool', the function calls show_dependency_error_and_exit which raises typer.Exit
        to terminate the program with an appropriate error message.
        """
        import typer

        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        error_message = "exiftool not found"
        mock_et.get_metadata.side_effect = FileNotFoundError(error_message)

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            get_dict(sample_image_path)

        # Verify the exit code is 1 (default error exit code)
        assert exc_info.value.exit_code == 1, f"Expected exit code 1, but got {exc_info.value.exit_code}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_handles_non_exiftool_file_not_found_error(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test handling FileNotFoundError not related to exiftool.

        This test verifies that when get_metadata raises a FileNotFoundError that does not
        contain 'exiftool' in its message, the function returns None without calling
        show_dependency_error_and_exit. This covers cases like missing image files.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        error_message = "Image file not found"
        mock_et.get_metadata.side_effect = FileNotFoundError(error_message)

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result is None, f"Expected None for non-exiftool FileNotFoundError, but got {result}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_handles_exiftool_exception(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test ExifToolException handling in get_dict function.

        This test verifies that when ExifToolHelper.get_metadata raises an ExifToolException
        (e.g., due to corrupted EXIF data or unsupported file format), the get_dict function
        properly catches it and returns None instead of propagating the exception.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        exception_message = "File format error: corrupted EXIF header"
        mock_et.get_metadata.side_effect = ExifToolException(exception_message)

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result is None, f"Expected None when ExifToolException occurs, but got {result}"
        mock_exiftool_helper.assert_called_once_with()
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_returns_first_when_multiple_metadata_entries(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test getting EXIF data when multiple items in metadata list.

        This test verifies that when ExifTool returns multiple metadata entries,
        the function correctly returns only the first entry from the list and ignores
        subsequent entries.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        first_metadata = {"FileName": "image1.jpg", "ImageWidth": 800}
        second_metadata = {"FileName": "image2.jpg", "ImageWidth": 600}
        metadata_list = [first_metadata, second_metadata]
        mock_et.get_metadata.return_value = metadata_list

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result == first_metadata, f"Expected first metadata entry {first_metadata}, but got {result}"
        assert result != second_metadata, f"Should not return the second metadata entry {second_metadata}"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_preserves_various_metadata_types(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test getting EXIF data with various metadata field types.

        This test verifies that the function correctly handles and preserves
        various types of metadata fields including strings, integers, floats,
        and GPS coordinates when returned by ExifTool.
        """
        # Arrange
        sample_image_path = tmp_path / "test_image.jpg"
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        complex_metadata = {
            "FileName": "test_image.jpg",
            "ImageWidth": 800,
            "ImageHeight": 600,
            "Make": "Canon",
            "Model": "EOS R5",
            "DateTime": "2023:12:25 10:30:00",
            "GPS:GPSLatitude": 37.7749,
            "GPS:GPSLongitude": -122.4194,
            "EXIF:ExposureTime": "1/60",
            "EXIF:FNumber": 5.6,
        }
        mock_et.get_metadata.return_value = [complex_metadata]

        # Act
        result = get_dict(sample_image_path)

        # Assert
        assert result == complex_metadata, f"Expected complete metadata unchanged, but got {result}"
        assert isinstance(result["FileName"], str), "String fields should be preserved as strings"
        assert isinstance(result["ImageWidth"], int), "Integer fields should be preserved as integers"
        assert isinstance(result["GPS:GPSLatitude"], float), "Float fields should be preserved as floats"
        mock_et.get_metadata.assert_called_once_with(str(sample_image_path))

    @pytest.mark.unit
    def test_get_dict_multiple_images(
        self,
        mocker: MockerFixture,
        test_data_factory: "TestDataFactory",
        tmp_path: Path,
    ) -> None:
        """Test getting EXIF data from multiple images sequentially.

        This unit test verifies that get_dict correctly processes multiple image files
        when called sequentially with mocked ExifTool responses. Tests the function's
        behavior with different metadata for different images.
        """
        # Arrange
        test_images = test_data_factory.create_test_images(tmp_path, image_count=3, image_size="minimal")
        mock_exiftool_helper = mocker.patch("exiftool.ExifToolHelper")
        mock_et = mocker.Mock()
        mock_exiftool_helper.return_value.__enter__.return_value = mock_et

        # Act & Assert
        results = []
        for i, image_path in enumerate(test_images):
            image_metadata = {
                "FileName": image_path.name,
                "ImageWidth": 800 + i * 100,
                "ImageHeight": 600 + i * 50,
            }
            mock_et.get_metadata.return_value = [image_metadata]

            result = get_dict(image_path)

            assert result == image_metadata, f"Image {i}: Expected metadata {image_metadata}, but got {result}"
            assert result["FileName"] == image_path.name, f"Image {i}: Filename should match the actual file"
            results.append(result)

        # Verify overall behavior
        assert len(results) == 3, "Should have processed exactly 3 images"
        assert all(result is not None for result in results), "All images should have returned metadata"
        assert mock_et.get_metadata.call_count == 3, "Should have called get_metadata for each image"

    @pytest.mark.integration
    def test_get_dict_real_exiftool_interaction(self, test_data_factory: "TestDataFactory", tmp_path: Path) -> None:
        """Test get_dict with real ExifTool interaction for integration testing.

        This integration test verifies that get_dict correctly integrates with the actual
        ExifTool binary when processing real image files. The test factory creates minimal
        JPEG files that ExifTool can process and extract basic file metadata from.
        """
        # Arrange - Create a test image with minimal fake JPEG data
        test_image = test_data_factory.create_test_images(tmp_path, image_count=1, image_size="minimal")[0]

        # Act - Call get_dict without mocking ExifTool (real integration)
        result = get_dict(test_image)

        # Assert - Verify ExifTool correctly processes the image and returns metadata
        assert result is not None, (
            f"Expected metadata dictionary from ExifTool for valid JPEG file '{test_image}', but got None. "
            "ExifTool should be able to extract basic file metadata from minimal JPEG files."
        )
        assert isinstance(result, dict), (
            f"Expected dict from ExifTool for file '{test_image}', but got {type(result)}. "
            "ExifTool should return metadata as a dictionary."
        )
        assert len(result) > 0, (
            f"Metadata dictionary should not be empty for valid JPEG file '{test_image}'. "
            "ExifTool should extract at least basic file information."
        )

        # Verify basic file metadata that ExifTool should always provide for JPEG files
        assert (
            "File:FileName" in result
        ), f"ExifTool should provide filename metadata for '{test_image}'. Available keys: {list(result.keys())}"
        assert (
            "File:FileType" in result
        ), f"ExifTool should provide file type metadata for '{test_image}'. Available keys: {list(result.keys())}"
        assert (
            result["File:FileName"] == test_image.name
        ), f"Expected filename '{test_image.name}' in metadata, but got '{result.get('File:FileName')}'"
        assert (
            result["File:FileType"] == "JPEG"
        ), f"Expected JPEG file type in metadata, but got '{result.get('File:FileType')}'"

    @pytest.mark.integration
    def test_get_dict_real_exiftool_file_not_found(self, tmp_path: Path) -> None:
        """Test get_dict with real ExifTool when file does not exist.

        This integration test verifies that get_dict correctly handles file not found
        errors when using the real ExifTool binary. This tests the real error handling
        path without mocking and ensures the function gracefully returns None rather
        than propagating exceptions for missing files.
        """
        # Arrange - Use a non-existent file path that would typically trigger FileNotFoundError
        non_existent_file = tmp_path / "does_not_exist.jpg"
        assert not non_existent_file.exists(), "Test file should not exist to properly test error handling"

        # Act - Call get_dict with non-existent file (real integration)
        result = get_dict(non_existent_file)

        # Assert - Verify ExifTool correctly handles missing file and returns None
        assert result is None, (
            f"Expected None for non-existent file '{non_existent_file}', but got {result}. "
            "The function should gracefully handle missing files by returning None."
        )
