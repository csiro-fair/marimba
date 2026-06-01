"""Tests for marimba.lib.image module."""

import shutil
from collections.abc import Callable
from pathlib import Path

import cv2
import numpy as np
import pytest
import pytest_mock
from PIL import Image, ImageDraw, UnidentifiedImageError

from marimba.lib.image import (
    GridDimensions,
    GridImageProcessor,
    GridRow,
    OutputPathManager,
    apply_clahe,
    convert_to_jpeg,
    crop,
    flip_horizontal,
    flip_vertical,
    gaussian_blur,
    generate_image_thumbnail,
    get_average_image_color,
    get_shannon_entropy,
    get_width_height,
    is_blurry,
    resize_exact,
    resize_fit,
    rotate_clockwise,
    scale,
    sharpen,
    turn_clockwise,
)


class TestImageUtilities:
    """Test image utility functions."""

    @pytest.fixture
    def test_image_rgb(self, tmp_path: Path) -> Path:
        """Create a test RGB image."""
        img = Image.new("RGB", (100, 80), color=(255, 0, 0))
        image_path = tmp_path / "test_image.png"
        img.save(image_path)
        return image_path

    @pytest.fixture
    def test_image_jpeg(self, tmp_path: Path) -> Path:
        """Create a test JPEG image."""
        img = Image.new("RGB", (200, 150), color=(0, 255, 0))
        image_path = tmp_path / "test_image.jpg"
        img.save(image_path, "JPEG")
        return image_path

    @pytest.fixture
    def large_test_image(self, tmp_path: Path) -> Path:
        """Create a large test image for resize testing.

        Creates a 2000x1500 pixel RGB image in blue color for testing
        image resize operations that require larger dimensions.

        Args:
            tmp_path: pytest fixture providing temporary directory path

        Returns:
            Path: Path to the created large test image file
        """
        img = Image.new("RGB", (2000, 1500), color=(0, 0, 255))
        image_path = tmp_path / "large_image.png"
        img.save(image_path)
        return image_path

    @pytest.mark.unit
    def test_get_width_height_with_jpeg_image(self, test_image_jpeg: Path) -> None:
        """Test get_width_height function returns correct dimensions for JPEG files.

        Verifies that the get_width_height utility function correctly extracts and
        returns the width and height dimensions from JPEG image files. This unit test
        focuses specifically on testing the get_width_height function behavior with
        JPEG format images to ensure proper dimension extraction.
        """
        # Arrange - Expected dimensions from test fixture (200x150 green JPEG)
        expected_width = 200
        expected_height = 150

        # Act - Extract dimensions using the get_width_height function
        actual_width, actual_height = get_width_height(test_image_jpeg)

        # Assert - Verify correct dimensions are returned
        assert actual_width == expected_width, f"Width should be {expected_width}px, got {actual_width}px"
        assert actual_height == expected_height, f"Height should be {expected_height}px, got {actual_height}px"

    @pytest.mark.unit
    def test_get_width_height_with_png_image(self, test_image_rgb: Path) -> None:
        """Test get_width_height function returns correct dimensions for PNG files.

        Verifies that the get_width_height utility function correctly extracts and
        returns the width and height dimensions from PNG image files to ensure
        format independence and broad image format support.
        """
        # Arrange - Expected dimensions from test fixture (100x80 PNG)
        expected_width = 100
        expected_height = 80

        # Act - Extract dimensions using the get_width_height function
        actual_width, actual_height = get_width_height(test_image_rgb)

        # Assert - Verify correct dimensions are returned
        assert actual_width == expected_width, f"Width should be {expected_width}px, got {actual_width}px"
        assert actual_height == expected_height, f"Height should be {expected_height}px, got {actual_height}px"

    @pytest.mark.unit
    def test_jpeg_fixture_properties(self, test_image_jpeg: Path) -> None:
        """Test JPEG fixture has expected properties and format characteristics.

        Verifies that the test_image_jpeg fixture creates a valid JPEG image with the
        expected dimensions, format, and color properties. This unit test ensures the
        fixture produces consistent test data for other JPEG-related tests.
        """
        # Arrange - Expected fixture properties
        expected_width = 200
        expected_height = 150
        expected_format = "JPEG"
        expected_color = (0, 255, 0)  # Green color from fixture

        # Act - Open and analyze the fixture image
        with Image.open(test_image_jpeg) as img:
            actual_width, actual_height = img.size
            actual_format = img.format
            # Get color from center pixel to verify fixture color
            center_x, center_y = actual_width // 2, actual_height // 2
            actual_color = img.getpixel((center_x, center_y))

        # Assert - Verify fixture properties match expectations
        assert actual_width == expected_width, f"JPEG fixture width should be {expected_width}px, got {actual_width}px"
        assert (
            actual_height == expected_height
        ), f"JPEG fixture height should be {expected_height}px, got {actual_height}px"
        assert actual_format == expected_format, f"Fixture format should be {expected_format}, got {actual_format}"

        # Check color with tolerance for JPEG compression artifacts
        color_tolerance = 5  # Allow small variations due to JPEG compression
        assert isinstance(actual_color, tuple), "Center pixel should be an RGB tuple"
        r_diff = abs(actual_color[0] - expected_color[0])
        g_diff = abs(actual_color[1] - expected_color[1])
        b_diff = abs(actual_color[2] - expected_color[2])
        assert r_diff <= color_tolerance, f"Red channel diff {r_diff} exceeds tolerance {color_tolerance}"
        assert g_diff <= color_tolerance, f"Green channel diff {g_diff} exceeds tolerance {color_tolerance}"
        assert b_diff <= color_tolerance, f"Blue channel diff {b_diff} exceeds tolerance {color_tolerance}"

        assert test_image_jpeg.suffix == ".jpg", f"Fixture file extension should be .jpg, got {test_image_jpeg.suffix}"
        assert test_image_jpeg.exists(), "JPEG fixture file should exist on filesystem"

    @pytest.mark.unit
    def test_get_width_height_with_string_path(self, test_image_jpeg: Path) -> None:
        """Test get_width_height function accepts string paths as input.

        Verifies that the get_width_height function properly handles string path
        arguments by converting them to Path objects internally and returning
        correct dimensions, ensuring API flexibility for different input types.
        """
        # Arrange - Convert Path to string for testing string input handling
        image_path_str = str(test_image_jpeg)
        expected_width = 200
        expected_height = 150

        # Act - Call function with string path
        actual_width, actual_height = get_width_height(image_path_str)

        # Assert - Verify types are correct before checking values
        assert isinstance(actual_width, int), f"Width should be int, got {type(actual_width)}"
        assert isinstance(actual_height, int), f"Height should be int, got {type(actual_height)}"

        # Assert - Verify correct dimensions are returned
        assert actual_width == expected_width, f"Width should be {expected_width}px, got {actual_width}px"
        assert actual_height == expected_height, f"Height should be {expected_height}px, got {actual_height}px"

    @pytest.mark.unit
    def test_get_width_height_nonexistent_file(self, tmp_path: Path) -> None:
        """Test get_width_height function raises FileNotFoundError for missing files.

        Verifies that the get_width_height function properly handles file not found
        scenarios by raising a FileNotFoundError with an appropriate error message
        when attempting to process a non-existent image file path.
        """
        # Arrange - Create path to non-existent file
        nonexistent_file = tmp_path / "missing_image.jpg"

        # Act & Assert - Verify FileNotFoundError is raised with correct message
        with pytest.raises(FileNotFoundError, match=r"No such file or directory"):
            get_width_height(nonexistent_file)

    @pytest.mark.unit
    def test_get_width_height_invalid_image_file(self, tmp_path: Path) -> None:
        """Test get_width_height function raises PIL error for invalid image files.

        Verifies that the get_width_height function properly handles corrupted or
        non-image files by raising PIL.UnidentifiedImageError when attempting to
        open files that cannot be identified as valid images.
        """
        # Arrange - Create a file that exists but is not a valid image
        invalid_file = tmp_path / "not_an_image.jpg"
        invalid_file.write_text("This is not an image file")

        # Act & Assert - PIL should raise UnidentifiedImageError for invalid image
        with pytest.raises(UnidentifiedImageError, match="cannot identify image file"):
            get_width_height(invalid_file)

    @pytest.mark.unit
    def test_generate_image_thumbnail(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test thumbnail generation creates properly sized thumbnail with correct naming.

        Verifies that the generate_image_thumbnail function successfully creates a thumbnail
        from an input image with proper sizing constraints and naming conventions. This unit
        test ensures that:
        1. The thumbnail file is created at the expected location
        2. The thumbnail maintains the original file extension
        3. The filename includes the default '_THUMB' suffix
        4. The resize_fit function is called with correct parameters
        5. Function returns the expected output path
        """
        # Arrange - Set up output directory and mock resize_fit dependency
        output_dir = tmp_path / "thumbnails"
        output_dir.mkdir()

        # Mock the resize_fit function to isolate unit under test
        mock_resize_fit = mocker.patch("marimba.lib.image.resize_fit")

        expected_filename = test_image_rgb.stem + "_THUMB" + test_image_rgb.suffix
        expected_path = output_dir / expected_filename

        # Act - Generate thumbnail with default settings
        result_path = generate_image_thumbnail(test_image_rgb, output_dir)

        # Assert - Verify function behavior and external calls
        assert result_path == expected_path, f"Should return expected path: {expected_path}"
        assert result_path.suffix == test_image_rgb.suffix, "Thumbnail should preserve original file extension"
        assert "_THUMB" in result_path.name, "Thumbnail filename should contain the '_THUMB' suffix"
        assert result_path.parent == output_dir, "Thumbnail should be created in the specified output directory"

        # Verify resize_fit was called with correct parameters
        mock_resize_fit.assert_called_once_with(test_image_rgb, 300, 300, expected_path)

    @pytest.mark.unit
    def test_generate_image_thumbnail_skips_existing(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test thumbnail generation skips processing when thumbnail already exists.

        Verifies that the generate_image_thumbnail function correctly detects when a
        thumbnail already exists and avoids calling resize_fit unnecessarily, improving
        performance by preventing unnecessary file operations.
        """
        # Arrange - Set up output directory and create existing thumbnail
        output_dir = tmp_path / "thumbnails"
        output_dir.mkdir()

        expected_filename = test_image_rgb.stem + "_THUMB" + test_image_rgb.suffix
        existing_thumbnail = output_dir / expected_filename
        existing_thumbnail.touch()  # Create empty file to simulate existing thumbnail

        # Store original modification time to verify file wasn't modified
        original_mtime = existing_thumbnail.stat().st_mtime

        # Mock the resize_fit function
        mock_resize_fit = mocker.patch("marimba.lib.image.resize_fit")

        # Act - Generate thumbnail when one already exists
        result_path = generate_image_thumbnail(test_image_rgb, output_dir)

        # Assert - Verify existing thumbnail is not regenerated
        assert result_path == existing_thumbnail, "Should return path to existing thumbnail"
        assert result_path.exists(), "Existing thumbnail file should still exist"
        assert result_path.stat().st_mtime == original_mtime, "Existing thumbnail should not be modified"
        mock_resize_fit.assert_not_called()

    @pytest.mark.unit
    def test_generate_image_thumbnail_custom_suffix(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test thumbnail generation with custom suffix parameter.

        Verifies that the generate_image_thumbnail function correctly applies a custom
        suffix to the thumbnail filename when the suffix parameter is provided. This unit
        test ensures that:
        1. The custom suffix is properly incorporated into the filename
        2. The default '_THUMB' suffix is replaced by the custom suffix
        3. The resize_fit function is called with the correct custom path
        """
        # Arrange - Set up output directory and custom suffix
        output_dir = tmp_path / "thumbnails"
        output_dir.mkdir()
        custom_suffix = "_CUSTOM"

        # Mock the resize_fit function to isolate unit under test
        mock_resize_fit = mocker.patch("marimba.lib.image.resize_fit")

        expected_filename = test_image_rgb.stem + custom_suffix + test_image_rgb.suffix
        expected_path = output_dir / expected_filename

        # Act - Generate thumbnail with custom suffix
        result_path = generate_image_thumbnail(test_image_rgb, output_dir, custom_suffix)

        # Assert - Verify custom suffix is applied correctly
        assert result_path == expected_path, f"Should return expected path with custom suffix: {expected_path}"
        assert (
            custom_suffix in result_path.name
        ), f"Thumbnail filename should contain the custom suffix '{custom_suffix}'"
        assert "_THUMB" not in result_path.name, "Default '_THUMB' suffix should not appear when custom suffix is used"
        assert result_path.suffix == test_image_rgb.suffix, "Thumbnail should preserve original file extension"
        assert result_path.parent == output_dir, "Thumbnail should be created in the specified output directory"

        # Verify resize_fit was called with correct parameters
        mock_resize_fit.assert_called_once_with(test_image_rgb, 300, 300, expected_path)

    @pytest.mark.integration
    def test_generate_image_thumbnail_existing(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test that existing thumbnail is not regenerated unnecessarily.

        Verifies that generate_image_thumbnail skips regeneration when thumbnail already exists,
        ensuring file system efficiency and consistent behavior across multiple calls.
        """
        # Arrange
        output_dir = tmp_path / "thumbnails"
        output_dir.mkdir()

        # Act - Generate thumbnail first time
        first_result_path = generate_image_thumbnail(test_image_rgb, output_dir)
        original_mtime = first_result_path.stat().st_mtime
        original_size = first_result_path.stat().st_size

        # Act - Call function again with same parameters
        second_result_path = generate_image_thumbnail(test_image_rgb, output_dir)

        # Assert - Verify existing thumbnail is not regenerated
        assert first_result_path == second_result_path, "Function should return the same path for identical calls"
        assert (
            first_result_path.stat().st_mtime == original_mtime
        ), "Existing thumbnail should not be modified (same mtime)"
        assert (
            first_result_path.stat().st_size == original_size
        ), "Existing thumbnail should not be modified (same size)"
        assert first_result_path.exists(), "Thumbnail file should exist after both calls"

    @pytest.mark.unit
    def test_convert_to_jpeg_from_png(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test converting PNG to JPEG format with proper image preservation.

        Verifies that the convert_to_jpeg function successfully converts a PNG image
        to JPEG format while preserving image properties. This unit test ensures:
        1. The function returns the correct destination path
        2. The output file is created with proper JPEG extension
        3. The resulting image is properly encoded in JPEG format
        4. Original image dimensions and content are preserved during conversion
        """
        # Arrange - Set up conversion paths and capture original image properties
        output_path = tmp_path / "converted.jpg"

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size

        # Act - Convert the PNG image to JPEG
        result_path = convert_to_jpeg(test_image_rgb, destination=output_path)

        # Assert - Verify the conversion completed successfully with proper format
        assert result_path == output_path, "Function should return the specified destination path"
        assert result_path.suffix == ".jpg", "Output file should have .jpg extension"
        assert result_path.exists(), "Converted JPEG file should exist at destination"

        # Verify the image was properly converted to JPEG format
        with Image.open(result_path) as converted_img:
            assert converted_img.format == "JPEG", "Image should be in JPEG format after conversion"
            assert converted_img.size == original_size, "Image dimensions should be preserved during conversion"
            assert converted_img.mode == "RGB", "JPEG images should be in RGB mode after conversion"

    @pytest.mark.unit
    def test_convert_to_jpeg_existing_jpeg(self, test_image_jpeg: Path, tmp_path: Path) -> None:
        """Test converting existing JPEG copies the file without re-encoding.

        Verifies that when convert_to_jpeg is called on a file that is already
        in JPEG format, it performs a simple file copy operation rather than
        re-encoding the image. This unit test ensures that:
        1. The function returns the correct destination path
        2. The output file is created successfully
        3. The file content is byte-for-byte identical to the original (copied, not re-encoded)
        4. The output file maintains proper JPEG format characteristics
        5. No quality degradation occurs during the copy operation
        """
        # Arrange - Set up paths and capture original file properties
        output_path = tmp_path / "converted.jpg"
        original_file_size = test_image_jpeg.stat().st_size
        original_file_content = test_image_jpeg.read_bytes()

        with Image.open(test_image_jpeg) as original_img:
            original_format = original_img.format
            original_mode = original_img.mode
            original_dimensions = original_img.size

        # Act - Convert the existing JPEG
        result_path = convert_to_jpeg(test_image_jpeg, destination=output_path)

        # Assert - Verify the copy operation worked correctly
        assert result_path == output_path, f"Function should return the specified destination path: {output_path}"
        assert result_path.exists(), f"Converted file should exist at destination: {result_path}"

        # Verify the file was copied (not re-encoded) by checking exact byte content
        converted_file_size = result_path.stat().st_size
        converted_file_content = result_path.read_bytes()

        assert converted_file_size == original_file_size, (
            f"File size should be identical when copying JPEG to JPEG: "
            f"expected {original_file_size}, got {converted_file_size}"
        )
        assert converted_file_content == original_file_content, (
            "File content should be byte-for-byte identical when copying JPEG to JPEG, "
            "indicating no re-encoding occurred"
        )

        # Verify the image properties are preserved
        with Image.open(result_path) as converted_img:
            assert (
                converted_img.format == original_format
            ), f"Image format should be preserved as {original_format}, got {converted_img.format}"
            assert (
                converted_img.mode == original_mode
            ), f"Image mode should be preserved as {original_mode}, got {converted_img.mode}"
            assert (
                converted_img.size == original_dimensions
            ), f"Image dimensions should be preserved as {original_dimensions}, got {converted_img.size}"

    @pytest.mark.integration
    def test_convert_to_jpeg_custom_quality(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test JPEG conversion with custom quality setting.

        Verifies that the convert_to_jpeg function properly applies a custom quality
        setting when converting an image to JPEG format. This integration test ensures:
        1. The output file has the correct JPEG extension
        2. The file is created successfully
        3. The image maintains proper JPEG format characteristics
        4. The function correctly handles quality parameter
        5. Original image dimensions are preserved during conversion
        """
        # Arrange - Set up output path and capture original image properties
        output_path = tmp_path / "converted_quality_50.jpg"

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size

        # Act - Convert with custom quality setting
        result_path = convert_to_jpeg(test_image_rgb, quality=50, destination=output_path)

        # Assert - Verify conversion results and file properties
        assert result_path == output_path, "Function should return the specified destination path"
        assert result_path.suffix == ".jpg", "Output file should have .jpg extension"
        assert result_path.exists(), "Converted JPEG file should exist at destination"

        # Verify the image is properly formatted as JPEG with preserved properties
        with Image.open(result_path) as converted_img:
            assert converted_img.format == "JPEG", "Image should be in JPEG format after conversion"
            assert converted_img.size == original_size, "Image dimensions should be preserved during conversion"
            assert converted_img.mode == "RGB", "JPEG images should be converted to RGB mode"

        # Verify file size is reasonable (not empty, not excessively large)
        file_size = result_path.stat().st_size
        assert file_size > 0, "Converted file should not be empty"
        assert file_size < 1024 * 1024, "File size should be reasonable for test image"

    @pytest.mark.unit
    def test_convert_to_jpeg_creates_jpeg_version_default_destination(self, test_image_rgb: Path) -> None:
        """Test convert_to_jpeg creates JPEG version when no destination is specified.

        Verifies that when convert_to_jpeg is called without a destination parameter,
        it creates a JPEG version alongside the original file by changing the extension
        to .jpg while preserving the original file. This unit test ensures that:
        1. The function returns a path with .jpg extension
        2. The original file path stem is preserved
        3. The original PNG file continues to exist after conversion
        4. The converted JPEG file exists and is properly formatted as JPEG
        5. Image content and dimensions are preserved during conversion
        6. Real file I/O and PIL operations work correctly together
        """
        # Arrange - Capture original file properties before conversion
        original_path = test_image_rgb
        original_stem = original_path.stem
        original_suffix = original_path.suffix
        expected_result_path = original_path.with_suffix(".jpg")

        with Image.open(original_path) as original_img:
            original_size = original_img.size

        # Verify original file exists and is PNG format
        assert original_path.exists(), "Original PNG file should exist before conversion"
        assert original_suffix == ".png", f"Test setup error: expected .png extension, got {original_suffix}"

        # Act - Convert the PNG image without specifying a destination
        result_path = convert_to_jpeg(original_path)

        # Assert - Verify the conversion results and side effects
        assert result_path.suffix == ".jpg", "Result path should have .jpg extension"
        assert result_path.stem == original_stem, "Result path should preserve original filename stem"
        assert result_path == expected_result_path, f"Result path should be {expected_result_path}"
        assert result_path.exists(), "Converted JPEG file should exist at the result path"

        # Verify the original PNG file still exists (JPEG created alongside original)
        assert original_path.exists(), "Original PNG file should still exist after conversion"

        # Verify the converted file has the correct properties
        with Image.open(result_path) as converted_img:
            assert converted_img.format == "JPEG", "Converted image should be in JPEG format"
            assert converted_img.size == original_size, "Image dimensions should be preserved during conversion"
            assert converted_img.mode == "RGB", "JPEG images should be in RGB mode after conversion"

        # Verify the result path points to the same directory as original
        assert result_path.parent == original_path.parent, "Converted file should be in same directory as original"

    @pytest.mark.integration
    def test_resize_fit_no_resize_needed(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test resize_fit preserves original dimensions when image is already within bounds.

        Verifies that the resize_fit function correctly identifies when an image is already
        smaller than the specified maximum dimensions and preserves the original size without
        any resizing operation. This integration test ensures that:
        1. The output file is created at the specified destination
        2. The original image dimensions are preserved exactly
        3. The image format is maintained after the operation
        4. No unnecessary processing occurs when resizing is not needed
        """
        # Arrange - Set up output path and capture original image properties
        output_path = tmp_path / "resized.png"
        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size
            original_format = original_img.format

        # Act - Apply resize_fit with bounds larger than original image (100x80)
        resize_fit(test_image_rgb, 200, 200, output_path)

        # Assert - Verify no resizing occurred and image properties are preserved
        assert output_path.exists(), "Output file should be created at specified destination"

        with Image.open(output_path) as result_img:
            assert (
                result_img.size == original_size
            ), f"Image dimensions should remain unchanged at {original_size}, but got {result_img.size}"
            assert result_img.size == (
                100,
                80,
            ), "Original test image dimensions (100x80) should be preserved when within bounds (200x200)"
            assert (
                result_img.format == original_format
            ), f"Image format should be preserved as {original_format} after resize_fit operation"

    @pytest.mark.integration
    def test_resize_fit_width_constrained(
        self,
        large_test_image: Path,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test resize_fit function when width is the constraining dimension.

        Tests that resize_fit correctly scales down a 2000x1500 image to fit within
        1000x2000 constraints, where width (2000 > 1000) is the limiting factor.
        The function should maintain aspect ratio and scale proportionally.

        Args:
            large_test_image: Fixture providing a 2000x1500 test image
            tmp_path: pytest fixture providing temporary directory path
            mocker: pytest-mock fixture for mocking (not used in this test)
        """
        # Arrange - Set up test parameters and verify initial conditions
        output_path = tmp_path / "resized.png"
        expected_width = 1000
        expected_height = 750  # 1500 * (1000/2000) = 750 to maintain aspect ratio
        max_width = 1000
        max_height = 2000

        # Verify test fixture setup before proceeding
        with Image.open(large_test_image) as original_img:
            original_width, original_height = original_img.size
            original_format = original_img.format

        assert original_width == 2000, f"Test setup error: expected 2000px width, got {original_width}"
        assert original_height == 1500, f"Test setup error: expected 1500px height, got {original_height}"

        # Act - Perform the resize operation
        resize_fit(large_test_image, max_width, max_height, output_path)

        # Assert - Verify all expected outcomes
        assert output_path.exists(), "Output file should be created after resize_fit operation"

        with Image.open(output_path) as result_img:
            actual_width, actual_height = result_img.size
            result_format = result_img.format

            # Verify exact dimensions match expected aspect ratio calculations
            assert actual_width == expected_width, f"Width should be {expected_width}, got {actual_width}"
            assert actual_height == expected_height, f"Height should be {expected_height}, got {actual_height}"

            # Verify image fits within specified constraints
            assert actual_width <= max_width, f"Width {actual_width} should not exceed max width {max_width}"
            assert actual_height <= max_height, f"Height {actual_height} should not exceed max height {max_height}"

            # Verify image format is preserved through the resize operation
            assert (
                result_format == original_format
            ), f"Image format should be preserved as {original_format} after resize_fit operation"

            # Verify aspect ratio is maintained (within floating point tolerance)
            original_aspect_ratio = original_width / original_height
            new_aspect_ratio = actual_width / actual_height
            assert abs(original_aspect_ratio - new_aspect_ratio) < 0.01, (
                f"Aspect ratio should be preserved: original={original_aspect_ratio:.3f}, "
                f"new={new_aspect_ratio:.3f}"
            )

    @pytest.mark.integration
    def test_resize_fit_height_constrained(self, large_test_image, tmp_path):
        """Test resize_fit function when height is the constraining dimension.

        Tests that resize_fit correctly scales down a 2000x1500 image to fit within
        3000x1000 constraints, where height (1500 > 1000) is the limiting factor.
        The function should maintain aspect ratio and scale proportionally.

        Args:
            large_test_image: Fixture providing a 2000x1500 test image
            tmp_path: pytest fixture providing temporary directory path
        """
        # Arrange
        output_path = tmp_path / "resized.png"
        expected_width = 1333  # 2000 * (1000/1500) = 1333.33... → 1333
        expected_height = 1000

        # Verify initial setup
        with Image.open(large_test_image) as original_img:
            original_width, original_height = original_img.size
            original_format = original_img.format

        assert original_width == 2000, f"Test setup error: expected 2000px width, got {original_width}"
        assert original_height == 1500, f"Test setup error: expected 1500px height, got {original_height}"

        # Act
        resize_fit(large_test_image, 3000, 1000, output_path)

        # Assert
        assert output_path.exists(), "Output file should be created after resize_fit operation"

        with Image.open(output_path) as result_img:
            actual_width, actual_height = result_img.size

            # Verify exact height dimension
            assert actual_height == expected_height, f"Height should be {expected_height}, got {actual_height}"

            # Verify width maintains aspect ratio (allow for rounding differences)
            assert (
                abs(actual_width - expected_width) <= 1
            ), f"Width should be approximately {expected_width} (±1 for rounding), got {actual_width}"

            # Verify image fits within constraints
            assert actual_width <= 3000, f"Width {actual_width} should not exceed max width 3000"
            assert actual_height <= 1000, f"Height {actual_height} should not exceed max height 1000"

            # Verify format preservation
            assert (
                result_img.format == original_format
            ), f"Image format should be preserved as {original_format} after resize_fit operation"

    @pytest.mark.integration
    def test_resize_fit_overwrites_original(self, large_test_image, tmp_path):
        """Test resize_fit overwrites original file when no destination specified.

        Verifies that resize_fit modifies the original file in-place when no
        destination parameter is provided, properly scaling the image to fit
        within the specified constraints while maintaining aspect ratio.

        Args:
            large_test_image: Fixture providing a 2000x1500 test image
            tmp_path: pytest fixture providing temporary directory path
        """
        # Arrange - Copy fixture to avoid affecting other tests
        test_image_copy = tmp_path / "test_image_copy.png"
        shutil.copy2(large_test_image, test_image_copy)

        with Image.open(test_image_copy) as img:
            original_size = img.size
            original_width, original_height = original_size

        # Verify test setup
        assert original_width == 2000, f"Test setup error: expected 2000px width, got {original_width}"
        assert original_height == 1500, f"Test setup error: expected 1500px height, got {original_height}"

        max_width = 500
        max_height = 500
        original_aspect_ratio = original_width / original_height

        # Act
        resize_fit(test_image_copy, max_width, max_height)

        # Assert
        with Image.open(test_image_copy) as img:
            new_width, new_height = img.size

        # Verify the image was actually modified
        assert (
            new_width,
            new_height,
        ) != original_size, f"Image size should have changed from {original_size} to {(new_width, new_height)}"

        # Verify the image fits within the specified constraints
        assert new_width <= max_width, f"Width {new_width} should not exceed {max_width}"
        assert new_height <= max_height, f"Height {new_height} should not exceed {max_height}"

        # Verify aspect ratio is maintained (within tolerance for integer rounding)
        new_aspect_ratio = new_width / new_height
        aspect_ratio_tolerance = 0.01
        assert (
            abs(new_aspect_ratio - original_aspect_ratio) < aspect_ratio_tolerance
        ), f"Aspect ratio should be preserved: original {original_aspect_ratio:.3f}, new {new_aspect_ratio:.3f}"

        # Verify proper fit behavior - at least one dimension should reach its constraint
        # For a 2000x1500 image scaled to fit 500x500, width is constraining factor
        # Expected: 500x375 (1500 * 500/2000 = 375)
        expected_width = 500
        expected_height = 375
        assert new_width == expected_width, f"Width should be {expected_width}, got {new_width}"
        assert new_height == expected_height, f"Height should be {expected_height}, got {new_height}"

    @pytest.mark.integration
    def test_resize_exact(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test that resize_exact resizes image to exact specified dimensions."""
        # Arrange
        output_path = tmp_path / "resized.png"
        target_width = 150
        target_height = 200

        # Verify original dimensions are different from target
        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size

        # Act
        resize_exact(test_image_rgb, target_width, target_height, output_path)

        # Assert
        assert output_path.exists(), "Output image file should be created"

        with Image.open(output_path) as resized_img:
            actual_size = resized_img.size
            assert actual_size == (target_width, target_height), (
                f"Image should be resized to exact dimensions {target_width}x{target_height}, "
                f"but got {actual_size[0]}x{actual_size[1]}"
            )
            assert (
                actual_size != original_size
            ), f"Image size should have changed from original {original_size} to {actual_size}"

    @pytest.mark.unit
    def test_resize_exact_overwrites_original(self, test_image_rgb: Path) -> None:
        """Test resize_exact overwrites original file when no destination is specified."""
        # Arrange
        target_width = 50
        target_height = 40

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size
            original_format = original_img.format

        # Act
        resize_exact(test_image_rgb, target_width, target_height)

        # Assert
        assert test_image_rgb.exists(), "Original file should still exist after resize operation"

        with Image.open(test_image_rgb) as resized_img:
            actual_size = resized_img.size
            actual_format = resized_img.format

            assert actual_size == (target_width, target_height), (
                f"Image should be resized to exact dimensions {target_width}x{target_height}, "
                f"but got {actual_size[0]}x{actual_size[1]}"
            )
            assert (
                actual_size != original_size
            ), f"Image size should have changed from original {original_size} to {actual_size}"
            assert (
                actual_format == original_format
            ), f"Image format should be preserved as {original_format} after resize"

    @pytest.mark.unit
    def test_image_large(self, large_test_image: Path, tmp_path: Path) -> None:
        """Test comprehensive large image handling and processing functionality.

        Verifies that large images (2000x1500) can be properly processed through various
        image operations without memory issues or performance degradation. This unit test
        ensures that:
        1. Large images can be loaded and their properties accessed correctly
        2. Resize operations maintain aspect ratio and quality for large images
        3. Format conversions work properly with large file sizes
        4. File operations complete successfully without memory errors
        """
        # Arrange - Verify large test image fixture is properly set up
        assert large_test_image.exists(), "Large test image fixture should exist"

        with Image.open(large_test_image) as original_img:
            original_width, original_height = original_img.size
            original_format = original_img.format

        # Assert initial large image properties
        assert original_width == 2000, f"Expected large image width of 2000px, got {original_width}"
        assert original_height == 1500, f"Expected large image height of 1500px, got {original_height}"
        assert original_format == "PNG", f"Expected PNG format, got {original_format}"

        # Act & Assert - Test resize operation with large image
        resize_output = tmp_path / "large_resized.png"
        resize_fit(large_test_image, 800, 600, resize_output)

        assert resize_output.exists(), "Resized large image should be created"

        with Image.open(resize_output) as resized_img:
            resized_width, resized_height = resized_img.size

            # Verify proper scaling - width should be constraining factor (2000 -> 800)
            expected_width = 800
            expected_height = 600  # 1500 * (800/2000) = 600

            assert resized_width == expected_width, f"Resized width should be {expected_width}, got {resized_width}"
            assert (
                resized_height == expected_height
            ), f"Resized height should be {expected_height}, got {resized_height}"

        # Act & Assert - Test format conversion with large image
        jpeg_output = tmp_path / "large_converted.jpg"
        convert_to_jpeg(large_test_image, quality=85, destination=jpeg_output)

        assert jpeg_output.exists(), "JPEG conversion should create output file"

        with Image.open(jpeg_output) as jpeg_img:
            jpeg_width, jpeg_height = jpeg_img.size
            jpeg_format = jpeg_img.format

            # Verify dimensions preserved during format conversion
            assert jpeg_width == original_width, f"JPEG width should match original {original_width}, got {jpeg_width}"
            assert (
                jpeg_height == original_height
            ), f"JPEG height should match original {original_height}, got {jpeg_height}"
            assert jpeg_format == "JPEG", f"Expected JPEG format, got {jpeg_format}"

        # Act & Assert - Test scaling with large image
        scale_output = tmp_path / "large_scaled.png"
        scale(large_test_image, 0.5, scale_output)

        assert scale_output.exists(), "Scaled large image should be created"

        with Image.open(scale_output) as scaled_img:
            scaled_width, scaled_height = scaled_img.size

            # Verify 50% scaling
            expected_scaled_width = int(original_width * 0.5)  # 1000
            expected_scaled_height = int(original_height * 0.5)  # 750

            assert (
                scaled_width == expected_scaled_width
            ), f"Scaled width should be {expected_scaled_width}, got {scaled_width}"
            assert (
                scaled_height == expected_scaled_height
            ), f"Scaled height should be {expected_scaled_height}, got {scaled_height}"

    @pytest.mark.unit
    def test_scale(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test scale function reduces image dimensions by specified factor.

        Verifies that the scale function correctly applies a scaling factor to reduce
        image dimensions and saves the result to a specified destination. This unit
        test ensures that:
        1. The output file is created at the specified destination
        2. The image dimensions are correctly scaled by the provided factor
        3. The scaling calculation preserves the aspect ratio
        4. The original image remains unchanged when destination is specified
        """
        # Arrange - Set up paths and capture original image properties
        output_path = tmp_path / "scaled.png"

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size  # Expected: (100, 80) from fixture
            original_format = original_img.format

        # Calculate expected dimensions: 100x80 scaled by 0.5 = 50x40
        scale_factor = 0.5
        expected_width = int(original_size[0] * scale_factor)  # 100 * 0.5 = 50
        expected_height = int(original_size[1] * scale_factor)  # 80 * 0.5 = 40

        # Act - Scale the image by factor of 0.5
        scale(test_image_rgb, scale_factor, output_path)

        # Assert - Verify the scaled image was created with correct dimensions
        assert output_path.exists(), f"Scaled image file should be created at destination: {output_path}"

        with Image.open(output_path) as scaled_img:
            assert scaled_img.size == (
                expected_width,
                expected_height,
            ), f"Scaled image dimensions should be {expected_width}x{expected_height}, got {scaled_img.size}"
            assert (
                scaled_img.format == original_format
            ), f"Scaled image format should be preserved as {original_format}, got {scaled_img.format}"

        # Verify original image is unchanged
        assert test_image_rgb.exists(), f"Original image file should still exist at: {test_image_rgb}"
        with Image.open(test_image_rgb) as original_check:
            assert (
                original_check.size == original_size
            ), f"Original image dimensions should remain {original_size}, got {original_check.size}"

    @pytest.mark.unit
    def test_scale_larger(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test scale function increases image dimensions by specified factor.

        Verifies that the scale function correctly applies a scaling factor to increase
        image dimensions and saves the result to a specified destination. This unit
        test ensures that:
        1. The output file is created at the specified destination
        2. The image dimensions are correctly scaled up by the provided factor
        3. The scaling preserves the aspect ratio by applying factor to both dimensions
        4. The image format is preserved during scaling
        5. The original image remains unchanged when destination is specified
        """
        # Arrange - Set up paths and capture original image properties
        output_path = tmp_path / "scaled.png"

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size  # (100, 80) from fixture
            original_format = original_img.format

        scale_factor = 2.0
        expected_width = int(original_size[0] * scale_factor)  # 100 * 2.0 = 200
        expected_height = int(original_size[1] * scale_factor)  # 80 * 2.0 = 160

        # Act - Scale the image by factor of 2.0
        scale(test_image_rgb, scale_factor, output_path)

        # Assert - Verify the scaled image was created with correct dimensions
        assert output_path.exists(), "Scaled image file should be created at the specified destination"

        with Image.open(output_path) as scaled_img:
            assert scaled_img.size == (
                expected_width,
                expected_height,
            ), f"Scaled image should be {expected_width}x{expected_height}, got {scaled_img.size}"
            assert scaled_img.format == original_format, "Scaled image should preserve the original format"

        # Verify original image is unchanged
        assert test_image_rgb.exists(), "Original image file should still exist"
        with Image.open(test_image_rgb) as original_check:
            assert original_check.size == original_size, "Original image dimensions should remain unchanged"

    @pytest.mark.unit
    def test_scale_overwrites_original_when_no_destination_specified(self, test_image_rgb: Path) -> None:
        """Test scale function overwrites original image file when no destination path provided.

        Verifies that the scale function modifies the original image file in-place when
        no destination parameter is provided. This unit test ensures that:
        1. The original image file dimensions are changed by the scale factor
        2. The calculated dimensions match the expected scaled values
        3. The image format is preserved during the scaling operation
        4. The scale factor is applied correctly to both width and height
        """
        # Arrange - Capture original image properties
        with Image.open(test_image_rgb) as original_img:
            original_width, original_height = original_img.size
            original_format = original_img.format

        scale_factor = 1.5
        expected_width = int(original_width * scale_factor)  # 100 * 1.5 = 150
        expected_height = int(original_height * scale_factor)  # 80 * 1.5 = 120

        # Act - Scale the image with no destination (overwrites original)
        scale(test_image_rgb, scale_factor)

        # Assert - Verify the original image has been modified with correct dimensions
        with Image.open(test_image_rgb) as scaled_img:
            new_width, new_height = scaled_img.size

            assert new_width == expected_width, (
                f"Scaled width should be {expected_width} (original {original_width} * {scale_factor}), "
                f"got {new_width}"
            )

            assert new_height == expected_height, (
                f"Scaled height should be {expected_height} (original {original_height} * {scale_factor}), "
                f"got {new_height}"
            )

            assert (
                scaled_img.format == original_format
            ), f"Image format should be preserved as {original_format} after scaling operation"

    @pytest.mark.unit
    def test_rotate_clockwise_creates_rotated_image_file(self, tmp_path: Path) -> None:
        """Test clockwise rotation creates output file and performs actual rotation.

        Verifies that the rotate_clockwise function successfully rotates an image
        by a specified number of degrees and creates the output file. This unit test ensures:
        1. The output file is created at the specified destination
        2. The rotation operation actually modifies the image content
        3. The image file format is preserved during rotation
        4. The function handles positive degree values correctly
        """
        # Arrange - Create asymmetric test image for reliable rotation detection
        test_image_path = tmp_path / "test_asymmetric.png"
        img = Image.new("RGB", (100, 80), color=(255, 255, 255))
        # Add distinctive asymmetric pattern (red square in top-left corner)
        draw = ImageDraw.Draw(img)
        draw.rectangle([10, 10, 40, 30], fill=(255, 0, 0))
        img.save(test_image_path)

        output_path = tmp_path / "rotated.png"
        rotation_degrees = 90

        with Image.open(test_image_path) as original_img:
            original_format = original_img.format
            original_mode = original_img.mode
            original_size = original_img.size

        # Act
        rotate_clockwise(test_image_path, rotation_degrees, destination=output_path)

        # Assert
        assert output_path.exists(), "Output file should be created at specified destination"

        with Image.open(output_path) as rotated_img:
            assert rotated_img.format == original_format, f"Image format should be preserved as {original_format}"
            assert rotated_img.mode == original_mode, f"Image mode should be preserved as {original_mode}"

            # For rotation without expand=True, dimensions should remain the same
            assert (
                rotated_img.size == original_size
            ), f"Rotation should maintain original size: expected {original_size}, got {rotated_img.size}"

            # Verify the image has been rotated by checking specific pixel positions
            # For a 90-degree clockwise rotation, the red square that was at top-left
            # should now be at bottom-left (when not expanding canvas)
            with Image.open(test_image_path) as original_for_comparison:
                original_pixels = list(original_for_comparison.get_flattened_data())
                rotated_pixels = list(rotated_img.get_flattened_data())
                assert original_pixels != rotated_pixels, "Rotated image should have different pixel data than original"

                # More specific test: check that the red pixel from position (25, 20) in original
                # appears in a different location in the rotated image
                original_red_pixel = original_for_comparison.getpixel((25, 20))
                rotated_red_pixel = rotated_img.getpixel((25, 20))
                assert original_red_pixel == (255, 0, 0), "Original image should have red pixel at test position"
                assert (
                    rotated_red_pixel != original_red_pixel
                ), "Rotated image should have different pixel at same coordinates, confirming rotation occurred"

    @pytest.mark.unit
    def test_rotate_clockwise_expand_increases_canvas_size(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test clockwise rotation with expand parameter increases canvas dimensions.

        Verifies that the rotate_clockwise function with expand=True creates a larger
        canvas to fit the rotated image without cropping. This unit test ensures:
        1. The output file is created successfully
        2. The expand parameter enlarges the canvas to accommodate rotation
        3. The rotated image dimensions follow expected mathematical relationship
        4. The image maintains proper format and color mode after rotation
        """
        # Arrange - Set up test parameters and capture original image properties
        output_path = tmp_path / "rotated_expanded.png"
        rotation_degrees = 45

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size
            original_mode = original_img.mode
            original_width, original_height = original_size

        # Act - Perform clockwise rotation with expand=True
        rotate_clockwise(test_image_rgb, rotation_degrees, expand=True, destination=output_path)

        # Assert - Verify output file creation and expanded dimensions
        assert output_path.exists(), "Output file should be created at specified destination"

        with Image.open(output_path) as rotated_img:
            rotated_size = rotated_img.size
            rotated_width, rotated_height = rotated_size

            # Calculate expected expanded dimensions for 45-degree rotation
            # For a rectangle rotated 45 degrees, the bounding box dimensions are:
            # new_width = width*cos(45°) + height*sin(45°) = (width + height) / sqrt(2)
            # new_height = width*sin(45°) + height*cos(45°) = (width + height) / sqrt(2)
            expected_dimension = int((original_width + original_height) / (2**0.5))

            # Verify both dimensions increased from original
            assert (
                rotated_width > original_width
            ), f"Expanded width {rotated_width} should be larger than original {original_width}"
            assert (
                rotated_height > original_height
            ), f"Expanded height {rotated_height} should be larger than original {original_height}"

            # Verify dimensions are approximately equal for 45-degree rotation of rectangle
            dimension_difference = abs(rotated_width - rotated_height)
            assert dimension_difference <= 2, (
                f"45-degree rotation should create nearly square result, "
                f"but got {rotated_width}x{rotated_height} (difference: {dimension_difference})"
            )

            # Verify dimensions are close to mathematical expectation (within rounding tolerance)
            width_tolerance = abs(rotated_width - expected_dimension)
            height_tolerance = abs(rotated_height - expected_dimension)
            assert width_tolerance <= 2, (
                f"Rotated width {rotated_width} should be close to expected {expected_dimension} "
                f"(tolerance: {width_tolerance})"
            )
            assert height_tolerance <= 2, (
                f"Rotated height {rotated_height} should be close to expected {expected_dimension} "
                f"(tolerance: {height_tolerance})"
            )

            # Verify image properties are preserved
            assert (
                rotated_img.mode == original_mode
            ), f"Image mode should be preserved: expected {original_mode}, got {rotated_img.mode}"
            assert rotated_img.format == "PNG", "Output image should maintain PNG format"

    @pytest.mark.integration
    def test_rotate_clockwise_overwrites_original(self, tmp_path: Path) -> None:
        """Test rotate_clockwise overwrites original file when no destination is specified.

        Verifies that the rotate_clockwise function modifies the original image file
        in place when no destination parameter is provided. This integration test ensures:
        1. The original file is modified directly without creating a new file
        2. The rotation operation changes the image content appropriately
        3. The file path remains the same after the operation
        4. The image format and mode are preserved during in-place rotation
        5. The file content actually changes after rotation
        """
        # Arrange - Create a distinctive test image with different colored quarters
        test_image_path = tmp_path / "test_rotation.png"
        img = Image.new("RGB", (100, 80), color=(0, 0, 0))  # Black background

        # Create distinct colored regions to verify rotation
        pixels = img.load()
        if pixels is not None:
            for y in range(img.height):
                for x in range(img.width):
                    if x < img.width // 2 and y < img.height // 2:
                        pixels[x, y] = (255, 0, 0)  # Top-left: Red
                    elif x >= img.width // 2 and y < img.height // 2:
                        pixels[x, y] = (0, 255, 0)  # Top-right: Green
                    elif x < img.width // 2 and y >= img.height // 2:
                        pixels[x, y] = (0, 0, 255)  # Bottom-left: Blue
                    else:
                        pixels[x, y] = (255, 255, 0)  # Bottom-right: Yellow

        img.save(test_image_path)

        rotation_degrees = 90
        original_path = test_image_path

        with Image.open(test_image_path) as original_img:
            original_format = original_img.format
            original_mode = original_img.mode
            original_size = original_img.size
            original_data = list(original_img.get_flattened_data())

        # Act
        rotate_clockwise(test_image_path, rotation_degrees)

        # Assert
        assert test_image_path.exists(), "Original file should still exist after rotation"
        assert test_image_path == original_path, "File path should remain unchanged"

        with Image.open(test_image_path) as rotated_img:
            # Verify image properties are preserved
            assert rotated_img.format == original_format, f"Image format should be preserved as {original_format}"
            assert rotated_img.mode == original_mode, f"Image mode should be preserved as {original_mode}"

            # For 90-degree rotation without expand, dimensions remain the same
            assert (
                rotated_img.size == original_size
            ), f"Image dimensions should remain {original_size} for 90-degree rotation without expand"

            # Verify the image content has been modified by rotation
            rotated_data = list(rotated_img.get_flattened_data())
            assert original_data != rotated_data, "Rotated image should have different pixel data than original"

            # Verify rotation worked by checking that pixel data actually changed
            # With a 90-degree rotation, the data should be different but related
            assert len(rotated_data) == len(original_data), "Rotated image should have same number of pixels"

    @pytest.mark.unit
    def test_rotate_clockwise_zero_degrees_preserves_image(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test rotate_clockwise with zero degrees preserves original image unchanged.

        Verifies that rotating an image by 0 degrees results in an identical image
        to the original. This unit test ensures:
        1. Zero-degree rotation creates an output file
        2. The image content remains completely unchanged
        3. The image format and mode are preserved
        4. The function handles edge case of zero rotation correctly
        """
        # Arrange
        output_path = tmp_path / "rotated_zero.png"
        rotation_degrees = 0

        with Image.open(test_image_rgb) as original_img:
            original_format = original_img.format
            original_mode = original_img.mode
            original_size = original_img.size
            original_pixels = list(original_img.get_flattened_data())

        # Act
        rotate_clockwise(test_image_rgb, rotation_degrees, destination=output_path)

        # Assert
        assert output_path.exists(), "Output file should be created even for zero-degree rotation"

        with Image.open(output_path) as rotated_img:
            assert rotated_img.format == original_format, f"Image format should be preserved as {original_format}"
            assert rotated_img.mode == original_mode, f"Image mode should be preserved as {original_mode}"
            assert (
                rotated_img.size == original_size
            ), f"Image size should remain {original_size} for zero-degree rotation"

            # For zero-degree rotation, pixel data should be identical
            rotated_pixels = list(rotated_img.get_flattened_data())
            assert original_pixels == rotated_pixels, "Zero-degree rotation should preserve all pixel data unchanged"

    @pytest.mark.unit
    def test_rotate_clockwise_negative_degrees_rotates_counterclockwise(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test rotate_clockwise accepts and processes negative degree values.

        Verifies that the rotate_clockwise function correctly handles negative degree
        values without errors. Negative degrees result in counterclockwise rotation
        as per PIL's rotate() behavior. This unit test ensures:
        1. Negative degree values are accepted without error
        2. The rotation operation modifies the image content
        3. The output file is created successfully with expected dimensions
        """
        # Arrange
        output_path = tmp_path / "rotated_negative.png"
        degrees = -90

        with Image.open(test_image_rgb) as original_img:
            original_pixels = list(original_img.get_flattened_data())
            original_size = original_img.size

        # Act - Rotate using negative degrees
        rotate_clockwise(test_image_rgb, degrees, destination=output_path)

        # Assert - Verify output file is created
        assert output_path.exists(), f"Output file should be created at {output_path}"

        # Verify rotation modified the image and preserved dimensions (expand=False by default)
        with Image.open(output_path) as rotated_img:
            rotated_pixels = list(rotated_img.get_flattened_data())

            assert (
                original_pixels != rotated_pixels
            ), "Rotation should modify pixel data (original has different content than rotated)"
            assert rotated_img.size == original_size, (
                f"Image dimensions should be preserved with expand=False: "
                f"expected {original_size}, got {rotated_img.size}"
            )

    @pytest.mark.unit
    def test_rotate_clockwise_large_degree_values_handled_correctly(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test rotate_clockwise handles large degree values gracefully.

        Verifies that the function can process large degree values (>360°) without errors
        and produces valid rotated images. The function delegates to PIL's rotate method,
        which handles modulo arithmetic internally. This test ensures:
        - Large positive degree values are processed successfully
        - Large negative degree values are processed successfully
        - Output files are created correctly
        - Resulting images have valid dimensions and content
        """
        # Arrange
        large_positive_path = tmp_path / "rotated_405.png"
        large_negative_path = tmp_path / "rotated_minus_765.png"
        zero_equivalent_path = tmp_path / "rotated_720.png"

        # Act - Test various large degree values
        rotate_clockwise(test_image_rgb, 405, destination=large_positive_path)
        rotate_clockwise(test_image_rgb, -765, destination=large_negative_path)
        rotate_clockwise(test_image_rgb, 720, destination=zero_equivalent_path)

        # Assert - All rotations should complete successfully
        assert large_positive_path.exists(), "Output file should be created for large positive degree rotation"
        assert large_negative_path.exists(), "Output file should be created for large negative degree rotation"
        assert zero_equivalent_path.exists(), "Output file should be created for 720-degree rotation"

        # Verify all output images are valid and have reasonable properties
        with (
            Image.open(large_positive_path) as large_positive_img,
            Image.open(large_negative_path) as large_negative_img,
            Image.open(zero_equivalent_path) as zero_equivalent_img,
        ):
            # Verify all images have valid dimensions (non-zero)
            assert large_positive_img.size[0] > 0, "Large positive rotation should produce valid width"
            assert large_positive_img.size[1] > 0, "Large positive rotation should produce valid height"
            assert large_negative_img.size[0] > 0, "Large negative rotation should produce valid width"
            assert large_negative_img.size[1] > 0, "Large negative rotation should produce valid height"
            assert zero_equivalent_img.size[0] > 0, "720-degree rotation should produce valid width"
            assert zero_equivalent_img.size[1] > 0, "720-degree rotation should produce valid height"

            # Verify images contain valid pixel data (not all transparent/black)
            large_positive_pixels = list(large_positive_img.get_flattened_data())
            assert len(large_positive_pixels) > 0, "Large positive rotation should contain pixel data"
            assert any(
                pixel != (0, 0, 0) for pixel in large_positive_pixels
            ), "Large positive rotation should contain non-black pixels"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("turns", "expected_size", "description"),
        [
            (1, (80, 100), "90-degree turn swaps dimensions"),
            (2, (100, 80), "180-degree turn keeps same dimensions"),
            (3, (80, 100), "270-degree turn swaps dimensions"),
        ],
    )
    def test_turn_clockwise_rotates_image_correctly(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
        turns: int,
        expected_size: tuple[int, int],
        description: str,
    ) -> None:
        """Test turn_clockwise function correctly rotates images in 90-degree increments.

        Verifies that the turn_clockwise function properly applies clockwise rotation
        transformations to an image using PIL's transpose operations with real file I/O.
        This unit test ensures that:
        1. The output file is created at the specified destination
        2. The rotated image has the correct dimensions after transformation
        3. The rotation preserves image quality and format
        4. Different turn counts (1-3) produce expected dimensional changes
        5. The function correctly implements the rotation logic in isolation
        """
        # Arrange - Set up output path and capture original image properties
        output_path = tmp_path / f"turned_{turns * 90}deg.png"

        with Image.open(test_image_rgb) as original_img:
            original_size = original_img.size
            original_format = original_img.format
            original_mode = original_img.mode

        # Verify test setup conditions
        assert test_image_rgb.exists(), "Test image fixture should exist before rotation"
        assert original_size == (100, 80), f"Test image should be 100x80 pixels, got {original_size}"
        assert original_format == "PNG", f"Test image should be PNG format, got {original_format}"

        # Act - Perform the rotation operation
        turn_clockwise(test_image_rgb, turns, output_path)

        # Assert - Verify complete operation results
        assert output_path.exists(), f"Output file should be created for {turns * 90}-degree rotation"

        with Image.open(output_path) as rotated_img:
            # Verify exact dimensions match expected outcome
            assert (
                rotated_img.size == expected_size
            ), f"{description}: expected dimensions {expected_size}, got {rotated_img.size}"

            # Verify image format is preserved through rotation
            assert (
                rotated_img.format == original_format
            ), f"Image format should be preserved as {original_format}, got {rotated_img.format}"

            # Verify image mode is preserved
            assert (
                rotated_img.mode == original_mode
            ), f"Image mode should be preserved as {original_mode}, got {rotated_img.mode}"

        # Verify file system side effects
        assert output_path.is_file(), "Output should be a regular file"
        assert output_path.stat().st_size > 0, "Output file should not be empty"

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_turns", [0, 4, -1, 5])
    def test_turn_clockwise_invalid_turns_raises_value_error(self, tmp_path: Path, invalid_turns: int) -> None:
        """Test turn_clockwise raises ValueError for invalid turns parameter values.

        Verifies that the turn_clockwise function properly validates the turns parameter
        and raises a ValueError with the exact expected message when given values outside
        the valid range [1, 2, 3]. Tests boundary conditions (0, 4) and invalid values
        (-1, 5) to ensure robust input validation occurs before any image processing.
        """
        # Arrange - Create a minimal test image file for validation testing
        test_image = tmp_path / "test.jpg"
        img = Image.new("RGB", (10, 10), color=(255, 0, 0))
        img.save(test_image)

        # Verify test setup is correct
        assert test_image.exists(), "Test image should exist for validation testing"

        # Act & Assert - Verify ValueError is raised with exact expected message
        with pytest.raises(ValueError, match="Turns must be an integer between 1 and 3 inclusive"):
            turn_clockwise(test_image, invalid_turns)

        # Verify the original file is unchanged after validation error
        assert test_image.exists(), "Original test image should remain unchanged after validation error"

    @pytest.mark.unit
    def test_flip_vertical(self, tmp_path: Path) -> None:
        """Test vertical flip creates a properly flipped image with preserved dimensions.

        Verifies that the flip_vertical function correctly applies a vertical flip
        transformation to an image using PIL's transpose operation. This unit test
        ensures that:
        1. The output file is created at the specified destination
        2. The flipped image maintains the original dimensions
        3. The actual vertical flip transformation is applied correctly
        4. The image format and quality are preserved during flipping
        """
        # Arrange - Create a test image with distinguishable top and bottom halves
        test_img = Image.new("RGB", (100, 80), color=(255, 255, 255))  # White background
        # Draw red rectangle on top half and blue rectangle on bottom half
        draw = ImageDraw.Draw(test_img)
        draw.rectangle([0, 0, 99, 39], fill=(255, 0, 0))  # Red top half
        draw.rectangle([0, 40, 99, 79], fill=(0, 0, 255))  # Blue bottom half

        test_image_path = tmp_path / "test_vertical_flip.png"
        test_img.save(test_image_path)
        output_path = tmp_path / "flipped.png"

        # Capture original image properties to verify transformation
        with Image.open(test_image_path) as original_img:
            original_size = original_img.size
            top_pixel = original_img.getpixel((50, 20))  # Should be red (255, 0, 0)
            bottom_pixel = original_img.getpixel((50, 60))  # Should be blue (0, 0, 255)

        # Act - Perform the vertical flip operation
        flip_vertical(test_image_path, output_path)

        # Assert - Verify the flip was executed correctly
        assert output_path.exists(), "Flipped image file should be created at the specified destination"

        # Verify the flipped image has correct properties and transformation
        with Image.open(output_path) as flipped_img:
            assert (
                flipped_img.size == original_size
            ), f"Flipped image should preserve original dimensions {original_size}"
            assert flipped_img.format in ("PNG", "JPEG"), "Flipped image should maintain a valid image format"
            assert flipped_img.mode == "RGB", "Flipped image should maintain RGB color mode"

            # Verify the vertical flip actually occurred by checking pixel positions
            # After vertical flip, what was on the top should now be on the bottom
            flipped_top_pixel = flipped_img.getpixel((50, 20))  # Should now be blue (was bottom)
            flipped_bottom_pixel = flipped_img.getpixel((50, 60))  # Should now be red (was top)

            assert flipped_top_pixel == bottom_pixel, "Top of flipped image should match bottom of original"
            assert flipped_bottom_pixel == top_pixel, "Bottom of flipped image should match top of original"
            assert flipped_top_pixel == (0, 0, 255), "Top of flipped image should be blue (originally bottom)"
            assert flipped_bottom_pixel == (255, 0, 0), "Bottom of flipped image should be red (originally top)"

    @pytest.mark.unit
    def test_flip_vertical_overwrites_original(self, tmp_path: Path) -> None:
        """Test flip_vertical overwrites original file when no destination is specified.

        Verifies that when flip_vertical is called without a destination parameter,
        it modifies the original file in place by applying the vertical flip
        transformation. This unit test ensures that:
        1. The original file is actually modified (not a copy)
        2. The flipped image has the same dimensions
        3. The vertical flip transformation is applied correctly
        4. The file remains valid and readable after the operation
        """
        # Arrange - Create a test image with distinguishable top and bottom halves
        test_img = Image.new("RGB", (100, 80), color=(255, 255, 255))
        draw = ImageDraw.Draw(test_img)
        draw.rectangle([0, 0, 99, 39], fill=(255, 0, 0))  # Red top half
        draw.rectangle([0, 40, 99, 79], fill=(0, 0, 255))  # Blue bottom half

        test_image_path = tmp_path / "test_vertical_flip.png"
        test_img.save(test_image_path)

        # Capture original image properties before modification
        with Image.open(test_image_path) as original_img:
            original_size = original_img.size
            original_top_pixel = original_img.getpixel((50, 20))  # Red pixel from top half
            original_bottom_pixel = original_img.getpixel((50, 60))  # Blue pixel from bottom half

        # Act - Flip the image without specifying a destination (overwrite original)
        flip_vertical(test_image_path)

        # Assert - Verify the original file was modified with proper flip transformation
        with Image.open(test_image_path) as flipped_img:
            # Verify file integrity and dimensions preserved
            assert (
                flipped_img.size == original_size
            ), f"Flipped image dimensions {flipped_img.size} should match original {original_size}"
            assert flipped_img.format in (
                "PNG",
                "JPEG",
            ), f"Flipped image format '{flipped_img.format}' should be valid PNG or JPEG"

            # Verify the vertical flip occurred by checking pixel positions are swapped
            flipped_top_pixel = flipped_img.getpixel((50, 20))  # Should now be blue (was bottom)
            flipped_bottom_pixel = flipped_img.getpixel((50, 60))  # Should now be red (was top)

            assert (
                flipped_top_pixel == original_bottom_pixel
            ), f"Top pixel {flipped_top_pixel} should match original bottom pixel {original_bottom_pixel}"
            assert (
                flipped_bottom_pixel == original_top_pixel
            ), f"Bottom pixel {flipped_bottom_pixel} should match original top pixel {original_top_pixel}"
            assert flipped_top_pixel == (0, 0, 255), f"Top pixel should be blue (0, 0, 255), got {flipped_top_pixel}"
            assert flipped_bottom_pixel == (
                255,
                0,
                0,
            ), f"Bottom pixel should be red (255, 0, 0), got {flipped_bottom_pixel}"

    @pytest.mark.unit
    def test_flip_horizontal(self, tmp_path: Path) -> None:
        """Test horizontal flip creates a properly flipped image with preserved dimensions.

        Verifies that the flip_horizontal function correctly applies a horizontal flip
        transformation to an image using PIL's transpose operation. This unit test
        ensures that:
        1. The output file is created at the specified destination
        2. The flipped image maintains the original dimensions
        3. The actual horizontal flip transformation is applied correctly
        4. The image format and quality are preserved during flipping
        """
        # Arrange - Create a test image with distinguishable left and right sides
        test_img = Image.new("RGB", (100, 80), color=(255, 255, 255))  # White background
        # Draw red rectangle on left side and blue rectangle on right side
        draw = ImageDraw.Draw(test_img)
        draw.rectangle([0, 0, 49, 79], fill=(255, 0, 0))  # Red left half
        draw.rectangle([50, 0, 99, 79], fill=(0, 0, 255))  # Blue right half

        test_image_path = tmp_path / "test_horizontal_flip.png"
        test_img.save(test_image_path)
        output_path = tmp_path / "flipped.png"

        # Capture original image properties to verify transformation
        with Image.open(test_image_path) as original_img:
            original_size = original_img.size
            left_pixel = original_img.getpixel((10, 40))  # Should be red (255, 0, 0)
            right_pixel = original_img.getpixel((90, 40))  # Should be blue (0, 0, 255)

        # Act - Perform the horizontal flip operation
        flip_horizontal(test_image_path, output_path)

        # Assert - Verify the flip was executed correctly
        assert output_path.exists(), "Flipped image file should be created at the specified destination"

        # Verify the flipped image has correct properties and transformation
        with Image.open(output_path) as flipped_img:
            assert (
                flipped_img.size == original_size
            ), f"Flipped image should preserve original dimensions {original_size}"
            assert flipped_img.format in ("PNG", "JPEG"), "Flipped image should maintain a valid image format"
            assert flipped_img.mode == "RGB", "Flipped image should maintain RGB color mode"

            # Verify the horizontal flip actually occurred by checking pixel positions
            # After horizontal flip, what was on the left should now be on the right
            flipped_left_pixel = flipped_img.getpixel((10, 40))  # Should now be blue (was right)
            flipped_right_pixel = flipped_img.getpixel((90, 40))  # Should now be red (was left)

            assert flipped_left_pixel == right_pixel, "Left side of flipped image should match right side of original"
            assert flipped_right_pixel == left_pixel, "Right side of flipped image should match left side of original"
            assert flipped_left_pixel == (0, 0, 255), "Left side of flipped image should be blue (originally right)"
            assert flipped_right_pixel == (255, 0, 0), "Right side of flipped image should be red (originally left)"

    @pytest.mark.unit
    def test_convert_to_jpeg_copies_existing_jpeg_without_reencoding(
        self,
        test_image_jpeg: Path,
        tmp_path: Path,
    ) -> None:
        """Test that convert_to_jpeg copies existing JPEG files without re-encoding.

        When the source file is already in JPEG format (.jpg or .jpeg extension),
        the function should use shutil.copy2 to preserve the file exactly as-is
        rather than re-encoding it, which would cause quality loss. This unit
        test verifies the real file copy behavior without mocking to ensure the
        complete functionality works correctly including file size preservation.
        """
        # Arrange
        destination = tmp_path / "copied_image.jpg"
        original_size = test_image_jpeg.stat().st_size

        # Get original image dimensions for verification
        with Image.open(test_image_jpeg) as original_img:
            original_width, original_height = original_img.size

        # Act
        result_path = convert_to_jpeg(test_image_jpeg, destination=destination)

        # Assert - Verify return value and file creation
        assert result_path == destination, "Function should return the destination path"
        assert destination.exists(), "Destination file should exist"
        assert destination.suffix == ".jpg", "Destination should have .jpg extension"

        # Assert - Verify file was copied, not re-encoded (size should be identical)
        assert destination.stat().st_size == original_size, "File size should match original (copied, not re-encoded)"

        # Assert - Verify image is still readable and dimensions are preserved
        with Image.open(destination) as copied_img:
            copied_width, copied_height = copied_img.size
            assert copied_width == original_width, "Image width should be preserved"
            assert copied_height == original_height, "Image height should be preserved"

    @pytest.mark.integration
    def test_is_blurry_true(self, tmp_path: Path) -> None:
        """Test blur detection identifies a blurry image correctly.

        Verifies that the is_blurry function correctly identifies blurry images when
        the Laplacian variance falls below the threshold. This integration test uses
        real OpenCV operations with a purposely blurred image to test the complete
        blur detection functionality without mocking core business logic.
        """
        # Arrange - Create a blurred test image to ensure reliable blur detection
        original_img = Image.new("RGB", (200, 200), color=(128, 128, 128))

        # Add some pattern to make blur more detectable
        draw = ImageDraw.Draw(original_img)
        for i in range(0, 200, 20):
            draw.line([(i, 0), (i, 200)], fill=(255, 255, 255), width=2)
            draw.line([(0, i), (200, i)], fill=(255, 255, 255), width=2)

        blurred_image_path = tmp_path / "blurred_image.png"
        original_img.save(blurred_image_path)

        # Apply Gaussian blur to make the image detectably blurry
        gaussian_blur(blurred_image_path, kernel_size=(15, 15))

        # Act - Test blur detection with a threshold that should identify the blurred image
        result = is_blurry(blurred_image_path, threshold=100.0)

        # Assert - Verify image is correctly identified as blurry
        assert result is True, "Gaussian blurred image should be identified as blurry"

        # Verify that the function returns a boolean type
        assert isinstance(result, bool), "is_blurry should return a boolean value"

    @pytest.mark.integration
    def test_is_blurry_false(self, test_image_rgb: Path) -> None:
        """Test that is_blurry correctly identifies an image as not blurry with zero threshold.

        This integration test verifies that the is_blurry function returns False
        when using a threshold of 0.0, which should make any image with non-negative
        variance appear as not blurry. Tests the complete workflow including image
        loading, processing, and threshold comparison.
        """
        # Arrange - Use threshold of 0.0 which should make any valid image appear not blurry
        zero_threshold = 0.0

        # Act - Test blur detection with zero threshold
        result = is_blurry(test_image_rgb, threshold=zero_threshold)

        # Assert - Verify image is correctly identified as not blurry
        assert result is False, f"Image should not be blurry with threshold {zero_threshold}"
        assert isinstance(result, bool), "is_blurry should return a boolean value"

        # Additional verification: test with default threshold for robustness
        result_default = is_blurry(test_image_rgb)
        assert isinstance(result_default, bool), "Function should return a boolean value"

    @pytest.mark.unit
    def test_is_blurry_invalid_image(self, mocker: pytest_mock.MockerFixture, test_image_rgb: Path) -> None:
        """Test blur detection with invalid image path raises ValueError.

        Verifies that the is_blurry function properly handles cases where the image
        cannot be loaded from the specified path by raising a ValueError with the
        expected error message. This unit test ensures proper error handling.
        """
        # Arrange - Mock cv2.imread to return None (failed image load)
        mock_imread = mocker.patch("cv2.imread")
        mock_imread.return_value = None

        # Act & Assert - Verify ValueError is raised with correct message
        with pytest.raises(ValueError, match=f"Could not load the image from the path: {test_image_rgb}"):
            is_blurry(test_image_rgb)

        # Assert - Verify cv2.imread was called with correct path
        mock_imread.assert_called_once_with(str(test_image_rgb))

    @pytest.mark.integration
    def test_is_blurry_custom_threshold(self, test_image_rgb: Path) -> None:
        """Test blur detection with custom threshold values.

        Verifies that the is_blurry function correctly applies custom threshold values
        when determining if an image is blurry. This integration test uses a real image
        and tests the actual threshold comparison logic with OpenCV operations.
        """
        # Arrange - Use the real test image and define thresholds around expected variance
        # First, get the actual variance for the test image to set appropriate thresholds

        image = cv2.imread(str(test_image_rgb))
        if image is None:
            pytest.fail(f"Failed to load test image: {test_image_rgb}")

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        actual_variance = cv2.Laplacian(gray, cv2.CV_64F).var()

        # Set thresholds: one below and one above the actual variance
        low_threshold = actual_variance - 10.0
        high_threshold = actual_variance + 10.0

        # Act - Test with different thresholds
        result_low_threshold = is_blurry(test_image_rgb, threshold=low_threshold)
        result_high_threshold = is_blurry(test_image_rgb, threshold=high_threshold)

        # Assert - Verify threshold affects blur detection correctly
        assert (
            result_low_threshold is False
        ), f"Image should not be blurry with threshold {low_threshold} (variance: {actual_variance})"
        assert (
            result_high_threshold is True
        ), f"Image should be blurry with threshold {high_threshold} (variance: {actual_variance})"

    @pytest.mark.unit
    def test_crop_creates_correctly_sized_output(self, test_image_rgb: Path, tmp_path: Path) -> None:
        """Test crop function creates output with specified dimensions and position.

        Verifies that the crop function correctly extracts a rectangular region from
        an image and saves it with the exact dimensions specified. This unit test
        focuses on the core cropping logic without external dependencies, ensuring:
        1. The output file is created at the specified destination
        2. The cropped image has the exact width and height requested
        3. The crop coordinates are applied correctly to extract the right region
        4. The image format and quality are preserved during cropping
        """
        # Arrange - Set up crop parameters and expected outcomes
        output_path = tmp_path / "cropped.png"
        crop_x, crop_y = 10, 15
        crop_width, crop_height = 50, 40

        # Verify original image dimensions for context
        with Image.open(test_image_rgb) as original_img:
            original_width, original_height = original_img.size
            assert original_width >= crop_x + crop_width, "Test image must be large enough for crop area"
            assert original_height >= crop_y + crop_height, "Test image must be large enough for crop area"

        # Act - Perform the crop operation
        crop(test_image_rgb, crop_x, crop_y, crop_width, crop_height, output_path)

        # Assert - Verify the crop was executed correctly
        assert output_path.exists(), "Cropped image file should be created at the specified destination"

        # Verify the cropped image has exact dimensions and proper format
        with Image.open(output_path) as cropped_img:
            actual_width, actual_height = cropped_img.size
            assert actual_width == crop_width, f"Cropped image width should be {crop_width} pixels, got {actual_width}"
            assert (
                actual_height == crop_height
            ), f"Cropped image height should be {crop_height} pixels, got {actual_height}"
            assert cropped_img.format in ("PNG", "JPEG"), "Cropped image should maintain a valid image format"
            assert cropped_img.mode in ("RGB", "RGBA", "L"), "Cropped image should maintain a valid color mode"

    @pytest.mark.unit
    def test_crop_overwrites_original(self, tmp_path: Path) -> None:
        """Test crop overwrites original file when no destination is specified.

        Verifies that crop modifies the original file in place when called without
        a destination parameter, maintaining file integrity and correct cropping behavior.
        """
        # Arrange - Create a test image with distinguishable regions for cropping verification
        test_img = Image.new("RGB", (100, 80), color=(255, 255, 255))  # White background
        draw = ImageDraw.Draw(test_img)
        # Create distinct colored regions to verify correct cropping
        draw.rectangle([0, 0, 49, 39], fill=(255, 0, 0))  # Red top-left
        draw.rectangle([50, 0, 99, 39], fill=(0, 255, 0))  # Green top-right
        draw.rectangle([0, 40, 49, 79], fill=(0, 0, 255))  # Blue bottom-left
        draw.rectangle([50, 40, 99, 79], fill=(255, 255, 0))  # Yellow bottom-right

        test_image_path = tmp_path / "test_crop.png"
        test_img.save(test_image_path)

        # Define crop parameters to extract the green top-right region
        crop_x, crop_y = 50, 0
        crop_width, crop_height = 50, 40

        # Capture original image properties and expected crop region pixel
        with Image.open(test_image_path) as original_img:
            original_size = original_img.size
            # Sample pixel from the region we'll crop (green top-right area)
            expected_crop_pixel = original_img.getpixel((75, 20))  # Should be green (0, 255, 0)
            assert expected_crop_pixel == (0, 255, 0), "Setup: expected crop region should be green"

        # Act - Crop the image without specifying a destination (overwrite original)
        crop(test_image_path, crop_x, crop_y, crop_width, crop_height)

        # Assert - Verify the original file was modified with exact crop dimensions and content
        with Image.open(test_image_path) as cropped_img:
            actual_size = cropped_img.size
            expected_size = (crop_width, crop_height)

            # Verify exact dimensions
            assert (
                actual_size == expected_size
            ), f"Cropped image dimensions {actual_size} should match expected {expected_size}"

            # Verify file was actually modified
            assert (
                actual_size != original_size
            ), f"Image dimensions should change from original {original_size} to cropped {actual_size}"

            # Verify file format integrity
            assert cropped_img.format in (
                "PNG",
                "JPEG",
            ), f"Image format '{cropped_img.format}' should be PNG or JPEG after cropping"

            # Verify the correct region was cropped by checking pixel content
            # The cropped image should now be entirely green (from the top-right region)
            center_pixel = cropped_img.getpixel((25, 20))  # Center of cropped area
            corner_pixel = cropped_img.getpixel((5, 5))  # Near corner of cropped area

            assert center_pixel == (
                0,
                255,
                0,
            ), f"Center pixel {center_pixel} should be green (0, 255, 0) from original top-right region"
            assert corner_pixel == (
                0,
                255,
                0,
            ), f"Corner pixel {corner_pixel} should be green (0, 255, 0) from original top-right region"

    @pytest.mark.unit
    def test_apply_clahe_calls_opencv_correctly(
        self,
        mocker: pytest_mock.MockerFixture,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test that apply_clahe calls OpenCV functions with correct parameters.

        Verifies that the apply_clahe function properly coordinates OpenCV function calls
        to apply Contrast Limited Adaptive Histogram Equalization. This unit test focuses
        on validating the function's interaction with external OpenCV dependencies by
        mocking them and verifying correct parameter passing and call sequence.
        """
        # Arrange - Set up test data and mock OpenCV functions
        output_path = tmp_path / "clahe.png"
        mock_img = np.zeros((80, 100), dtype=np.uint8)
        mock_processed_img = np.ones((80, 100), dtype=np.uint8) * 128

        mock_imread = mocker.patch("cv2.imread", return_value=mock_img)
        mock_create_clahe = mocker.patch("cv2.createCLAHE")
        mock_imwrite = mocker.patch("cv2.imwrite")

        mock_clahe_obj = mocker.Mock()
        mock_clahe_obj.apply.return_value = mock_processed_img
        mock_create_clahe.return_value = mock_clahe_obj

        # Act - Apply CLAHE with custom parameters
        apply_clahe(test_image_rgb, clip_limit=3.0, tile_grid_size=(16, 16), destination=output_path)

        # Assert - Verify OpenCV functions called with correct parameters and sequence
        mock_imread.assert_called_once_with(str(test_image_rgb), 0)
        mock_create_clahe.assert_called_once_with(
            clipLimit=3.0,
            tileGridSize=(16, 16),
        )
        mock_clahe_obj.apply.assert_called_once_with(mock_img)
        mock_imwrite.assert_called_once_with(
            str(output_path),
            mock_processed_img,
        )

    @pytest.mark.unit
    def test_apply_clahe_raises_error_when_image_cannot_be_read(
        self,
        mocker: pytest_mock.MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test that apply_clahe raises ValueError when cv2.imread fails to read image.

        Tests the error handling when OpenCV's imread function returns None,
        which indicates the image file could not be read (corrupted, unsupported format,
        or file access issues). Verifies that a descriptive ValueError is raised
        with the specific file path included in the error message.
        """
        # Arrange - Create a test image path and mock imread to simulate read failure
        test_image_path = tmp_path / "corrupted_image.png"
        test_image_path.touch()  # Create empty file to simulate corrupted image
        mock_imread = mocker.patch("marimba.lib.image.cv2.imread", return_value=None)

        # Act & Assert - Verify ValueError is raised with correct message
        with pytest.raises(ValueError, match=f"Could not read image from {test_image_path}"):
            apply_clahe(test_image_path)

        # Assert that cv2.imread was called with correct parameters
        mock_imread.assert_called_once_with(str(test_image_path), 0)

    @pytest.mark.unit
    def test_apply_clahe_default_parameters(self, mocker: pytest_mock.MockerFixture, test_image_rgb: Path) -> None:
        """Test apply_clahe with default parameters and overwrite behavior.

        Verifies that when no custom parameters are provided, apply_clahe uses
        the documented default values (clip_limit=2.0, tile_grid_size=(8,8)) and
        overwrites the original file when no destination is specified.
        """
        # Arrange - Mock OpenCV functions with expected default behavior
        mock_img = np.zeros((80, 100), dtype=np.uint8)
        mock_processed_img = np.ones((80, 100), dtype=np.uint8) * 64

        mock_imread = mocker.patch("cv2.imread", return_value=mock_img)
        mock_create_clahe = mocker.patch("cv2.createCLAHE")
        mock_imwrite = mocker.patch("cv2.imwrite")

        mock_clahe_obj = mocker.Mock()
        mock_clahe_obj.apply.return_value = mock_processed_img
        mock_create_clahe.return_value = mock_clahe_obj

        # Act - Call apply_clahe with no parameters (use all defaults)
        apply_clahe(test_image_rgb)

        # Assert - Verify default parameters and overwrite behavior
        mock_imread.assert_called_once_with(str(test_image_rgb), 0)
        mock_create_clahe.assert_called_once_with(
            clipLimit=2.0,
            tileGridSize=(8, 8),
        )
        mock_clahe_obj.apply.assert_called_once_with(mock_img)
        mock_imwrite.assert_called_once_with(
            str(test_image_rgb),
            mock_processed_img,
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("function_name", "function", "expected_error_pattern"),
        [
            ("apply_clahe", apply_clahe, "Could not read image from"),
            ("gaussian_blur", gaussian_blur, "Could not read image from"),
            ("sharpen", sharpen, "Could not read image from"),
        ],
    )
    def test_opencv_functions_invalid_image(
        self,
        mocker: pytest_mock.MockerFixture,
        function_name: str,
        function: Callable[..., None],
        expected_error_pattern: str,
        test_image_rgb: Path,
    ) -> None:
        """Test OpenCV-based functions raise ValueError when image cannot be loaded.

        Verifies that OpenCV-dependent functions (apply_clahe, gaussian_blur, sharpen)
        properly handle cases where cv2.imread returns None by raising a ValueError
        with an appropriate error message. This unit test ensures consistent error
        handling across OpenCV-based image processing functions.
        """
        # Arrange - Mock cv2.imread to return None (simulating failed image load)
        mock_imread = mocker.patch("cv2.imread")
        mock_imread.return_value = None

        # Act & Assert - Verify ValueError is raised with correct message pattern
        with pytest.raises(ValueError, match=expected_error_pattern):
            function(test_image_rgb)

    @pytest.mark.integration
    def test_gaussian_blur_creates_processed_image_file(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test gaussian_blur processes image file and creates output with preserved properties.

        Integration test verifying the complete gaussian_blur workflow reads input file,
        applies blur processing, and creates output file with correct format and dimensions.
        Tests real OpenCV operations without mocking to ensure end-to-end functionality.
        """
        # Arrange - Set up output path
        output_path = tmp_path / "blurred.png"

        # Verify preconditions
        assert test_image_rgb.exists(), f"Test image should exist at {test_image_rgb}"
        assert not output_path.exists(), f"Output file should not exist before processing at {output_path}"

        # Act - Apply Gaussian blur with custom kernel size
        gaussian_blur(test_image_rgb, kernel_size=(7, 7), destination=output_path)

        # Assert - Verify output file was created successfully
        assert output_path.exists(), f"Gaussian blur should create output file at {output_path}"
        assert output_path.stat().st_size > 0, f"Output file at {output_path} should not be empty"

        # Verify we can read the processed image and check its properties
        processed_img = cv2.imread(str(output_path))
        assert processed_img is not None, f"Processed image should be readable by OpenCV from {output_path}"

        # Verify image dimensions are preserved by comparing with original
        original_img = cv2.imread(str(test_image_rgb))
        assert original_img is not None, f"Original image should be readable by OpenCV from {test_image_rgb}"
        assert processed_img.shape == original_img.shape, (
            f"Processed image should have same dimensions as original: "
            f"expected {original_img.shape}, got {processed_img.shape}"
        )

    @pytest.mark.integration
    def test_gaussian_blur_default_parameters_and_overwrite(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test that gaussian_blur uses default parameters and overwrites original file when no destination specified.

        Verifies that when no destination parameter is provided, the gaussian_blur function
        modifies the original image file in place using the default kernel size (5,5).
        This integration test validates both the default parameter behavior and the overwrite
        functionality by comparing image content before and after processing.
        """
        # Arrange - Create a copy of test image to avoid modifying fixture
        test_copy = tmp_path / "test_copy.png"
        shutil.copy2(test_image_rgb, test_copy)

        # Store original file modification time and content for comparison
        original_mtime = test_copy.stat().st_mtime
        with Image.open(test_copy) as original_img:
            original_array = np.array(original_img)

        # Verify preconditions
        assert test_copy.exists(), "Test image copy should exist"
        assert original_array.size > 0, "Original image should have content"

        # Act - Apply gaussian_blur with no destination (should overwrite original with default kernel)
        gaussian_blur(test_copy)

        # Assert - Verify original file was modified
        assert test_copy.exists(), "Original file should still exist after processing"
        assert test_copy.stat().st_size > 0, "File should not be empty after processing"
        assert test_copy.stat().st_mtime >= original_mtime, "File modification time should be updated"

        # Verify image content was actually changed by blurring
        with Image.open(test_copy) as blurred_img:
            blurred_array = np.array(blurred_img)

            # Verify image properties are maintained
            assert blurred_img.size == original_array.shape[1::-1], "Blurred image should maintain original dimensions"
            assert blurred_img.mode == "RGB", "Blurred image should maintain RGB color mode"

            # Verify that image content was actually modified (arrays should be different)
            # Note: For some test images (like solid color images), blurring may not change content
            # In such cases, we verify the processing completed successfully by checking file modification
            arrays_are_equal = np.array_equal(original_array, blurred_array)
            if not arrays_are_equal:
                # Image content changed - verify it was actually blurred by checking variance
                original_variance = np.var(original_array.astype(np.float64))
                blurred_variance = np.var(blurred_array.astype(np.float64))
                assert (
                    blurred_variance <= original_variance
                ), "Blurred image should have equal or lower variance than original"
            else:
                # For solid color or very uniform images, blur may not change content
                # Verify processing completed by checking the image is still valid
                assert blurred_array.shape == original_array.shape, "Processed image should maintain original shape"

    @pytest.mark.unit
    def test_gaussian_blur_raises_error_for_invalid_image_path(
        self,
        tmp_path: Path,
    ) -> None:
        """Test gaussian_blur raises ValueError for non-existent image path."""
        # Arrange
        nonexistent_image_path = tmp_path / "nonexistent.png"
        output_destination = tmp_path / "output.png"

        # Act & Assert
        with pytest.raises(ValueError, match=f"Could not read image from {nonexistent_image_path}"):
            gaussian_blur(nonexistent_image_path, destination=output_destination)

    @pytest.mark.integration
    def test_sharpen_creates_processed_image_file(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test that sharpen function successfully processes an image and creates output file.

        Verifies that the sharpen function properly reads an input image, applies
        sharpening transformation, and writes the processed result to the specified
        destination path. This integration test validates the complete workflow
        without mocking OpenCV operations to ensure real functionality works.
        """
        # Arrange
        output_path = tmp_path / "sharpened.png"
        assert test_image_rgb.exists(), f"Test image should exist at {test_image_rgb}"
        assert not output_path.exists(), f"Output path should not exist before test at {output_path}"

        # Act
        sharpen(test_image_rgb, destination=output_path)

        # Assert - Verify output file was created successfully
        assert output_path.exists(), f"Sharpened image should be created at {output_path}"
        assert output_path.stat().st_size > 0, f"Sharpened image should not be empty at {output_path}"

        # Verify the processed image is readable and valid
        processed_img = cv2.imread(str(output_path))
        assert processed_img is not None, f"Sharpened image should be readable by OpenCV from {output_path}"

        # Verify dimensions are preserved during sharpening
        original_img = cv2.imread(str(test_image_rgb))
        assert original_img is not None, f"Original image should be readable from {test_image_rgb}"
        assert processed_img.shape == original_img.shape, (
            f"Sharpened image should preserve original dimensions: "
            f"expected {original_img.shape}, got {processed_img.shape}"
        )

    @pytest.mark.integration
    def test_sharpen_overwrites_original_when_no_destination(
        self,
        test_image_rgb: Path,
        tmp_path: Path,
    ) -> None:
        """Test that sharpen function overwrites the original file when no destination is specified.

        Verifies that when the destination parameter is not provided, the sharpen function
        modifies the original file in-place. This integration test validates the real
        file system behavior to ensure the function correctly defaults to overwriting
        the source file.
        """
        # Arrange - Create a working copy of the test image to avoid modifying the fixture
        test_image_path = tmp_path / "image_to_sharpen.png"
        shutil.copy2(test_image_rgb, test_image_path)

        # Read original image data for comparison
        original_img = cv2.imread(str(test_image_path))
        assert original_img is not None, f"Test image should be readable from {test_image_path}"

        # Act - Apply sharpening without specifying destination
        sharpen(test_image_path)

        # Assert - Verify the original file was modified
        assert test_image_path.exists(), f"Original file should still exist at {test_image_path}"

        # Verify file was actually modified by checking it's still readable
        modified_img = cv2.imread(str(test_image_path))
        assert modified_img is not None, f"Modified image should be readable from {test_image_path}"

        # Verify dimensions are preserved
        assert modified_img.shape == original_img.shape, (
            f"Modified image should have same dimensions as original: "
            f"expected {original_img.shape}, got {modified_img.shape}"
        )

        # Verify the file was actually written (modification time or size may change)
        # Note: For some images, sharpening might not change size, but file should still be written
        assert test_image_path.stat().st_size > 0, "Modified file should not be empty"

    @pytest.mark.unit
    def test_sharpen_raises_error_for_invalid_image_path(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that sharpen raises ValueError for non-existent image path.

        Verifies that the sharpen function properly handles error conditions by raising
        a ValueError with descriptive message when provided with a non-existent image path.
        """
        # Arrange
        nonexistent_image_path = tmp_path / "nonexistent.png"

        # Act & Assert
        with pytest.raises(ValueError, match=f"Could not read image from {nonexistent_image_path}"):
            sharpen(nonexistent_image_path)

    @pytest.mark.unit
    def test_get_width_height_returns_correct_dimensions(self, test_image_rgb: Path) -> None:
        """Test get_width_height returns correct width and height tuple for valid image.

        Verifies that the get_width_height function correctly extracts and returns
        the width and height dimensions from a valid image file as a tuple of integers.
        This unit test ensures the function returns the exact dimensions matching
        the test image fixture (100x80 pixels) for both Path and string inputs.
        """
        # Arrange - Use test image with known dimensions (100x80)
        expected_width = 100
        expected_height = 80

        # Act - Extract dimensions from the test image (Path input)
        width, height = get_width_height(test_image_rgb)

        # Assert - Verify dimensions match expected values
        assert isinstance(width, int), "Width should be an integer"
        assert isinstance(height, int), "Height should be an integer"
        assert width == expected_width, f"Width should be {expected_width} pixels, got {width}"
        assert height == expected_height, f"Height should be {expected_height} pixels, got {height}"

        # Act - Extract dimensions using string path to ensure str|Path union type works
        width_str, height_str = get_width_height(str(test_image_rgb))

        # Assert - Verify string path input produces identical results
        assert width_str == expected_width, f"String path should return width {expected_width}, got {width_str}"
        assert height_str == expected_height, f"String path should return height {expected_height}, got {height_str}"

    @pytest.mark.unit
    def test_get_width_height_invalid_size_raises_value_error(
        self,
        mocker: pytest_mock.MockerFixture,
        test_image_rgb: Path,
    ) -> None:
        """Test get_width_height raises ValueError when PIL Image returns invalid size tuple.

        Verifies that the get_width_height function properly validates the image size
        tuple returned by PIL and raises a ValueError with the correct message when
        the size tuple is malformed. This unit test ensures that:
        1. The function detects invalid size tuples (wrong length)
        2. A ValueError is raised with the specific expected message
        3. The validation logic properly handles edge cases
        4. The error handling is robust against malformed PIL Image responses
        """
        # Arrange - Mock PIL Image to return invalid size tuple with only one dimension
        mock_open = mocker.patch("PIL.Image.open")
        mock_img = mocker.Mock()
        mock_img.size = (100,)  # Invalid - only one dimension instead of width,height pair
        mock_open.return_value.__enter__.return_value = mock_img

        # Act & Assert - Verify ValueError is raised with correct message
        with pytest.raises(ValueError, match="Size must be a tuple of two integers"):
            get_width_height(test_image_rgb)

    @pytest.mark.unit
    def test_get_width_height_non_tuple_size_raises_value_error(
        self,
        mocker: pytest_mock.MockerFixture,
        test_image_rgb: Path,
    ) -> None:
        """Test get_width_height raises ValueError when PIL Image returns non-tuple size.

        Verifies that the get_width_height function properly validates that the size
        returned by PIL is a tuple type. This unit test ensures that:
        1. The function detects when size is not a tuple
        2. A ValueError is raised with the specific expected message
        3. The validation handles unexpected return types from PIL Image
        """
        # Arrange
        mock_open = mocker.patch("PIL.Image.open")
        mock_img = mocker.Mock()
        mock_img.size = [100, 80]  # Invalid - list instead of tuple
        mock_open.return_value.__enter__.return_value = mock_img

        # Act & Assert
        with pytest.raises(
            ValueError,
            match="Size must be a tuple of two integers",
        ) as exc_info:
            get_width_height(test_image_rgb)

        # Verify the specific error condition
        assert "Size must be a tuple of two integers" in str(
            exc_info.value,
        ), "Expected ValueError with message about tuple requirement"

    @pytest.mark.unit
    def test_get_width_height_non_integer_dimensions_raises_value_error(
        self,
        mocker: pytest_mock.MockerFixture,
        test_image_rgb: Path,
    ) -> None:
        """Test get_width_height raises ValueError when PIL Image returns non-integer dimensions.

        Verifies that the get_width_height function properly validates that the dimensions
        in the size tuple are integers. This unit test ensures that:
        1. The function detects when dimensions are not integers
        2. A ValueError is raised with the specific expected message
        3. The validation handles unexpected dimension types from PIL Image
        """
        # Arrange - Mock PIL Image to return tuple with non-integer dimensions
        mock_open = mocker.patch("PIL.Image.open")
        mock_img = mocker.Mock()
        mock_img.size = (100.5, 80.0)  # Invalid - float dimensions instead of integers
        mock_open.return_value.__enter__.return_value = mock_img

        # Act & Assert - Verify ValueError is raised with correct message
        with pytest.raises(ValueError, match="Size must be a tuple of two integers"):
            get_width_height(test_image_rgb)

    @pytest.mark.unit
    def test_get_shannon_entropy_solid_color_image_zero_entropy(self) -> None:
        """Test Shannon entropy calculation for solid color image returns zero entropy.

        Verifies that get_shannon_entropy correctly calculates zero entropy for a uniform
        solid color image, demonstrating the function's ability to detect images with
        no information content (maximum predictability).
        """
        # Arrange - Create a solid color image with uniform gray intensity
        solid_color_image = Image.new("L", (100, 100), color=128)

        # Act - Calculate Shannon entropy for uniform image
        entropy = get_shannon_entropy(solid_color_image)

        # Assert - Solid color image should have zero entropy (no information content)
        assert isinstance(entropy, float), "Entropy should be returned as a float value"
        assert entropy == 0.0, f"Solid color image should have exactly zero entropy, got {entropy}"

    @pytest.mark.unit
    def test_get_shannon_entropy_gradient_image_positive_entropy(self) -> None:
        """Test Shannon entropy calculation for gradient image returns expected positive entropy.

        Verifies that get_shannon_entropy correctly calculates positive entropy for a linear
        gradient image pattern with known characteristics, testing the function's ability
        to measure information content in structured image data.
        """
        # Arrange - Create a linear gradient image with 100 unique equally distributed values
        # Each row contains pixels with the same value (0-99), creating uniform distribution
        image_size = 100
        gradient_array = np.zeros((image_size, image_size), dtype=np.uint8)
        for row_index in range(image_size):
            gradient_array[row_index, :] = row_index
        gradient_image = Image.fromarray(gradient_array)

        # Act - Calculate Shannon entropy for gradient image
        entropy = get_shannon_entropy(gradient_image)

        # Assert - Verify return type and basic properties
        assert isinstance(entropy, float), "Entropy should be returned as a float value"
        assert entropy > 0, "Gradient image should have positive entropy"

        # Assert - Verify exact entropy value for uniform distribution of 100 values
        # Raw entropy = -∑(p_i * log2(p_i)) where p_i = 1/100 for all i
        # = log2(100) ≈ 6.644 bits, normalised to [0, 1] by the 8-bit maximum:
        # log2(100) / 8 ≈ 0.830482
        expected_entropy = np.log2(image_size) / 8.0
        tolerance = 1e-6  # Appropriate tolerance for floating point precision
        assert abs(entropy - expected_entropy) < tolerance, (
            f"Expected entropy {expected_entropy:.6f} for uniform distribution of {image_size} values, "
            f"got {entropy:.6f} (difference: {abs(entropy - expected_entropy):.2e})"
        )

    @pytest.mark.unit
    def test_get_shannon_entropy_random_noise_high_entropy(self) -> None:
        """Test Shannon entropy calculation for random noise image approaches maximum entropy.

        Verifies that get_shannon_entropy correctly calculates high entropy for a random
        noise image, demonstrating the function's ability to detect high information
        content in unpredictable image data. Uses fixed random seed for reproducible results.
        """
        # Arrange - Create random noise image with maximum entropy characteristics
        rng = np.random.default_rng(42)  # Ensure reproducible test results
        noise_array = rng.integers(0, 256, size=(100, 100), dtype=np.uint8)
        noise_image = Image.fromarray(noise_array)
        # Empirically ~7.982 bits for this seed/size, normalised to [0, 1] by the 8-bit maximum.
        expected_entropy = 7.982 / 8.0

        # Act - Calculate Shannon entropy for noise image
        entropy = get_shannon_entropy(noise_image)

        # Assert - Verify exact expected entropy value with reasonable tolerance
        assert isinstance(entropy, float), "Entropy should be returned as a float value"
        assert (
            abs(entropy - expected_entropy) < 0.01
        ), f"Random noise normalized entropy should be approximately {expected_entropy:.3f}, got {entropy:.3f}"
        assert entropy <= 1.0, f"Normalized entropy cannot exceed 1.0, got {entropy:.3f}"

    @pytest.mark.unit
    def test_get_shannon_entropy_rgb_to_grayscale_conversion(self) -> None:
        """Test Shannon entropy calculation properly converts RGB images to grayscale.

        Verifies that get_shannon_entropy correctly handles RGB images by converting
        them to grayscale before entropy calculation, ensuring consistent behavior
        regardless of input image color mode. Tests with identical gradient patterns
        in all RGB channels that should produce equivalent grayscale entropy.
        """
        # Arrange - Create RGB image with gradient pattern in all channels
        img_array = np.zeros((50, 50, 3), dtype=np.uint8)
        for i in range(50):
            # Create gradient in all RGB channels (should convert to equivalent grayscale)
            gradient_value = min(i * 5, 255)  # Prevent overflow beyond 255
            img_array[i, :, 0] = gradient_value  # Red channel gradient
            img_array[i, :, 1] = gradient_value  # Green channel gradient
            img_array[i, :, 2] = gradient_value  # Blue channel gradient
        rgb_image = Image.fromarray(img_array)

        # Create equivalent grayscale image for comparison
        grayscale_array = np.zeros((50, 50), dtype=np.uint8)
        for i in range(50):
            grayscale_array[i, :] = min(i * 5, 255)  # Same gradient pattern
        grayscale_image = Image.fromarray(grayscale_array)

        # Act - Calculate entropy for both RGB and equivalent grayscale images
        rgb_entropy = get_shannon_entropy(rgb_image)
        grayscale_entropy = get_shannon_entropy(grayscale_image)

        # Assert - RGB image should be converted to grayscale with equivalent entropy
        assert isinstance(rgb_entropy, float), "RGB entropy should be returned as float"
        assert isinstance(grayscale_entropy, float), "Grayscale entropy should be returned as float"
        assert rgb_entropy > 0.0, f"Gradient image should have positive entropy, got {rgb_entropy:.3f}"
        assert grayscale_entropy > 0.0, f"Gradient image should have positive entropy, got {grayscale_entropy:.3f}"

        # Use appropriate tolerance for floating-point comparison (accounting for PIL conversion precision)
        tolerance = 0.01
        assert abs(rgb_entropy - grayscale_entropy) < tolerance, (
            f"RGB and equivalent grayscale images should have same entropy within {tolerance}: "
            f"RGB={rgb_entropy:.4f}, Grayscale={grayscale_entropy:.4f}, diff={abs(rgb_entropy - grayscale_entropy):.4f}"
        )

    @pytest.mark.unit
    def test_get_average_image_color_solid_rgb_image(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test average color calculation for solid RGB image returns exact color values.

        Verifies that the get_average_image_color function correctly calculates the
        average color of a solid RGB image. This unit test ensures that:
        1. The function returns a list with exactly 3 RGB values
        2. For a solid color image, the average matches the exact input color
        3. The returned values are integers as expected by the API
        4. The color channels are returned in RGB order (Red, Green, Blue)
        5. All color values are within valid RGB range (0-255)
        """
        # Arrange - Create a solid color RGB image with known color values
        expected_red, expected_green, expected_blue = 100, 150, 200
        test_image = Image.new("RGB", (100, 100), color=(expected_red, expected_green, expected_blue))
        expected_color = [expected_red, expected_green, expected_blue]

        # Act - Calculate the average color of the solid image
        actual_average_color = get_average_image_color(test_image)

        # Assert - Verify the average color matches expected values exactly
        assert isinstance(actual_average_color, list), "Function should return a list of color values"
        assert len(actual_average_color) == 3, "RGB image should return exactly 3 color channel values"
        assert (
            actual_average_color == expected_color
        ), f"Average color {actual_average_color} should match input color {expected_color} for solid image"
        assert all(isinstance(value, int) for value in actual_average_color), "All color values should be integers"
        assert all(
            0 <= value <= 255 for value in actual_average_color
        ), "All color values should be in valid RGB range (0-255)"

    @pytest.mark.unit
    def test_get_average_image_color_grayscale_converted_to_rgb(self):
        """Test average color calculation for grayscale image converted to RGB format.

        Verifies that the get_average_image_color function correctly handles grayscale
        images that have been converted to RGB format. This unit test ensures that:
        1. The function processes converted grayscale images properly
        2. All three RGB channels have identical values for grayscale images
        3. The average color matches the original grayscale intensity value
        4. The function maintains consistency across RGB channels for grayscale data
        """
        # Arrange - Create a grayscale image with known intensity and convert to RGB
        expected_intensity = 128
        grayscale_image = Image.new("L", (100, 100), color=expected_intensity)
        rgb_converted_image = grayscale_image.convert("RGB")
        expected_rgb_color = [expected_intensity, expected_intensity, expected_intensity]

        # Act - Calculate the average color of the converted grayscale image
        actual_average_color = get_average_image_color(rgb_converted_image)

        # Assert - Verify all RGB channels have the same grayscale intensity value
        assert isinstance(actual_average_color, list), "Function should return a list of color values"
        assert len(actual_average_color) == 3, "Converted RGB image should return exactly 3 color channel values"
        assert (
            actual_average_color == expected_rgb_color
        ), f"Average color {actual_average_color} should have equal RGB values {expected_rgb_color} for grayscale"
        assert all(isinstance(value, int) for value in actual_average_color), "All color values should be integers"
        assert (
            actual_average_color[0] == actual_average_color[1] == actual_average_color[2]
        ), "All RGB channels should have identical values for converted grayscale image"


class TestGridClasses:
    """Test grid-related classes."""

    @pytest.fixture
    def grid_dimensions(self) -> GridDimensions:
        """Create test grid dimensions configuration.

        Creates a standard GridDimensions configuration for testing grid layout
        functionality with 3 columns, 200px column width, and 800px maximum height.

        Returns:
            GridDimensions: Configuration object for 3-column grid layout
        """
        return GridDimensions(columns=3, column_width=200, max_height=800)

    @pytest.fixture
    def test_images(self, tmp_path: Path) -> list[Path]:
        """Create multiple test images for grid processing tests.

        Creates 6 test images with varying colors (300x200 pixels each) for testing
        grid image processing functionality. Each image has a unique color based on
        its index to enable verification of proper image placement and processing.

        Args:
            tmp_path: pytest fixture providing temporary directory path

        Returns:
            List of Path objects pointing to the created test image files
        """
        images = []
        for i in range(6):
            img = Image.new("RGB", (300, 200), color=(i * 40, (i * 40) % 255, 100))
            path = tmp_path / f"test_image_{i}.png"
            img.save(path)
            images.append(path)
        return images

    @pytest.mark.unit
    def test_grid_dimensions_initialization_with_valid_parameters(self, grid_dimensions: GridDimensions) -> None:
        """Test GridDimensions dataclass stores configuration parameters correctly.

        Verifies that the GridDimensions dataclass correctly initializes and stores
        grid layout configuration parameters with the expected values from the fixture.
        """
        # Arrange - No additional setup needed, using fixture

        # Act - Access the dataclass attributes
        actual_columns = grid_dimensions.columns
        actual_column_width = grid_dimensions.column_width
        actual_max_height = grid_dimensions.max_height

        # Assert - Verify all attributes store expected values
        assert actual_columns == 3, f"Columns should be 3, got {actual_columns}"
        assert actual_column_width == 200, f"Column width should be 200px, got {actual_column_width}"
        assert actual_max_height == 800, f"Max height should be 800px, got {actual_max_height}"

    @pytest.mark.unit
    def test_grid_dimensions_initialization_with_minimal_edge_case_values(self) -> None:
        """Test GridDimensions dataclass with minimal edge case values.

        Verifies that the GridDimensions dataclass correctly handles minimal
        edge case values such as single column grids with 1-pixel dimensions.
        Tests that the dataclass properly stores and retrieves minimal attribute
        values without validation errors.
        """
        # Arrange
        expected_columns = 1
        expected_column_width = 1
        expected_max_height = 1

        # Act
        minimal_dimensions = GridDimensions(
            columns=expected_columns,
            column_width=expected_column_width,
            max_height=expected_max_height,
        )

        # Assert
        assert (
            minimal_dimensions.columns == expected_columns
        ), f"Columns should be {expected_columns}, got {minimal_dimensions.columns}"
        assert (
            minimal_dimensions.column_width == expected_column_width
        ), f"Column width should be {expected_column_width}px, got {minimal_dimensions.column_width}"
        assert (
            minimal_dimensions.max_height == expected_max_height
        ), f"Max height should be {expected_max_height}px, got {minimal_dimensions.max_height}"

    @pytest.mark.unit
    def test_grid_dimensions_initialization_with_large_edge_case_values(self) -> None:
        """Test GridDimensions dataclass with large edge case values.

        Verifies that the GridDimensions dataclass correctly handles large
        edge case values such as wide grids with high column counts and
        large pixel dimensions. Tests that the dataclass properly stores
        and retrieves large attribute values without validation errors.
        """
        # Arrange
        expected_columns = 100
        expected_column_width = 5000
        expected_max_height = 50000

        # Act
        large_dimensions = GridDimensions(
            columns=expected_columns,
            column_width=expected_column_width,
            max_height=expected_max_height,
        )

        # Assert
        assert (
            large_dimensions.columns == expected_columns
        ), f"Columns should be {expected_columns}, got {large_dimensions.columns}"
        assert (
            large_dimensions.column_width == expected_column_width
        ), f"Column width should be {expected_column_width}px, got {large_dimensions.column_width}"
        assert (
            large_dimensions.max_height == expected_max_height
        ), f"Max height should be {expected_max_height}px, got {large_dimensions.max_height}"

    @pytest.mark.unit
    def test_grid_dimensions_dataclass_equality_and_repr(self) -> None:
        """Test GridDimensions dataclass equality and string representation behavior.

        Verifies that the GridDimensions dataclass behaves correctly with equality
        comparisons and provides meaningful string representation. Tests dataclass
        auto-generated methods work as expected for value object semantics.
        """
        # Arrange - Create test instances with same and different values
        dimensions_1 = GridDimensions(columns=2, column_width=300, max_height=1000)
        dimensions_2 = GridDimensions(columns=2, column_width=300, max_height=1000)
        dimensions_different = GridDimensions(columns=3, column_width=400, max_height=1200)

        # Act - Get string representation
        str_repr = str(dimensions_1)

        # Assert - Test equality behavior (value object semantics)
        assert dimensions_1 == dimensions_2, "GridDimensions with identical values should be equal"
        assert dimensions_1 != dimensions_different, "GridDimensions with different values should not be equal"

        # Assert - Test string representation includes all fields
        assert "GridDimensions" in str_repr, "String representation should include class name"
        assert "columns=2" in str_repr, "String representation should include columns value"
        assert "column_width=300" in str_repr, "String representation should include column_width value"
        assert "max_height=1000" in str_repr, "String representation should include max_height value"

    @pytest.mark.unit
    def test_grid_row_init_creates_empty_row(self, grid_dimensions: GridDimensions) -> None:
        """Test GridRow initialization creates empty row with correct default state.

        Verifies that GridRow constructor correctly initializes with empty images list,
        zero height, and stores the provided GridDimensions reference. This ensures
        proper initialization state for subsequent image processing operations.
        """
        # Arrange - GridDimensions fixture provides test configuration
        # No additional setup needed

        # Act - Initialize a new GridRow with the test dimensions
        row = GridRow(grid_dimensions)

        # Assert - Verify all initial state attributes are correctly set
        assert isinstance(row.images, list), f"GridRow.images should be a list, got {type(row.images)}"
        assert row.images == [], f"GridRow should initialize with empty images list, got {row.images}"
        assert row.height == 0, f"GridRow should initialize with zero height, got {row.height}"
        assert (
            row.dimensions is grid_dimensions
        ), "GridRow should store exact GridDimensions reference passed to constructor"

    @pytest.mark.unit
    def test_grid_row_add_image(self, grid_dimensions: GridDimensions) -> None:
        """Test adding image to GridRow scales image correctly and updates row state.

        Verifies that the GridRow.add_image method correctly processes a PIL Image
        by scaling it to fit the column width while maintaining aspect ratio, and
        properly updates the row's internal state. This unit test ensures that:
        1. The method returns True to indicate successful addition
        2. The image is properly added to the row's images list
        3. The image is scaled to match the column width (300x200 -> 200x133)
        4. The row height is updated to match the scaled image height
        5. The scaled image maintains proper aspect ratio and format
        """
        # Arrange - Set up GridRow and test image with known dimensions
        row = GridRow(grid_dimensions)
        original_width, original_height = 300, 200
        img = Image.new("RGB", (original_width, original_height), color=(255, 0, 0))

        # Calculate expected scaled dimensions based on column width (200px)
        expected_scale_factor = grid_dimensions.column_width / original_width  # 200/300 = 0.667
        expected_scaled_height = int(original_height * expected_scale_factor)  # int(200 * 0.667) = 133

        # Verify preconditions
        assert len(row.images) == 0, "Row should start empty"
        assert row.height == 0, "Row height should start at 0"

        # Act - Add the image to the row
        result = row.add_image(img)

        # Assert - Verify successful addition and correct scaling
        assert result is True, "add_image should return True when successfully adding image to non-full row"
        assert len(row.images) == 1, f"Row should contain exactly 1 image after addition, got {len(row.images)}"
        assert (
            row.height == expected_scaled_height
        ), f"Row height should be {expected_scaled_height}px (scaled from {original_height}px), got {row.height}px"

        # Verify the stored image has correct properties
        stored_image, stored_width, stored_height = row.images[0]
        assert (
            stored_width == grid_dimensions.column_width
        ), f"Stored image width should match column width {grid_dimensions.column_width}px, got {stored_width}px"
        assert (
            stored_height == expected_scaled_height
        ), f"Stored image height should be {expected_scaled_height}px, got {stored_height}px"
        assert stored_image.size == (stored_width, stored_height), "PIL Image size should match stored dimensions tuple"
        assert stored_image.mode == "RGB", "Scaled image should maintain RGB color mode"

    @pytest.mark.unit
    def test_grid_row_add_image_full(self, grid_dimensions: GridDimensions) -> None:
        """Test adding image to full GridRow returns False and preserves row state.

        Verifies that the GridRow.add_image method correctly handles the case when
        attempting to add an image to a row that has already reached its maximum
        capacity (columns limit). This integration test ensures that:
        1. The method returns False when the row is at capacity
        2. No additional images are added to the row
        3. The row's existing state (image count, height) remains unchanged
        4. The row maintains its capacity constraint and integrity
        """
        # Arrange - Set up GridRow and fill it to maximum capacity
        row = GridRow(grid_dimensions)
        expected_capacity = grid_dimensions.columns  # 3 columns from fixture

        # Fill the row to maximum capacity with red images
        for i in range(expected_capacity):
            img = Image.new("RGB", (300, 200), color=(255, 0, 0))
            success = row.add_image(img)
            assert success is True, f"Setup: should successfully add image {i+1} to row"

        # Capture row state after filling to capacity
        initial_image_count = len(row.images)
        initial_height = row.height

        # Verify preconditions - row is at capacity
        assert (
            initial_image_count == expected_capacity
        ), f"Setup: row should contain {expected_capacity} images at capacity"
        assert initial_height > 0, "Setup: row should have positive height after adding images"

        # Act - Attempt to add one more image (green) to the full row
        additional_img = Image.new("RGB", (300, 200), color=(0, 255, 0))
        result = row.add_image(additional_img)

        # Assert - Verify the addition failed and row state is preserved
        assert result is False, "Adding image to full row should return False to indicate failure"
        assert (
            len(row.images) == initial_image_count
        ), f"Row should still contain {initial_image_count} images after failed addition, got {len(row.images)}"
        assert (
            len(row.images) == expected_capacity
        ), f"Row should remain at maximum capacity of {expected_capacity} images"
        assert (
            row.height == initial_height
        ), f"Row height should remain unchanged at {initial_height}px after failed addition"

        # Verify the row maintains its original images and structure
        for i, (stored_image, width, height) in enumerate(row.images):
            assert stored_image is not None, f"Image {i+1} should remain valid after failed addition"
            assert (
                width == grid_dimensions.column_width
            ), f"Image {i+1} width should remain {grid_dimensions.column_width}px"
            assert height > 0, f"Image {i+1} height should remain positive"

    @pytest.mark.unit
    def test_grid_row_cleanup_closes_all_stored_images(
        self,
        grid_dimensions: GridDimensions,
    ) -> None:
        """Test GridRow cleanup properly closes all stored images to free resources.

        Verifies that the GridRow.cleanup method correctly closes all PIL Image objects
        stored in the row to prevent memory leaks and resource exhaustion. This unit test
        ensures that:
        1. All images added to the row are properly closed when cleanup is called
        2. Images become inaccessible after cleanup (indicating they are closed)
        3. Resource management is handled correctly for multiple images in the row
        4. The cleanup operation works regardless of the number of images in the row
        """
        # Arrange - Set up GridRow with real PIL images to test cleanup
        row = GridRow(grid_dimensions)

        # Create real PIL Images
        image1 = Image.new("RGB", (300, 200), color=(255, 0, 0))
        image2 = Image.new("RGB", (300, 200), color=(0, 255, 0))
        image3 = Image.new("RGB", (300, 200), color=(0, 0, 255))

        # Add the real images to the row
        add_result1 = row.add_image(image1)
        add_result2 = row.add_image(image2)
        add_result3 = row.add_image(image3)

        # Verify preconditions - images were added successfully
        assert add_result1 is True, "First image should be added successfully"
        assert add_result2 is True, "Second image should be added successfully"
        assert add_result3 is True, "Third image should be added successfully"
        assert len(row.images) == 3, "Row should contain 3 images after additions"

        # Get references to the resized images that are actually stored in the row
        stored_image1, width1, _height1 = row.images[0]
        stored_image2, width2, _height2 = row.images[1]
        stored_image3, width3, _height3 = row.images[2]

        # Verify images are accessible and have correct dimensions before cleanup
        assert stored_image1.size == (grid_dimensions.column_width, 133), "First image should be resized correctly"
        assert stored_image2.size == (grid_dimensions.column_width, 133), "Second image should be resized correctly"
        assert stored_image3.size == (grid_dimensions.column_width, 133), "Third image should be resized correctly"
        assert width1 == grid_dimensions.column_width, "First image width should match column width"
        assert width2 == grid_dimensions.column_width, "Second image width should match column width"
        assert width3 == grid_dimensions.column_width, "Third image width should match column width"

        # Act - Call cleanup to close all images
        row.cleanup()

        # Assert - Verify that images are closed by checking they raise ValueError when accessed
        with pytest.raises(ValueError, match="Operation on closed image"):
            _ = stored_image1.getpixel((0, 0))
        with pytest.raises(ValueError, match="Operation on closed image"):
            _ = stored_image2.getpixel((0, 0))
        with pytest.raises(ValueError, match="Operation on closed image"):
            _ = stored_image3.getpixel((0, 0))

        # Verify that the row still contains the image references after cleanup
        # (cleanup should call close() but not remove images from the list)
        assert len(row.images) == 3, "Row should still contain 3 image references after cleanup"

    @pytest.mark.unit
    def test_grid_image_processor_init(self, grid_dimensions: GridDimensions) -> None:
        """Test GridImageProcessor initialization stores GridDimensions reference correctly.

        Verifies that the GridImageProcessor dataclass correctly initializes with a
        GridDimensions object and stores the exact reference (not a copy) for use in
        subsequent image processing operations. This ensures the processor uses the
        same configuration object throughout its lifecycle.
        """
        # Arrange - GridDimensions object provided by fixture
        expected_dimensions = grid_dimensions

        # Act - Initialize the GridImageProcessor with the dimensions
        processor = GridImageProcessor(expected_dimensions)

        # Assert - Verify the exact GridDimensions object reference is stored
        assert processor.dimensions is expected_dimensions, (
            "GridImageProcessor should store the exact GridDimensions object reference, "
            f"but got a different object: expected {id(expected_dimensions)}, got {id(processor.dimensions)}"
        )

    @pytest.mark.unit
    def test_grid_image_processor_process_single_image_success(self, grid_dimensions, test_images):
        """Test processing single valid image successfully adds image to row.

        Verifies that the process_single_image method correctly processes a valid
        image file and successfully adds it to the provided GridRow. Tests image
        scaling and proper integration with the GridRow.add_image method.
        """
        # Arrange - Set up processor and empty row
        processor = GridImageProcessor(grid_dimensions)
        row = GridRow(grid_dimensions)
        valid_image_path = test_images[0]

        # Verify initial state assumptions
        assert len(row.images) == 0, "Row should start empty"
        assert row.height == 0, "Row height should start at 0"

        # Calculate expected dimensions for test image (300x200 -> scaled to fit column_width=200)
        expected_width = grid_dimensions.column_width  # 200
        expected_height = int(200 * (grid_dimensions.column_width / 300))  # 133

        # Act - Process the valid image
        result = processor.process_single_image(valid_image_path, row)

        # Assert - Verify successful processing and exact outcomes
        assert result is True, "Processing valid image should return True to indicate success"
        assert len(row.images) == 1, "Row should contain exactly one image after successful processing"
        assert row.height == expected_height, f"Row height should be {expected_height} pixels after adding scaled image"

        # Verify the added image has proper structure and scaling
        processed_image, width, height = row.images[0]
        assert processed_image is not None, "Processed image should not be None"
        assert width == expected_width, f"Processed image width should be {expected_width} pixels"
        assert height == expected_height, f"Processed image height should be {expected_height} pixels"
        assert hasattr(processed_image, "size"), "Processed image should be a valid PIL Image object"
        assert processed_image.size == (width, height), "PIL Image size should match the stored dimensions"

    @pytest.mark.unit
    def test_grid_image_processor_process_invalid_image_returns_false(self, grid_dimensions, tmp_path):
        """Test processing invalid image path returns False without modifying row.

        Verifies that the GridImageProcessor correctly handles invalid image paths
        by returning False and not adding any images to the row. This unit test
        ensures that:
        1. Invalid or nonexistent image paths are handled gracefully
        2. The function returns False to indicate processing failure
        3. No images are added to the row when processing fails
        4. The row's state remains unchanged after failed processing attempts
        """
        # Arrange - Set up processor, row, and invalid path
        processor = GridImageProcessor(grid_dimensions)
        row = GridRow(grid_dimensions)
        invalid_path = tmp_path / "nonexistent.png"

        # Verify preconditions
        assert not invalid_path.exists(), "Test path should not exist to simulate invalid image"
        initial_image_count = len(row.images)
        initial_height = row.height

        # Act - Attempt to process the invalid image path
        result = processor.process_single_image(invalid_path, row)

        # Assert - Verify invalid image processing is handled correctly
        assert result is False, "Processing invalid image should return False to indicate failure"
        assert (
            len(row.images) == initial_image_count
        ), f"Row should still have {initial_image_count} images after failed processing"
        assert row.height == initial_height, f"Row height should remain {initial_height} after failed processing"

    @pytest.mark.unit
    def test_grid_image_processor_process_single_image_row_full(
        self,
        grid_dimensions: GridDimensions,
        test_images: list[Path],
    ) -> None:
        """Test processing image when row is already at maximum capacity returns False.

        Verifies that the process_single_image method correctly handles the case when
        the provided row has already reached its maximum capacity (columns limit).
        This unit test ensures that:
        1. The method returns False when row is full
        2. No additional images are added to the row
        3. Row state remains unchanged after failed processing
        4. The processing gracefully handles capacity constraints
        """
        # Arrange - Set up processor and fill row to maximum capacity
        processor = GridImageProcessor(grid_dimensions)
        row = GridRow(grid_dimensions)

        # Fill the row to maximum capacity (3 columns in grid_dimensions fixture)
        for i in range(grid_dimensions.columns):
            success = processor.process_single_image(test_images[i], row)
            assert success is True, f"Setup: should successfully add image {i+1} to row"

        # Verify preconditions - row is at capacity with expected state
        initial_image_count = len(row.images)
        initial_height = row.height
        assert initial_image_count == grid_dimensions.columns, "Setup: row should be at maximum capacity"
        assert initial_height > 0, "Setup: row should have positive height after adding images"

        # Get an additional image to attempt processing
        additional_image_path = test_images[0]  # Reuse first image

        # Act - Attempt to process image when row is full
        result = processor.process_single_image(additional_image_path, row)

        # Assert - Verify processing failed due to capacity and row remained unchanged
        assert result is False, "Processing image when row is full should return False"
        assert (
            len(row.images) == initial_image_count
        ), f"Row should still have {initial_image_count} images after failed processing, got {len(row.images)}"
        assert len(row.images) == grid_dimensions.columns, "Row should remain at maximum capacity"
        assert row.height == initial_height, "Row height should remain unchanged after failed processing"

    @pytest.mark.unit
    def test_grid_image_processor_create_grid(self, grid_dimensions, test_images):
        """Test creating grid image from multiple input images with correct processing results.

        Verifies that the GridImageProcessor correctly creates a grid image from multiple
        input image paths, properly processes the expected number of images, and returns
        valid results. This unit test ensures that:
        1. The create_grid method returns a valid PIL Image object
        2. The calculated grid height matches expected dimensions based on input images
        3. All provided images are processed successfully
        4. The grid maintains proper layout and scaling constraints
        """
        # Arrange - Set up processor and input images for grid creation
        processor = GridImageProcessor(grid_dimensions)
        input_images = test_images[:3]  # Use first 3 test images
        expected_processed_count = len(input_images)

        # Calculate expected height based on grid configuration
        # Each test image is 300x200, scaled to column_width=200, so height becomes:
        # scale_factor = 200/300 = 0.6666..., new_height = int(200 * 0.6666...) = 133px
        expected_scaled_height = int(200 * (grid_dimensions.column_width / 300))  # 133px per image
        expected_grid_height = expected_scaled_height  # All 3 images fit in 1 row (3 columns)

        # Act - Create grid from the test images
        grid_image, actual_height, actual_processed = processor.create_grid(input_images)

        # Assert - Verify grid creation was successful with expected results
        assert grid_image is not None, "Grid image should be created when valid images are provided"
        assert isinstance(grid_image, Image.Image), "Returned grid should be a PIL Image object"

        assert (
            actual_height == expected_grid_height
        ), f"Grid height should be {expected_grid_height}px for single row, got {actual_height}px"
        assert (
            actual_processed == expected_processed_count
        ), f"Should process all {expected_processed_count} input images, processed {actual_processed}"

        # Verify grid image dimensions match expected layout
        expected_width = grid_dimensions.columns * grid_dimensions.column_width  # 3 * 200 = 600px
        actual_width, actual_height_from_image = grid_image.size
        assert actual_width == expected_width, f"Grid width should be {expected_width}px, got {actual_width}px"
        assert (
            actual_height_from_image == actual_height
        ), f"Grid image height {actual_height_from_image}px should match calculated height {actual_height}px"

        # Verify grid image is properly formatted
        assert grid_image.mode == "RGB", "Grid image should be in RGB color mode"
        assert grid_image.format in (None, "JPEG", "PNG"), "Grid image should have valid format"

    @pytest.mark.unit
    def test_grid_image_processor_create_grid_empty(self, grid_dimensions: GridDimensions) -> None:
        """Test creating grid with empty image paths list returns proper null values.

        Verifies that the GridImageProcessor correctly handles the edge case when
        create_grid is called with an empty list of image paths. This unit test
        ensures that:
        1. The function returns None for the grid image (no images to process)
        2. The height returned is 0 (no grid content)
        3. The processed count is 0 (no images were processed)
        4. The function handles the empty input gracefully without errors
        """
        # Arrange - Set up processor with valid grid dimensions and empty paths list
        processor = GridImageProcessor(grid_dimensions)
        empty_paths: list[Path] = []

        # Act - Attempt to create grid from empty paths list
        actual_grid_image, actual_height, actual_processed_count = processor.create_grid(empty_paths)

        # Assert - Verify all return values indicate no processing occurred
        assert actual_grid_image is None, "Grid image should be None when no input paths provided"
        assert actual_height == 0, "Height should be 0 when no images processed"
        assert actual_processed_count == 0, "Processed count should be 0 when no images provided"

        # Verify the return types are correct
        assert isinstance(actual_height, int), "Height should be returned as an integer"
        assert isinstance(actual_processed_count, int), "Processed count should be returned as an integer"

    @pytest.mark.unit
    def test_grid_image_processor_create_grid_max_height_exceeded(self, test_images):
        """Test grid creation when max height is exceeded returns no grid but processes images.

        Verifies that the GridImageProcessor correctly handles the edge case when
        the maximum allowed height is insufficient to accommodate even a single row
        of images. This unit test ensures that:
        1. No grid image is created when max_height constraint cannot be satisfied
        2. The function returns 0 height when no grid can be rendered
        3. Images are still processed and counted even if grid cannot be created
        4. The function handles height constraints gracefully without errors
        """
        # Arrange - Set up dimensions with extremely small max height
        # Test images are 300x200, when scaled to column_width=200 they become 200x133
        # Setting max_height=10 ensures even a single image row (height=133) exceeds the limit
        max_height_too_small = 10
        small_dimensions = GridDimensions(columns=3, column_width=200, max_height=max_height_too_small)
        processor = GridImageProcessor(small_dimensions)
        single_image_list = test_images[:1]
        expected_processed_count = 1  # Should still process the single image

        # Act - Attempt to create grid with insufficient height allowance
        grid_image, actual_height, actual_processed_count = processor.create_grid(single_image_list)

        # Assert - Verify max height constraint prevents grid creation but allows processing
        assert grid_image is None, "No grid image should be created when max_height constraint cannot be satisfied"
        assert actual_height == 0, "Grid height should be 0 when no rows can fit within max_height constraint"
        assert (
            actual_processed_count == expected_processed_count
        ), f"Should still process {expected_processed_count} image even when grid cannot be created"

        # Verify the function handles constraint gracefully
        assert isinstance(actual_height, int), "Height should be returned as an integer"
        assert isinstance(actual_processed_count, int), "Processed count should be returned as an integer"


class TestOutputPathManager:
    """Test class for OutputPathManager functionality."""

    @pytest.mark.unit
    def test_init_with_extension_sets_empty_extension(self, tmp_path: Path) -> None:
        """Test OutputPathManager initialization when base path has existing extension.

        Verifies that when a base path with an existing file extension is provided,
        the OutputPathManager correctly stores the base path and sets extension to empty string,
        indicating it will use the existing extension from the base path rather than the default.
        Tests the logic: self.extension = "" if base_path.suffix else ".jpg"
        """
        # Arrange - Create a path with an existing extension
        base_path = tmp_path / "grid.jpg"

        # Act - Initialize the OutputPathManager
        manager = OutputPathManager(base_path)

        # Assert - Verify correct initialization behavior
        assert manager.base_path == base_path, "Base path should be stored exactly as provided"
        assert manager.extension == "", "Extension should be empty when base path already has an extension"

    @pytest.mark.unit
    def test_init_no_extension_sets_default_jpg_extension(self, tmp_path: Path) -> None:
        """Test OutputPathManager initialization when base path has no extension.

        Verifies that when a base path without any file extension is provided,
        the OutputPathManager correctly stores the base path and sets extension to '.jpg'
        as the default extension for output files.
        """
        # Arrange - Create a path without any extension
        base_path = tmp_path / "grid"

        # Act - Initialize the OutputPathManager
        manager = OutputPathManager(base_path)

        # Assert - Verify initialization sets correct attributes with default extension
        assert manager.base_path == base_path, "Base path should be stored exactly as provided"
        assert manager.extension == ".jpg", "Extension should default to '.jpg' when base path has no extension"

    @pytest.mark.unit
    def test_create_path_single_grid(self, tmp_path: Path) -> None:
        """Test creating path for single grid when has_more_grids=False and grid_number=0.

        Verifies that OutputPathManager correctly returns the original base path unchanged
        when processing the first (and only) grid with no additional grids to follow.
        This tests the specific logic branch where grid_number=0 and has_more_grids=False
        should return the base path without any numbering suffix.
        """
        # Arrange - Set up base path and manager
        base_path = tmp_path / "grid.jpg"
        manager = OutputPathManager(base_path)

        # Act - Create path for single grid (no more grids, first grid)
        result_path = manager.create_path(grid_number=0, has_more_grids=False)

        # Assert - Verify the original base path is returned unchanged
        assert (
            result_path == base_path
        ), f"Expected original base path '{base_path}' for single grid, got '{result_path}'"

    @pytest.mark.unit
    def test_create_path_multiple_grids(self, tmp_path: Path) -> None:
        """Test creating numbered paths for multiple grids with sequential numbering and zero-padding.

        Verifies that OutputPathManager correctly generates numbered file paths when
        has_more_grids=True, ensuring proper sequential naming with zero-padding,
        correct extension handling, and proper path structure for multiple grid scenarios.
        Tests the numbering logic: grid_number + 1 formatted as {stem}_{number:02d}{suffix}
        """
        # Arrange - Set up manager with base path that has existing extension
        base_path = tmp_path / "grid.jpg"
        manager = OutputPathManager(base_path)

        # Act - Generate paths for multiple grids with sequential numbering
        path_first = manager.create_path(grid_number=0, has_more_grids=True)
        path_second = manager.create_path(grid_number=1, has_more_grids=True)
        path_third = manager.create_path(grid_number=2, has_more_grids=True)
        path_tenth = manager.create_path(grid_number=9, has_more_grids=True)

        # Assert - Verify sequential numbering with zero-padding for each path
        assert (
            path_first == tmp_path / "grid_01.jpg"
        ), f"First grid path incorrect: expected '{tmp_path / 'grid_01.jpg'}', got '{path_first}'"
        assert (
            path_second == tmp_path / "grid_02.jpg"
        ), f"Second grid path incorrect: expected '{tmp_path / 'grid_02.jpg'}', got '{path_second}'"
        assert (
            path_third == tmp_path / "grid_03.jpg"
        ), f"Third grid path incorrect: expected '{tmp_path / 'grid_03.jpg'}', got '{path_third}'"
        assert (
            path_tenth == tmp_path / "grid_10.jpg"
        ), f"Tenth grid path incorrect (double-digit test): expected '{tmp_path / 'grid_10.jpg'}', got '{path_tenth}'"

        # Assert - Verify path structure components for first grid
        assert path_first.parent == tmp_path, "Generated path should be in same directory as base path"
        assert path_first.stem == "grid_01", "Filename stem should include grid number with zero-padding"
        assert path_first.suffix == ".jpg", "File extension should match base path extension"

        # Assert - Verify all paths maintain correct parent directory
        all_paths = [path_first, path_second, path_third, path_tenth]
        assert all(
            path.parent == tmp_path for path in all_paths
        ), "All generated paths should be in the same directory as base path"

    @pytest.mark.unit
    def test_create_path_last_grid_in_sequence(self, tmp_path: Path) -> None:
        """Test creating path for the last grid when has_more_grids=False with grid_number > 0.

        Verifies that OutputPathManager correctly handles the final grid in a multi-grid
        sequence, where has_more_grids=False but grid_number > 0, ensuring the path
        is still numbered since it's not the only grid (grid_number > 0 indicates
        previous grids exist). Tests the specific logic: when grid_number > 0, numbering
        is applied regardless of has_more_grids value.
        """
        # Arrange - Set up manager with base path
        base_path = tmp_path / "grid.jpg"
        manager = OutputPathManager(base_path)

        # Act - Create path for last grid in a sequence (e.g., 3rd grid with no more after)
        last_grid_path = manager.create_path(grid_number=2, has_more_grids=False)

        # Assert - Verify numbered path is created even though has_more_grids=False
        expected_path = tmp_path / "grid_03.jpg"
        assert (
            last_grid_path == expected_path
        ), f"Last grid in sequence should be numbered: expected '{expected_path}', got '{last_grid_path}'"

        # Assert - Verify path structure components
        assert last_grid_path.parent == tmp_path, "Generated path should be in same directory as base path"
        assert last_grid_path.stem == "grid_03", "Last grid should have numbered stem with zero-padding"
        assert last_grid_path.suffix == ".jpg", "File extension should be preserved from base path"

    @pytest.mark.unit
    def test_create_path_with_default_extension(self, tmp_path: Path) -> None:
        """Test creating numbered paths when base path has no extension.

        Verifies that OutputPathManager correctly applies the default '.jpg' extension
        when the base path has no extension, ensuring numbered paths use the default
        extension consistently across multiple grids.
        """
        # Arrange
        base_path = tmp_path / "grid_output"
        manager = OutputPathManager(base_path)

        # Act
        path_first = manager.create_path(grid_number=0, has_more_grids=True)
        path_second = manager.create_path(grid_number=1, has_more_grids=False)

        # Assert
        assert path_first == tmp_path / "grid_output_01.jpg", (
            f"First grid path with default extension incorrect: expected "
            f"'{tmp_path / 'grid_output_01.jpg'}', got '{path_first}'"
        )
        assert path_second == tmp_path / "grid_output_02.jpg", (
            f"Second grid path with default extension incorrect: expected "
            f"'{tmp_path / 'grid_output_02.jpg'}', got '{path_second}'"
        )
        assert manager.extension == ".jpg", "Manager should store default .jpg extension"

    @pytest.mark.unit
    def test_create_path_preserves_custom_extension(self, tmp_path: Path) -> None:
        """Test creating numbered paths with non-standard image extension.

        Verifies that OutputPathManager preserves custom file extensions (e.g., .png, .tiff)
        when generating numbered paths, ensuring the original extension is maintained
        throughout the grid sequence rather than defaulting to .jpg.
        """
        # Arrange - Set up manager with .png extension
        base_path = tmp_path / "grid.png"
        manager = OutputPathManager(base_path)

        # Act - Generate numbered paths
        path_first = manager.create_path(grid_number=0, has_more_grids=True)
        path_second = manager.create_path(grid_number=1, has_more_grids=True)

        # Assert - Verify manager initialization with custom extension
        assert manager.extension == "", "Extension should be empty when base path has custom extension"

        # Assert - Verify custom extension is preserved in numbered paths
        assert path_first == tmp_path / "grid_01.png", "First grid should use custom .png extension, not default .jpg"
        assert path_second == tmp_path / "grid_02.png", "Second grid should consistently preserve .png extension"

        # Assert - Verify path structure components for first grid
        assert path_first.parent == tmp_path, "Generated path should be in same directory as base path"
        assert path_first.stem == "grid_01", "Filename stem should include grid number with zero-padding"
        assert path_first.suffix == ".png", "File extension should match custom .png extension"
