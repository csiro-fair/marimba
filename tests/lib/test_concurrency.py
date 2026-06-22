"""Tests for marimba.lib.concurrency module."""

import inspect
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from pytest_mock import MockerFixture

from marimba.lib.concurrency import (
    multithreaded_generate_image_thumbnails,
    multithreaded_generate_video_thumbnails,
)

if TYPE_CHECKING:
    from marimba.core.pipeline import BasePipeline


class TestConcurrencyUtilities:
    """Test concurrency utility functions."""

    @pytest.fixture
    def mock_pipeline(self, mocker: MockerFixture) -> "BasePipeline":
        """Create a mock BasePipeline instance."""
        pipeline = mocker.Mock()
        pipeline._root_path = "/mock/root/path"
        return cast("BasePipeline", pipeline)

    @pytest.fixture
    def test_image_paths(self, tmp_path: Path) -> list[Path]:
        """Create test image paths."""
        # Create actual files for more realistic testing
        paths = [
            tmp_path / "image1.jpg",
            tmp_path / "image2.jpg",
            tmp_path / "image3.jpg",
        ]
        for path in paths:
            path.touch()  # Create empty files
        return paths

    @pytest.fixture
    def test_video_paths(self, tmp_path: Path) -> list[Path]:
        """Create test video paths."""
        # Create actual files for more realistic testing
        paths = [
            tmp_path / "video1.mp4",
            tmp_path / "video2.mp4",
        ]
        for path in paths:
            path.touch()  # Create empty files
        return paths

    @pytest.mark.unit
    def test_multithreaded_generate_image_thumbnails_function_signature(self, mock_pipeline: "BasePipeline") -> None:
        """Test that multithreaded_generate_image_thumbnails has correct function signature and imports.

        This unit test verifies that the function is properly imported and has the expected
        signature with correct parameter types and default values, ensuring API consistency.
        """
        # Arrange & Act
        signature = inspect.signature(multithreaded_generate_image_thumbnails)
        parameters = signature.parameters

        # Assert
        assert "self" in parameters, "Function should have 'self' parameter"
        assert "image_list" in parameters, "Function should have 'image_list' parameter"
        assert "output_directory" in parameters, "Function should have 'output_directory' parameter"
        assert "logger" in parameters, "Function should have 'logger' parameter"
        assert "max_workers" in parameters, "Function should have 'max_workers' parameter"

        # Verify default values
        assert parameters["logger"].default is None, "Logger parameter should default to None"
        assert parameters["max_workers"].default is None, "Max workers parameter should default to None"

        # Verify return type annotation exists
        assert signature.return_annotation is not inspect.Signature.empty, "Function should have return type annotation"

    @pytest.mark.integration
    def test_multithreaded_generate_image_thumbnails(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_image_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded image thumbnail generation with minimal external dependency mocking.

        This integration test verifies that the multithreaded wrapper correctly calls the
        underlying thumbnail generation function for each image and properly handles the
        results, while mocking only the external image processing dependency.
        """
        # Arrange
        mock_generate_thumbnail = mocker.patch("marimba.lib.concurrency.generate_image_thumbnail")
        output_dir = tmp_path / "thumbnails"
        expected_thumbnails = [
            output_dir / "image1_thumb.jpg",
            output_dir / "image2_thumb.jpg",
            output_dir / "image3_thumb.jpg",
        ]
        mock_generate_thumbnail.side_effect = expected_thumbnails

        # Act
        result = multithreaded_generate_image_thumbnails(mock_pipeline, test_image_paths, output_dir)

        # Assert
        assert isinstance(result, list)
        assert len(result) == len(test_image_paths)
        assert output_dir.exists()
        assert mock_generate_thumbnail.call_count == len(test_image_paths)
        assert set(result) == set(expected_thumbnails)

    @pytest.mark.integration
    def test_multithreaded_generate_image_thumbnails_with_logger(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_image_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded image thumbnail generation with logger and custom max_workers.

        This integration test verifies that the function properly integrates with logging,
        respects the max_workers parameter, and processes multiple images concurrently with
        proper thread synchronization and logging for each generated thumbnail.
        """
        # Arrange
        mock_generate_thumbnail = mocker.patch("marimba.lib.concurrency.generate_image_thumbnail")
        output_dir = tmp_path / "thumbnails"
        mock_logger = mocker.Mock()
        expected_thumbnails = [
            output_dir / "image1_thumb.jpg",
            output_dir / "image2_thumb.jpg",
            output_dir / "image3_thumb.jpg",
        ]
        mock_generate_thumbnail.side_effect = expected_thumbnails

        # Act
        result = multithreaded_generate_image_thumbnails(
            mock_pipeline,
            test_image_paths,
            output_dir,
            logger=mock_logger,
            max_workers=2,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == len(test_image_paths)
        assert set(result) == set(expected_thumbnails)
        assert output_dir.exists()
        assert mock_generate_thumbnail.call_count == len(test_image_paths)
        assert mock_logger.debug.call_count == len(test_image_paths)

    @pytest.mark.integration
    def test_multithreaded_generate_video_thumbnails(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_video_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded video thumbnail generation with minimal external dependency mocking.

        This integration test verifies that the multithreaded wrapper correctly processes
        multiple videos concurrently, creates appropriate output directories, and returns
        the expected video-thumbnail path pairs with proper handling of threading.
        Tests the real integration between the multithreaded wrapper and directory creation
        while mocking only the external video processing dependency.
        """
        # Arrange
        mock_generate_thumbnails = mocker.patch("marimba.lib.concurrency.generate_video_thumbnails")
        output_dir = tmp_path / "video_thumbnails"
        expected_results = [
            (test_video_paths[0], [tmp_path / "v1_thumb1.jpg", tmp_path / "v1_thumb2.jpg"]),
            (test_video_paths[1], [tmp_path / "v2_thumb1.jpg"]),
        ]
        mock_generate_thumbnails.side_effect = expected_results

        # Act
        result = multithreaded_generate_video_thumbnails(
            mock_pipeline,
            test_video_paths,
            output_dir,
            interval=5,
            suffix="_TEST",
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == len(test_video_paths)
        assert mock_generate_thumbnails.call_count == len(test_video_paths)

        # Verify subdirectories are created for each video
        for video_path in test_video_paths:
            video_subdir = output_dir / video_path.stem
            assert video_subdir.exists()

        # Verify results contain expected video paths
        result_videos = {r[0] for r in result}
        expected_videos = set(test_video_paths)
        assert result_videos == expected_videos

    @pytest.mark.integration
    def test_multithreaded_generate_video_thumbnails_with_options(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_video_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded video thumbnail generation with various options including logger and overwrite.

        This integration test verifies that all optional parameters (interval, suffix, logger,
        max_workers, overwrite) are correctly passed through to the underlying thumbnail generation
        function and that logging works as expected during multithreaded processing.
        """
        # Arrange
        mock_generate_thumbnails = mocker.patch("marimba.lib.concurrency.generate_video_thumbnails")
        output_dir = tmp_path / "video_thumbnails"
        mock_logger = mocker.Mock()
        video_path = test_video_paths[0]
        expected_thumbnail_paths = [tmp_path / "custom_thumb1.jpg", tmp_path / "custom_thumb2.jpg"]
        expected_result = (video_path, expected_thumbnail_paths)
        mock_generate_thumbnails.return_value = expected_result

        # Act
        result = multithreaded_generate_video_thumbnails(
            mock_pipeline,
            [video_path],
            output_dir,
            interval=15,
            suffix="_CUSTOM",
            logger=mock_logger,
            max_workers=1,
            overwrite=True,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == expected_result

        # Verify subdirectory creation
        video_subdir = output_dir / video_path.stem
        assert video_subdir.exists()
        assert video_subdir.is_dir()

        # Verify function called with correct parameters
        assert mock_generate_thumbnails.call_count == 1
        call_args = mock_generate_thumbnails.call_args
        assert call_args[0][0] == video_path
        assert call_args[0][1] == video_subdir
        assert call_args[0][2] == 15
        assert call_args[0][3] == "_CUSTOM"
        assert call_args[1]["overwrite"] is True

        # Verify logger integration
        assert mock_logger.debug.call_count == 1

    @pytest.mark.integration
    def test_multithreaded_generate_image_thumbnails_empty_list(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        tmp_path: Path,
    ) -> None:
        """Test multithreaded image thumbnail generation with empty input list.

        This integration test verifies that the multithreaded wrapper correctly handles
        empty input lists without calling the underlying thumbnail generation function,
        while still creating the output directory as expected.
        """
        # Arrange
        mock_generate_thumbnail = mocker.patch("marimba.lib.concurrency.generate_image_thumbnail")
        output_dir = tmp_path / "thumbnails"

        # Act
        result = multithreaded_generate_image_thumbnails(mock_pipeline, [], output_dir)

        # Assert
        assert result == []
        assert mock_generate_thumbnail.call_count == 0
        assert output_dir.exists()

    @pytest.mark.integration
    def test_multithreaded_generate_video_thumbnails_empty_list(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        tmp_path: Path,
    ) -> None:
        """Test multithreaded video thumbnail generation with empty input list.

        This integration test verifies that the multithreaded wrapper correctly handles
        empty input lists without calling the underlying video thumbnail generation function,
        ensuring proper initialization and cleanup for edge cases.
        """
        # Arrange
        mock_generate_thumbnails = mocker.patch("marimba.lib.concurrency.generate_video_thumbnails")
        output_dir = tmp_path / "video_thumbnails"

        # Act
        result = multithreaded_generate_video_thumbnails(mock_pipeline, [], output_dir)

        # Assert
        assert isinstance(result, list)
        assert result == []
        assert mock_generate_thumbnails.call_count == 0

    @pytest.mark.integration
    def test_multithreaded_generate_image_thumbnails_handles_none_return(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_image_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded image thumbnail generation when generate_image_thumbnail returns None.

        This integration test verifies that the function correctly filters out None results
        when thumbnail generation fails for some images, ensuring robust error handling
        while testing the integration between the multithreaded wrapper and error filtering logic.
        """
        # Arrange
        mock_generate_thumbnail = mocker.patch("marimba.lib.concurrency.generate_image_thumbnail")
        output_dir = tmp_path / "thumbnails"
        successful_thumbnail = tmp_path / "thumb2.jpg"
        # Mock returns None for some images (failed generation)
        mock_generate_thumbnail.side_effect = [None, successful_thumbnail, None]

        # Act
        result = multithreaded_generate_image_thumbnails(mock_pipeline, test_image_paths, output_dir)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert successful_thumbnail in result
        assert mock_generate_thumbnail.call_count == 3
        assert output_dir.exists()

        # Verify all results are valid Path objects
        for path in result:
            assert path is not None
            assert isinstance(path, Path)

    @pytest.mark.integration
    def test_multithreaded_generate_video_thumbnails_handles_empty_thumbnail_list(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_video_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded video thumbnail generation when generate_video_thumbnails returns empty thumbnail lists.

        This integration test verifies that the function correctly filters out failed video processing
        when video thumbnail generation returns empty thumbnail lists for some videos, ensuring
        robust error handling while testing the actual filtering logic of the multithreaded wrapper.
        The source code checks "if video_path and thumbnail_paths:" so empty lists are filtered out.
        """
        # Arrange
        mock_generate_thumbnails = mocker.patch("marimba.lib.concurrency.generate_video_thumbnails")
        output_dir = tmp_path / "video_thumbnails"
        successful_thumbnail_paths = [tmp_path / "thumb1.jpg"]
        successful_result = (test_video_paths[0], successful_thumbnail_paths)
        failed_result: tuple[Path, list[Path]] = (test_video_paths[1], [])  # Failed generation - empty thumbnail list
        mock_generate_thumbnails.side_effect = [successful_result, failed_result]

        # Act
        result = multithreaded_generate_video_thumbnails(mock_pipeline, test_video_paths, output_dir)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == successful_result
        assert mock_generate_thumbnails.call_count == len(test_video_paths)

        # Verify subdirectories are created for all videos
        for video_path in test_video_paths:
            video_subdir = output_dir / video_path.stem
            assert video_subdir.exists()
            assert video_subdir.is_dir()

        # Verify only successful results with non-empty thumbnail lists are included
        for video_path, thumbnail_paths in result:
            assert video_path is not None
            assert isinstance(video_path, Path)
            assert isinstance(thumbnail_paths, list)
            assert len(thumbnail_paths) > 0

    @pytest.mark.integration
    def test_multithreaded_generate_video_thumbnails_handles_none_return(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        test_video_paths: list[Path],
        tmp_path: Path,
    ) -> None:
        """Test multithreaded video thumbnail generation when generate_video_thumbnails returns None video path.

        This integration test verifies that the function correctly filters out None results
        when video thumbnail generation fails for some videos (returns None video path),
        ensuring robust error handling while testing the integration between the multithreaded
        wrapper and error filtering logic based on the source code condition "if video_path and thumbnail_paths:".
        """
        # Arrange
        mock_generate_thumbnails = mocker.patch("marimba.lib.concurrency.generate_video_thumbnails")
        output_dir = tmp_path / "video_thumbnails"
        successful_thumbnail_paths = [tmp_path / "thumb1.jpg", tmp_path / "thumb2.jpg"]
        successful_result = (test_video_paths[0], successful_thumbnail_paths)
        # Mock returns None for video_path in the second case (failed generation)
        failed_result: tuple[None, list[Path]] = (None, [tmp_path / "failed_thumb.jpg"])
        mock_generate_thumbnails.side_effect = [successful_result, failed_result]

        # Act
        result = multithreaded_generate_video_thumbnails(mock_pipeline, test_video_paths, output_dir)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == successful_result
        assert mock_generate_thumbnails.call_count == len(test_video_paths)

        # Verify subdirectories are created even for failed processing
        for video_path in test_video_paths:
            video_subdir = output_dir / video_path.stem
            assert video_subdir.exists()

        # Verify failed generations (None video paths) are filtered out
        for video_path, thumbnail_paths in result:
            assert video_path is not None
            assert isinstance(video_path, Path)
            assert isinstance(thumbnail_paths, list)

    @pytest.mark.unit
    def test_multithreaded_generate_video_thumbnails_function_signature(self, mock_pipeline: "BasePipeline") -> None:
        """Test that multithreaded_generate_video_thumbnails has correct function signature and imports.

        This unit test verifies that the function is properly imported and has the expected
        signature with correct parameter types and default values, ensuring API consistency.
        """
        # Arrange & Act
        signature = inspect.signature(multithreaded_generate_video_thumbnails)
        parameters = signature.parameters

        # Assert
        assert "self" in parameters, "Function should have 'self' parameter"
        assert "video_list" in parameters, "Function should have 'video_list' parameter"
        assert "output_base_directory" in parameters, "Function should have 'output_base_directory' parameter"
        assert "interval" in parameters, "Function should have 'interval' parameter"
        assert "suffix" in parameters, "Function should have 'suffix' parameter"
        assert "logger" in parameters, "Function should have 'logger' parameter"
        assert "max_workers" in parameters, "Function should have 'max_workers' parameter"
        assert "overwrite" in parameters, "Function should have 'overwrite' parameter"

        # Verify default values
        assert parameters["interval"].default == 10, "Interval parameter should default to 10"
        assert parameters["suffix"].default == "_THUMB", "Suffix parameter should default to '_THUMB'"
        assert parameters["logger"].default is None, "Logger parameter should default to None"
        assert parameters["max_workers"].default is None, "Max workers parameter should default to None"
        assert parameters["overwrite"].default is False, "Overwrite parameter should default to False"

        # Verify return type annotation exists
        assert signature.return_annotation is not inspect.Signature.empty, "Function should have return type annotation"

    @pytest.mark.integration
    def test_multithreaded_generate_image_thumbnails_single_image_with_logging(
        self,
        mocker: MockerFixture,
        mock_pipeline: "BasePipeline",
        tmp_path: Path,
    ) -> None:
        """Test multithreaded image thumbnail generation with single image and logger integration.

        This integration test verifies that the function correctly processes a single image,
        creates the output directory, calls the thumbnail generation function with correct
        parameters, and logs the operation. Tests the integration between the multithreaded
        wrapper, directory creation, thumbnail generation, and logging components.
        """
        # Arrange
        mock_generate_thumbnail = mocker.patch("marimba.lib.concurrency.generate_image_thumbnail")
        output_dir = tmp_path / "thumbnails"
        test_image = tmp_path / "test.jpg"
        test_image.touch()
        mock_logger = mocker.Mock()

        expected_thumbnail = tmp_path / "test_thumb.jpg"
        mock_generate_thumbnail.return_value = expected_thumbnail

        # Act
        result = multithreaded_generate_image_thumbnails(
            mock_pipeline,
            [test_image],
            output_dir,
            logger=mock_logger,
        )

        # Assert
        assert isinstance(result, list), "Result should be a list"
        assert len(result) == 1, "Result should contain exactly one thumbnail"
        assert result[0] == expected_thumbnail, "Result should contain expected thumbnail path"
        assert output_dir.exists(), "Output directory should be created"
        assert output_dir.is_dir(), "Output directory should be a directory"

        # Verify thumbnail generation was called with correct parameters
        mock_generate_thumbnail.assert_called_once_with(test_image, output_dir)

        # Verify logger was called exactly once with debug level
        assert mock_logger.debug.call_count == 1, "Logger should be called once for single image"

        # Verify logger message contains relevant information about the generated thumbnail
        log_call_args = mock_logger.debug.call_args[0][0]
        assert "Generated thumbnail for image" in log_call_args, "Log message should mention thumbnail generation"
        assert (
            test_image.name in log_call_args or str(test_image) in log_call_args
        ), "Log message should reference the image file"
