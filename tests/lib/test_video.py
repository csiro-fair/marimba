"""Tests for marimba.lib.video module."""

from pathlib import Path

import pytest
import pytest_mock

from marimba.lib.video import (
    filter_existing_thumbnails,
    generate_potential_filenames,
    generate_video_thumbnails,
    get_stream_properties,
    save_thumbnail,
)


class TestVideoUtilities:
    """Test video utility functions."""

    @pytest.fixture
    def mock_video_stream(self, mocker: pytest_mock.MockerFixture) -> pytest_mock.MockType:
        """Create a mock video stream."""
        stream = mocker.Mock()
        stream.average_rate = 30.0
        stream.time_base = 1 / 30.0
        stream.frames = 900
        return stream  # type: ignore[no-any-return]

    @pytest.fixture
    def test_video_path(self, tmp_path: Path) -> Path:
        """Create a test video path for testing video processing functions.

        This fixture provides a standardized video file path that can be used
        across multiple test functions to ensure consistent test data.
        """
        return tmp_path / "test_video.mp4"

    @pytest.mark.unit
    def test_get_stream_properties(self, mock_video_stream: pytest_mock.MockType) -> None:
        """Test extracting stream properties from a video stream.

        This test verifies that the get_stream_properties function correctly
        extracts frame rate, time base, and total frames from a mocked video stream.
        """
        # Arrange
        # Mock video stream is set up in fixture with:
        # - average_rate = 30.0
        # - time_base = 1 / 30.0
        # - frames = 900

        # Act
        frame_rate, time_base, total_frames = get_stream_properties(mock_video_stream)

        # Assert
        assert frame_rate == 30.0, "Should return correct frame rate"
        assert time_base == 1 / 30.0, "Should return correct time base"
        assert total_frames == 900, "Should return correct total frames count"

    @pytest.mark.unit
    def test_get_stream_properties_none_frame_rate(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test get_stream_properties raises ValueError when frame rate is None.

        This test ensures proper error handling when video stream has invalid frame rate.
        The function should validate stream properties and raise a clear error message
        when frame rate is None, preventing downstream processing errors.
        """
        # Arrange
        stream = mocker.Mock()
        stream.average_rate = None  # Invalid frame rate
        stream.time_base = 1 / 30.0  # Valid time base
        stream.frames = 900  # Valid frame count

        # Act & Assert
        with pytest.raises(ValueError, match="Frame rate or time base is None"):
            get_stream_properties(stream)

    @pytest.mark.unit
    def test_get_stream_properties_none_time_base(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test get_stream_properties raises ValueError when time base is None.

        This test ensures proper error handling when video stream has invalid time base.
        """
        # Arrange
        stream = mocker.Mock()
        stream.average_rate = 30.0
        stream.time_base = None
        stream.frames = 900

        # Act & Assert
        with pytest.raises(ValueError, match="Frame rate or time base is None"):
            get_stream_properties(stream)

    @pytest.mark.unit
    def test_generate_potential_filenames(self, test_video_path: Path, tmp_path: Path) -> None:
        """Test generating potential filenames for video frames.

        This test verifies that the generate_potential_filenames function:
        1. Creates the correct number of filenames based on frame count and interval
        2. Uses proper zero-padding for frame numbers
        3. Formats filenames correctly with video stem, frame number, and suffix
        4. Returns a dictionary mapping frame numbers to Path objects
        """
        # Arrange
        output_dir = tmp_path / "output"
        total_frames = 100
        frame_interval = 10
        suffix = "_THUMB"

        # Act
        filenames = generate_potential_filenames(test_video_path, output_dir, total_frames, frame_interval, suffix)

        # Assert
        expected_count = total_frames // frame_interval  # 100 frames / 10 interval = 10
        assert len(filenames) == expected_count, f"Should generate {expected_count} filenames"

        # Check that frame numbers are correct (0, 10, 20, ..., 90)
        expected_frame_numbers = list(range(0, total_frames, frame_interval))
        assert set(filenames.keys()) == set(expected_frame_numbers), "Frame numbers should match expected range"

        # Check first and last frame numbers
        assert 0 in filenames, "Should include frame 0"
        assert 90 in filenames, "Should include frame 90"
        assert 100 not in filenames, "Should not include frame 100 (out of range)"

        # Check filename format for first frame (3-digit padding for 100 frames)
        expected_filename = "test_video_000_THUMB.JPG"
        assert filenames[0].name == expected_filename, "First frame filename should use correct format and padding"

        # Check filename format for last frame
        expected_last_filename = "test_video_090_THUMB.JPG"
        assert filenames[90].name == expected_last_filename, "Last frame filename should use correct format"

        # Check that all paths have correct parent directory
        for frame_num, path in filenames.items():
            assert path.parent == output_dir, f"Frame {frame_num} path should be in output directory"

    @pytest.mark.unit
    def test_generate_potential_filenames_padding(self, test_video_path: Path, tmp_path: Path) -> None:
        """Test filename padding adjusts correctly for different frame counts.

        This test verifies that zero-padding width adapts to the total frame count
        to ensure consistent filename sorting and readability. It tests the pure
        business logic of the generate_potential_filenames function in isolation.
        """
        # Arrange
        output_dir = tmp_path / "output"
        suffix = "_THUMB"

        # Act & Assert: Test with 1000 frames (4-digit padding)
        filenames_1000 = generate_potential_filenames(test_video_path, output_dir, 1000, 100, suffix)

        expected_filename_1000 = "test_video_0000_THUMB.JPG"
        assert filenames_1000[0].name == expected_filename_1000, "Should use 4-digit padding for 1000 frames"
        assert len(filenames_1000) == 10, "Should generate 10 filenames for 1000 frames with interval 100"

        # Act & Assert: Test with 10 frames (2-digit padding)
        filenames_10 = generate_potential_filenames(test_video_path, output_dir, 10, 1, suffix)

        expected_filename_10 = "test_video_00_THUMB.JPG"
        assert filenames_10[0].name == expected_filename_10, "Should use 2-digit padding for 10 frames"
        assert len(filenames_10) == 10, "Should generate 10 filenames for 10 frames with interval 1"

        # Act & Assert: Test with 1 frame (1-digit padding)
        filenames_1 = generate_potential_filenames(test_video_path, output_dir, 1, 1, suffix)

        expected_filename_1 = "test_video_0_THUMB.JPG"
        assert filenames_1[0].name == expected_filename_1, "Should use 1-digit padding for 1 frame"
        assert len(filenames_1) == 1, "Should generate 1 filename for 1 frame with interval 1"

        # Assert: Verify all paths have correct parent directory
        for path in filenames_1000.values():
            assert path.parent == output_dir, "All generated paths should be in output directory"

    @pytest.mark.unit
    def test_generate_potential_filenames_custom_suffix(self, test_video_path: Path, tmp_path: Path) -> None:
        """Test that custom suffix is properly applied to all generated filenames.

        This test verifies that the generate_potential_filenames function correctly:
        1. Applies the custom suffix to all generated filenames
        2. Generates the correct number of filenames based on frame count and interval
        3. Uses proper zero-padding for frame numbers with custom suffix
        4. Places all filenames in the correct output directory
        """
        # Arrange
        output_dir = tmp_path / "output"
        custom_suffix = "_CUSTOM_THUMB"
        total_frames = 50
        frame_interval = 10

        # Act
        filenames = generate_potential_filenames(
            test_video_path,
            output_dir,
            total_frames,
            frame_interval,
            custom_suffix,
        )

        # Assert
        # Check total number of generated filenames
        expected_count = total_frames // frame_interval  # 50 frames / 10 interval = 5
        assert len(filenames) == expected_count, f"Should generate {expected_count} filenames"

        # Check that frame numbers are correct (0, 10, 20, 30, 40)
        expected_frame_numbers = list(range(0, total_frames, frame_interval))
        assert set(filenames.keys()) == set(expected_frame_numbers), "Frame numbers should match expected range"

        # Check that all filenames use the custom suffix
        for frame_num, path in filenames.items():
            assert custom_suffix in path.name, f"Frame {frame_num} filename should contain custom suffix"
            assert path.name.endswith(".JPG"), f"Frame {frame_num} filename should end with .JPG"
            assert path.parent == output_dir, f"Frame {frame_num} path should be in output directory"

        # Check specific filename format for first and last frames
        expected_first_filename = "test_video_00_CUSTOM_THUMB.JPG"
        assert (
            filenames[0].name == expected_first_filename
        ), "First frame filename should use correct custom suffix format"

        expected_last_filename = "test_video_40_CUSTOM_THUMB.JPG"
        assert (
            filenames[40].name == expected_last_filename
        ), "Last frame filename should use correct custom suffix format"

    @pytest.mark.unit
    def test_generate_potential_filenames_edge_cases(self, tmp_path: Path) -> None:
        """Test edge cases and boundary conditions for generate_potential_filenames function.

        This test verifies that the function handles various edge cases correctly:
        1. Special characters in video filenames (hyphens, dots, underscores)
        2. Zero padding calculation with minimal frame counts
        3. Large frame intervals that result in single frame output
        4. Frame intervals equal to total frames (boundary condition)

        These edge cases ensure the function is robust across different input scenarios
        and maintains consistent filename formatting regardless of input characteristics.
        """
        # Arrange
        output_dir = tmp_path / "output"
        suffix = "_THUMB"

        # Test Case 1: Special characters in video filename
        video_with_special_chars = tmp_path / "test-video_with.dots.mp4"

        # Act
        filenames_special_chars = generate_potential_filenames(
            video_with_special_chars,
            output_dir,
            20,
            5,
            suffix,
        )

        # Assert
        expected_filename = "test-video_with.dots_00_THUMB.JPG"
        assert (
            filenames_special_chars[0].name == expected_filename
        ), "Should handle special characters in video filename"
        assert len(filenames_special_chars) == 4, "Should generate 4 filenames for 20 frames with interval 5"

        # Test Case 2: Large frame interval resulting in single frame
        video_single_frame = tmp_path / "single_frame_test.mp4"

        # Act
        filenames_single = generate_potential_filenames(
            video_single_frame,
            output_dir,
            10,
            10,
            suffix,
        )

        # Assert
        assert len(filenames_single) == 1, "Should generate 1 filename when frame_interval equals total_frames"
        assert 0 in filenames_single, "Should include frame 0 when interval equals total frames"
        expected_single_filename = "single_frame_test_00_THUMB.JPG"
        assert (
            filenames_single[0].name == expected_single_filename
        ), "Should use correct padding based on total_frames count (10 frames = 2-digit padding)"

        # Test Case 3: Frame interval larger than total frames
        video_no_frames = tmp_path / "no_frames_test.mp4"

        # Act
        filenames_empty = generate_potential_filenames(
            video_no_frames,
            output_dir,
            5,
            10,
            suffix,
        )

        # Assert
        assert len(filenames_empty) == 1, "Should generate 1 filename when interval > total_frames (frame 0 only)"
        assert 0 in filenames_empty, "Should include frame 0 when interval > total_frames"

        # Test Case 4: Verify all paths have correct parent directory for all edge cases
        for frame_num, path in filenames_special_chars.items():
            assert path.parent == output_dir, f"Frame {frame_num} path should be in output directory"
        for frame_num, path in filenames_single.items():
            assert path.parent == output_dir, f"Frame {frame_num} path should be in output directory"
        for frame_num, path in filenames_empty.items():
            assert path.parent == output_dir, f"Frame {frame_num} path should be in output directory"

    @pytest.mark.unit
    def test_filter_existing_thumbnails_no_overwrite(self, tmp_path: Path) -> None:
        """Test that filter_existing_thumbnails correctly identifies and removes existing files when overwrite=False.

        This test verifies that when overwrite is disabled, the function:
        1. Returns a list of existing thumbnail paths
        2. Removes existing files from the potential_filenames dict in-place
        3. Preserves non-existing files in the potential_filenames dict
        """
        # Arrange
        existing_file1 = tmp_path / "file1.jpg"
        existing_file2 = tmp_path / "file2.jpg"
        non_existing_file = tmp_path / "file3.jpg"

        existing_file1.touch()
        existing_file2.touch()

        potential_filenames = {
            0: existing_file1,
            1: existing_file2,
            2: non_existing_file,
        }

        # Act
        existing_paths = filter_existing_thumbnails(potential_filenames, overwrite=False)

        # Assert
        assert len(existing_paths) == 2, "Should return exactly 2 existing file paths"
        assert existing_file1 in existing_paths, "Should include first existing file in results"
        assert existing_file2 in existing_paths, "Should include second existing file in results"
        assert non_existing_file not in existing_paths, "Should not include non-existing file in results"

        # Verify in-place modification of potential_filenames dict
        assert len(potential_filenames) == 1, "Should have only 1 non-existing file remaining in dict"
        assert 2 in potential_filenames, "Should preserve frame number 2 (non-existing file)"
        assert potential_filenames[2] == non_existing_file, "Should preserve correct path for non-existing file"
        assert 0 not in potential_filenames, "Should remove frame number 0 (existing file)"
        assert 1 not in potential_filenames, "Should remove frame number 1 (existing file)"

    @pytest.mark.unit
    def test_filter_existing_thumbnails_with_overwrite(self, tmp_path: Path) -> None:
        """Test that filter_existing_thumbnails preserves all files when overwrite=True.

        This test verifies that when overwrite is enabled, the function:
        1. Returns an empty list (no existing files to report)
        2. Preserves all files in the potential_filenames dict (no in-place modification)
        3. Allows all files to be processed regardless of existence
        4. Does not modify the original dictionary when overwrite=True
        """
        # Arrange
        existing_file1 = tmp_path / "file1.jpg"
        existing_file2 = tmp_path / "file2.jpg"
        non_existing_file = tmp_path / "file3.jpg"

        existing_file1.touch()
        existing_file2.touch()
        # non_existing_file is intentionally not created

        potential_filenames = {
            0: existing_file1,
            1: existing_file2,
            2: non_existing_file,
        }
        original_dict_size = len(potential_filenames)
        original_keys = set(potential_filenames.keys())

        # Act
        existing_paths = filter_existing_thumbnails(potential_filenames, overwrite=True)

        # Assert
        assert len(existing_paths) == 0, "Should return empty list when overwrite=True"
        assert len(potential_filenames) == original_dict_size, "Should preserve all files in dict when overwrite=True"
        assert set(potential_filenames.keys()) == original_keys, "Should not modify dictionary keys when overwrite=True"
        assert 0 in potential_filenames, "Should preserve frame number 0 (existing file)"
        assert 1 in potential_filenames, "Should preserve frame number 1 (existing file)"
        assert 2 in potential_filenames, "Should preserve frame number 2 (non-existing file)"
        assert potential_filenames[0] == existing_file1, "Should preserve correct path for existing file 1"
        assert potential_filenames[1] == existing_file2, "Should preserve correct path for existing file 2"
        assert potential_filenames[2] == non_existing_file, "Should preserve correct path for non-existing file"

    @pytest.mark.unit
    def test_filter_existing_thumbnails_no_overwrite_logs_existing_files(
        self,
        mocker: pytest_mock.MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test that filter_existing_thumbnails logs existing files and modifies dict when overwrite=False.

        This test verifies that when overwrite=False and existing files are found:
        1. The function logs each existing file with the correct message format
        2. The function returns existing file paths in the result list
        3. The function removes existing files from potential_filenames dict in-place
        4. The function preserves non-existing files in the dict
        """
        # Arrange
        mock_logger = mocker.patch("marimba.lib.video.logger")
        existing_file1 = tmp_path / "file1.jpg"
        existing_file2 = tmp_path / "file2.jpg"
        non_existing_file = tmp_path / "file3.jpg"

        existing_file1.touch()
        existing_file2.touch()
        # non_existing_file is intentionally not created

        potential_filenames = {
            0: existing_file1,
            1: existing_file2,
            2: non_existing_file,
        }

        # Act
        result_paths = filter_existing_thumbnails(potential_filenames, overwrite=False)

        # Assert
        # Verify logging behavior
        assert mock_logger.info.call_count == 2, "Should log exactly 2 info messages for the 2 existing files"
        expected_calls = [
            mocker.call(f"Thumbnail already exists: {existing_file1}"),
            mocker.call(f"Thumbnail already exists: {existing_file2}"),
        ]
        mock_logger.info.assert_has_calls(expected_calls, any_order=True)

        # Verify return value contains existing files
        assert len(result_paths) == 2, "Should return exactly 2 existing file paths"
        assert existing_file1 in result_paths, "Should include first existing file in results"
        assert existing_file2 in result_paths, "Should include second existing file in results"
        assert non_existing_file not in result_paths, "Should not include non-existing file in results"

        # Verify in-place modification of potential_filenames dict
        assert len(potential_filenames) == 1, "Should have only 1 non-existing file remaining in dict after filtering"
        assert 2 in potential_filenames, "Should preserve frame number 2 (non-existing file) in dict"
        assert potential_filenames[2] == non_existing_file, "Should preserve correct path for non-existing file"
        assert 0 not in potential_filenames, "Should remove frame number 0 (existing file) from dict"
        assert 1 not in potential_filenames, "Should remove frame number 1 (existing file) from dict"

    @pytest.mark.unit
    def test_save_thumbnail(self, mocker: pytest_mock.MockerFixture, tmp_path: Path) -> None:
        """Test saving thumbnail from video frame.

        This unit test verifies that save_thumbnail correctly processes a video frame
        by testing the function in isolation with mocked external dependencies.
        It validates that the function converts frames to images, applies proper resizing,
        and saves with the correct parameters.
        """
        # Arrange
        output_path = tmp_path / "thumb.jpg"

        # Mock external PyAV frame object
        import av

        mock_frame = mocker.Mock(spec=av.video.frame.VideoFrame)

        # Mock the PIL Image object returned by to_image()
        mock_image = mocker.Mock()
        mock_frame.to_image.return_value = mock_image

        # Act
        save_thumbnail(mock_frame, output_path)

        # Assert
        mock_frame.to_image.assert_called_once_with(), "Should convert frame to image"

        # Verify thumbnail resizing with specific parameters
        from PIL import Image

        mock_image.thumbnail.assert_called_once_with(
            (300, 300),
            Image.Resampling.LANCZOS,
        ), "Should resize image to 300x300 max with LANCZOS resampling"

        mock_image.save.assert_called_once_with(output_path), "Should save thumbnail to specified output path"

    @pytest.mark.unit
    def test_save_thumbnail_io_error(self, mocker: pytest_mock.MockerFixture, tmp_path: Path) -> None:
        """Test save_thumbnail properly propagates OSError during image save operation.

        This test verifies that when the PIL Image.save() method raises an OSError
        (such as permission denied or disk full), the exception is properly propagated
        to the caller without being caught or transformed. It ensures that frame
        processing occurs normally up to the save operation, then the error is raised.
        """
        # Arrange
        output_path = tmp_path / "thumb.jpg"

        import av

        mock_frame = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_image = mocker.Mock()
        mock_frame.to_image.return_value = mock_image

        # Mock save to raise OSError simulating disk space or permission issues
        error_message = "No space left on device"
        mock_image.save.side_effect = OSError(error_message)

        # Act & Assert
        with pytest.raises(OSError, match=r"No space left on device"):
            save_thumbnail(mock_frame, output_path)

        # Verify that frame processing occurred before the error
        mock_frame.to_image.assert_called_once(), "Should convert frame to image before error"

        # Verify thumbnail resizing with specific parameters
        from PIL import Image

        mock_image.thumbnail.assert_called_once_with(
            (300, 300),
            Image.Resampling.LANCZOS,
        ), "Should resize image to 300x300 max with LANCZOS resampling before error"

        mock_image.save.assert_called_once_with(output_path), "Should attempt to save image before raising error"

    @pytest.mark.integration
    def test_generate_video_thumbnails_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test basic video thumbnail generation workflow.

        This integration test verifies the complete workflow of video thumbnail generation
        by mocking only external dependencies (PyAV) while testing real interactions
        between internal components. It processes one frame to validate the entire pipeline.
        """
        # Arrange
        output_dir = tmp_path / "output"

        # Mock only external dependency (PyAV)
        mock_av_open = mocker.patch("av.open")
        mock_save_thumbnail = mocker.patch("marimba.lib.video.save_thumbnail")

        # Create mock video frame with proper type
        import av

        mock_frame = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_frame.pts = 0  # Frame at beginning of video

        # Create mock packet that yields frames
        mock_packet = mocker.Mock()
        mock_packet.decode.return_value = [mock_frame]

        # Create mock container and stream
        mock_container = mocker.Mock()
        mock_stream = mocker.Mock()

        # Set up realistic stream properties for real component integration
        mock_stream.average_rate = 30.0
        mock_stream.time_base = 1 / 30.0
        mock_stream.frames = 900
        mock_container.streams.video = [mock_stream]
        mock_container.demux.return_value = [mock_packet]
        mock_av_open.return_value = mock_container

        # Act
        result_video, result_paths = generate_video_thumbnails(test_video_path, output_dir)

        # Assert
        assert result_video == test_video_path, "Should return the input video path"
        assert len(result_paths) == 1, "Should return one generated thumbnail path"
        assert output_dir.exists(), "Should create output directory"

        # Verify the generated thumbnail path uses real filename logic
        generated_path = result_paths[0]
        assert generated_path.name == "test_video_000_THUMB.JPG", "Should use correct filename format"
        assert generated_path.parent == output_dir, "Should place thumbnail in output directory"

        # Verify frame processing integration
        mock_container.demux.assert_called_once_with(mock_stream)
        mock_packet.decode.assert_called_once()
        mock_save_thumbnail.assert_called_once_with(mock_frame, generated_path)

        # Verify external dependency interactions
        mock_av_open.assert_called_once_with(str(test_video_path))

    @pytest.mark.integration
    def test_generate_video_thumbnails_no_potential_frames(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test video thumbnail generation when no potential filenames are generated.

        This integration test verifies that when no potential filenames are generated
        (e.g., due to zero frames in video), the complete workflow properly handles the edge case:
        1. Stream properties are correctly extracted from the video
        2. Potential filenames logic generates empty dictionary when zero frames
        3. Existing file filtering works with empty input
        4. Frame processing is skipped appropriately
        5. Output directory is created and proper return values provided
        """
        # Arrange
        output_dir = tmp_path / "output"

        # Mock only external dependency (PyAV) - test real internal component interactions
        mock_av_open = mocker.patch("av.open")
        mock_container = mocker.Mock()
        mock_stream = mocker.Mock()

        # Set up stream properties that will generate no potential filenames
        # Zero frames ensures range(0, 0, frame_interval) creates empty range in generate_potential_filenames
        mock_stream.average_rate = 30.0
        mock_stream.time_base = 1 / 30.0
        mock_stream.frames = 0  # Zero frames ensures no potential filenames generated
        mock_container.streams.video = [mock_stream]
        mock_av_open.return_value = mock_container

        # Act
        result_video, result_paths = generate_video_thumbnails(
            test_video_path,
            output_dir,
            interval=10,
        )

        # Assert
        assert result_video == test_video_path, "Should return the input video path unchanged"
        assert result_paths == [], "Should return empty list when no potential filenames are generated"
        assert output_dir.exists(), "Should create output directory even when no frames to process"

        # Verify external dependency interactions
        mock_av_open.assert_called_once_with(str(test_video_path))

        # Verify demux is not called when there are no potential filenames to process
        mock_container.demux.assert_not_called()

    @pytest.mark.unit
    def test_generate_video_thumbnails_av_error(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test video thumbnail generation with AV error.

        This unit test verifies that when av.open() raises a generic AV error (not FFmpeg-related),
        the exception is properly propagated to the caller without being caught and handled
        by the FFmpeg dependency error handling logic. This tests the error handling behavior
        in isolation to ensure proper exception propagation for non-FFmpeg AV errors.
        """
        # Arrange
        output_dir = tmp_path / "output"
        error_message = "Generic AV error"

        mock_av_open = mocker.patch("av.open")
        mock_av_open.side_effect = RuntimeError(error_message)

        # Act & Assert
        with pytest.raises(RuntimeError, match=r"Generic AV error"):
            generate_video_thumbnails(test_video_path, output_dir)

        # Verify av.open was called with correct parameters
        mock_av_open.assert_called_once_with(str(test_video_path))

        # Verify output directory creation still occurs before error
        assert output_dir.exists(), "Should create output directory before processing video"

    @pytest.mark.unit
    def test_generate_video_thumbnails_ffmpeg_error(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test video thumbnail generation with FFmpeg dependency error.

        This unit test verifies that FFmpeg-related errors are properly detected and handled
        by calling the dependency error handler with the correct parameters and error message.
        The function should detect FFmpeg dependency issues and call show_dependency_error_and_exit
        rather than propagating the original FileNotFoundError. This tests error handling
        logic in isolation with mocked external dependencies.
        """
        # Arrange
        output_dir = tmp_path / "output"
        ffmpeg_error_message = "No such file or directory: ffmpeg not found"

        mock_av_open = mocker.patch("av.open")
        mock_av_open.side_effect = FileNotFoundError(ffmpeg_error_message)

        mock_show_error = mocker.patch("marimba.lib.video.show_dependency_error_and_exit")
        # Mock the function to raise SystemExit to simulate its real behavior
        mock_show_error.side_effect = SystemExit(1)

        # Act & Assert
        with pytest.raises(SystemExit):
            generate_video_thumbnails(test_video_path, output_dir)

        # Verify the dependency error handler was called with correct parameters
        mock_show_error.assert_called_once()
        call_args = mock_show_error.call_args

        # Verify first argument is ToolDependency.FFMPEG
        from marimba.core.utils.dependencies import ToolDependency

        assert call_args[0][0] == ToolDependency.FFMPEG, "Should pass ToolDependency.FFMPEG as first argument"

        # Verify second argument contains the original error message
        error_message = call_args[0][1]
        assert "PyAV requires FFmpeg libraries" in error_message, "Should indicate PyAV dependency issue"
        assert ffmpeg_error_message in error_message, "Should include original error message"

        # Verify av.open was called with correct parameters
        mock_av_open.assert_called_once_with(str(test_video_path))

    @pytest.mark.integration
    def test_generate_video_thumbnails_custom_params(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test video thumbnail generation with custom parameters.

        This integration test verifies that custom parameters (interval, suffix, overwrite)
        are properly passed through the processing pipeline and affect the actual
        filename generation and filtering logic. It tests real component interactions
        while mocking only the external PyAV dependency.
        """
        # Arrange
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create a pre-existing file to test overwrite behavior
        existing_frame_path = output_dir / "test_video_125_CUSTOM.JPG"
        existing_frame_path.touch()
        assert existing_frame_path.exists(), "Pre-existing file should be created for test setup"

        # Mock only external dependency (PyAV)
        mock_av_open = mocker.patch("av.open")
        mock_container = mocker.Mock()
        mock_stream = mocker.Mock()

        # Set up realistic stream properties for 25fps video with 750 frames (30 seconds)
        mock_stream.average_rate = 25.0
        mock_stream.time_base = 1 / 25.0
        mock_stream.frames = 750
        mock_container.streams.video = [mock_stream]

        # Create mock frame that matches the expected frame calculation
        # With interval=5 seconds, frame_interval becomes 125 frames (25fps * 5s)
        # Frame number calculation: frame_number = int(pts * time_base * frame_rate)
        # To generate a thumbnail at frame 125, we need pts value of 125
        import av

        mock_frame = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_frame.pts = 125  # This will calculate to frame number 125

        # Create mock packet containing our frame
        mock_packet = mocker.Mock()
        mock_packet.decode.return_value = [mock_frame]
        mock_container.demux.return_value = [mock_packet]
        mock_av_open.return_value = mock_container

        # Mock the save_thumbnail function to avoid actual file I/O
        mock_save_thumbnail = mocker.patch("marimba.lib.video.save_thumbnail")

        # Act
        result_video, result_paths = generate_video_thumbnails(
            test_video_path,
            output_dir,
            interval=5,
            suffix="_CUSTOM",
            overwrite=True,
        )

        # Assert
        assert result_video == test_video_path, "Should return the input video path"
        assert len(result_paths) == 1, "Should return exactly one generated thumbnail path"

        # Verify that the custom suffix was used in the generated path
        generated_path = result_paths[0]
        assert "_CUSTOM" in generated_path.name, "Generated path should contain custom suffix"
        assert generated_path.name == "test_video_125_CUSTOM.JPG", "Should use correct custom filename format"
        assert generated_path.parent == output_dir, "Generated path should be in output directory"

        # Verify that overwrite=True allowed processing of existing file
        assert existing_frame_path in result_paths, "Should include existing file when overwrite=True"
        assert generated_path == existing_frame_path, "Generated path should match pre-existing file path"

        # Verify frame processing occurred with correct parameters
        mock_save_thumbnail.assert_called_once_with(mock_frame, existing_frame_path)

        # Verify external dependency interactions
        mock_av_open.assert_called_once_with(str(test_video_path))
        mock_container.demux.assert_called_once_with(mock_stream)

        # Verify custom interval was correctly applied in frame calculations
        # interval=5 seconds * 25fps = 125 frame interval, so frame 125 should be processed
        expected_frame_number = int(mock_frame.pts * mock_stream.time_base * mock_stream.average_rate)
        assert expected_frame_number == 125, "Frame number calculation should match expected interval"

    @pytest.mark.integration
    def test_generate_video_thumbnails_with_frame_processing(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test video thumbnail generation with frame processing integration.

        This integration test verifies the complete workflow from video stream processing
        through frame extraction to thumbnail generation. It mocks only the external PyAV
        dependency while testing real interactions between internal components including
        stream property extraction, filename generation, existing file filtering, and
        thumbnail saving coordination.
        """
        # Arrange
        output_dir = tmp_path / "output"

        # Mock only external dependency (PyAV) - keep internal logic real
        mock_av_open = mocker.patch("av.open")
        mock_save_thumbnail = mocker.patch("marimba.lib.video.save_thumbnail")

        # Create mock video frame with proper type
        import av

        mock_frame = mocker.Mock(spec=av.video.frame.VideoFrame)
        # Set pts to 0 to match the first frame that will be generated
        # With interval=10 seconds and 30fps, frame_interval=300, so frames at 0, 300, 600
        mock_frame.pts = 0

        # Create mock packet that yields frames
        mock_packet = mocker.Mock()
        mock_packet.decode.return_value = [mock_frame]

        # Create mock container with demux that yields packets
        mock_container = mocker.Mock()
        mock_stream = mocker.Mock()

        # Set up realistic stream properties for real component integration
        mock_stream.average_rate = 30.0
        mock_stream.time_base = 1 / 30.0
        mock_stream.frames = 900

        mock_container.streams.video = [mock_stream]
        mock_container.demux.return_value = [mock_packet]
        mock_av_open.return_value = mock_container

        # Act
        result_video, result_paths = generate_video_thumbnails(test_video_path, output_dir)

        # Assert
        assert result_video == test_video_path, "Should return the input video path"
        assert len(result_paths) == 1, "Should return one generated thumbnail path"
        assert output_dir.exists(), "Should create output directory"

        # Verify frame processing integration
        mock_container.demux.assert_called_once_with(mock_stream), "Should demux the video stream"
        mock_packet.decode.assert_called_once_with(), "Should decode packets to extract frames"

        # Verify thumbnail generation with real filename logic
        # frame_number = int(frame.pts * time_base * frame_rate) = int(0 * (1/30) * 30) = 0
        # With 900 frames, padding width = 3, so frame 0 becomes "000"
        expected_frame_number = 0
        expected_filename = f"test_video_{expected_frame_number:03d}_THUMB.JPG"
        generated_path = result_paths[0]
        assert generated_path.name == expected_filename, "Should use correct filename format from real logic"
        assert generated_path.parent == output_dir, "Should place thumbnail in output directory"

        # Verify save_thumbnail was called with correct parameters
        mock_save_thumbnail.assert_called_once_with(mock_frame, generated_path)

        # Verify external dependency interactions
        mock_av_open.assert_called_once_with(str(test_video_path))

    @pytest.mark.integration
    def test_generate_video_thumbnails_early_exit_when_all_frames_processed(
        self,
        mocker: pytest_mock.MockerFixture,
        test_video_path: Path,
        tmp_path: Path,
    ) -> None:
        """Test early exit when overwrite=False and all potential filenames are processed.

        This integration test verifies that when overwrite=False, the function correctly
        exits early when all frames in potential_filenames have been processed, even if
        more frames are available in the video stream. It tests the real early exit
        logic while mocking only the external PyAV dependency.
        """
        # Arrange
        output_dir = tmp_path / "output"

        # Mock only external PyAV dependency to control frame processing
        mock_av_open = mocker.patch("av.open")
        mock_save_thumbnail = mocker.patch("marimba.lib.video.save_thumbnail")

        # Create mock container and stream with realistic properties
        mock_container = mocker.Mock()
        mock_stream = mocker.Mock()

        # Set up stream properties: 30fps, 900 total frames (30 second video)
        mock_stream.average_rate = 30.0
        mock_stream.time_base = 1 / 30.0
        mock_stream.frames = 900
        mock_container.streams.video = [mock_stream]

        # Create mock frames to test early exit behavior
        # With interval=15 seconds, frame_interval becomes 450 frames (30fps * 15s)
        # Expected frames: 0, 450 (only 2 frames within 900 total frames)
        import av

        mock_frame1 = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_frame1.pts = 0  # Frame 0

        mock_frame2 = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_frame2.pts = 450  # Frame 450

        # Create mock frame that should NOT be processed due to early exit
        mock_frame_unprocessed = mocker.Mock(spec=av.video.frame.VideoFrame)
        mock_frame_unprocessed.pts = 600  # Frame 600 - beyond our expected frames

        # Create mock packets that yield the frames sequentially
        mock_packet1 = mocker.Mock()
        mock_packet1.decode.return_value = [mock_frame1]
        mock_packet2 = mocker.Mock()
        mock_packet2.decode.return_value = [mock_frame2]
        mock_packet3 = mocker.Mock()
        mock_packet3.decode.return_value = [mock_frame_unprocessed]

        # Set up demux to return packets, but early exit should prevent processing packet3
        mock_container.demux.return_value = [mock_packet1, mock_packet2, mock_packet3]
        mock_av_open.return_value = mock_container

        # Act: Generate thumbnails with large interval to create only 2 potential filenames
        result_video, result_paths = generate_video_thumbnails(
            test_video_path,
            output_dir,
            interval=15,  # 15 second intervals = 450 frame intervals
            overwrite=False,
        )

        # Assert: Verify early exit behavior and real filename generation
        assert result_video == test_video_path, "Should return the input video path"
        assert output_dir.exists(), "Should create output directory"
        assert len(result_paths) == 2, "Should return exactly 2 generated thumbnail paths"

        # Verify correct thumbnail paths were generated using real filename logic
        expected_paths = [
            output_dir / "test_video_000_THUMB.JPG",  # Frame 0
            output_dir / "test_video_450_THUMB.JPG",  # Frame 450
        ]
        assert set(result_paths) == set(expected_paths), "Should generate thumbnails for frames 0 and 450"

        # Verify early exit: save_thumbnail should be called exactly twice before early return
        assert mock_save_thumbnail.call_count == 2, "Should process exactly 2 frames before early exit"

        # Verify correct frames were processed and unprocessed frame was NOT processed
        call_args = mock_save_thumbnail.call_args_list
        processed_frames = [call[0][0] for call in call_args]  # First argument is the frame
        processed_paths = [call[0][1] for call in call_args]  # Second argument is the path

        assert mock_frame1 in processed_frames, "Should process frame 0"
        assert mock_frame2 in processed_frames, "Should process frame 450"
        assert (
            mock_frame_unprocessed not in processed_frames
        ), "Should NOT process frame beyond expected frames due to early exit"
        assert set(processed_paths) == set(expected_paths), "Should save to correct paths"

        # Verify that packet3.decode was never called due to early exit
        mock_packet3.decode.assert_not_called()

        # Verify external dependency interactions
        mock_av_open.assert_called_once_with(str(test_video_path))
        mock_container.demux.assert_called_once_with(mock_stream)
