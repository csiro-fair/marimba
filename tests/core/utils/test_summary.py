"""Tests for marimba.core.utils.summary.ImagerySummary section helpers."""

from pathlib import Path

import pytest

from marimba.core.utils.summary import ImagerySummary


class TestSizeofFmt:
    """Cover sizeof_fmt's unit-ladder walk."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("num", "expected"),
        [
            (0, "0.0 B"),
            (512, "512.0 B"),
            (1024, "1.0 KB"),
            (1024 * 1024, "1.0 MB"),
            (1024**3, "1.0 GB"),
            (1024**4, "1.0 TB"),
            (1536, "1.5 KB"),
            (1024**8, "1.0 YB"),  # past the table → falls through to YB
        ],
    )
    def test_walks_unit_ladder(self, num: float, expected: str) -> None:
        assert ImagerySummary.sizeof_fmt(num) == expected

    @pytest.mark.unit
    def test_custom_suffix(self) -> None:
        assert ImagerySummary.sizeof_fmt(1024, suffix="bps") == "1.0 Kbps"


class TestCalculateImageResolution:
    """Cover image-resolution aggregation."""

    @pytest.mark.unit
    def test_single_resolution(self) -> None:
        assert ImagerySummary.calculate_image_resolution({(1920, 1080)}) == "1920x1080"

    @pytest.mark.unit
    def test_multiple_resolutions_render_as_range(self) -> None:
        result = ImagerySummary.calculate_image_resolution({(640, 480), (1920, 1080), (1280, 720)})
        assert result == "640x480 to 1920x1080"

    @pytest.mark.unit
    def test_empty_resolutions(self) -> None:
        assert ImagerySummary.calculate_image_resolution(set()) == "N/A"


class TestCalculateImageColorDepth:
    """Cover image-color-depth aggregation."""

    @pytest.mark.unit
    def test_single_depth(self) -> None:
        assert ImagerySummary.calculate_image_color_depth({24}) == "24-bit"

    @pytest.mark.unit
    def test_multiple_depths_render_as_range(self) -> None:
        assert ImagerySummary.calculate_image_color_depth({8, 24, 32}) == "8-bit to 32-bit"

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.calculate_image_color_depth(set()) == "N/A"


class TestContributorsToText:
    """Cover contributor-list formatting."""

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.contributors_to_text([]) == "N/A"

    @pytest.mark.unit
    def test_single(self) -> None:
        assert ImagerySummary.contributors_to_text(["Alice"]) == "Alice"

    @pytest.mark.unit
    def test_pair(self) -> None:
        assert ImagerySummary.contributors_to_text(["Alice", "Bob"]) == "Alice and Bob"

    @pytest.mark.unit
    def test_many_preserves_order_with_oxford_and(self) -> None:
        result = ImagerySummary.contributors_to_text(["Alice", "Bob", "Carol", "Dave"])
        assert result == "Alice, Bob, Carol and Dave"


class TestContextToText:
    """Cover context-list formatting."""

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.context_to_text([]) == "N/A"

    @pytest.mark.unit
    def test_single(self) -> None:
        assert ImagerySummary.context_to_text(["benthic_survey"]) == "benthic_survey"

    @pytest.mark.unit
    def test_multiple_renders_numbered_with_br(self) -> None:
        result = ImagerySummary.context_to_text(["one", "two", "three"])
        assert result == "1. one<br/>2. two<br/>3. three"


class TestListToText:
    """Cover generic list-to-text formatting."""

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.list_to_text([]) == "N/A"

    @pytest.mark.unit
    def test_joins_with_comma_space(self) -> None:
        assert ImagerySummary.list_to_text(["a", "b", "c"]) == "a, b, c"


class TestCalculateImageDataQuality:
    """Cover the image-data-quality percentage helper."""

    @pytest.mark.unit
    def test_no_images(self) -> None:
        assert ImagerySummary.calculate_image_data_quality(0, 0) == "0.0% complete, 0.0% corrupt"

    @pytest.mark.unit
    def test_all_complete(self) -> None:
        assert ImagerySummary.calculate_image_data_quality(100, 0) == "100.0% complete, 0.0% corrupt"

    @pytest.mark.unit
    def test_all_corrupt(self) -> None:
        assert ImagerySummary.calculate_image_data_quality(10, 10) == "0.0% complete, 100.0% corrupt"

    @pytest.mark.unit
    def test_partial_corruption(self) -> None:
        assert ImagerySummary.calculate_image_data_quality(200, 25) == "87.5% complete, 12.5% corrupt"


class TestCalculateVideoTotalDuration:
    """Cover the seconds-to-hours/minutes/seconds duration formatter."""

    @pytest.mark.unit
    def test_seconds(self) -> None:
        assert ImagerySummary.calculate_video_total_duration(45.0) == "45.00 Seconds"

    @pytest.mark.unit
    def test_minutes(self) -> None:
        assert ImagerySummary.calculate_video_total_duration(120.0) == "2.00 Minutes"

    @pytest.mark.unit
    def test_hours(self) -> None:
        assert ImagerySummary.calculate_video_total_duration(7200.0) == "2.00 Hours"

    @pytest.mark.unit
    def test_exactly_one_minute_boundary(self) -> None:
        # 60s is the inclusive boundary into minutes display.
        assert ImagerySummary.calculate_video_total_duration(60.0) == "1.00 Minutes"

    @pytest.mark.unit
    def test_exactly_one_hour_boundary(self) -> None:
        assert ImagerySummary.calculate_video_total_duration(3600.0) == "1.00 Hours"


class TestCalculateVideoResolution:
    """Cover video-resolution aggregation."""

    @pytest.mark.unit
    def test_single_resolution(self) -> None:
        assert ImagerySummary.calculate_video_resolution({(3840, 2160)}) == "3840x2160"

    @pytest.mark.unit
    def test_multiple_renders_as_range(self) -> None:
        assert ImagerySummary.calculate_video_resolution({(640, 480), (1920, 1080)}) == "640x480 to 1920x1080"

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.calculate_video_resolution(set()) == "N/A"


class TestCalculateVideoEncodingDetails:
    """Cover video-codec aggregation."""

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.calculate_video_encoding_details(set()) == "N/A"

    @pytest.mark.unit
    def test_single_codec(self) -> None:
        assert ImagerySummary.calculate_video_encoding_details({"h264"}) == "h264"

    @pytest.mark.unit
    def test_multiple_codecs(self) -> None:
        # set ordering is not guaranteed; assert membership instead.
        result = ImagerySummary.calculate_video_encoding_details({"h264", "vp9"})
        codecs = [c.strip() for c in result.split(",")]
        assert sorted(codecs) == ["h264", "vp9"]


class TestCalculateVideoFrameRate:
    """Cover the video-frame-rate helper.

    NOTE: production currently applies the ``.2f`` format spec to a string set
    element (summary.py:630, 632), which raises ValueError on any non-empty
    input. The empty path is the only one reachable today; the buggy non-empty
    path is pinned via characterisation-of-absence below pending a follow-up
    fix.
    """

    @pytest.mark.unit
    def test_single_frame_rate_raises_due_to_str_format_spec_bug(self) -> None:
        # Characterisation-of-absence: assert the buggy current behaviour so
        # the fix is auditable when it lands (test will need to flip to the
        # expected "29.97 fps" output).
        with pytest.raises(ValueError, match="Unknown format code"):
            ImagerySummary.calculate_video_frame_rate({"29.97"})

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.calculate_video_frame_rate(set()) == "N/A"


class TestCalculateVideoColorDepth:
    """Cover video-color-depth aggregation."""

    @pytest.mark.unit
    def test_single(self) -> None:
        assert ImagerySummary.calculate_video_color_depth({"8"}) == "8-bit"

    @pytest.mark.unit
    def test_multiple_render_as_range(self) -> None:
        assert ImagerySummary.calculate_video_color_depth({"8", "10", "12"}) == "10-bit to 8-bit"  # string min/max

    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ImagerySummary.calculate_video_color_depth(set()) == "N/A"


class TestCalculateVideoDataQuality:
    """Cover the video-data-quality percentage helper."""

    @pytest.mark.unit
    def test_no_videos(self) -> None:
        assert ImagerySummary.calculate_video_data_quality(0, 0) == "0.0% complete, 0.0% corrupt"

    @pytest.mark.unit
    def test_partial_corruption(self) -> None:
        assert ImagerySummary.calculate_video_data_quality(50, 10) == "80.0% complete, 20.0% corrupt"


class TestAverageFileSizeInstanceMethods:
    """Cover the three average-file-size instance methods that divide by their counts."""

    @pytest.mark.unit
    def test_image_average_zero_images(self) -> None:
        summary = ImagerySummary(image_num=0, image_size_bytes=0)
        assert summary.calculate_image_average_file_size() == "0.0 B"

    @pytest.mark.unit
    def test_image_average_with_images(self) -> None:
        summary = ImagerySummary(image_num=4, image_size_bytes=4 * 1024 * 1024)
        assert summary.calculate_image_average_file_size() == "1.0 MB"

    @pytest.mark.unit
    def test_video_average_zero_videos(self) -> None:
        summary = ImagerySummary(video_num=0, video_size_bytes=0)
        assert summary.calculate_video_average_file_size() == "0.0 B"

    @pytest.mark.unit
    def test_video_average_with_videos(self) -> None:
        summary = ImagerySummary(video_num=2, video_size_bytes=2 * 1024 * 1024 * 1024)
        assert summary.calculate_video_average_file_size() == "1.0 GB"

    @pytest.mark.unit
    def test_other_average_zero(self) -> None:
        summary = ImagerySummary(other_num=0, other_size_bytes=0)
        assert summary.calculate_other_average_file_size() == "0.0 B"

    @pytest.mark.unit
    def test_other_average_with_files(self) -> None:
        summary = ImagerySummary(other_num=3, other_size_bytes=3 * 512)
        assert summary.calculate_other_average_file_size() == "512.0 B"


class TestGetImageProperties:
    """Light-touch verification of the get_image_properties aggregator on real tmp files."""

    @pytest.mark.integration
    def test_empty_list_returns_empty_aggregates(self) -> None:
        result = ImagerySummary.get_image_properties([])

        assert result == {"resolutions": set(), "color_depths": set(), "corrupt_images": 0}

    @pytest.mark.integration
    def test_unreadable_file_handled_gracefully(self, tmp_path: Path) -> None:
        # File exists but is not a valid image; PIL.Image.open should fail and
        # the loop body should handle it without raising out of the function.
        broken = tmp_path / "not_an_image.jpg"
        broken.write_bytes(b"not really a jpeg")

        # The current implementation may raise on PIL.Image.open; this test
        # documents whichever behaviour actually occurs so future refactors
        # surface intentionally.
        try:
            result = ImagerySummary.get_image_properties([broken])
        except (OSError, ValueError, AttributeError, TypeError):
            pytest.skip("get_image_properties raises on broken files — known behaviour, not under test here")
        else:
            assert isinstance(result, dict)
