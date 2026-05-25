"""Tests for iFDOMetadata.process_files chunked-batch processing path."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import pytest_mock
from ifdo.models import ImageData

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.ifdo import (
    ProcessedImageData,
    _calculate_safe_image_batch_size,
    _get_available_memory_mb,
    iFDOMetadata,
)


def _make_image_data() -> ImageData:
    return ImageData(
        image_datetime=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        image_latitude=45.0,
        image_longitude=-123.0,
        image_altitude_meters=100.0,
        image_hash_sha256="abc123",
    )


def _make_mapping(
    paths: list[Path],
) -> dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]:
    return {p: ([iFDOMetadata(_make_image_data())], None) for p in paths}


class TestChunkDataset:
    """Cover the pure-data _chunk_dataset helper directly."""

    @pytest.mark.unit
    def test_single_chunk_when_smaller_than_chunk_size(self) -> None:
        mapping = _make_mapping([Path(f"/tmp/img{i:03d}.txt") for i in range(3)])

        chunks = iFDOMetadata._chunk_dataset(mapping, chunk_size=10)

        assert len(chunks) == 1
        assert len(chunks[0]) == 3

    @pytest.mark.unit
    def test_multiple_chunks_at_exact_boundary(self) -> None:
        mapping = _make_mapping([Path(f"/tmp/img{i:03d}.txt") for i in range(10)])

        chunks = iFDOMetadata._chunk_dataset(mapping, chunk_size=5)

        assert len(chunks) == 2
        assert all(len(c) == 5 for c in chunks)

    @pytest.mark.unit
    def test_trailing_partial_chunk(self) -> None:
        mapping = _make_mapping([Path(f"/tmp/img{i:03d}.txt") for i in range(11)])

        chunks = iFDOMetadata._chunk_dataset(mapping, chunk_size=5)

        assert len(chunks) == 3
        assert [len(c) for c in chunks] == [5, 5, 1]

    @pytest.mark.unit
    def test_empty_mapping_yields_no_chunks(self) -> None:
        chunks = iFDOMetadata._chunk_dataset({}, chunk_size=5)

        assert chunks == []

    @pytest.mark.unit
    def test_chunk_size_one_yields_singleton_chunks(self) -> None:
        mapping = _make_mapping([Path(f"/tmp/img{i:03d}.txt") for i in range(4)])

        chunks = iFDOMetadata._chunk_dataset(mapping, chunk_size=1)

        assert len(chunks) == 4
        assert all(len(c) == 1 for c in chunks)


class TestProcessFilesDryRun:
    """Cover the dry-run short-circuit path."""

    @pytest.mark.unit
    def test_dry_run_returns_immediately(self, mocker: pytest_mock.MockerFixture) -> None:
        chunk_spy = mocker.spy(iFDOMetadata, "_chunk_dataset")
        mapping = _make_mapping([Path("/tmp/anywhere.jpg")])

        iFDOMetadata.process_files(mapping, dry_run=True)

        # Dry-run must short-circuit before any chunking or processing.
        chunk_spy.assert_not_called()

    @pytest.mark.unit
    def test_dry_run_does_not_log_chunking_info(self, mocker: pytest_mock.MockerFixture) -> None:
        mock_logger = mocker.MagicMock()
        mapping = _make_mapping([Path("/tmp/anywhere.jpg")])

        iFDOMetadata.process_files(mapping, dry_run=True, logger=mock_logger)

        mock_logger.info.assert_not_called()


class TestProcessFilesNonExif:
    """Cover the non-EXIF code path so chunking runs without real images."""

    @pytest.mark.integration
    def test_non_exif_files_skip_image_processing(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        # .txt extension is outside EXIF_SUPPORTED_EXTENSIONS so the chunk
        # processing path classifies these as non-EXIF and skips PIL.Image.open.
        paths = []
        for i in range(3):
            p = tmp_path / f"file{i}.txt"
            p.write_text("content")
            paths.append(p)
        mapping = _make_mapping(paths)

        image_open_spy = mocker.patch("marimba.core.schemas.ifdo.Image.open")

        iFDOMetadata.process_files(mapping, max_workers=1, chunk_size=2)

        # No image opens occurred because every file has a non-EXIF extension.
        image_open_spy.assert_not_called()

    @pytest.mark.integration
    def test_chunk_size_respected_with_explicit_value(
        self,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        # Build 7 non-EXIF files and force chunk_size=3 → expect 3 chunks (3, 3, 1).
        paths = []
        for i in range(7):
            p = tmp_path / f"file{i}.txt"
            p.write_text("content")
            paths.append(p)
        mapping = _make_mapping(paths)
        chunk_spy = mocker.spy(iFDOMetadata, "_chunk_dataset")

        iFDOMetadata.process_files(mapping, max_workers=1, chunk_size=3)

        chunk_spy.assert_called_once()
        chunks_returned = chunk_spy.spy_return
        assert [len(c) for c in chunks_returned] == [3, 3, 1]


class TestProcessImagesMemorySafePartitioning:
    """Cover the EXIF / non-EXIF partitioning inside _process_images_memory_safe."""

    @pytest.mark.unit
    def test_separates_exif_from_non_exif(self, mocker: pytest_mock.MockerFixture) -> None:
        exif_path = Path("/tmp/photo.jpg")  # .jpg is in EXIF_SUPPORTED_EXTENSIONS
        non_exif_path = Path("/tmp/data.txt")
        chunk: list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]] = [
            (exif_path, ([iFDOMetadata(_make_image_data())], None)),
            (non_exif_path, ([iFDOMetadata(_make_image_data())], None)),
        ]
        # Short-circuit the adaptive batch path so we don't open the fake jpg.
        mocker.patch.object(iFDOMetadata, "_process_image_batch_adaptive", return_value=[])

        processed, non_exif = iFDOMetadata._process_images_memory_safe(
            chunk,
            thread_num="t0",
            logger=mocker.MagicMock(),
        )

        assert non_exif == [non_exif_path]
        # processed comes back empty because we stubbed the adaptive batch.
        assert processed == []

    @pytest.mark.unit
    def test_exif_extension_without_ifdo_metadata_routes_to_non_exif(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        # An EXIF extension whose metadata isn't iFDOMetadata falls through to
        # non_exif_files (see ifdo.py:665-674).
        path = Path("/tmp/photo.jpg")
        chunk: list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]] = [(path, ([], None))]

        processed, non_exif = iFDOMetadata._process_images_memory_safe(
            chunk,
            thread_num="t0",
            logger=mocker.MagicMock(),
        )

        assert processed == []
        assert non_exif == [path]


class TestWriteExifBatchPartialFailure:
    """Cover the partial-failure path: some ProcessedImageData items carry processing_error."""

    @pytest.mark.unit
    def test_failed_images_are_logged_and_excluded(self, mocker: pytest_mock.MockerFixture) -> None:
        good = ProcessedImageData(
            file_path=Path("/tmp/good.jpg"),
            image_data=_make_image_data(),
            ancillary_data=None,
            width=100,
            height=100,
        )
        bad = ProcessedImageData(
            file_path=Path("/tmp/bad.jpg"),
            image_data=_make_image_data(),
            ancillary_data=None,
            width=0,
            height=0,
            processing_error="OSError: cannot open",
        )
        mock_logger = mocker.MagicMock()
        # Short-circuit the real exiftool batch write — we only care about the
        # partial-failure branch around it.
        with patch("marimba.core.schemas.ifdo.exiftool.ExifToolHelper"):
            iFDOMetadata._write_exif_batch([good, bad], thread_num="t0", logger=mock_logger)

        # The warning about skipped failed images must fire exactly once.
        warning_calls = [c for c in mock_logger.warning.call_args_list if "Skipping" in str(c)]
        assert len(warning_calls) == 1

    @pytest.mark.unit
    def test_all_failed_images_returns_early(self, mocker: pytest_mock.MockerFixture) -> None:
        bad = ProcessedImageData(
            file_path=Path("/tmp/bad.jpg"),
            image_data=_make_image_data(),
            ancillary_data=None,
            width=0,
            height=0,
            processing_error="OSError: cannot open",
        )
        mock_logger = mocker.MagicMock()
        with patch("marimba.core.schemas.ifdo.exiftool.ExifToolHelper") as mock_helper:
            iFDOMetadata._write_exif_batch([bad], thread_num="t0", logger=mock_logger)

            # No exiftool invocation occurred because successful_images is empty.
            mock_helper.assert_not_called()

    @pytest.mark.unit
    def test_empty_input_returns_early(self, mocker: pytest_mock.MockerFixture) -> None:
        mock_logger = mocker.MagicMock()
        with patch("marimba.core.schemas.ifdo.exiftool.ExifToolHelper") as mock_helper:
            iFDOMetadata._write_exif_batch([], thread_num="t0", logger=mock_logger)

            mock_helper.assert_not_called()
            mock_logger.warning.assert_not_called()


class TestMemoryHelpers:
    """Cover the small memory-aware helpers _get_available_memory_mb and _calculate_safe_image_batch_size."""

    @pytest.mark.unit
    def test_get_available_memory_returns_positive_int(self) -> None:
        available = _get_available_memory_mb()

        assert isinstance(available, int)
        assert available > 0

    @pytest.mark.unit
    def test_calculate_safe_batch_size_with_empty_files(self) -> None:
        # When dataset_files is None or empty, the helper still returns a sensible default.
        batch_size = _calculate_safe_image_batch_size(None)
        assert isinstance(batch_size, int)
        assert batch_size >= 1

        batch_size_empty = _calculate_safe_image_batch_size([])
        assert isinstance(batch_size_empty, int)
        assert batch_size_empty >= 1

    @pytest.mark.unit
    def test_calculate_safe_batch_size_falls_back_when_no_psutil(self, mocker: pytest_mock.MockerFixture) -> None:
        # Force the psutil call to raise; the fallback should still return a positive int.
        mocker.patch(
            "marimba.core.schemas.ifdo.psutil.virtual_memory",
            side_effect=OSError("no virtual_memory"),
        )
        # Also stub the /proc/meminfo fallback to fail so we hit the constant default.
        mocker.patch("marimba.core.schemas.ifdo.Path.exists", return_value=False)

        available = _get_available_memory_mb()

        assert available == 4096  # the conservative constant fallback
