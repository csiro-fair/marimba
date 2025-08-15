"""
Marimba iFDO Metadata Implementation.

This module provides functionality for handling iFDO metadata, including creation, processing, and embedding of
metadata in image files. It implements the BaseMetadata interface for iFDO-specific metadata and offers methods
for creating dataset metadata and processing image files with EXIF data.

Imports:
    json: Handles JSON data encoding and decoding
    datetime: Supplies classes for working with dates and times
    Path: Offers object-oriented filesystem paths
    typing: Provides support for type hints
    uuid: Generates universally unique identifiers
    exiftool: Handles reading and writing of EXIF data in images via ExifTool
    PIL: Python Imaging Library for opening, manipulating, and saving image files
    rich: Offers rich text and beautiful formatting in the terminal

Classes:
    iFDOMetadata: Implements the BaseMetadata interface for iFDO-specific metadata
"""

import json
import logging
import os
import tempfile
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import exiftool
from exiftool.exceptions import ExifToolException
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import EXIF_SUPPORTED_EXTENSIONS
from marimba.core.utils.dependencies import ToolDependency, show_dependency_error_and_exit
from marimba.core.utils.log import get_logger
from marimba.core.utils.metadata import yaml_saver
from marimba.core.utils.rich import get_default_columns
from marimba.lib import image
from marimba.lib.decorators import multithreaded

if TYPE_CHECKING:
    from ifdo import ImageData, ImageSetHeader
    from ifdo.models.ifdo import iFDO
else:
    from ifdo import ImageData, ImageSetHeader
    from ifdo.models.ifdo import iFDO


logger = get_logger(__name__)

# Configuration for batch processing
DEFAULT_CHUNK_SIZE = 50  # Number of files to process in each batch

# Constants for adaptive chunk sizing
_VERY_SMALL_DATASET_THRESHOLD = 20
_SMALL_DATASET_THRESHOLD = 100
_MEDIUM_DATASET_THRESHOLD = 500
_LARGE_DATASET_THRESHOLD = 2000


def _calculate_optimal_chunk_size(dataset_size: int, max_workers: int | None = None) -> int:
    """
    Calculate optimal chunk size based on dataset size and available workers.

    Args:
        dataset_size: Total number of files to process
        max_workers: Maximum number of worker threads

    Returns:
        Optimal chunk size for the given dataset

    Environment Variables:
        MARIMBA_EXIF_CHUNK_SIZE: Override chunk size calculation
        MARIMBA_EXIF_MIN_CHUNK_SIZE: Minimum chunk size (default: 1)
        MARIMBA_EXIF_MAX_CHUNK_SIZE: Maximum chunk size (default: 200)
    """
    # Check for environment variable override
    env_chunk_size = os.environ.get("MARIMBA_EXIF_CHUNK_SIZE")
    if env_chunk_size:
        try:
            return max(1, int(env_chunk_size))
        except ValueError:
            pass  # Fall back to calculation

    # Get configuration from environment
    min_chunk_size = int(os.environ.get("MARIMBA_EXIF_MIN_CHUNK_SIZE", "1"))
    max_chunk_size = int(os.environ.get("MARIMBA_EXIF_MAX_CHUNK_SIZE", "200"))

    # Determine effective worker count
    effective_workers = max_workers if max_workers else os.cpu_count() or 4

    # Calculate base chunk size to ensure good thread utilization
    # Aim for 2-4 chunks per worker to allow for load balancing
    target_chunks = effective_workers * 3
    base_chunk_size = max(1, dataset_size // target_chunks)

    # Apply size-based adjustments
    if dataset_size <= _VERY_SMALL_DATASET_THRESHOLD:
        # Very small datasets: process all files in single chunk to minimize overhead
        calculated_size = dataset_size
    elif dataset_size <= _SMALL_DATASET_THRESHOLD:
        # Small datasets: use smaller chunks but not too small
        calculated_size = max(base_chunk_size, min(dataset_size // 4, _VERY_SMALL_DATASET_THRESHOLD))
    elif dataset_size <= _MEDIUM_DATASET_THRESHOLD:
        # Medium datasets: balanced approach
        calculated_size = max(base_chunk_size, min(dataset_size // 6, 75))
    elif dataset_size <= _LARGE_DATASET_THRESHOLD:
        # Large datasets: larger chunks for efficiency
        calculated_size = max(base_chunk_size, min(dataset_size // 8, 150))
    else:
        # Very large datasets: optimize for memory and stability
        calculated_size = max(base_chunk_size, min(dataset_size // 10, 200))

    # Apply environment-based constraints
    return max(min_chunk_size, min(calculated_size, max_chunk_size))


class iFDOMetadata(BaseMetadata):  # noqa: N801
    """
    iFDO metadata implementation that adapts ImageData to the BaseMetadata interface.

    Supports both single ImageData (for still images) and list of ImageData (for videos
    with time-varying metadata) as per iFDO v2.1.0 specification.
    """

    DEFAULT_METADATA_NAME = "ifdo"

    def __init__(
        self,
        image_data: ImageData | list[ImageData],
    ) -> None:
        """Initialize with an ImageData instance or list of ImageData instances.

        Args:
            image_data: Single ImageData for still images, or list of ImageData for videos
                       with time-varying metadata per iFDO specification.
        """
        self._image_data = image_data
        self._metadata_name = self.DEFAULT_METADATA_NAME

    @property
    def image_data(self) -> ImageData | list[ImageData]:
        """Get the underlying ImageData instance(s)."""
        return self._image_data

    @property
    def is_video(self) -> bool:
        """Check if this metadata represents video data (list of ImageData)."""
        return isinstance(self._image_data, list)

    @property
    def primary_image_data(self) -> ImageData:
        """Get the primary ImageData instance (first for videos, single for images)."""
        if isinstance(self._image_data, list):
            return self._image_data[0]
        return self._image_data

    @property
    def datetime(self) -> datetime | None:
        """Get the date and time when the image was captured."""
        value = self.primary_image_data.image_datetime
        if value is None or isinstance(value, datetime):
            return value
        # If the value is not None and not a datetime, it's an error
        raise TypeError(f"Expected datetime or None, got {type(value)}")

    @property
    def latitude(self) -> float | None:
        """Get the geographic latitude in decimal degrees."""
        value = self.primary_image_data.image_latitude
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def longitude(self) -> float | None:
        """Get the geographic longitude in decimal degrees."""
        value = self.primary_image_data.image_longitude
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def altitude(self) -> float | None:
        """Get the altitude in meters."""
        value = self.primary_image_data.image_altitude_meters
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def context(self) -> str | None:
        """Get the contextual information about the image."""
        if self.primary_image_data.image_context is None:
            return None
        return cast("str", self.primary_image_data.image_context.name)

    @property
    def license(self) -> str | None:
        """Get the license information."""
        if self.primary_image_data.image_license is None:
            return None
        return cast("str", self.primary_image_data.image_license.name)

    @property
    def creators(self) -> list[str]:
        """Get the list of creator names."""
        if not self.primary_image_data.image_creators:
            return []
        return [cast("str", creator.name) for creator in self.primary_image_data.image_creators]

    @property
    def hash_sha256(self) -> str | None:
        """Get the SHA256 hash from the underlying ImageData."""
        value = self.primary_image_data.image_hash_sha256
        if value is None or isinstance(value, str):
            return value
        # If the value is not None and not str, it's an error
        raise TypeError(f"Expected str or None, got {type(value)}")

    @hash_sha256.setter
    def hash_sha256(self, value: str) -> None:
        """Set the SHA256 hash in the underlying ImageData."""
        self.primary_image_data.image_hash_sha256 = value

    @staticmethod
    def _is_video_file(filename: str) -> bool:
        """Check if a file is a video based on its extension."""
        video_extensions = {
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".webm",
            ".mkv",
            ".m4v",
            ".3gp",
            ".ogv",
            ".ts",
            ".mts",
            ".m2ts",
            ".vob",
            ".rm",
            ".rmvb",
            ".asf",
            ".dv",
            ".f4v",
            ".m1v",
            ".m2v",
            ".mpe",
            ".mpeg",
            ".mpg",
            ".mpv",
            ".qt",
            ".swf",
            ".viv",
            ".vivo",
            ".yuv",
        }
        return Path(filename).suffix.lower() in video_extensions

    @classmethod
    def _process_video_metadata(
        cls,
        ifdo_items: list["iFDOMetadata"],
        path: Path,
    ) -> list[ImageData]:
        """Process video metadata items into a list of ImageData."""
        image_data_list: list[ImageData] = []
        for item in ifdo_items:
            if item.is_video:
                # If the metadata is already a video (list), extend with all entries
                image_data_list.extend(cast("list[ImageData]", item.image_data))
            else:
                # If single ImageData, add it to the list
                image_data_list.append(cast("ImageData", item.image_data))

        # Set image-set-local-path for subdirectory files
        if path.parent != Path():
            for img_data in image_data_list:
                img_data.image_set_local_path = str(path.parent)

        return image_data_list

    @classmethod
    def _process_image_metadata(
        cls,
        ifdo_items: list["iFDOMetadata"],
        path: Path,
    ) -> ImageData:
        """Process image metadata items into a single ImageData."""
        # Take the first iFDO metadata item
        item = ifdo_items[0]
        image_data = cast("ImageData", item.image_data[0] if item.is_video else item.image_data)

        # Set image-set-local-path for subdirectory files
        if path.parent != Path():
            image_data.image_set_local_path = str(path.parent)

        return image_data

    @classmethod
    def create_dataset_metadata(
        cls,
        dataset_name: str,
        root_dir: Path,
        items: dict[str, list["BaseMetadata"]],
        metadata_name: str | None = None,
        *,
        dry_run: bool = False,
        saver_overwrite: Callable[[Path, str, dict[str, Any]], None] | None = None,
    ) -> None:
        """Create an iFDO from the metadata items."""
        saver = yaml_saver if saver_overwrite is None else saver_overwrite

        # Convert BaseMetadata items to ImageData for iFDO
        # Use filename only as keys per iFDO standard instead of full paths
        image_set_items = {}
        for path_str, metadata_items in items.items():
            path = Path(path_str)
            filename = path.name

            # Check if this is a video file
            is_video = cls._is_video_file(filename)

            ifdo_items = [item for item in metadata_items if isinstance(item, iFDOMetadata)]
            if not ifdo_items:
                continue

            if is_video:
                image_data_list = cls._process_video_metadata(ifdo_items, path)
                if image_data_list:
                    image_set_items[filename] = image_data_list
            else:
                image_data = cls._process_image_metadata(ifdo_items, path)
                image_set_items[filename] = image_data

        ifdo = iFDO(
            image_set_header=ImageSetHeader(
                image_set_name=dataset_name,
                image_set_uuid=str(uuid.uuid4()),
                image_set_handle="",  # TODO @<cjackett>: Populate from distribution target URL
            ),
            image_set_items=image_set_items,
        )

        # If no metadata_name provided, use default
        if not metadata_name:
            output_name = cls.DEFAULT_METADATA_NAME
        # If metadata_name is provided but missing extension, add it
        else:
            output_name = metadata_name if metadata_name.endswith(".ifdo") else f"{metadata_name}.ifdo"

        if not dry_run:
            saver(root_dir, output_name, ifdo.model_dump(mode="json", by_alias=True, exclude_none=True))

    @staticmethod
    def _chunk_dataset(
        dataset_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> list[list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]]]:
        """
        Split the dataset into chunks for batch processing.

        Args:
            dataset_mapping: The dataset mapping to chunk.
            chunk_size: Number of files per chunk.

        Returns:
            List of chunks, where each chunk is a list of (file_path, metadata) tuples.
        """
        items = list(dataset_mapping.items())
        chunks = []

        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            chunks.append(chunk)

        return chunks

    @classmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
        max_workers: int | None = None,
        logger: logging.Logger | None = None,
        *,
        dry_run: bool = False,
        chunk_size: int | None = None,
    ) -> None:
        """Process dataset_mapping using metadata with chunked batch processing."""
        if dry_run:
            return

        # Use the provided logger if available, otherwise use the module logger
        log = logger or get_logger(__name__)

        # Calculate optimal chunk size if not provided
        if chunk_size is None:
            chunk_size = _calculate_optimal_chunk_size(len(dataset_mapping), max_workers)
            log.info(f"Auto-calculated chunk size: {chunk_size} for {len(dataset_mapping)} files")

        # Split dataset into chunks for batch processing
        chunks = cls._chunk_dataset(dataset_mapping, chunk_size)
        log.info(f"Processing {len(dataset_mapping)} files in {len(chunks)} chunks of up to {chunk_size} files each")

        @multithreaded(max_workers=max_workers)
        def process_chunk(
            cls: type[iFDOMetadata],
            thread_num: str,
            item: list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]],
            logger: logging.Logger,
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            chunk = item
            exif_batch, non_exif_files = cls._prepare_chunk_for_processing(chunk, logger)
            cls._process_exif_batch(exif_batch, thread_num, logger)
            cls._log_non_exif_files(non_exif_files, thread_num, logger)

            # Update progress for entire chunk
            if progress and task is not None:
                progress.advance(task, advance=len(chunk))

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Processing files with metadata (4/12)", total=len(dataset_mapping))
            process_chunk(cls, items=chunks, progress=progress, task=task, logger=log)  # type: ignore[call-arg]

    @classmethod
    def _prepare_chunk_for_processing(
        cls,
        chunk: list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]],
        logger: logging.Logger,
    ) -> tuple[list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]], list[Path]]:
        """Prepare a chunk of files for EXIF processing."""
        exif_batch = []
        non_exif_files = []

        for file_path, (metadata_items, ancillary_data) in chunk:
            file_extension = file_path.suffix.lower()

            if file_extension in EXIF_SUPPORTED_EXTENSIONS:
                # Get the ImageData from the metadata items
                ifdo_metadata_items = [item for item in metadata_items if isinstance(item, iFDOMetadata)]

                if ifdo_metadata_items:
                    # Use the primary ImageData from the first iFDO metadata item
                    image_data = ifdo_metadata_items[0].primary_image_data

                    try:
                        # Open image file for processing with proper context management
                        with Image.open(file_path) as image_file:
                            # Extract image properties first while image is still open
                            cls._extract_image_properties(image_file, image_data)

                            # Add to batch for exiftool processing
                            exif_batch.append((file_path, image_data, ancillary_data, image_file.copy()))

                    except OSError as e:
                        logger.warning(f"Failed to open image file {file_path}: {e}")
            else:
                non_exif_files.append(file_path)

        return exif_batch, non_exif_files

    @classmethod
    def _process_exif_batch(
        cls,
        exif_batch: list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]],
        thread_num: str,
        logger: logging.Logger,
    ) -> None:
        """Process a batch of EXIF files."""
        if exif_batch:
            try:
                cls._inject_metadata_with_exiftool_batch(exif_batch, logger)
                logger.debug(f"Thread {thread_num} - Processed batch of {len(exif_batch)} EXIF files")
            except (OSError, ExifToolException) as e:
                logger.warning(f"Thread {thread_num} - Failed to process EXIF batch: {e}")

    @classmethod
    def _log_non_exif_files(
        cls,
        non_exif_files: list[Path],
        thread_num: str,
        logger: logging.Logger,
    ) -> None:
        """Log non-EXIF files that were skipped."""
        for file_path in non_exif_files:
            logger.debug(f"Thread {thread_num} - Skipping EXIF processing for non-supported file: {file_path}")

    @staticmethod
    def _inject_metadata_with_exiftool_batch(
        file_batch: list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]],
        logger: logging.Logger,
    ) -> None:
        """
        Inject metadata into EXIF for a batch of files using a single exiftool instance.

        Args:
            file_batch: List of tuples containing (file_path, image_data, ancillary_data, image_file).
            logger: Logger instance.
        """
        if not file_batch:
            return

        try:
            with exiftool.ExifToolHelper() as et:
                existing_exif_map = iFDOMetadata._get_existing_metadata_map(et, file_batch)
                batch_file_tags = iFDOMetadata._build_batch_tags(file_batch, existing_exif_map)
                iFDOMetadata._apply_batch_tags(et, batch_file_tags)
                iFDOMetadata._add_batch_thumbnails(file_batch, logger)

                logger.debug(f"Successfully processed batch of {len(file_batch)} files with exiftool")

        except FileNotFoundError as e:
            if "exiftool" in str(e).lower():
                show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
            else:
                logger.warning(f"File not found during batch EXIF processing: {e}")
        except ExifToolException as e:
            logger.warning(f"Failed to inject EXIF metadata in batch with exiftool: {e}")

    @staticmethod
    def _get_existing_metadata_map(
        et: exiftool.ExifToolHelper,
        file_batch: list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]],
    ) -> dict[str, dict[str, Any]]:
        """Get existing EXIF metadata for all files in the batch."""
        file_paths = [str(item[0]) for item in file_batch]
        existing_metadata_list = et.get_metadata(file_paths)

        existing_exif_map = {}
        for i, metadata in enumerate(existing_metadata_list):
            existing_exif_map[file_paths[i]] = metadata if metadata else {}

        return existing_exif_map

    @staticmethod
    def _build_batch_tags(
        file_batch: list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]],
        existing_exif_map: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Build EXIF tags for all files in the batch."""
        batch_file_tags: dict[str, dict[str, Any]] = {}

        for file_path, image_data, ancillary_data, image_file in file_batch:
            file_path_str = str(file_path)
            existing_exif = existing_exif_map.get(file_path_str, {})

            exif_tags: dict[str, Any] = {}

            # Build EXIF tags using helper methods
            iFDOMetadata._add_image_dimensions(exif_tags, existing_exif, image_file)
            iFDOMetadata._add_datetime_tags(exif_tags, image_data)
            iFDOMetadata._add_identifier_tags(exif_tags, image_data)
            iFDOMetadata._add_gps_tags(exif_tags, image_data)
            iFDOMetadata._add_user_comment(exif_tags, image_data, ancillary_data)

            if exif_tags:
                batch_file_tags[file_path_str] = exif_tags

        return batch_file_tags

    @staticmethod
    def _apply_batch_tags(
        et: exiftool.ExifToolHelper,
        batch_file_tags: dict[str, dict[str, Any]],
    ) -> None:
        """Apply EXIF tags to all files in the batch."""
        if batch_file_tags:
            # Apply tags per file using batch mode
            for file_path_str, tags in batch_file_tags.items():
                et.set_tags([file_path_str], tags, params=["-overwrite_original_in_place"])

    @staticmethod
    def _add_batch_thumbnails(
        file_batch: list[tuple[Path, ImageData, dict[str, Any] | None, Image.Image]],
        logger: logging.Logger,
    ) -> None:
        """Add thumbnails to all files in the batch."""
        failed_thumbnails = []

        # Process all thumbnails and collect failures
        for file_path, _image_data, _ancillary_data, image_file in file_batch:
            error = iFDOMetadata._safe_add_thumbnail(file_path, image_file, logger)
            if error:
                failed_thumbnails.append((file_path, error))

        # Log all failures at once
        for file_path, error in failed_thumbnails:
            logger.warning(f"Failed to add thumbnail for {file_path}: {error}")

    @staticmethod
    def _safe_add_thumbnail(
        file_path: Path,
        image_file: Image.Image,
        logger: logging.Logger,
    ) -> str | None:
        """Safely add thumbnail and return error message if failed."""
        try:
            iFDOMetadata._add_thumbnail_to_exif(file_path, image_file, logger)
        except (OSError, ExifToolException) as e:
            return str(e)
        else:
            return None

    @staticmethod
    def _inject_metadata_with_exiftool(
        file_path: Path,
        image_data: ImageData,
        ancillary_data: dict[str, Any] | None,
        image_file: Image.Image,
        logger: logging.Logger,
    ) -> None:
        """
        Inject metadata into EXIF using exiftool (single file fallback).

        Args:
            file_path: Path to the image file.
            image_data: The image data containing metadata information.
            ancillary_data: Any ancillary data to include.
            image_file: PIL Image object for extracting dimensions.
            logger: Logger instance.
        """
        # Use batch processing with single file for consistency
        iFDOMetadata._inject_metadata_with_exiftool_batch(
            [(file_path, image_data, ancillary_data, image_file)],
            logger,
        )

    @staticmethod
    def _add_image_dimensions(
        exif_tags: dict[str, Any],
        existing_exif: dict[str, Any],
        image_file: Image.Image,
    ) -> None:
        """Add image dimensions to EXIF tags if missing."""
        if "EXIF:ExifImageWidth" not in existing_exif:
            width, height = image_file.size
            exif_tags["EXIF:ExifImageWidth"] = width

        if "EXIF:ExifImageHeight" not in existing_exif:
            width, height = image_file.size
            exif_tags["EXIF:ExifImageHeight"] = height

    @staticmethod
    def _add_datetime_tags(exif_tags: dict[str, Any], image_data: ImageData) -> None:
        """Add datetime-related EXIF tags."""
        if image_data.image_datetime is not None:
            dt = image_data.image_datetime
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                dt = dt.astimezone(timezone.utc)
                offset_str = "+00:00"
                exif_tags["EXIF:OffsetTime"] = offset_str
                exif_tags["EXIF:OffsetTimeOriginal"] = offset_str

            datetime_str = dt.strftime("%Y:%m:%d %H:%M:%S")
            subsec_str = str(dt.microsecond)

            exif_tags["EXIF:DateTime"] = datetime_str
            exif_tags["EXIF:DateTimeOriginal"] = datetime_str
            exif_tags["EXIF:SubSecTime"] = subsec_str
            exif_tags["EXIF:SubSecTimeOriginal"] = subsec_str

    @staticmethod
    def _add_identifier_tags(exif_tags: dict[str, Any], image_data: ImageData) -> None:
        """Add identifier-related EXIF tags."""
        if image_data.image_uuid:
            exif_tags["EXIF:ImageUniqueID"] = str(image_data.image_uuid)

    @staticmethod
    def _add_gps_tags(exif_tags: dict[str, Any], image_data: ImageData) -> None:
        """Add GPS-related EXIF tags."""
        if image_data.image_latitude is not None:
            exif_tags["EXIF:GPSLatitude"] = abs(image_data.image_latitude)
            exif_tags["EXIF:GPSLatitudeRef"] = "N" if image_data.image_latitude >= 0 else "S"

        if image_data.image_longitude is not None:
            exif_tags["EXIF:GPSLongitude"] = abs(image_data.image_longitude)
            exif_tags["EXIF:GPSLongitudeRef"] = "E" if image_data.image_longitude >= 0 else "W"

        if image_data.image_altitude_meters is not None:
            exif_tags["EXIF:GPSAltitude"] = abs(float(image_data.image_altitude_meters))
            exif_tags["EXIF:GPSAltitudeRef"] = "0" if image_data.image_altitude_meters >= 0 else "1"

    @staticmethod
    def _add_user_comment(
        exif_tags: dict[str, Any],
        image_data: ImageData,
        ancillary_data: dict[str, Any] | None,
    ) -> None:
        """Add user comment with iFDO metadata."""
        image_data_dict = image_data.model_dump(mode="json", by_alias=True, exclude_none=True)
        user_comment_data = {
            "metadata": {"ifdo": image_data_dict, "ancillary": ancillary_data},
        }
        user_comment_json = json.dumps(user_comment_data)
        exif_tags["EXIF:UserComment"] = user_comment_json

    @staticmethod
    def _add_thumbnail_to_exif(file_path: Path, image_file: Image.Image, logger: logging.Logger) -> None:
        """
        Add a thumbnail to the EXIF metadata using PIL and exiftool.

        Args:
            file_path: Path to the image file.
            image_file: PIL Image object.
            logger: Logger instance.
        """
        try:
            # Create a copy for the thumbnail to avoid modifying original
            with image_file.copy() as thumb:
                # Set max dimension to 240px - aspect ratio will be maintained
                thumbnail_size = (240, 240)

                # Use LANCZOS resampling for better quality
                thumb.thumbnail(thumbnail_size, Image.Resampling.LANCZOS)

                # Convert to RGB if not already
                if thumb.mode != "RGB":
                    with (
                        thumb.convert("RGB") as rgb_thumb,
                        tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file,
                    ):
                        rgb_thumb.save(temp_file.name, format="JPEG", quality=90, optimize=True)
                        temp_thumb_path = temp_file.name
                else:
                    # Save thumbnail to a temporary file
                    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                        thumb.save(temp_file.name, format="JPEG", quality=90, optimize=True)
                        temp_thumb_path = temp_file.name

                # Use exiftool to embed the thumbnail
                with exiftool.ExifToolHelper() as et:
                    et.execute("-ThumbnailImage<=" + temp_thumb_path, "-overwrite_original_in_place", str(file_path))

                # Clean up temporary file
                Path(temp_thumb_path).unlink()

        except ExifToolException as e:
            logger.debug(f"Failed to add thumbnail to {file_path}: {e}")

    @staticmethod
    def _extract_image_properties(
        image_file: Image.Image,
        image_data: ImageData,
    ) -> None:
        """
        Extract image properties and update the image data.

        Args:
            image_file: The PIL Image object from which to extract properties.
            image_data: The ImageData object to update with extracted properties.
        """
        # Inject the image entropy and average image color into the iFDO
        image_data.image_entropy = image.get_shannon_entropy(image_file)
        image_data.image_average_color = image.get_average_image_color(image_file)
