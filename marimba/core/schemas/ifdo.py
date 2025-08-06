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
import tempfile
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import exiftool
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.schemas.base import BaseMetadata
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
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def longitude(self) -> float | None:
        """Get the geographic longitude in decimal degrees."""
        value = self.primary_image_data.image_longitude
        if value is None or isinstance(value, int | float):
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def altitude(self) -> float | None:
        """Get the altitude in meters."""
        value = self.primary_image_data.image_altitude_meters
        if value is None or isinstance(value, int | float):
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def context(self) -> str | None:
        """Get the contextual information about the image."""
        if self.primary_image_data.image_context is None:
            return None
        return cast(str, self.primary_image_data.image_context.name)

    @property
    def license(self) -> str | None:
        """Get the license information."""
        if self.primary_image_data.image_license is None:
            return None
        return cast(str, self.primary_image_data.image_license.name)

    @property
    def creators(self) -> list[str]:
        """Get the list of creator names."""
        if not self.primary_image_data.image_creators:
            return []
        return [cast(str, creator.name) for creator in self.primary_image_data.image_creators]

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
                image_data_list.extend(cast(list[ImageData], item.image_data))
            else:
                # If single ImageData, add it to the list
                image_data_list.append(cast(ImageData, item.image_data))

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
        image_data = cast(ImageData, item.image_data[0] if item.is_video else item.image_data)

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

    @classmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
        max_workers: int | None = None,
        logger: logging.Logger | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """Process dataset_mapping using metadata."""
        if dry_run:
            return

        # Use the provided logger if available, otherwise use the module logger
        log = logger or get_logger(__name__)

        @multithreaded(max_workers=max_workers)
        def process_file(
            cls: type[iFDOMetadata],
            thread_num: str,
            item: tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
            logger: logging.Logger,
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            file_path, (metadata_items, ancillary_data) = item

            # Formats with reliable EXIF support
            exif_supported_extensions = {
                # Standard formats with native EXIF support
                ".jpg",
                ".jpeg",
                ".tiff",
                ".tif",
                # Common RAW formats that support EXIF
                ".cr2",  # Canon
                ".cr3",  # Canon
                ".nef",  # Nikon
                ".arw",  # Sony
                ".dng",  # Adobe Digital Negative
                ".raf",  # Fujifilm
                ".orf",  # Olympus
                ".pef",  # Pentax
                ".rw2",  # Panasonic
            }

            file_extension = file_path.suffix.lower()

            try:
                # If it's an EXIF-supported file, process EXIF metadata
                if file_extension in exif_supported_extensions:
                    try:
                        # Get the ImageData from the metadata items
                        ifdo_metadata_items = [item for item in metadata_items if isinstance(item, iFDOMetadata)]

                        if ifdo_metadata_items:
                            # Use the primary ImageData from the first iFDO metadata item
                            image_data = ifdo_metadata_items[0].primary_image_data

                            # Open image file for processing
                            image_file = Image.open(file_path)

                            # Apply EXIF metadata using exiftool
                            cls._inject_metadata_with_exiftool(
                                file_path,
                                image_data,
                                ancillary_data,
                                image_file,
                                logger,
                            )

                            # Extract image properties
                            cls._extract_image_properties(image_file, image_data)

                            logger.debug(
                                f"Thread {thread_num} - Applied iFDO metadata to EXIF tags for image {file_path}",
                            )
                    except (OSError, exiftool.ExifToolException) as e:
                        logger.warning(
                            f"Failed to process EXIF metadata for {file_path}: {e}",
                        )
                else:
                    # For non-EXIF files (like videos), just log that we're skipping EXIF processing
                    logger.debug(
                        f"Thread {thread_num} - Skipping EXIF processing for non-supported file: {file_path}",
                    )

            finally:
                # Always increment the progress bar, regardless of file type or processing success
                if progress and task is not None:
                    progress.advance(task)

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Processing files with metadata (4/12)", total=len(dataset_mapping))
            process_file(cls, items=dataset_mapping.items(), progress=progress, task=task, logger=log)  # type: ignore[call-arg]

    @staticmethod
    def _inject_metadata_with_exiftool(
        file_path: Path,
        image_data: ImageData,
        ancillary_data: dict[str, Any] | None,
        image_file: Image.Image,
        logger: logging.Logger,
    ) -> None:
        """
        Inject metadata into EXIF using exiftool.

        Args:
            file_path: Path to the image file.
            image_data: The image data containing metadata information.
            ancillary_data: Any ancillary data to include.
            image_file: PIL Image object for extracting dimensions.
            logger: Logger instance.
        """
        try:
            with exiftool.ExifToolHelper() as et:
                existing_metadata = et.get_metadata(str(file_path))
                existing_exif = existing_metadata[0] if existing_metadata else {}

                exif_tags: dict[str, Any] = {}

                # Build EXIF tags using helper methods
                iFDOMetadata._add_image_dimensions(exif_tags, existing_exif, image_file)
                iFDOMetadata._add_datetime_tags(exif_tags, image_data)
                iFDOMetadata._add_identifier_tags(exif_tags, image_data)
                iFDOMetadata._add_gps_tags(exif_tags, image_data)
                iFDOMetadata._add_user_comment(exif_tags, image_data, ancillary_data)

                # Apply all tags at once
                if exif_tags:
                    et.set_tags([str(file_path)], exif_tags, params=["-overwrite_original_in_place"])

                # Add thumbnail after applying other EXIF tags
                iFDOMetadata._add_thumbnail_to_exif(file_path, image_file, logger)

        except FileNotFoundError as e:
            if "exiftool" in str(e).lower():
                show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
            else:
                logger.warning(f"File not found during EXIF processing: {e}")
        except exiftool.ExifToolException as e:
            logger.warning(f"Failed to inject EXIF metadata with exiftool: {e}")

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
            thumb = image_file.copy()

            # Set max dimension to 240px - aspect ratio will be maintained
            thumbnail_size = (240, 240)

            # Use LANCZOS resampling for better quality
            thumb.thumbnail(thumbnail_size, Image.Resampling.LANCZOS)

            # Convert to RGB if not already
            if thumb.mode != "RGB":
                thumb = thumb.convert("RGB")

            # Save thumbnail to a temporary file
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                thumb.save(temp_file.name, format="JPEG", quality=90, optimize=True)
                temp_thumb_path = temp_file.name

            # Use exiftool to embed the thumbnail
            with exiftool.ExifToolHelper() as et:
                et.execute("-ThumbnailImage<=" + temp_thumb_path, "-overwrite_original_in_place", str(file_path))

            # Clean up temporary file
            Path(temp_thumb_path).unlink()

        except exiftool.ExifToolException as e:
            logger.debug(f"Failed to add thumbnail to {file_path}: {e}")
        except OSError as e:
            logger.debug(f"Failed to create thumbnail for {file_path}: {e}")

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
