"""
Marimba iFDO Metadata Implementation.

This module provides functionality for handling iFDO metadata, including creation, processing, and embedding of
metadata in image files. It implements the BaseMetadata interface for iFDO-specific metadata and offers methods
for creating dataset metadata and processing image files with EXIF data.

Imports:
    io: Provides tools for working with I/O operations
    json: Handles JSON data encoding and decoding
    datetime: Supplies classes for working with dates and times
    Fraction: Represents rational numbers with numerator and denominator
    Path: Offers object-oriented filesystem paths
    typing: Provides support for type hints
    uuid: Generates universally unique identifiers
    piexif: Handles reading and writing of EXIF data in images
    PIL: Python Imaging Library for opening, manipulating, and saving image files
    rich: Offers rich text and beautiful formatting in the terminal

Classes:
    iFDOMetadata: Implements the BaseMetadata interface for iFDO-specific metadata
"""

import io
import json
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import piexif
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.log import get_logger
from marimba.core.utils.rich import get_default_columns
from marimba.lib import image
from marimba.lib.decorators import multithreaded
from marimba.lib.gps import convert_degrees_to_gps_coordinate

if TYPE_CHECKING:
    from ifdo.models import ImageData, ImageSetHeader, iFDO
else:
    from ifdo.models import ImageData, ImageSetHeader, iFDO


logger = get_logger(__name__)


class iFDOMetadata(BaseMetadata):  # noqa: N801
    """
    iFDO metadata implementation that adapts ImageData to the BaseMetadata interface.
    """

    DEFAULT_METADATA_NAME = "ifdo.yml"

    def __init__(
        self,
        image_data: ImageData,
    ) -> None:
        """Initialize with an ImageData instance."""
        self._image_data = image_data
        self._metadata_name = self.DEFAULT_METADATA_NAME

    @property
    def image_data(self) -> ImageData:
        """Get the underlying ImageData instance."""
        return self._image_data

    @property
    def datetime(self) -> datetime | None:
        """Get the date and time when the image was captured."""
        value = self._image_data.image_datetime
        if value is None or isinstance(value, datetime):
            return value
        # If the value is not None and not a datetime, it's an error
        raise TypeError(f"Expected datetime or None, got {type(value)}")

    @property
    def latitude(self) -> float | None:
        """Get the geographic latitude in decimal degrees."""
        value = self._image_data.image_latitude
        if value is None or isinstance(value, int | float):
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def longitude(self) -> float | None:
        """Get the geographic longitude in decimal degrees."""
        value = self._image_data.image_longitude
        if value is None or isinstance(value, int | float):
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def altitude(self) -> float | None:
        """Get the altitude in meters."""
        value = self._image_data.image_altitude_meters
        if value is None or isinstance(value, int | float):
            return cast(float | None, value)
        # If the value is not None and not a number, it's an error
        raise TypeError(f"Expected float or None, got {type(value)}")

    @property
    def context(self) -> str | None:
        """Get the contextual information about the image."""
        if self._image_data.image_context is None:
            return None
        return cast(str, self._image_data.image_context.uri)

    @property
    def license(self) -> str | None:
        """Get the license information."""
        if self._image_data.image_license is None:
            return None
        return cast(str, self._image_data.image_license.name)

    @property
    def creators(self) -> list[str]:
        """Get the list of creator names."""
        if not self._image_data.image_creators:
            return []
        return [cast(str, creator.name) for creator in self._image_data.image_creators]

    @property
    def hash_sha256(self) -> bytes | None:
        """Get the SHA256 hash from the underlying ImageData."""
        value = self._image_data.image_hash_sha256
        if value is None or isinstance(value, bytes):
            return value
        # If the value is not None and not bytes, it's an error
        raise TypeError(f"Expected bytes or None, got {type(value)}")

    @hash_sha256.setter
    def hash_sha256(self, value: bytes) -> None:
        """Set the SHA256 hash in the underlying ImageData."""
        self._image_data.image_hash_sha256 = value

    @classmethod
    def create_dataset_metadata(
        cls,
        dataset_name: str,
        root_dir: Path,
        items: dict[str, list["BaseMetadata"]],
        metadata_name: str | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """Create an iFDO from the metadata items."""
        if not dry_run:
            # Convert BaseMetadata items to ImageData for iFDO
            image_set_items = {
                path: [item.image_data for item in metadata_items if isinstance(item, iFDOMetadata)]
                for path, metadata_items in items.items()
            }

            ifdo = iFDO(
                image_set_header=ImageSetHeader(
                    image_set_name=dataset_name,
                    image_set_uuid=str(uuid4()),
                    image_set_handle="",  # TODO(@cjackett): Populate from distribution target URL
                ),
                image_set_items=image_set_items,
            )

            # If no metadata_name provided, use default
            if not metadata_name:
                output_name = cls.DEFAULT_METADATA_NAME
            # If metadata_name is provided but missing extension, add it
            else:
                output_name = metadata_name if metadata_name.endswith(".ifdo.yml") else f"{metadata_name}.ifdo.yml"

            ifdo.save(root_dir / output_name)

    @classmethod
    def process_files(
        cls,
        dataset_mapping: dict[Path, tuple[list["BaseMetadata"], dict[str, Any] | None]],
        max_workers: int | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """Process dataset_mapping using metadata."""
        if dry_run:
            return

        @multithreaded(max_workers=max_workers)
        def process_file(
            cls: type["iFDOMetadata"],
            thread_num: str,
            item: tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
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

            if file_extension in exif_supported_extensions:

                try:
                    exif_dict = piexif.load(str(file_path))
                except piexif.InvalidImageDataError as e:
                    logger.warning(f"Failed to load EXIF metadata from {file_path}: {e}")
                    return

                # Get the ImageData from the metadata items
                image_data_list = [item.image_data for item in metadata_items if isinstance(item, iFDOMetadata)]

                if not image_data_list:
                    return

                image_data = image_data_list[0]  # Use first item's metadata

                # Apply EXIF metadata
                cls._inject_datetime(image_data, exif_dict)
                cls._inject_gps_coordinates(image_data, exif_dict)
                image_file = cls._add_thumbnail(file_path, exif_dict)
                cls._extract_image_properties(image_file, image_data)
                cls._embed_exif_metadata(image_data, ancillary_data, exif_dict)

                try:
                    exif_bytes = piexif.dump(exif_dict)
                    piexif.insert(exif_bytes, str(file_path))
                    logger.debug(f"Thread {thread_num} | Applied iFDO metadata to EXIF tags for image {file_path}")
                except piexif.InvalidImageDataError:
                    logger.warning(f"Failed to write EXIF metadata to {file_path}")

                if progress and task is not None:
                    progress.advance(task)

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Processing files with metadata (4/11)", total=len(dataset_mapping))
            process_file(cls, items=dataset_mapping.items(), progress=progress, task=task)  # type: ignore[call-arg]

    @staticmethod
    def _inject_datetime(image_data: ImageData, exif_dict: dict[str, Any]) -> None:
        """
        Inject datetime information into EXIF metadata.

        Args:
            image_data: The image data containing datetime information.
            exif_dict: The EXIF metadata dictionary.
        """
        if image_data.image_datetime is not None:
            dt = image_data.image_datetime
            offset_str = None
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                dt = dt.astimezone(timezone.utc)
                offset_str = "+00:00"

            datetime_str = dt.strftime("%Y:%m:%d %H:%M:%S")
            subsec_str = str(dt.microsecond)

            ifd_0th = exif_dict["0th"]
            ifd_exif = exif_dict["Exif"]

            ifd_0th[piexif.ImageIFD.DateTime] = datetime_str
            ifd_exif[piexif.ExifIFD.DateTimeOriginal] = datetime_str
            ifd_exif[piexif.ExifIFD.SubSecTime] = subsec_str
            ifd_exif[piexif.ExifIFD.SubSecTimeOriginal] = subsec_str
            if offset_str is not None:
                ifd_exif[piexif.ExifIFD.OffsetTime] = offset_str
                ifd_exif[piexif.ExifIFD.OffsetTimeOriginal] = offset_str

    @staticmethod
    def _inject_gps_coordinates(image_data: ImageData, exif_dict: dict[str, Any]) -> None:
        """
        Inject GPS coordinates into EXIF metadata.

        Args:
            image_data: The image data containing GPS information.
            exif_dict: The EXIF metadata dictionary.
        """
        ifd_gps = exif_dict["GPS"]

        if image_data.image_latitude is not None:
            d_lat, m_lat, s_lat = convert_degrees_to_gps_coordinate(image_data.image_latitude)
            ifd_gps[piexif.GPSIFD.GPSLatitude] = ((d_lat, 1), (m_lat, 1), (s_lat, 1000))
            ifd_gps[piexif.GPSIFD.GPSLatitudeRef] = "N" if image_data.image_latitude > 0 else "S"
        if image_data.image_longitude is not None:
            d_lon, m_lon, s_lon = convert_degrees_to_gps_coordinate(image_data.image_longitude)
            ifd_gps[piexif.GPSIFD.GPSLongitude] = ((d_lon, 1), (m_lon, 1), (s_lon, 1000))
            ifd_gps[piexif.GPSIFD.GPSLongitudeRef] = "E" if image_data.image_longitude > 0 else "W"
        if image_data.image_altitude_meters is not None:
            altitude_fraction = Fraction(abs(float(image_data.image_altitude_meters))).limit_denominator()
            altitude_rational = (altitude_fraction.numerator, altitude_fraction.denominator)
            ifd_gps[piexif.GPSIFD.GPSAltitude] = altitude_rational
            ifd_gps[piexif.GPSIFD.GPSAltitudeRef] = 0 if image_data.image_altitude_meters >= 0 else 1

    @staticmethod
    def _add_thumbnail(path: Path, exif_dict: dict[str, Any]) -> Image.Image:
        """
        Add a thumbnail to the EXIF metadata.

        Args:
            path: The path to the image file.
            exif_dict: The EXIF metadata dictionary.
        """
        image_file = Image.open(path)
        thumbnail_size = (320, 240)
        image_file.thumbnail(thumbnail_size)  # type: ignore[no-untyped-call]
        thumbnail_io = io.BytesIO()
        image_file.save(thumbnail_io, format="JPEG", quality=90)
        exif_dict["thumbnail"] = thumbnail_io.getvalue()
        return image_file

    @staticmethod
    def _extract_image_properties(image_file: Image.Image, image_data: ImageData) -> None:
        """
        Extract image properties and update the image data.

        Args:
            image_file: The PIL Image object from which to extract properties.
            image_data: The ImageData object to update with extracted properties.
        """
        # Inject the image entropy and average image color into the iFDO
        image_data.image_entropy = image.get_shannon_entropy(image_file)
        image_data.image_average_color = image.get_average_image_color(image_file)

    @staticmethod
    def _embed_exif_metadata(
        image_data: ImageData,
        ancillary_data: dict[str, Any] | None,
        exif_dict: dict[str, Any],
    ) -> None:
        """
        Add a user comment with iFDO metadata to the EXIF metadata.

        Args:
            image_data: The image data to include in the user comment.
            ancillary_data: Any ancillary data to include in the user comment.
            exif_dict: The EXIF metadata dictionary.
        """
        image_data_dict = image_data.to_dict()
        user_comment_data = {"metadata": {"ifdo": image_data_dict, "ancillary": ancillary_data}}
        user_comment_json = json.dumps(user_comment_data)
        user_comment_bytes = user_comment_json.encode("utf-8")
        exif_dict["Exif"][piexif.ExifIFD.UserComment] = user_comment_bytes
