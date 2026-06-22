"""
Marimba iFDO Metadata Implementation.

This module provides functionality for handling iFDO metadata, including creation, processing, and embedding of
metadata in image files. It implements the BaseMetadata interface for iFDO-specific metadata and offers methods
for creating dataset metadata and processing image files with EXIF data.

"""

import importlib.metadata
import io
import json
import logging
import random
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import exiftool
import psutil
from exiftool.exceptions import ExifToolException
from ifdo import ImageData, ImageSetHeader
from ifdo.models.ifdo import iFDO
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import DEFAULT_EXIF_THUMBNAIL_SIZE, EXIF_SUPPORTED_EXTENSIONS
from marimba.core.utils.dependencies import ToolDependency, show_dependency_error_and_exit
from marimba.core.utils.log import get_logger
from marimba.core.utils.metadata import yaml_saver
from marimba.core.utils.rich import get_default_columns
from marimba.lib.decorators import multithreaded

logger = get_logger(__name__)

# Memory-aware batch processing (no fixed chunk size)

# Memory management constants
_ESTIMATED_IMAGE_MEMORY_MB = 100  # Conservative estimate per 24MP image
_MEMORY_SAFETY_FACTOR = 0.7  # Use 70% of available memory

# iFDO specification version
IFDO_VERSION = "v2.2.1"

# Citations injected into every iFDO image-set-related-material header by default. Pipelines can prepend their
# own entries or re-word a default by supplying an entry with the same URI (see create_dataset_metadata).
DEFAULT_RELATED_MATERIAL: list[dict[str, str]] = [
    {
        "uri": "https://doi.org/10.1016/j.softx.2025.102251",
        "title": "Marimba: A Python framework for structuring and processing FAIR scientific image datasets",
        "relation": "The Marimba software framework used to structure and package this image dataset",
    },
    {
        "uri": "https://doi.org/10.1038/s41597-022-01491-3",
        "title": "Making marine image data FAIR",
        "relation": "The iFDO metadata standard to which this image dataset conforms",
    },
]


def _image_curation_protocol() -> str:
    """Curation-protocol sentence for the iFDO image-set-header, stamped with the Marimba version.

    The detailed machine-readable processing provenance (tool versions, pipeline git commits, packaging
    timestamp) lives in the provenance.json record at the dataset root.
    """
    version = importlib.metadata.version("marimba")
    return (
        f"Structured, processed, and packaged into a FAIR image dataset with Marimba v{version} "
        "(https://github.com/csiro-fair/marimba). Full machine-readable processing provenance is recorded in "
        "provenance.json (W3C PROV-O)."
    )


@dataclass
class ProcessedImageData:
    """Lightweight structure for processed image information."""

    file_path: Path
    image_data: "ImageData"
    ancillary_data: dict[str, Any] | None
    # Pre-computed image properties (no PIL objects stored)
    width: int
    height: int
    thumbnail_data: bytes | None = None
    processing_error: str | None = None


def _get_available_memory_mb() -> int:
    """Get available system memory in MB with cross-platform support."""
    # Primary: Use psutil for cross-platform accuracy
    try:
        available_bytes = psutil.virtual_memory().available
        return int(available_bytes // (1024 * 1024))
    except (AttributeError, OSError):
        pass

    # Secondary fallback using Linux /proc/meminfo for older systems
    try:
        meminfo_path = Path("/proc/meminfo")
        if meminfo_path.exists():
            with meminfo_path.open() as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) // 1024
    except (FileNotFoundError, ValueError, IndexError, OSError):
        pass

    # Conservative fallback for all platforms
    return 4096  # 4GB default - safe for most systems


def _estimate_memory_from_headers(file_path: Path) -> int:
    """
    Fast estimation using only image headers (~1-5ms per file).

    Args:
        file_path: Path to image file

    Returns:
        Estimated memory in MB for this image
    """
    try:
        with Image.open(file_path) as img:
            # PIL loads headers automatically, but not pixel data
            channels = len(img.getbands()) or 3  # Fallback to RGB
            memory_bytes = img.width * img.height * channels * 4  # 4 bytes per channel
            return int(memory_bytes // (1024 * 1024))  # Convert to MB
    except (OSError, ValueError, AttributeError):
        return _ESTIMATED_IMAGE_MEMORY_MB  # Fallback estimate


def _estimate_dataset_memory_requirements(dataset_files: list[Path], sample_size: int = 5) -> int:
    """
    Estimate memory requirements per image by sampling actual files from the dataset.

    Args:
        dataset_files: List of image file paths to sample from
        sample_size: Number of files to sample for estimation

    Returns:
        Estimated memory in MB per image
    """
    if not dataset_files:
        return _ESTIMATED_IMAGE_MEMORY_MB

    # Sample a few files, but not more than available
    actual_sample_size = min(sample_size, len(dataset_files))
    sample_files = random.sample(dataset_files, actual_sample_size)

    total_memory_mb = 0
    successful_samples = 0

    for file_path in sample_files:
        memory_mb = _estimate_memory_from_headers(file_path)
        if memory_mb > 0:  # Only count successful estimates
            total_memory_mb += memory_mb
            successful_samples += 1

    if successful_samples == 0:
        # Fallback to conservative estimate if no files could be sampled
        return _ESTIMATED_IMAGE_MEMORY_MB

    avg_memory_per_image = total_memory_mb / successful_samples
    # Add some buffer for processing overhead (20%)
    estimated_memory = int(avg_memory_per_image * 1.2)

    # Reasonable bounds: at least 10MB, at most 2GB per image
    return max(10, min(estimated_memory, 2048))


def _calculate_safe_image_batch_size(dataset_files: list[Path] | None = None) -> int:
    """
    Calculate safe batch size for image processing based on available memory.

    Args:
        dataset_files: Optional list of files to sample for better memory estimation

    Returns:
        Safe batch size (number of images to process simultaneously)
    """
    available_mb = _get_available_memory_mb()
    usable_mb = int(available_mb * _MEMORY_SAFETY_FACTOR)

    # Use dataset-specific estimation if files provided
    if dataset_files:
        estimated_memory_per_image = _estimate_dataset_memory_requirements(dataset_files)
    else:
        estimated_memory_per_image = _ESTIMATED_IMAGE_MEMORY_MB

    safe_batch_size = max(1, usable_mb // estimated_memory_per_image)

    # Cap at reasonable limits (at least 1, at most 100 images)
    return max(1, min(safe_batch_size, 100))


class iFDOMetadata(BaseMetadata):  # noqa: N801
    """
    iFDO metadata implementation that adapts ImageData to the BaseMetadata interface.

    Supports both single ImageData (for still images) and list of ImageData (for videos
    with time-varying metadata) as per iFDO v2.2.1 specification.
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

    @staticmethod
    def derive_image_set_uuid(dataset_name: str, metadata_name: str | None = None) -> str:
        """
        Derive a deterministic image-set UUID from the set name (and collection, if any).

        Using uuid5 keeps the identity stable across re-packaging runs, unlike a fresh uuid4. The same
        derivation namespaces per-image UUIDs (see ensure_image_uuid), so an image set and its images share
        one reproducible identity. Reproducible but not globally unique across projects that reuse a name -
        the resolvable persistent identifier for that is the image-set-handle / DOI.
        """
        key = dataset_name if metadata_name is None else f"{dataset_name}/{metadata_name}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"urn:marimba:image-set:{key}"))

    def ensure_image_uuid(self, image_set_uuid: str, relative_path: str) -> None:
        """
        Assign a deterministic per-image UUID where one is not already set.

        The UUID is derived from the image-set UUID and the image's dataset-relative path, so it is
        reproducible across packaging runs and unique within (and across) datasets. A pipeline that already
        supplied an image-uuid is honoured: its value is left untouched. For videos every frame entry
        receives the same UUID, since they describe a single file.
        """
        derived = str(uuid.uuid5(uuid.UUID(image_set_uuid), relative_path))
        data_list = self._image_data if isinstance(self._image_data, list) else [self._image_data]
        for data in data_list:
            if not data.image_uuid:
                data.image_uuid = derived

    @property
    def datetime(self) -> datetime | None:
        """Get the date and time when the image was captured."""
        value = self.primary_image_data.image_datetime
        if value is None or isinstance(value, datetime):
            return value
        # If the value is not None and not a datetime, it's an error
        msg = f"Expected datetime or None, got {type(value)}"
        raise TypeError(msg)

    @property
    def latitude(self) -> float | None:
        """Get the geographic latitude in decimal degrees."""
        value = self.primary_image_data.image_latitude
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        msg = f"Expected float or None, got {type(value)}"
        raise TypeError(msg)

    @property
    def longitude(self) -> float | None:
        """Get the geographic longitude in decimal degrees."""
        value = self.primary_image_data.image_longitude
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        msg = f"Expected float or None, got {type(value)}"
        raise TypeError(msg)

    @property
    def altitude(self) -> float | None:
        """Get the altitude in meters."""
        value = self.primary_image_data.image_altitude_meters
        if value is None or isinstance(value, int | float):
            return cast("float | None", value)
        # If the value is not None and not a number, it's an error
        msg = f"Expected float or None, got {type(value)}"
        raise TypeError(msg)

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
        msg = f"Expected str or None, got {type(value)}"
        raise TypeError(msg)

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
    def _convert_items_to_image_data(
        cls,
        items: dict[str, list["BaseMetadata"]],
    ) -> dict[str, "ImageData | list[ImageData]"]:
        """Convert BaseMetadata items to ImageData for iFDO."""
        image_set_items: dict[str, ImageData | list[ImageData]] = {}
        for path_str, metadata_items in items.items():
            path = Path(path_str)
            filename = path.name
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

        return image_set_items

    @staticmethod
    def _find_common_fields(all_items: list["ImageData"]) -> dict[str, Any]:
        """
        Single-pass scan of flattened ImageData objects.

        Returns fields whose non-None value is identical across every item.
        Fields that are None in any item are excluded even if non-None elsewhere.
        """
        changing_fields: set[str] = set()
        common_fields: dict[str, Any] = {}
        none_fields: set[str] = set()

        for item in all_items:
            for key, value in item.model_dump().items():
                if key in changing_fields:
                    continue
                if value is None:
                    if key in common_fields:
                        changing_fields.add(key)
                        del common_fields[key]
                    else:
                        none_fields.add(key)
                elif key in none_fields:
                    changing_fields.add(key)
                    none_fields.discard(key)
                elif key not in common_fields:
                    common_fields[key] = value
                elif common_fields[key] != value:
                    changing_fields.add(key)
                    del common_fields[key]

        return common_fields

    @classmethod
    def _extract_common_header_fields(
        cls,
        image_set_items: dict[str, "ImageData | list[ImageData]"],
    ) -> dict[str, Any]:
        """
        Extract fields that are identical across ALL images.

        Scans all ImageData objects and identifies fields where every image
        has the same non-None value. These fields can be promoted to the header
        to reduce file size.

        Args:
            image_set_items: Dict mapping filenames to ImageData objects or lists of ImageData

        Returns:
            Dict of field names to values that are common across all images.
            Only includes fields that are non-None and identical for every image.
        """
        if not image_set_items:
            return {}

        all_items: list[ImageData] = []
        for item in image_set_items.values():
            if isinstance(item, list):
                all_items.extend(item)
            else:
                all_items.append(item)

        if len(all_items) < 2:  # noqa: PLR2004
            return {}

        return cls._find_common_fields(all_items)

    @staticmethod
    def _compute_spatial_extent(
        image_set_items: dict[str, "ImageData | list[ImageData]"],
    ) -> dict[str, float]:
        """
        Compute the dataset spatial bounding box from per-image coordinates.

        Flattens every ImageData (including video frames) and returns the iFDO image-set bounding-box header fields
        when any valid coordinate is present. Latitude and longitude extents are derived independently, matching the
        dataset summary and map. Uses naive min/max and is therefore not antimeridian-aware: an image set crossing the
        +/-180 degree dateline would yield an overly wide longitude box.

        Args:
            image_set_items: Dict mapping filenames to ImageData objects or lists of ImageData.

        Returns:
            Dict with image-set-min/max-latitude/longitude-degrees keys for whichever axes have valid coordinates.
        """
        lats: list[float] = []
        lons: list[float] = []
        for item in image_set_items.values():
            data_list = item if isinstance(item, list) else [item]
            for image_data in data_list:
                lat = image_data.image_latitude
                lon = image_data.image_longitude
                if lat is not None:
                    lats.append(lat)
                if lon is not None:
                    lons.append(lon)

        extent: dict[str, float] = {}
        if lats:
            extent["image_set_min_latitude_degrees"] = min(lats)
            extent["image_set_max_latitude_degrees"] = max(lats)
        if lons:
            extent["image_set_min_longitude_degrees"] = min(lons)
            extent["image_set_max_longitude_degrees"] = max(lons)
        return extent

    @classmethod
    def _remove_common_fields(
        cls,
        image_set_items: dict[str, "ImageData | list[ImageData]"],
        common_field_names: set[str],
    ) -> dict[str, "ImageData | list[ImageData]"]:
        """
        Remove fields from individual images that are in the header.

        Creates new ImageData objects with common fields set to None,
        since they're now in the header.

        Args:
            image_set_items: Original image items
            common_field_names: Names of fields that are in the header

        Returns:
            New dict with deduplicated ImageData objects
        """
        deduplicated: dict[str, ImageData | list[ImageData]] = {}

        for filename, image_data in image_set_items.items():
            if isinstance(image_data, list):
                deduplicated_list = []
                for img in image_data:
                    data_dict = img.model_dump()
                    for field_name in common_field_names:
                        if field_name in data_dict:
                            data_dict[field_name] = None
                    deduplicated_list.append(ImageData(**data_dict))
                deduplicated[filename] = deduplicated_list
            else:
                data_dict = image_data.model_dump()
                for field_name in common_field_names:
                    if field_name in data_dict:
                        data_dict[field_name] = None
                deduplicated[filename] = ImageData(**data_dict)

        return deduplicated

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

        image_set_items = cls._convert_items_to_image_data(items)
        common_fields = cls._extract_common_header_fields(image_set_items)

        if common_fields:
            logger.debug(f"Deduplicated {len(common_fields)} field(s) to header: {', '.join(common_fields.keys())}")

        header_data: dict[str, Any] = {
            "image_set_name": dataset_name,
            "image_set_uuid": cls.derive_image_set_uuid(dataset_name, metadata_name),
            "image_set_handle": "",  # TODO @<cjackett>: Populate from distribution target URL
            "image_set_ifdo_version": IFDO_VERSION,
            "image_curation_protocol": _image_curation_protocol(),
        }
        header_data.update({k: v for k, v in common_fields.items() if k not in header_data})

        # Ensure the Marimba and iFDO citations are present in image-set-related-material. Any entries the
        # pipeline supplied are kept in front of the defaults; a default is skipped if the pipeline already
        # provided an entry with the same URI, so pipelines can prepend extra material or re-word a default.
        pipeline_related_material = header_data.get("image_set_related_material") or []
        pipeline_uris = {entry["uri"] for entry in pipeline_related_material if isinstance(entry, dict)}
        header_data["image_set_related_material"] = [
            *pipeline_related_material,
            *(material for material in DEFAULT_RELATED_MATERIAL if material["uri"] not in pipeline_uris),
        ]

        # Populate the image-set spatial bounding box from the per-image coordinates.
        header_data.update(cls._compute_spatial_extent(image_set_items))

        image_set_header = ImageSetHeader(**header_data)
        deduplicated_items = cls._remove_common_fields(image_set_items, set(common_fields.keys()))

        ifdo = iFDO(image_set_header=image_set_header, image_set_items=deduplicated_items)

        output_name = (
            metadata_name
            if metadata_name and metadata_name.endswith(".ifdo")
            else (f"{metadata_name}.ifdo" if metadata_name else cls.DEFAULT_METADATA_NAME)
        )

        if not dry_run:
            saver(root_dir, output_name, ifdo.model_dump(mode="json", by_alias=True, exclude_none=True))

    @staticmethod
    def _chunk_dataset(
        dataset_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
        chunk_size: int,
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

        # Use dataset-aware memory batch size as chunk size if not provided
        if chunk_size is None:
            # Get image files for sampling
            image_files = [
                file_path
                for file_path, _ in dataset_mapping.items()
                if file_path.suffix.lower() in EXIF_SUPPORTED_EXTENSIONS
            ]

            chunk_size = _calculate_safe_image_batch_size(image_files)
            available_memory = _get_available_memory_mb()

            if image_files:
                log.info(
                    f"Dataset-aware batching: {available_memory}MB available, "
                    f"sampled {min(5, len(image_files))} images, using chunks of {chunk_size} files each",
                )
            else:
                log.info(
                    f"Memory-aware batching: {available_memory}MB available, "
                    f"using default chunks of {chunk_size} files each (no images to sample)",
                )
        else:
            log.info(f"Using specified chunk size: {chunk_size} files per chunk")

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

            # Phase 1: Memory-aware image processing
            processed_images, non_exif_files = cls._process_images_memory_safe(chunk, thread_num, logger)

            # Phase 2: Batch EXIF writing (no image loading)
            cls._write_exif_batch(processed_images, thread_num, logger)
            cls._log_non_exif_files(non_exif_files, thread_num, logger)

            # Update progress for entire chunk
            if progress and task is not None:
                progress.advance(task, advance=len(chunk))

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Processing files with metadata (4/12)", total=len(dataset_mapping))
            process_chunk(cls, items=chunks, progress=progress, task=task, logger=log)  # type: ignore[call-arg]

    @classmethod
    def _process_images_memory_safe(
        cls,
        chunk: list[tuple[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]],
        thread_num: str,
        logger: logging.Logger,
    ) -> tuple[list[ProcessedImageData], list[Path]]:
        """Phase 1: Image processing - chunk is already memory-sized."""
        processed_images: list[ProcessedImageData] = []
        non_exif_files: list[Path] = []

        # Separate EXIF-supported files from others
        exif_candidates = []
        for file_path, (metadata_items, ancillary_data) in chunk:
            file_extension = file_path.suffix.lower()

            if file_extension in EXIF_SUPPORTED_EXTENSIONS:
                # Get the ImageData from the metadata items
                ifdo_metadata_items = [item for item in metadata_items if isinstance(item, iFDOMetadata)]

                if ifdo_metadata_items:
                    image_data = ifdo_metadata_items[0].primary_image_data
                    exif_candidates.append((file_path, image_data, ancillary_data))
                    continue

            non_exif_files.append(file_path)

        if not exif_candidates:
            return processed_images, non_exif_files

        # Process all images with adaptive batch sizing
        logger.debug(f"Thread {thread_num} - Processing {len(exif_candidates)} images with adaptive batching")
        processed_images = cls._process_image_batch_adaptive(exif_candidates, thread_num, logger)

        return processed_images, non_exif_files

    @classmethod
    def _process_image_batch_adaptive(
        cls,
        image_batch: list[tuple[Path, "ImageData", dict[str, Any] | None]],
        thread_num: str,
        logger: logging.Logger,
        initial_batch_size: int | None = None,
    ) -> list[ProcessedImageData]:
        """
        Process images with adaptive batch sizing - retry with smaller batches on OOM.

        Args:
            image_batch: List of images to process
            thread_num: Thread identifier for logging
            logger: Logger instance
            initial_batch_size: Starting batch size (uses len(image_batch) if None)

        Returns:
            List of processed image data
        """
        if not image_batch:
            return []

        current_batch_size = initial_batch_size or len(image_batch)
        current_batch_size = min(current_batch_size, len(image_batch))
        all_results = []

        # Process in adaptive batches
        start_idx = 0
        while start_idx < len(image_batch):
            batch_slice = image_batch[start_idx : start_idx + current_batch_size]

            try:
                # Attempt to process current batch
                batch_results = cls._process_image_batch(batch_slice, thread_num, logger)
                all_results.extend(batch_results)
                start_idx += current_batch_size

                # Success - can potentially increase batch size for next iteration
                if current_batch_size < len(image_batch) and start_idx < len(image_batch):
                    # Cautiously increase batch size by 50% for next batch
                    current_batch_size = min(int(current_batch_size * 1.5), len(image_batch) - start_idx)

            except MemoryError:
                if current_batch_size <= 1:
                    # Can't reduce further - skip this problematic image
                    logger.warning(
                        f"Thread {thread_num} - Skipping image due to memory constraints: {batch_slice[0][0]}",
                    )
                    # Create error result for the failed image
                    failed_image = ProcessedImageData(
                        file_path=batch_slice[0][0],
                        image_data=batch_slice[0][1],
                        ancillary_data=batch_slice[0][2],
                        width=0,
                        height=0,
                        processing_error="Memory error - image too large to process",
                    )
                    all_results.append(failed_image)
                    start_idx += 1
                else:
                    # Halve batch size and retry
                    current_batch_size = max(1, current_batch_size // 2)
                    logger.warning(f"Thread {thread_num} - Memory error, reducing batch size to {current_batch_size}")
                    # Don't increment start_idx - retry the same batch with smaller size

        return all_results

    @classmethod
    def _process_image_batch(
        cls,
        image_batch: list[tuple[Path, "ImageData", dict[str, Any] | None]],
        thread_num: str,
        logger: logging.Logger,
    ) -> list[ProcessedImageData]:
        """Process a small batch of images with controlled threading."""
        results: list[ProcessedImageData] = []

        def process_single_image(
            file_data: tuple[Path, "ImageData", dict[str, Any] | None],
        ) -> ProcessedImageData:
            file_path, image_data, ancillary_data = file_data

            try:
                with Image.open(file_path) as image_file:
                    # Extract image properties
                    cls._extract_image_properties(image_file, image_data)
                    width, height = image_file.size

                    # Generate thumbnail data (in memory)
                    thumbnail_data = cls._create_thumbnail_data(image_file)

                    return ProcessedImageData(
                        file_path=file_path,
                        image_data=image_data,
                        ancillary_data=ancillary_data,
                        width=width,
                        height=height,
                        thumbnail_data=thumbnail_data,
                    )

            except (OSError, ValueError, AttributeError, MemoryError) as e:
                logger.warning(f"Thread {thread_num} - Failed to process image {file_path}: {e}")
                return ProcessedImageData(
                    file_path=file_path,
                    image_data=image_data,
                    ancillary_data=ancillary_data,
                    width=0,
                    height=0,
                    processing_error=str(e),
                )

        # Process images sequentially within each chunk (chunks are already parallel)
        for file_data in image_batch:
            result = process_single_image(file_data)
            results.append(result)

        return results

    @staticmethod
    def _create_thumbnail_data(image_file: Image.Image) -> bytes:
        """Create thumbnail data as bytes (no file I/O)."""
        # Create thumbnail
        thumbnail = image_file.copy()
        thumbnail.thumbnail(DEFAULT_EXIF_THUMBNAIL_SIZE, Image.Resampling.LANCZOS)

        # Convert to bytes
        buffer = io.BytesIO()
        thumbnail.save(buffer, format="JPEG", quality=85)
        return buffer.getvalue()

    @classmethod
    def _write_exif_batch(
        cls,
        processed_images: list[ProcessedImageData],
        thread_num: str,
        logger: logging.Logger,
    ) -> None:
        """Phase 2: Batch EXIF writing without loading images."""
        if not processed_images:
            return

        # Separate successful from failed processing
        successful_images = [img for img in processed_images if img.processing_error is None]
        failed_images = [img for img in processed_images if img.processing_error is not None]

        if failed_images:
            logger.warning(f"Thread {thread_num} - Skipping {len(failed_images)} images due to processing errors")

        if not successful_images:
            return

        logger.debug(f"Thread {thread_num} - Writing EXIF data for {len(successful_images)} images")

        try:
            with exiftool.ExifToolHelper() as et:
                # Get existing metadata for all files in batch
                file_paths = [str(img.file_path) for img in successful_images]
                existing_metadata_list = et.get_metadata(file_paths)

                # Build a mapping of existing EXIF data
                existing_exif_map = {}
                for i, metadata in enumerate(existing_metadata_list):
                    existing_exif_map[file_paths[i]] = metadata or {}

                # Process EXIF tags for all files
                cls._process_exif_tags_batch(et, successful_images, existing_exif_map)

                # Handle thumbnails separately (using pre-generated data)
                cls._embed_thumbnail_batch(et, successful_images, logger)

                logger.debug(f"Thread {thread_num} - Successfully wrote EXIF data for {len(successful_images)} images")

        except FileNotFoundError as e:
            if "exiftool" in str(e).lower():
                show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
            else:
                logger.warning(f"Thread {thread_num} - File not found during batch EXIF processing: {e}")
        except ExifToolException as e:
            logger.warning(f"Thread {thread_num} - Failed to write EXIF metadata in batch: {e}")

    @classmethod
    def _process_exif_tags_batch(
        cls,
        et: exiftool.ExifToolHelper,
        successful_images: list[ProcessedImageData],
        existing_exif_map: dict[str, dict[str, Any]],
    ) -> None:
        """Build and apply EXIF tags for all images in batch."""
        for img in successful_images:
            file_path_str = str(img.file_path)
            existing_exif = existing_exif_map.get(file_path_str, {})

            exif_tags: dict[str, Any] = {}

            # Add pre-computed image dimensions
            if img.width > 0 and "EXIF:ExifImageWidth" not in existing_exif:
                exif_tags["EXIF:ExifImageWidth"] = img.width
            if img.height > 0 and "EXIF:ExifImageHeight" not in existing_exif:
                exif_tags["EXIF:ExifImageHeight"] = img.height

            # Add metadata-only EXIF tags (no image required)
            cls._add_datetime_tags(exif_tags, img.image_data)
            cls._add_identifier_tags(exif_tags, img.image_data)
            cls._add_gps_tags(exif_tags, img.image_data)
            cls._add_user_comment(exif_tags, img.image_data, img.ancillary_data)

            # Apply tags if any were built
            if exif_tags:
                et.set_tags([file_path_str], exif_tags, params=["-overwrite_original_in_place"])

    @staticmethod
    def _embed_thumbnail_batch(
        et: exiftool.ExifToolHelper,
        processed_images: list[ProcessedImageData],
        logger: logging.Logger,
    ) -> None:
        """Embed pre-generated thumbnails using ExifTool."""
        for img in processed_images:
            if img.thumbnail_data:
                tmp_path: Path | None = None
                try:
                    # Write thumbnail data to temporary file
                    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_file:
                        tmp_path = Path(tmp_file.name)
                        tmp_file.write(img.thumbnail_data)
                        tmp_file.flush()

                    # Embed thumbnail using ExifTool
                    et.set_tags(
                        [str(img.file_path)],
                        {"ThumbnailImage": str(tmp_path)},
                        params=["-overwrite_original_in_place", "-tagsfromfile", str(tmp_path)],
                    )

                except (OSError, ExifToolException) as e:
                    logger.warning(f"Failed to embed thumbnail for {img.file_path}: {e}")

                finally:
                    if tmp_path is not None:
                        tmp_path.unlink(missing_ok=True)

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
    def _add_datetime_tags(exif_tags: dict[str, Any], image_data: ImageData) -> None:
        """Add datetime-related EXIF tags."""
        if image_data.image_datetime is not None:
            dt = image_data.image_datetime
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                dt = dt.astimezone(UTC)
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
        has_position = image_data.image_latitude is not None or image_data.image_longitude is not None

        if image_data.image_latitude is not None:
            exif_tags["EXIF:GPSLatitude"] = abs(image_data.image_latitude)
            exif_tags["EXIF:GPSLatitudeRef"] = "N" if image_data.image_latitude >= 0 else "S"

        if image_data.image_longitude is not None:
            exif_tags["EXIF:GPSLongitude"] = abs(image_data.image_longitude)
            exif_tags["EXIF:GPSLongitudeRef"] = "E" if image_data.image_longitude >= 0 else "W"

        if image_data.image_altitude_meters is not None:
            exif_tags["EXIF:GPSAltitude"] = abs(float(image_data.image_altitude_meters))
            exif_tags["EXIF:GPSAltitudeRef"] = "0" if image_data.image_altitude_meters >= 0 else "1"

        if has_position:
            # Declare the geodetic datum of the coordinates. Honour the pipeline-supplied coordinate reference
            # system verbatim; fall back to WGS-84 (the EXIF and iFDO default) only when none was set.
            exif_tags["EXIF:GPSMapDatum"] = image_data.image_coordinate_reference_system or "WGS-84"

            # Record the UTC time of the GPS fix when the capture datetime is timezone-aware. A naive datetime
            # cannot be asserted as UTC, so its GPS timestamp is omitted (the offset is unknown).
            dt = image_data.image_datetime
            if dt is not None and dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                dt_utc = dt.astimezone(UTC)
                exif_tags["EXIF:GPSDateStamp"] = dt_utc.strftime("%Y:%m:%d")
                exif_tags["EXIF:GPSTimeStamp"] = dt_utc.strftime("%H:%M:%S")

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
        # Inject the image entropy and average image color into the iFDO.
        # Lazy-imported to keep cv2 / numpy / PIL out of CLI startup.
        from marimba.lib import image  # noqa: PLC0415

        image_data.image_entropy = image.get_shannon_entropy(image_file)
        image_data.image_average_color = image.get_average_image_color(image_file)
