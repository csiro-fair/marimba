import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from textwrap import dedent
from typing import Dict, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

import piexif
from ifdo.models import ImageData, ImageSetHeader, iFDO

from marimba.core.utils.log import LogMixin, get_file_handler
from marimba.core.utils.map import make_summary_map
from marimba.lib.gps import convert_degrees_to_gps_coordinate


def sizeof_fmt(num: int, suffix: str = "B") -> str:
    """
    Convert a number of bytes to a human-readable format.

    Args:
        num: The number of bytes.
        suffix: The suffix to use for the size (default is "B").

    Returns:
        A string representing the human-readable size.
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024:
            return f"{num:.1f} {unit}{suffix}"
        num /= 1024
    return f"{num:.1f} Yi{suffix}"


@dataclass
class ImagerySummary:
    """
    Summary of an imagery collection.
    """

    num_images: int
    size_images_bytes: int
    num_videos: int
    size_videos_bytes: int
    num_other: int
    size_other_bytes: int

    @property
    def num_files(self) -> int:
        return self.num_images + self.num_videos + self.num_other

    @property
    def size_files_bytes(self) -> int:
        return self.size_images_bytes + self.size_videos_bytes + self.size_other_bytes

    @classmethod
    def from_dataset(cls, dataset_wrapper: "DatasetWrapper") -> "ImagerySummary":
        """
        Create an imagery summary from a dataset wrapper.

        Args:
            dataset_wrapper: The dataset wrapper.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_dir(dataset_wrapper.data_dir)

    @classmethod
    def from_dir(cls, directory: Path) -> "ImagerySummary":
        """
        Create an imagery summary from a directory.

        Args:
            directory: The directory.

        Returns:
            An imagery summary.
        """
        num_images = 0
        size_images_bytes = 0
        num_videos = 0
        size_videos_bytes = 0
        num_other = 0
        size_other_bytes = 0

        for path in directory.glob("**/*"):
            if path.is_dir():
                continue

            # Match the suffix to determine the file type and update the counts/sizes
            suffix = path.suffix.lower()
            if suffix in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif"]:
                num_images += 1
                size_images_bytes += path.stat().st_size
            elif suffix in [".mp4", ".mov", ".avi", ".wmv", ".flv", ".mkv", ".webm"]:
                num_videos += 1
                size_videos_bytes += path.stat().st_size
            else:
                num_other += 1
                size_other_bytes += path.stat().st_size

        return cls(num_images, size_images_bytes, num_videos, size_videos_bytes, num_other, size_other_bytes)

    def __str__(self) -> str:
        return dedent(
            f"""\
            Dataset summary:
            {self.num_images} images ({sizeof_fmt(self.size_images_bytes)})
            {self.num_videos} videos ({sizeof_fmt(self.size_videos_bytes)})
            {self.num_other} other files ({sizeof_fmt(self.size_other_bytes)})
            {self.num_files} total files ({sizeof_fmt(self.size_files_bytes)})"""
        )


@dataclass
class Manifest:
    """
    Dataset manifest. Used to validate datasets to check if the underlying data has been corrupted or modified.
    """

    hashes: Dict[Path, bytes]

    @staticmethod
    def compute_hash(path: Path) -> bytes:
        """
        Compute the hash of a path.

        Args:
            path: The path.

        Returns:
            The hash of the path contents.
        """
        # SHA-256 hash
        hash = hashlib.sha256()

        if path.is_file():
            # Hash the file contents
            with path.open("rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash.update(chunk)

        # Hash the path
        hash.update(str(path.as_posix()).encode("utf-8"))

        return hash.digest()

    @classmethod
    def from_dir(cls, directory: Path, exclude_paths: Optional[Iterable[Path]] = None) -> "Manifest":
        """
        Create a manifest from a directory.

        Args:
            directory: The directory.
            exclude_paths: Paths to exclude from the manifest.

        Returns:
            A manifest.
        """
        hashes = {}
        exclude_paths = set(exclude_paths) if exclude_paths is not None else set()
        for path in directory.glob("**/*"):
            if path in exclude_paths:
                continue
            rel_path = path.resolve().relative_to(directory)
            hashes[rel_path] = Manifest.compute_hash(path)
        return cls(hashes)

    def validate(self, directory: Path, exclude_paths: Optional[Iterable[Path]] = None) -> bool:
        """
        Validate a directory against the manifest.

        Args:
            directory: The directory.
            exclude_paths: Paths to exclude from the manifest.

        Returns:
            True if the directory is valid, False otherwise.
        """
        # Create a manifest from the directory
        manifest = Manifest.from_dir(directory, exclude_paths=exclude_paths)

        return self == manifest

    def __eq__(self, other: "Manifest") -> bool:
        """
        Check if two manifests are equal.

        Args:
            other: The other manifest.

        Returns:
            True if the manifests are equal, False otherwise.
        """
        if len(self.hashes) != len(other.hashes):
            return False

        for path, hash in self.hashes.items():
            if hash != other.hashes.get(path):
                return False

        return True

    def save(self, path: Path):
        """
        Save the manifest to a file.

        Args:
            path: The path to the file.
        """
        with path.open("w") as f:
            for path, hash in self.hashes.items():
                f.write(f"{path.as_posix()}:{hash.hex()}\n")

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """
        Load a manifest from a file.

        Args:
            path: The path to the file.

        Returns:
            A manifest.
        """
        hashes = {}
        with path.open("r") as f:
            for line in f:
                if line:
                    path_str, hash_str = line.split(":")
                    hashes[Path(path_str)] = bytes.fromhex(hash_str)
        return cls(hashes)


class DatasetWrapper(LogMixin):
    """
    Dataset directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the dataset directory structure is invalid.
        """

        pass

    class InvalidDatasetMappingError(Exception):
        """
        Raised when a path mapping dictionary is invalid.
        """

        pass

    class ManifestError(Exception):
        """
        Raised when the dataset is inconsistent with its manifest.
        """

        pass

    def __init__(self, root_dir: Union[str, Path], dry_run: bool = False):
        self._root_dir = Path(root_dir)
        self._dry_run = dry_run

        self._check_file_structure()
        self._setup_logging()

    @classmethod
    def create(cls, root_dir: Union[str, Path], dry_run: bool = False) -> "DatasetWrapper":
        """
        Create a new dataset from an iFDO.

        Args:
            root_dir: The root directory of the dataset.
            dry_run: Whether to perform a dry run.

        Returns:
            A dataset wrapper instance.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the dataset directory structure
        root_dir = Path(root_dir)
        data_dir = root_dir / "data"
        logs_dir = root_dir / "logs"
        pipeline_logs_dir = logs_dir / "pipelines"

        # Check that the root directory doesn't already exist
        if root_dir.is_dir():
            raise FileExistsError(root_dir)

        # Create the file structure
        root_dir.mkdir(parents=True)
        data_dir.mkdir()
        logs_dir.mkdir()
        pipeline_logs_dir.mkdir()

        return cls(root_dir, dry_run=dry_run)

    def _check_file_structure(self):
        """
        Check that the file structure of the project directory is valid. If not, raise an InvalidStructureError with details.

        Raises:
            DatasetWrapper.InvalidStructureError: If the file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise DatasetWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.data_dir)
        check_dir_exists(self.logs_dir)
        check_dir_exists(self.pipeline_logs_dir)

    def validate(self):
        """
        Validate the dataset. If the dataset is inconsistent with its manifest (if present), raise a ManifestError.

        Raises:
            DatasetWrapper.ManifestError: If the dataset is inconsistent with its manifest.
        """
        if self.manifest_path.exists():
            manifest = Manifest.load(self.manifest_path)
            if not manifest.validate(self.root_dir, exclude_paths=[self.manifest_path, self.log_path]):
                raise DatasetWrapper.ManifestError(self.manifest_path)
            self.logger.debug(f"Dataset is consistent with manifest at {self.manifest_path}.")

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `dataset.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.root_dir, self.name, False, level=logging.DEBUG)

        # Add the file handler to the logger
        self.logger.addHandler(self._file_handler)

    def get_pipeline_data_dir(self, pipeline_name: str) -> Path:
        """
        Get the path to the data directory for the given pipeline.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The absolute path to the pipeline data directory.
        """
        return self.data_dir / pipeline_name

    def _apply_ifdo_exif_tags(self, metadata_mapping: Dict[Path, ImageData]):
        """
        Apply EXIF tags from iFDO metadata to the provided paths.

        Args:
            metadata_mapping: A dict mapping file paths to image data.
        """
        for path, image_data in metadata_mapping.items():
            try:
                # Load the existing EXIF metadata
                exif_dict = piexif.load(str(path))
            except piexif.InvalidImageDataError as e:
                self.logger.warning(f"Failed to load EXIF metadata from {path}: {e}")
                continue

            # Overwrite the EXIF metadata with iFDO metadata
            ifd_0th = exif_dict["0th"]
            ifd_exif = exif_dict["Exif"]
            ifd_gps = exif_dict["GPS"]
            # ifd_interop = exif_dict["Interop"]
            # ifd_1st = exif_dict["1st"]
            # ifd_thumbnail = exif_dict["thumbnail"]

            if image_data.image_datetime is not None:
                dt = image_data.image_datetime

                # Convert to UTC if dt is timezone-aware
                offset_str = None
                if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                    dt = dt.astimezone(timezone.utc)
                    offset_str = "+00:00"

                # Format the datetime and subsec strings
                datetime_str = datetime.strftime(image_data.image_datetime, "%Y:%m:%d %H:%M:%S")
                subsec_str = str(image_data.image_datetime.microsecond)

                # Inject them into the EXIF metadata
                ifd_0th[piexif.ImageIFD.DateTime] = datetime_str
                ifd_exif[piexif.ExifIFD.DateTimeOriginal] = datetime_str
                ifd_exif[piexif.ExifIFD.SubSecTime] = subsec_str
                ifd_exif[piexif.ExifIFD.SubSecTimeOriginal] = subsec_str
                if offset_str is not None:
                    ifd_exif[piexif.ExifIFD.OffsetTime] = offset_str
                    ifd_exif[piexif.ExifIFD.OffsetTimeOriginal] = offset_str

            # Inject the GPS coordinates into the EXIF metadata
            if image_data.image_latitude is not None:
                d_lat, m_lat, s_lat = convert_degrees_to_gps_coordinate(image_data.image_latitude)
                ifd_gps[piexif.GPSIFD.GPSLatitude] = ((d_lat, 1), (m_lat, 1), (s_lat, 1))
                ifd_gps[piexif.GPSIFD.GPSLatitudeRef] = "N" if image_data.image_latitude > 0 else "S"
            if image_data.image_longitude is not None:
                d_lon, m_lon, s_lon = convert_degrees_to_gps_coordinate(image_data.image_longitude)
                ifd_gps[piexif.GPSIFD.GPSLongitude] = ((d_lon, 1), (m_lon, 1), (s_lon, 1))
                ifd_gps[piexif.GPSIFD.GPSLongitudeRef] = "E" if image_data.image_longitude > 0 else "W"

            # Write the EXIF metadata back to the file
            try:
                exif_bytes = piexif.dump(exif_dict)
                piexif.insert(exif_bytes, str(path))
                self.logger.debug(f"Applied iFDO EXIF tags to {path}.")
            except piexif.InvalidImageDataError:
                self.logger.warning(f"Failed to write EXIF metadata to {path}")

    def populate(
        self,
        dataset_name: str,
        dataset_mapping: Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]],
        project_log_path: Path,
        pipeline_log_paths: Iterable[Path],
        copy: bool = True,
    ):
        """
        Populate the dataset with files from the given dataset mapping.

        Args:
            dataset_name: The name of the dataset.
            dataset_mapping: A dict mapping pipeline name -> { output file path -> (input file path, image data) }
            project_log_path: The path to the project log file.
            pipeline_log_paths: The paths to the pipeline log files.
            copy: Whether to copy (True) or move (False) the files.

        Raises:
            DatasetWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        # Verify that the path mapping is valid
        DatasetWrapper.check_dataset_mapping(dataset_mapping)

        # Copy or move the files and populate the iFDO image set items
        image_set_items = {}
        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            pipeline_data_dir = self.get_pipeline_data_dir(pipeline_name)
            for src, (relative_dst, image_data_list) in pipeline_data_mapping.items():
                # Compute the absolute destination path
                dst = pipeline_data_dir / relative_dst

                if image_data_list:  # Only consider items that have ImageData
                    # Compute the data directory-relative destination path for the iFDO
                    dst_relative = dst.relative_to(self.data_dir)
                    image_set_items[str(dst_relative)] = image_data_list

                # Create the parent directory if it doesn't exist
                dst.parent.mkdir(parents=True, exist_ok=True)

                # Copy or move the file if not in dry-run mode
                if not self.dry_run:
                    if copy:
                        copy2(src, dst)  # use copy2 to preserve metadata
                    else:
                        src.rename(dst)  # use rename to move the file
                self.logger.debug(f"{src.absolute()} -> {dst} ({copy=})")

        # Generate and write the iFDO
        ifdo = iFDO(
            image_set_header=ImageSetHeader(
                image_set_name=dataset_name,
                image_set_uuid=str(uuid4()),
                image_set_handle="",  # TODO: Populate this from the distribution target URL
            ),
            image_set_items=image_set_items,
        )
        if not self.dry_run:
            ifdo.save(self.metadata_path)

        # Apply iFDO EXIF metadata
        metadata_mapping = {}
        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            for _, (relative_dst, image_data_list) in pipeline_data_mapping.items():
                dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst
                if not image_data_list:  # Skip if no ImageData items
                    continue
                metadata_mapping[dst] = image_data_list[0]  # Use the first ImageData item
        self._apply_ifdo_exif_tags(metadata_mapping)

        # Update the dataset summary
        summary = self.summarize()
        if not self.dry_run:
            self.summary_path.write_text(str(summary))

        # Create a summary map if there are any geolocations
        geolocations = [
            (image_data.image_latitude, image_data.image_longitude)
            for image_data_list in image_set_items.values()
            for image_data in image_data_list
            if image_data.image_latitude is not None and image_data.image_longitude
        ]
        if geolocations:
            summary_map = make_summary_map(geolocations)
            if summary_map is not None:
                if not self.dry_run:
                    summary_map.save(self.root_dir / "map.png")

        # Copy in the project and pipeline logs
        copy2(project_log_path, self.logs_dir)
        for pipeline_log_path in pipeline_log_paths:
            copy2(pipeline_log_path, self.pipeline_logs_dir)

        # Generate and save the manifest
        manifest = Manifest.from_dir(self.root_dir, exclude_paths=[self.manifest_path, self.log_path])
        if not self.dry_run:
            manifest.save(self.manifest_path)

    def summarize(self) -> ImagerySummary:
        """
        Create an imagery summary for this dataset.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_dataset(self)

    @property
    def root_dir(self) -> Path:
        """
        The root directory.
        """
        return self._root_dir

    @property
    def data_dir(self) -> Path:
        """
        The path to the data directory.
        """
        return self._root_dir / "data"

    @property
    def metadata_path(self) -> Path:
        """
        The path to the iFDO.
        """
        return self._root_dir / "metadata.yml"

    @property
    def summary_path(self) -> Path:
        """
        The path to the dataset summary.
        """
        return self._root_dir / "summary.txt"

    @property
    def manifest_path(self) -> Path:
        """
        The path to the dataset manifest.
        """
        return self._root_dir / "manifest.txt"

    @property
    def name(self) -> str:
        """
        The name of the dataset.
        """
        return self._root_dir.name

    @property
    def log_path(self) -> Path:
        """
        The path to the dataset log file.
        """
        return self._root_dir / f"{self.name}.log"

    @property
    def logs_dir(self) -> Path:
        """
        The path to the logs directory.
        """
        return self._root_dir / "logs"

    @property
    def pipeline_logs_dir(self) -> Path:
        """
        The path to the pipeline logs directory.
        """
        return self.logs_dir / "pipelines"

    @property
    def dry_run(self) -> bool:
        """
        Whether the dataset generation should run in dry-run mode.
        """
        return self._dry_run

    @staticmethod
    def check_dataset_mapping(dataset_mapping: Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]]):
        """
        Verify that the given path mapping is valid.

        Args:
            path_mapping: A mapping from source paths to destination paths.

        Raises:
            DatasetWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            # Verify that all source paths exist
            for src in pipeline_data_mapping:
                if not src.exists():
                    raise DatasetWrapper.InvalidDatasetMappingError(f"Source path {src} does not exist.")

            # Verify that all source paths resolve to unique paths
            reverse_src_resolution = {}
            for src in pipeline_data_mapping:
                resolved = src.resolve().absolute()
                if resolved in reverse_src_resolution:
                    raise DatasetWrapper.InvalidDatasetMappingError(
                        f"Source paths {src} and {reverse_src_resolution[resolved]} both resolve to {resolved}."
                    )
                reverse_src_resolution[resolved] = src

            # Verify that all destination paths are relative
            for dst, _ in pipeline_data_mapping.values():
                if dst.is_absolute():
                    raise DatasetWrapper.InvalidDatasetMappingError(f"Destination path {dst} must be relative.")

            # Verify that there are no collisions in destination paths
            reverse_mapping = {dst.resolve(): src for src, (dst, _) in pipeline_data_mapping.items()}
            for src, (dst, _) in pipeline_data_mapping.items():
                src_other = reverse_mapping.get(dst.resolve())
                if src.resolve() != src_other.resolve():
                    raise DatasetWrapper.InvalidDatasetMappingError(
                        f"Resolved destination path {dst.resolve()} is the same for source paths {src} and {src_other}."
                    )
