import hashlib
import io
import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import timezone
from fractions import Fraction
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from textwrap import dedent
from typing import Dict, Iterable, Optional, Tuple, Union, Any, List
from uuid import uuid4

import piexif
from PIL import Image
from ifdo.models import ImageData, ImageSetHeader, iFDO
from rich.progress import Progress, SpinnerColumn

from marimba.core.utils.log import LogMixin, get_file_handler
from marimba.core.utils.map import make_summary_map
from marimba.core.utils.rich import get_default_columns
from marimba.lib import image
from marimba.lib.gps import convert_degrees_to_gps_coordinate


def sizeof_fmt(num: float, suffix: str = "B") -> str:
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
    def from_dir(
        cls, directory: Path, exclude_paths: Optional[Iterable[Path]] = None, progress: Optional[Progress] = None
    ) -> "Manifest":
        """
        Create a manifest from a directory.

        Args:
            directory: The directory.
            exclude_paths: Paths to exclude from the manifest.
            progress: A progress bar to monitor the manifest generation

        Returns:
            A manifest.
        """
        hashes = {}
        exclude_paths = set(exclude_paths) if exclude_paths is not None else set()

        globbed_files = list(directory.glob("**/*"))
        if progress:
            task = progress.add_task("[green]Generating manifest", total=len(globbed_files))

        for path in globbed_files:
            # Update the task
            if progress:
                progress.advance(task)
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

    def __eq__(self, other: object) -> bool:
        """
        Check if two manifests are equal.

        Args:
            other: The other manifest.

        Returns:
            True if the manifests are equal, False otherwise.
        """
        if not isinstance(other, Manifest):
            return NotImplemented

        if len(self.hashes) != len(other.hashes):
            return False

        for path, hash in self.hashes.items():
            if hash != other.hashes.get(path):
                return False

        return True

    def save(self, path: Path) -> None:
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

        if not dry_run:
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
        if not dry_run:
            root_dir.mkdir(parents=True)
            data_dir.mkdir()
            logs_dir.mkdir()
            pipeline_logs_dir.mkdir()

        return cls(root_dir, dry_run=dry_run)

    def _check_file_structure(self) -> None:
        """
        Check that the file structure of the project directory is valid. If not, raise an InvalidStructureError with
        details.

        Raises:
            DatasetWrapper.InvalidStructureError: If the file structure is invalid.
        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise DatasetWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.data_dir)
        check_dir_exists(self.logs_dir)
        check_dir_exists(self.pipeline_logs_dir)

    def validate(self) -> None:
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

    def _setup_logging(self) -> None:
        """
        Set up logging. Create file handler for this instance that writes to `dataset.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.root_dir, "dataset", False, level=logging.DEBUG)

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

    def _apply_ifdo_exif_tags(self, metadata_mapping: Dict[Path, Tuple[ImageData, Optional[Dict[str, Any]]]]) -> None:
        """
        Apply EXIF tags from iFDO metadata to the provided paths.

        Args:
            metadata_mapping: A dict mapping file paths to image data.
        """
        for path, (image_data, ancillary_data) in metadata_mapping.items():
            try:
                exif_dict = piexif.load(str(path))
            except piexif.InvalidImageDataError as e:
                self.logger.warning(f"Failed to load EXIF metadata from {path}: {e}")
                continue

            self._inject_datetime(image_data, exif_dict)
            self._inject_gps_coordinates(image_data, exif_dict)
            image_file = self._add_thumbnail(path, exif_dict)
            self._extract_image_properties(image_file, image_data)
            self._burn_in_exif_metadata(image_data, ancillary_data, exif_dict)

            try:
                exif_bytes = piexif.dump(exif_dict)
                piexif.insert(exif_bytes, str(path))
                self.logger.debug(f"Applied iFDO EXIF tags to {path}.")
            except piexif.InvalidImageDataError:
                self.logger.warning(f"Failed to write EXIF metadata to {path}")

    @staticmethod
    def _inject_datetime(image_data: ImageData, exif_dict: Dict[str, Any]) -> None:
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
    def _inject_gps_coordinates(image_data: ImageData, exif_dict: Dict[str, Any]) -> None:
        """
        Inject GPS coordinates into EXIF metadata.

        Args:
            image_data: The image data containing GPS information.
            exif_dict: The EXIF metadata dictionary.
        """
        ifd_gps = exif_dict["GPS"]

        if image_data.image_latitude is not None:
            d_lat, m_lat, s_lat = convert_degrees_to_gps_coordinate(image_data.image_latitude)
            ifd_gps[piexif.GPSIFD.GPSLatitude] = ((d_lat, 1), (m_lat, 1), (s_lat, 1))
            ifd_gps[piexif.GPSIFD.GPSLatitudeRef] = "N" if image_data.image_latitude > 0 else "S"
        if image_data.image_longitude is not None:
            d_lon, m_lon, s_lon = convert_degrees_to_gps_coordinate(image_data.image_longitude)
            ifd_gps[piexif.GPSIFD.GPSLongitude] = ((d_lon, 1), (m_lon, 1), (s_lon, 1))
            ifd_gps[piexif.GPSIFD.GPSLongitudeRef] = "E" if image_data.image_longitude > 0 else "W"
        if image_data.image_altitude is not None:
            altitude_fraction = Fraction(abs(float(image_data.image_altitude))).limit_denominator()
            altitude_rational = (altitude_fraction.numerator, altitude_fraction.denominator)
            ifd_gps[piexif.GPSIFD.GPSAltitude] = altitude_rational
            ifd_gps[piexif.GPSIFD.GPSAltitudeRef] = 0 if image_data.image_altitude >= 0 else 1

    @staticmethod
    def _add_thumbnail(path: Path, exif_dict: Dict[str, Any]) -> Image:
        """
        Add a thumbnail to the EXIF metadata.

        Args:
            path: The path to the image file.
            exif_dict: The EXIF metadata dictionary.
        """
        image_file = Image.open(path)
        thumbnail_size = (320, 240)
        image_file.thumbnail(thumbnail_size, Image.ANTIALIAS)
        thumbnail_io = io.BytesIO()
        image_file.save(thumbnail_io, format="JPEG", quality=90)
        exif_dict["thumbnail"] = thumbnail_io.getvalue()
        return image_file

    @staticmethod
    def _extract_image_properties(image_file: Image, image_data: ImageData) -> None:
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
    def _burn_in_exif_metadata(
        image_data: ImageData, ancillary_data: Optional[Dict[str, Any]], exif_dict: Dict[str, Any]
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

    def populate(
        self,
        dataset_name: str,
        dataset_mapping: Dict[str, Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]]],
        project_pipelines_dir: Path,
        project_log_path: Path,
        pipeline_log_paths: Iterable[Path],
        copy: bool = True,
    ) -> None:
        """
        Populate the dataset with files from the given dataset mapping.

        Args:
            dataset_name: The name of the dataset.
            dataset_mapping: A dict mapping pipeline name -> { output file path -> (input file path, image data) }
            project_pipelines_dir: The path to the project pipelines directory.
            project_log_path: The path to the project log file.
            pipeline_log_paths: The paths to the pipeline log files.
            copy: Whether to copy (True) or move (False) the files.

        Raises:
            DatasetWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        DatasetWrapper.check_dataset_mapping(dataset_mapping)
        self.logger.debug("Dataset mapping is valid.")

        image_set_items = self._populate_files(dataset_mapping, copy)

        self._apply_exif_metadata(dataset_mapping, image_set_items)
        self._generate_ifdo(dataset_name, image_set_items)
        self._generate_dataset_summary(image_set_items)
        self._copy_logs(project_log_path, pipeline_log_paths)
        self._copy_pipelines(project_pipelines_dir)
        self._generate_manifest()

    def _populate_files(
        self,
        dataset_mapping: Dict[str, Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]]],
        copy: bool,
    ) -> Dict[str, ImageData]:
        """
        Copy or move files from the dataset mapping to the destination directory.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
            copy: Boolean indicating whether to copy (True) or move (False) the files.

        Returns:
            Dict[str, ImageData]: A dictionary of image set items for further processing.
        """
        image_set_items = {}
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                pipeline_name: progress.add_task(
                    f"[green]Populating data for {pipeline_name} pipeline", total=len(pipeline_data_mapping)
                )
                for pipeline_name, pipeline_data_mapping in dataset_mapping.items()
                if len(pipeline_data_mapping) > 0
            }

            for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
                pipeline_data_dir = self.get_pipeline_data_dir(pipeline_name)
                for src, (relative_dst, image_data_list, ancillary_data) in pipeline_data_mapping.items():
                    dst = pipeline_data_dir / relative_dst
                    if image_data_list:
                        dst_relative = dst.relative_to(self.data_dir)
                        image_set_items[dst_relative.as_posix()] = image_data_list

                    if not self.dry_run:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        if copy:
                            copy2(src, dst)
                        else:
                            src.rename(dst)
                    self.logger.debug(f"{src.absolute()} -> {dst} ({copy=})")
                    progress.advance(tasks_by_pipeline_name[pipeline_name])
        return image_set_items

    def _apply_exif_metadata(
        self,
        dataset_mapping: Dict[str, Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]]],
        image_set_items: Dict[str, ImageData],
    ) -> None:
        """
        Apply EXIF metadata to the images in the dataset.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
            image_set_items: The dictionary of image set items for further processing.
        """
        metadata_mapping = {}
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                pipeline_name: progress.add_task(
                    f"[green]Applying iFDO EXIF tags for {pipeline_name} pipeline", total=len(pipeline_data_mapping)
                )
                for pipeline_name, pipeline_data_mapping in dataset_mapping.items()
                if len(pipeline_data_mapping) > 0
            }

            for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
                for _, (relative_dst, image_data_list, ancillary_data) in pipeline_data_mapping.items():
                    progress.advance(tasks_by_pipeline_name[pipeline_name])
                    dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst
                    if not image_data_list:
                        continue
                    metadata_mapping[dst] = (image_data_list[0], ancillary_data)

            if not self.dry_run:
                self._apply_ifdo_exif_tags(metadata_mapping)
            self.logger.debug(f"Applied iFDO EXIF tags to {len(metadata_mapping)} files")

    def _generate_ifdo(self, dataset_name: str, image_set_items: Dict[str, ImageData]) -> None:
        """
        Generate the iFDO (Image File Directory Object) metadata for the dataset.

        Args:
            dataset_name: The name of the dataset.
            image_set_items: The dictionary of image set items to include in the iFDO.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Generating iFDO", total=len(image_set_items) + 1)

            for image_path, image_data in image_set_items.items():
                image_data_path = Path(self.data_dir) / image_path
                hash = hashlib.sha256()
                if image_data_path.is_file():
                    with image_data_path.open("rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash.update(chunk)
                    for image_data_item in image_data:
                        image_data_item.image_hash_sha256 = hash.hexdigest()
                progress.advance(task)

            image_set_items = OrderedDict(sorted(image_set_items.items(), key=lambda item: item[0]))

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
            self.logger.debug(
                f"Generated iFDO at {self.metadata_path} with {len(ifdo.image_set_items)} image set items"
            )
            progress.advance(task)

    def _generate_dataset_summary(self, image_set_items: Dict[str, ImageData]) -> None:
        """
        Generate a summary of the dataset, including a map of geolocations if available.

        Args:
            image_set_items: The dictionary of image set items to summarize.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Generating dataset summary", total=1)
            summary = self.summarize()
            if not self.dry_run:
                self.summary_path.write_text(str(summary))
            self.logger.debug(f"Updated dataset summary at {self.summary_path}")

            geolocations = [
                (image_data.image_latitude, image_data.image_longitude)
                for image_data_list in image_set_items.values()
                for image_data in image_data_list
                if image_data.image_latitude is not None and image_data.image_longitude is not None
            ]
            if geolocations:
                progress.update(task, description="[green]Generating summary map")
                summary_map = make_summary_map(geolocations)
                if summary_map is not None:
                    map_path = self.root_dir / "map.png"
                    if not self.dry_run:
                        summary_map.save(map_path)
                    self.logger.debug(f"Generated summary map at {map_path}")
            progress.advance(task)

    def _copy_logs(self, project_log_path: Path, pipeline_log_paths: Iterable[Path]) -> None:
        """
        Copy project and pipeline log files to the appropriate directories.

        Args:
            project_log_path: The path to the project log file.
            pipeline_log_paths: The paths to the pipeline log files.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Copying logs", total=1)
            if not self.dry_run:
                copy2(project_log_path, self.logs_dir)
                for pipeline_log_path in pipeline_log_paths:
                    copy2(pipeline_log_path, self.pipeline_logs_dir)
            self.logger.debug(f"Copied project logs to {self.logs_dir}")
            progress.advance(task)

    def _copy_pipelines(self, project_pipelines_dir: Path) -> None:
        """
        Copy project pipelines to the appropriate directory, ignoring unnecessary files.

        Args:
            project_pipelines_dir: The path to the project pipelines directory.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Copying pipelines", total=1)
            if not self.dry_run:
                ignore = ignore_patterns(
                    ".git",
                    ".gitignore",
                    ".gitattributes",
                    "__pycache__",
                    "*.pyc",
                    ".DS_Store",
                    "*.log",
                )
                copytree(project_pipelines_dir, self.pipelines_dir, dirs_exist_ok=True, ignore=ignore)
            self.logger.debug(f"Copied project pipelines to {self.pipelines_dir}")
            progress.advance(task)

    # TODO: Possible speed improvement
    #  Pass through image_set_items to avoid duplicate computation of SHA256 hashes for images
    def _generate_manifest(self) -> None:
        """
        Generate and save the manifest for the dataset, excluding certain paths.

        The manifest provides a comprehensive list of files and their hashes for verification.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            manifest = Manifest.from_dir(
                self.root_dir, exclude_paths=[self.manifest_path, self.log_path], progress=progress
            )
            if not self.dry_run:
                manifest.save(self.manifest_path)
            self.logger.debug(f"Generated manifest at {self.manifest_path}")

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
        return self._root_dir / "dataset.log"

    @property
    def logs_dir(self) -> Path:
        """
        The path to the logs directory.
        """
        return self._root_dir / "logs"

    @property
    def pipelines_dir(self) -> Path:
        """
        The path to the pipelines directory.
        """
        return self._root_dir / "pipelines"

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
    def check_dataset_mapping(
        dataset_mapping: Dict[str, Dict[Path, Tuple[Path, Optional[List[Any]], Optional[Dict[str, Any]]]]]
    ) -> None:
        """
        Verify that the given dataset mapping is valid.

        Args:
            dataset_mapping: A mapping from source paths to destination paths and metadata.

        Raises:
            DatasetWrapper.InvalidDatasetMappingError: If the path mapping is invalid.
        """
        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            DatasetWrapper._verify_source_paths_exist(pipeline_data_mapping)
            DatasetWrapper._verify_unique_source_resolutions(pipeline_data_mapping)
            DatasetWrapper._verify_relative_destination_paths(pipeline_data_mapping)
            DatasetWrapper._verify_no_destination_collisions(pipeline_data_mapping)

    @staticmethod
    def _verify_source_paths_exist(
        pipeline_data_mapping: Dict[Path, Tuple[Path, Optional[List[Any]], Optional[Dict[str, Any]]]]
    ) -> None:
        for src in pipeline_data_mapping:
            if not src.exists():
                raise DatasetWrapper.InvalidDatasetMappingError(f"Source path {src} does not exist.")

    @staticmethod
    def _verify_unique_source_resolutions(
        pipeline_data_mapping: Dict[Path, Tuple[Path, Optional[List[Any]], Optional[Dict[str, Any]]]]
    ) -> None:
        reverse_src_resolution: Dict[Path, Path] = {}
        for src in pipeline_data_mapping:
            resolved = src.resolve().absolute()
            if resolved in reverse_src_resolution:
                raise DatasetWrapper.InvalidDatasetMappingError(
                    f"Source paths {src} and {reverse_src_resolution[resolved]} both resolve to {resolved}."
                )
            reverse_src_resolution[resolved] = src

    @staticmethod
    def _verify_relative_destination_paths(
        pipeline_data_mapping: Dict[Path, Tuple[Path, Optional[List[Any]], Optional[Dict[str, Any]]]]
    ) -> None:
        for dst, _, _ in pipeline_data_mapping.values():
            if dst.is_absolute():
                raise DatasetWrapper.InvalidDatasetMappingError(f"Destination path {dst} must be relative.")

    @staticmethod
    def _verify_no_destination_collisions(
        pipeline_data_mapping: Dict[Path, Tuple[Path, Optional[List[Any]], Optional[Dict[str, Any]]]]
    ) -> None:
        reverse_mapping: Dict[Path, Path] = {
            dst.resolve(): src for src, (dst, _, _) in pipeline_data_mapping.items() if dst is not None
        }
        for src, (dst, _, _) in pipeline_data_mapping.items():
            if dst is not None:
                src_other = reverse_mapping.get(dst.resolve())
                if src_other is not None and src.resolve() != src_other.resolve():
                    raise DatasetWrapper.InvalidDatasetMappingError(
                        f"Resolved destination path {dst.resolve()} is the same for source paths {src} and {src_other}."
                    )
