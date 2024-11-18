"""
Marimba Core Dataset Wrapper Module.

This module provides various utilities and classes for handling image datasets in the Marimba project. It includes
functionality for managing datasets, creating and validating manifests, summarizing imagery collections, and applying
EXIF metadata.

Imports:
    - hashlib: Provides hash functions for generating hashes.
    - io: Core tools for working with streams.
    - json: JSON encoding and decoding library.
    - logging: Logging facility for Python.
    - collections.OrderedDict: Dictionary that remembers the order entries were added.
    - dataclasses.dataclass: A decorator for generating special methods.
    - datetime.timezone: Timezone information objects.
    - fractions.Fraction: Rational number arithmetic.
    - pathlib.Path: Object-oriented filesystem paths.
    - shutil: High-level file operations.
    - textwrap.dedent: Remove any common leading whitespace from every line.
    - typing: Type hints for function signatures and variables.
    - uuid: Generate unique identifiers.
    - piexif: Library to insert and extract EXIF metadata from images.
    - ifdo.models: Data models for images.
    - PIL.Image: Python Imaging Library for opening, manipulating, and saving image files.
    - rich.progress: Utilities for creating progress bars.
    - marimba.core.utils.log: Utilities for logging.
    - marimba.core.utils.map: Utility for creating summary maps.
    - marimba.core.utils.rich: Utility for default columns in rich progress.
    - marimba.lib.image: Library for image processing.
    - marimba.lib.gps: Utility for GPS coordinate conversion.

Classes:
    - ImagerySummary: A summary of an imagery collection.
    - Manifest: A dataset manifest used to validate datasets for corruption or modification.
    - DatasetWrapper: A wrapper class for handling dataset directories.
"""

import hashlib
import io
import json
import os
from collections import OrderedDict
from collections.abc import Iterable
from datetime import timezone
from fractions import Fraction
from math import isnan
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from typing import Any
from uuid import uuid4

import piexif
from ifdo.models import ImageData, ImageSetHeader, iFDO
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.utils.constants import Operation
from marimba.core.utils.log import LogMixin, get_file_handler
from marimba.core.utils.manifest import Manifest
from marimba.core.utils.map import make_summary_map
from marimba.core.utils.rich import get_default_columns
from marimba.core.utils.summary import ImagerySummary
from marimba.lib import image
from marimba.lib.decorators import multithreaded
from marimba.lib.gps import convert_degrees_to_gps_coordinate


class DatasetWrapper(LogMixin):
    """
    Dataset directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the dataset directory structure is invalid.
        """

    class InvalidDatasetMappingError(Exception):
        """
        Raised when a path mapping dictionary is invalid.
        """

    class ManifestError(Exception):
        """
        Raised when the dataset is inconsistent with its manifest.
        """

    def __init__(
        self,
        root_dir: str | Path,
        version: str | None = "1.0",
        contact_name: str | None = None,
        contact_email: str | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize a new instance of the class.

        This method sets up a new instance with the provided parameters, initializing internal attributes for file
        processing, version control, and contact information. It also sets up a dry-run mode for testing purposes
        without making actual changes.

        Args:
            root_dir (Union[str, Path]): The root directory where the files will be processed.
            version (Optional[str]): The version number. Defaults to "1.0".
            contact_name (Optional[str]): The name of the contact person. Defaults to None.
            contact_email (Optional[str]): The email address of the contact person. Defaults to None.
            dry_run (bool): If True, the method runs in dry-run mode without making any changes. Defaults to False.

        Returns:
            None
        """
        self._root_dir = Path(root_dir)
        self._version = version
        self._contact_name = contact_name
        self._contact_email = contact_email
        self._dry_run = dry_run

        self._metadata_name = "ifdo.yml"
        self._summary_name = "summary.md"

        if not dry_run:
            self._check_file_structure()
            self._setup_logging()

    @classmethod
    def create(
        cls,
        root_dir: str | Path,
        version: str | None = "1.0",
        contact_name: str | None = None,
        contact_email: str | None = None,
        *,
        dry_run: bool = False,
    ) -> "DatasetWrapper":
        """
        Create a new dataset from an iFDO.

        This class method creates a new dataset structure based on the provided parameters. It sets up the necessary
        directory structure and returns a DatasetWrapper instance.

        Args:
            root_dir: The root directory where the dataset will be created.
            version: The version of the dataset. Defaults to '1.0'.
            contact_name: The name of the contact person for the dataset. Optional.
            contact_email: The email of the contact person for the dataset. Optional.
            dry_run: If True, simulates the creation without actually creating directories. Defaults to False.

        Returns:
            A DatasetWrapper instance representing the newly created dataset.

        Raises:
            - FileExistsError: If the root directory already exists and dry_run is False.
        """
        # Define the dataset directory structure
        root_dir = Path(root_dir)
        data_dir = root_dir / "data"
        logs_dir = root_dir / "logs"
        pipeline_logs_dir = logs_dir / "pipelines"

        # Create the file structure
        if not dry_run:
            root_dir.mkdir(parents=True)
            data_dir.mkdir()
            logs_dir.mkdir()
            pipeline_logs_dir.mkdir()

        return cls(root_dir, version=version, contact_name=contact_name, contact_email=contact_email, dry_run=dry_run)

    def _check_file_structure(self) -> None:
        """
        Check the file structure of the dataset.

        Parameters:
            self: the instance of the class

        Raises:
            InvalidStructureError: if any of the required directories do not exist or is not a directory

        Returns:
            None
        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise DatasetWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.data_dir)
        check_dir_exists(self.logs_dir)
        check_dir_exists(self.pipeline_logs_dir)

    def validate(self, dataset_name: str, progress: Progress | None = None, task: TaskID | None = None) -> None:
        """
        Validate the dataset. If the dataset is inconsistent with its manifest (if present), raise a ManifestError.

        Raises:
            DatasetWrapper.ManifestError: If the dataset is inconsistent with its manifest.
        """
        if self.manifest_path.exists():
            manifest = Manifest.load(self.manifest_path)
            if not manifest.validate(
                self.root_dir,
                exclude_paths=[self.manifest_path, self.log_path],
                progress=progress,
                task=task,
            ):
                raise DatasetWrapper.ManifestError(self.manifest_path)
            self.logger.debug(f'Packaged dataset "{dataset_name}" has been successfully validated against the manifest')

    def _setup_logging(self) -> None:
        """
        Set up logging. Create file handler for this instance that writes to `dataset.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.logs_dir, "dataset", self._dry_run)

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

    def _apply_ifdo_exif_tags(self, metadata_mapping: dict[Path, tuple[ImageData, dict[str, Any] | None]]) -> None:
        """
        Apply metadata from iFDO to the provided paths.

        Args:
            metadata_mapping: A dict mapping file paths to image data.
        """

        @multithreaded()
        def process_file(
            self: DatasetWrapper,
            thread_num: str,
            item: tuple[Path, tuple[ImageData, dict[str, Any] | None]],
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            path, (image_data, ancillary_data) = item
            if path.suffix.lower() in {".jpg", ".jpeg", ".png"}:
                self._process_image_file(path, image_data, ancillary_data, thread_num)

            if progress and task is not None:
                progress.advance(task)

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Applying iFDO metadata to files (4/11)", total=len(metadata_mapping))
            process_file(self, items=metadata_mapping.items(), progress=progress, task=task)  # type: ignore[call-arg]

    def _process_image_file(
        self,
        path: Path,
        image_data: ImageData,
        ancillary_data: dict[str, Any] | None,
        thread_num: str,
    ) -> None:
        try:
            exif_dict = piexif.load(str(path))
        except piexif.InvalidImageDataError as e:
            self.logger.warning(f"Failed to load EXIF metadata from {path}: {e}")
            return

        self._inject_datetime(image_data, exif_dict)
        self._inject_gps_coordinates(image_data, exif_dict)
        image_file = self._add_thumbnail(path, exif_dict)
        self._extract_image_properties(image_file, image_data)
        self._burn_in_exif_metadata(image_data, ancillary_data, exif_dict)

        try:
            exif_bytes = piexif.dump(exif_dict)
            piexif.insert(exif_bytes, str(path))
            self.logger.debug(f"Thread {thread_num} | Applied iFDO metadata to EXIF tags for image {path}")
        except piexif.InvalidImageDataError:
            self.logger.warning(f"Failed to write EXIF metadata to {path}")

    def _prepare_metadata(self, image_data: ImageData, ancillary_data: dict[str, Any] | None) -> dict[str, str]:
        metadata = {}

        if image_data.image_datetime is not None:
            dt = image_data.image_datetime
            metadata["CreateDate"] = dt.strftime("%Y:%m:%d %H:%M:%S")
            metadata["ModifyDate"] = dt.strftime("%Y:%m:%d %H:%M:%S")

        if image_data.image_latitude is not None and image_data.image_longitude is not None:
            metadata["GPSLatitude"] = image_data.image_latitude
            metadata["GPSLongitude"] = image_data.image_longitude
            metadata["GPSLatitudeRef"] = "N" if image_data.image_latitude > 0 else "S"
            metadata["GPSLongitudeRef"] = "E" if image_data.image_longitude > 0 else "W"

        image_data_dict = image_data.to_dict()
        user_comment_data = {"metadata": {"ifdo": image_data_dict, "ancillary": ancillary_data}}
        user_comment_json = json.dumps(user_comment_data)
        metadata["UserComment"] = user_comment_json

        return metadata

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
            ifd_gps[piexif.GPSIFD.GPSLatitude] = ((d_lat, 1), (m_lat, 1), (s_lat, 1))
            ifd_gps[piexif.GPSIFD.GPSLatitudeRef] = "N" if image_data.image_latitude > 0 else "S"
        if image_data.image_longitude is not None:
            d_lon, m_lon, s_lon = convert_degrees_to_gps_coordinate(image_data.image_longitude)
            ifd_gps[piexif.GPSIFD.GPSLongitude] = ((d_lon, 1), (m_lon, 1), (s_lon, 1))
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
    def _burn_in_exif_metadata(
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

    def populate(
        self,
        dataset_name: str,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]]],
        project_pipelines_dir: Path,
        project_log_path: Path,
        pipeline_log_paths: Iterable[Path],
        operation: Operation = Operation.copy,
        zoom: int | None = None,
    ) -> None:
        """
        Populate the dataset with files from the given dataset mapping.

        This function creates a new dataset by populating it with files from the provided dataset mapping. It performs
        various operations such as copying or moving files, applying EXIF metadata, generating IFDO, dataset summary,
        dataset map, copying pipelines and logs, and generating a manifest.

        Args:
            dataset_name: The name of the dataset to be created.
            dataset_mapping: A dictionary mapping pipeline names to output file paths and their corresponding input
              file paths, image data, and additional information.
            project_pipelines_dir: The path to the project pipelines directory.
            project_log_path: The path to the project log file.
            pipeline_log_paths: An iterable of paths to the pipeline log files.
            operation: The operation to perform on files (copy, move or link). Defaults to Operation.copy.
            zoom: The zoom level for generating the dataset map. Defaults to None.

        Returns:
            None

        Raises:
            - DatasetWrapper.InvalidPathMappingError: If the provided dataset mapping is invalid.
        """
        pipeline_label = "pipeline" if len(dataset_mapping) == 1 else "pipelines"
        self.logger.debug(f'Creating dataset "{dataset_name}" containing {len(dataset_mapping)} {pipeline_label}')

        self.check_dataset_mapping(dataset_mapping)
        image_set_items = self._populate_files(dataset_mapping, operation)
        self._apply_exif_metadata(dataset_mapping)
        self.generate_ifdo(dataset_name, image_set_items)
        self.generate_dataset_summary(image_set_items)
        self._generate_dataset_map(image_set_items, zoom)
        self._copy_pipelines(project_pipelines_dir)
        self._copy_logs(project_log_path, pipeline_log_paths)
        self._generate_manifest(image_set_items)

    def _populate_files(
        self,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]]],
        operation: Operation,
    ) -> dict[str, ImageData]:
        """
        Copy or move files from the dataset mapping to the destination directory.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
            operation: The operation to perform (copy, move, link).

        Returns:
            Dict[str, ImageData]: A dictionary of image set items for further processing.
        """

        @multithreaded()
        def process_file(
            self: DatasetWrapper,
            item: tuple[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]],
            thread_num: str,
            pipeline_name: str,
            operation: Operation,
            image_set_items: dict[str, list[ImageData]],
            progress: Progress | None = None,
            tasks_by_pipeline_name: dict[str, Any] | None = None,
        ) -> None:
            src, (relative_dst, image_data_list, _) = item
            dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst

            if image_data_list:
                dst_relative = dst.relative_to(self.data_dir)
                image_set_items[dst_relative.as_posix()] = image_data_list

            if not self.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if operation == Operation.copy:
                    copy2(src, dst)
                    self.logger.debug(f"Thread {thread_num} | Copying file {src.absolute()} -> {dst}")
                elif operation == Operation.move:
                    src.rename(dst)
                    self.logger.debug(f"Thread {thread_num} | Moving file {src.absolute()} -> {dst}")
                elif operation == Operation.link:
                    os.link(src, dst)
                    self.logger.debug(f"Thread {thread_num} | Linking file {src.absolute()} -> {dst}")

            if progress and tasks_by_pipeline_name:
                progress.advance(tasks_by_pipeline_name[pipeline_name])

        image_set_items: dict[str, list[ImageData]] = {}
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                pipeline_name: progress.add_task(
                    f"[green]Populating data for {pipeline_name} pipeline (3/11)",
                    total=len(pipeline_data_mapping),
                )
                for pipeline_name, pipeline_data_mapping in dataset_mapping.items()
                if len(pipeline_data_mapping) > 0
            }

            for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
                self.logger.debug(f"Populating data for {pipeline_name} pipeline")
                process_file(
                    self,
                    items=list(pipeline_data_mapping.items()),
                    pipeline_name=pipeline_name,
                    operation=operation,
                    image_set_items=image_set_items,
                    progress=progress,
                    tasks_by_pipeline_name=tasks_by_pipeline_name,
                )  # type: ignore[call-arg]

        return image_set_items

    def _apply_exif_metadata(
        self,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]]],
    ) -> None:
        """
        Apply EXIF metadata to the images in the dataset.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
        """
        metadata_mapping = {}

        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            for relative_dst, image_data_list, ancillary_data in pipeline_data_mapping.values():
                dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst
                if not image_data_list:
                    continue
                metadata_mapping[dst] = (image_data_list[0], ancillary_data)

        if not self.dry_run:
            self._apply_ifdo_exif_tags(metadata_mapping)
        self.logger.debug(f"Applied iFDO EXIF tags to {len(metadata_mapping)} files")

    def generate_ifdo(
        self,
        dataset_name: str,
        image_set_items: dict[str, ImageData],
        *,
        progress: bool = True,
    ) -> None:
        """
        Generate the iFDO for the dataset.

        This function creates an iFDO metadata file for a given dataset. It processes the provided image set items,
        calculates SHA256 hashes for each image, and generates an iFDO object. The function can optionally display
        a progress bar during execution.

        Args:
            dataset_name: A string representing the name of the dataset.
            image_set_items: A dictionary mapping image paths to ImageData objects containing metadata for each image.
            progress: A boolean indicating whether to display a progress bar during execution. Defaults to True.

        Returns:
            None

        Raises:
            - IOError: If there are issues reading image files or writing the iFDO to disk.
            - ValueError: If the input data is invalid or missing required information.
        """

        @multithreaded()
        def hash_image(
            self: DatasetWrapper,
            thread_num: str,  # noqa: ARG001
            item: tuple[str, ImageData],
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            image_path, image_data = item
            image_data_path = Path(self.data_dir) / image_path
            file_hash = hashlib.sha256()
            if image_data_path.is_file():
                with image_data_path.open("rb") as f:
                    while True:
                        chunk = f.read(4096)
                        if not chunk:
                            break
                        file_hash.update(chunk)
                for image_data_item in image_data:
                    image_data_item.image_hash_sha256 = file_hash.hexdigest()

            if progress and task is not None:
                progress.advance(task)

        def _process_items(
            image_set_items: dict[str, ImageData],
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> (dict)[str, ImageData]:
            items = [
                (Path(self.data_dir) / image_path, image_data) for image_path, image_data in image_set_items.items()
            ]
            hash_image(self, items=items, progress=progress, task=task)  # type: ignore[call-arg]
            return OrderedDict(sorted(image_set_items.items(), key=lambda item: item[0]))

        if progress:
            with Progress(SpinnerColumn(), *get_default_columns()) as progress_bar:
                task = progress_bar.add_task("[green]Generating iFDO (5/11)", total=len(image_set_items) + 1)
                image_set_items = _process_items(image_set_items, progress_bar, task)
                progress_bar.update(task, description="[green]Writing iFDO to disk (5/11)")
                self._create_ifdo(dataset_name, image_set_items)
                progress_bar.advance(task)  # Advance after creating the iFDO
        else:
            image_set_items = _process_items(image_set_items)
            self._create_ifdo(dataset_name, image_set_items)

        item_count = len(image_set_items)
        item_label = "item" if item_count == 1 else "items"
        self.logger.debug(f"Generated iFDO containing {item_count} image set {item_label} at {self.metadata_path}")

    def _create_ifdo(self, dataset_name: str, image_set_items: dict[str, ImageData]) -> iFDO:
        if not self.dry_run:
            ifdo = iFDO(
                image_set_header=ImageSetHeader(
                    image_set_name=dataset_name,
                    image_set_uuid=str(uuid4()),
                    image_set_handle="",  # TODO @<cjackett>: Populate this from the distribution target URL
                ),
                image_set_items=image_set_items,
            )
            ifdo.save(self.metadata_path)

    def generate_dataset_summary(
        self,
        image_set_items: dict[str, ImageData],
        *,
        progress: bool = True,
    ) -> None:
        """
        Generate a summary of the dataset.

        Args:
            image_set_items: The dictionary of image set items to summarize.
            progress: A flag to indicate whether to show a progress bar.
        """

        def generate_summary() -> None:
            summary = self.summarise(image_set_items)
            if not self.dry_run:
                self.summary_path.write_text(str(summary))
            self.logger.debug(f"Generated dataset summary at {self.summary_path}")

        if progress:
            with Progress(SpinnerColumn(), *get_default_columns()) as progress_bar:
                task = progress_bar.add_task("[green]Generating dataset summary (6/11)", total=1)
                generate_summary()
                progress_bar.advance(task)
        else:
            generate_summary()

    @staticmethod
    def _is_valid_coordinate(value: float | None, min_value: float, max_value: float) -> bool:
        """
        Validate if a coordinate is a valid real number within the given range.

        Args:
            value: The coordinate value to validate, which can be None or a float.
            min_value: The minimum acceptable value for the coordinate.
            max_value: The maximum acceptable value for the coordinate.

        Returns:
            bool: True if the value is within the specified range and is not NaN, otherwise False.
        """
        return value is not None and min_value <= value <= max_value and not isnan(value)

    def _validate_geolocations(self, lat: float | None, lon: float | None) -> bool:
        """
        Validate latitude and longitude values to ensure they are within acceptable ranges.

        Latitude must be within the range [-90, 90]. Longitude can either be within
        the range [-180, 180] or within [0, 360] to accommodate different dataset formats.

        Args:
            lat: Latitude value to validate.
            lon: Longitude value to validate.

        Returns:
            bool: True if both latitude and longitude are valid real numbers within their respective ranges, otherwise
            False.
        """
        valid_latitude = self._is_valid_coordinate(lat, -90.0, 90.0)
        valid_longitude = self._is_valid_coordinate(lon, -180.0, 180.0) or self._is_valid_coordinate(lon, 0.0, 360.0)
        return valid_latitude and valid_longitude

    def _generate_dataset_map(self, image_set_items: dict[str, ImageData], zoom: int | None = None) -> None:
        """
        Generate a summary of the dataset, including a map of geolocations if available.

        Args:
            image_set_items: The dictionary of image set items to summarize.
            zoom: Optional zoom level for the map.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Generating dataset map (7/11)", total=1)

            # Check for geolocations
            geolocations = [
                (image_data.image_latitude, image_data.image_longitude)
                for image_data_list in image_set_items.values()
                for image_data in image_data_list
                if self._validate_geolocations(image_data.image_latitude, image_data.image_longitude)
            ]
            if geolocations:
                summary_map = make_summary_map(geolocations, zoom=zoom)
                if summary_map is not None:
                    map_path = self.root_dir / "map.png"
                    if not self.dry_run:
                        summary_map.save(map_path)
                    coordinate_label = "spatial coordinate" if len(geolocations) == 1 else "spatial coordinates"
                    self.logger.debug(
                        f"Generated summary map containing {len(geolocations)} {coordinate_label} at {map_path}",
                    )
            progress.advance(task)

    def _copy_logs(self, project_log_path: Path, pipeline_log_paths: Iterable[Path]) -> None:
        """
        Copy project and pipeline log files to the appropriate directories.

        Args:
            project_log_path: The path to the project log file.
            pipeline_log_paths: The paths to the pipeline log files.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Copying logs (9/11)", total=1)
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
            task = progress.add_task("[green]Copying pipelines (8/11)", total=1)
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

    def _generate_manifest(self, image_set_items: dict[str, ImageData]) -> None:
        """
        Generate and save the manifest for the dataset, excluding certain paths.

        The manifest provides a comprehensive list of files and their hashes for verification.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            globbed_files = list(self.root_dir.glob("**/*"))
            task = progress.add_task("[green]Generating manifest (10/11)", total=len(globbed_files))
            manifest = Manifest.from_dir(
                self.root_dir,
                exclude_paths=[self.manifest_path, self.log_path],
                image_set_items=image_set_items,
                progress=progress,
                task=task,
            )
            if not self.dry_run:
                manifest.save(self.manifest_path)
            self.logger.debug(f"Generated manifest for {len(globbed_files)} files and paths at {self.manifest_path}")

    def summarise(self, image_set_items: dict[str, ImageData]) -> ImagerySummary:
        """
        Create an imagery summary for this dataset.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_dataset(self, image_set_items)

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
        The path to the iFDO metadata file.
        """
        return self._root_dir / self.metadata_name

    @property
    def metadata_name(self) -> str:
        """
        The name of the iFDO metadata file.
        """
        return self._metadata_name

    @metadata_name.setter
    def metadata_name(self, filename: str) -> None:
        """
        Setter for the name of the dataset metadata.

        Args:
            filename (str): The new filename for the metadata.
        """
        if filename:
            self._metadata_name = filename if filename.endswith(".ifdo.yml") else f"{filename}.ifdo.yml"
        else:
            self._metadata_name = "ifdo.yml"

    @property
    def summary_path(self) -> Path:
        """
        The path to the dataset summary.
        """
        return self._root_dir / self.summary_name

    @property
    def summary_name(self) -> str:
        """
        The name of the dataset summary file.
        """
        return self._summary_name

    @summary_name.setter
    def summary_name(self, filename: str) -> None:
        """
        Setter for the name of the dataset summary.

        Args:
            filename (str): The new filename for the summary.
        """
        if filename:
            self._summary_name = filename if filename.endswith(".summary.md") else f"{filename}.summary.md"
        else:
            self._summary_name = "summary.md"

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
    def logs_dir(self) -> Path:
        """
        The path to the logs directory.
        """
        return self._root_dir / "logs"

    @property
    def log_path(self) -> Path:
        """
        The path to the dataset log file.
        """
        return self.logs_dir / "dataset.log"

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
    def version(self) -> str | None:
        """
        Version of the dataset.
        """
        return self._version

    @property
    def contact_name(self) -> str | None:
        """
        Full name of the contact person for the packaged dataset.
        """
        return self._contact_name

    @property
    def contact_email(self) -> str | None:
        """
        Email address of the contact person for the packaged dataset.
        """
        return self._contact_email

    @property
    def dry_run(self) -> bool:
        """
        Whether the dataset generation should run in dry-run mode.
        """
        return self._dry_run

    @dry_run.setter
    def dry_run(self, value: bool) -> None:
        """
        Set the dry-run mode for the dataset generation.
        """
        self._dry_run = value

    def check_dataset_mapping(
        self,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]],
    ) -> None:
        """
        Verify that the given dataset mapping is valid.

        Args:
            dataset_mapping: A mapping from source paths to destination paths and metadata.

        Raises:
            DatasetWrapper.InvalidDatasetMappingError: If the path mapping is invalid.
        """
        total_tasks = 0
        for pipeline_data_mapping in dataset_mapping.values():
            total_tasks += len(pipeline_data_mapping) * 4

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Checking dataset mapping (2/11)", total=total_tasks)

            for pipeline_data_mapping in dataset_mapping.values():
                self._verify_source_paths_exist(pipeline_data_mapping, progress, task)
                self._verify_unique_source_resolutions(pipeline_data_mapping, progress, task)
                self._verify_relative_destination_paths(pipeline_data_mapping, progress, task)
                self._verify_no_destination_collisions(pipeline_data_mapping, progress, task)

        self.logger.debug("Dataset mapping is valid")

    def _verify_source_paths_exist(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
    ) -> None:
        @multithreaded()
        def verify_path(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            if not item.exists():
                raise DatasetWrapper.InvalidDatasetMappingError(f"Source path {item} does not exist")
            if progress and task is not None:
                progress.advance(task)

        verify_path(
            self,
            items=list(pipeline_data_mapping.keys()),
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_unique_source_resolutions(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
    ) -> None:
        reverse_src_resolution: dict[Path, Path] = {}

        @multithreaded()
        def verify_resolution(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            reverse_src_resolution: dict[Path, Path],
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            resolved = item.resolve().absolute()
            if resolved in reverse_src_resolution:
                raise DatasetWrapper.InvalidDatasetMappingError(
                    f"Source paths {item} and {reverse_src_resolution[resolved]} both resolve to {resolved}",
                )
            reverse_src_resolution[resolved] = item
            if progress and task is not None:
                progress.advance(task)

        verify_resolution(
            self,
            items=pipeline_data_mapping.keys(),
            reverse_src_resolution=reverse_src_resolution,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_relative_destination_paths(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
    ) -> None:
        destinations = [dst for dst, _, _ in pipeline_data_mapping.values()]

        @multithreaded()
        def verify_destination_path(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            if item.is_absolute():
                raise DatasetWrapper.InvalidDatasetMappingError(f"Destination path {item} must be relative")
            if progress and task is not None:
                progress.advance(task)

        verify_destination_path(
            self,
            items=destinations,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_no_destination_collisions(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
    ) -> None:
        reverse_mapping: dict[Path, Path] = {
            dst.resolve(): src for src, (dst, _, _) in pipeline_data_mapping.items() if dst is not None
        }

        @multithreaded()
        def verify_no_collision(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: tuple[Path, Path],
            reverse_mapping: dict[Path, Path],
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            (src, dst) = item
            if dst is not None:
                src_other = reverse_mapping.get(dst.resolve())
                if src_other is not None and src.resolve() != src_other.resolve():
                    raise DatasetWrapper.InvalidDatasetMappingError(
                        f"Resolved destination path {dst.resolve()} is the same for source paths {src} and {src_other}",
                    )
            if progress and task is not None:
                progress.advance(task)

        items = [(src, dst) for src, (dst, _, _) in pipeline_data_mapping.items()]

        verify_no_collision(
            self,
            items=items,
            reverse_mapping=reverse_mapping,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]
