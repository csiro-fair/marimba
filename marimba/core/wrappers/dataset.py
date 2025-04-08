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

import logging
import os
from collections import OrderedDict, defaultdict
from collections.abc import Callable, Iterable
from math import isnan
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from typing import Any

from rich.progress import Progress, SpinnerColumn, TaskID

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import Operation
from marimba.core.utils.dataset import (
    DATASET_MAPPING_TYPE,
    DECORATOR_TYPE,
    MAPPED_DATASET_ITEMS,
    execute_on_mapping,
    flatten_mapping,
    flatten_middle_mapping,
)
from marimba.core.utils.hash import compute_hash
from marimba.core.utils.log import LogMixin, get_file_handler, get_logger
from marimba.core.utils.manifest import Manifest
from marimba.core.utils.map import make_summary_map
from marimba.core.utils.paths import format_path_for_logging
from marimba.core.utils.rich import get_default_columns
from marimba.core.utils.summary import ImagerySummary
from marimba.lib.decorators import multithreaded


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
        metadata_saver_overwrite: Callable[[Path, str, dict[str, Any]], None] | None = None,
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
            metadata_saver_overwrite (Optional[Callable[[Path, str, dict[str, Any]], None]]): Saving function
                overwriting the default metadata saving function. Defaults to None.
        """
        self._root_dir = Path(root_dir)
        self._project_dir = self._root_dir.parent.parent
        self._version = version
        self._contact_name = contact_name
        self._contact_email = contact_email
        self._dry_run = dry_run
        self._summary_name = "summary.md"
        self._metadata_saver_overwrite = metadata_saver_overwrite

        if not dry_run:
            self._check_file_structure()
            self._setup_logging()

    @property
    def logger(self) -> logging.Logger:
        """Returns a clean logger instance for this dataset."""
        if not hasattr(self, "_logger"):
            # Get the base logger
            self._logger = get_logger(self.__class__.__name__)

            # Remove any existing handlers
            for handler in self._logger.handlers[:]:
                self._logger.removeHandler(handler)

            # Add back just the null handler
            self._logger.addHandler(logging.NullHandler())
        return self._logger

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

    @classmethod
    def create(
        cls,
        root_dir: str | Path,
        version: str | None = "1.0",
        contact_name: str | None = None,
        contact_email: str | None = None,
        *,
        dry_run: bool = False,
        metadata_saver: Callable[[Path, str, dict[str, Any]], None] | None = None,
    ) -> "DatasetWrapper":
        """
        Create a new dataset.

        This class method creates a new dataset structure based on the provided parameters. It sets up the necessary
        directory structure and returns a DatasetWrapper instance.

        Args:
            root_dir: The root directory where the dataset will be created.
            version: The version of the dataset. Defaults to '1.0'.
            contact_name: The name of the contact person for the dataset. Optional.
            contact_email: The email of the contact person for the dataset. Optional.
            dry_run: If True, simulates the creation without actually creating directories. Defaults to False.
            metadata_saver: Save function which takes a path, filename without extension and data as
                json serializable dict. Defaults to None.

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

        return cls(
            root_dir,
            version=version,
            contact_name=contact_name,
            contact_email=contact_email,
            dry_run=dry_run,
            metadata_saver_overwrite=metadata_saver,
        )

    def _check_file_structure(self) -> None:
        """
        Check the file structure of the dataset.

        Parameters:
            self: the instance of the class

        Raises:
            InvalidStructureError: if any of the required directories do not exist or is not a directory
        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise DatasetWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.data_dir)
        check_dir_exists(self.logs_dir)
        check_dir_exists(self.pipeline_logs_dir)

    def validate(self, progress: Progress | None = None, task: TaskID | None = None) -> None:
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
                logger=self.logger,
            ):
                raise DatasetWrapper.ManifestError(self.manifest_path)

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

    def populate(
        self,
        dataset_name: str,
        dataset_mapping: DATASET_MAPPING_TYPE,
        project_pipelines_dir: Path,
        project_log_path: Path,
        pipeline_log_paths: Iterable[Path],
        mapping_processor_decorator: list[DECORATOR_TYPE],
        post_package_processors: list[Callable[[Path], set[Path]]],
        operation: Operation = Operation.copy,
        zoom: int | None = None,
        max_workers: int | None = None,
    ) -> None:
        """

        Populate the dataset with files, metadata, and generate necessary artifacts.

        This function populates a dataset with files from multiple pipelines, processes metadata, generates dataset
        summary and map, copies pipeline files and logs, and creates a manifest. It handles various operations like
        copying or moving files and can generate a dataset map at a specified zoom l

        Args:
            dataset_name: The name of the dataset to be created.
            dataset_mapping: A dictionary mapping pipeline names to file information, including source and destination
                paths, metadata, and additional properties.
            project_pipelines_dir: A Path object pointing to the directory containing project pipeline files.
            project_log_path: A Path object pointing to the project log file.
            pipeline_log_paths: An iterable of Path objects pointing to individual pipeline log files.
            mapping_processor_decorator: Dataset mapping processor decorator.
            post_package_processors: Processors which are applied to the dataset after the metadata file is created.
            operation: An Operation enum specifying whether to copy or move files (default: Operation.copy).
            zoom: An optional integer specifying the zoom level for the dataset map generation (default: None).
            max_workers: Maximum number of worker processes to use. If None, uses all available CPU cores.


        Raises:
            ValueError: If the dataset_mapping is empty or contains invalid entries.
            IOError: If there are issues reading from or writing to files or directories.
            MetadataError: If there are problems processing or generating metadata.
        """
        pipeline_label = "pipeline" if len(dataset_mapping) == 1 else "pipelines"
        self.logger.info(
            f'Started packaging dataset "{dataset_name}" containing {len(dataset_mapping)} {pipeline_label}',
        )

        reduced_dataset_mapping = flatten_middle_mapping(dataset_mapping)
        self.check_dataset_mapping(reduced_dataset_mapping, max_workers)
        mapped_dataset_items = self._populate_files(dataset_mapping, operation, max_workers)
        self._process_files_with_metadata(reduced_dataset_mapping, max_workers)
        self.generate_metadata(dataset_name, mapped_dataset_items, mapping_processor_decorator, max_workers)
        dataset_items = flatten_mapping(flatten_middle_mapping(mapped_dataset_items))

        self.generate_dataset_summary(dataset_items)
        # TODO @<cjackett>: Generate summary method currently does not use multithreading
        self._generate_dataset_map(dataset_items, zoom)
        self._copy_pipelines(project_pipelines_dir)
        self._copy_logs(project_log_path, pipeline_log_paths)
        self._generate_manifest(dataset_items, max_workers)

        self.logger.info(f'Completed packaging dataset "{dataset_name}"')

        changed_files = self._run_post_package_processors(post_package_processors)
        self._update_manifest(changed_files, max_workers)

    def _populate_files(  # noqa: C901
        self,
        dataset_mapping: DATASET_MAPPING_TYPE,
        operation: Operation,
        max_workers: int | None = None,
    ) -> dict[str, dict[str, dict[str, list[BaseMetadata]]]]:
        """
        Copy or move files from the dataset mapping to the destination directory.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
            operation: The operation to perform (copy, move, link).
            max_workers: Maximum number of worker processes to use. If None, uses all available CPU cores.

        Returns:
            Dict[str, BaseMetadata]: A dictionary of dataset items for further processing.
        """

        @multithreaded(max_workers=max_workers)
        def process_file(
            self: DatasetWrapper,
            item: tuple[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]],
            thread_num: str,
            pipeline_name: str,
            operation: Operation,
            dataset_items: dict[str, list[BaseMetadata]],
            logger: logging.Logger | None = None,
            progress: Progress | None = None,
            tasks_by_pipeline_name: dict[str, Any] | None = None,
        ) -> None:
            src, (relative_dst, data_list, _) = item
            dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst

            if data_list:
                dst_relative = dst.relative_to(self.root_dir)
                dataset_items[dst_relative.as_posix()] = data_list

            if not self.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if operation == Operation.copy:
                    copy2(src, dst)
                    if logger:
                        logger.debug(
                            f"Thread {thread_num} - Copied file "
                            f"{format_path_for_logging(src, self._project_dir)} to "
                            f"{format_path_for_logging(dst, self._project_dir)}",
                        )
                elif operation == Operation.move:
                    src.rename(dst)
                    if logger:
                        logger.debug(
                            f"Thread {thread_num} - Moved file "
                            f"{format_path_for_logging(src, self._project_dir)} to "
                            f"{format_path_for_logging(dst, self._project_dir)}",
                        )
                # TODO @<cjackett>: We might need to check here that image files aren't linked to linked files in the
                #  import process because then EXIF writing might destructively change the original files
                elif operation == Operation.link:
                    os.link(src, dst)
                    if logger:
                        logger.debug(
                            f"Thread {thread_num} - Linked file "
                            f"{format_path_for_logging(src, self._project_dir)} to "
                            f"{format_path_for_logging(dst, self._project_dir)}",
                        )

            if progress and tasks_by_pipeline_name:
                progress.advance(tasks_by_pipeline_name[pipeline_name])

        dataset_items: dict[str, dict[str, dict[str, list[BaseMetadata]]]] = defaultdict(lambda: defaultdict(dict))
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                pipeline_name: progress.add_task(
                    f"[green]Populating data for {pipeline_name} pipeline (3/12)",
                    total=len(pipeline_data_mapping),
                )
                for pipeline_name, pipeline_data_mapping in dataset_mapping.items()
                if len(pipeline_data_mapping) > 0
            }

            for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
                for collection_name, collection_data_mapping in pipeline_data_mapping.items():
                    self.logger.info(f'Started populating data for pipeline "{pipeline_name}"')
                    process_file(
                        self,
                        items=list(collection_data_mapping.items()),
                        pipeline_name=pipeline_name,
                        operation=operation,
                        dataset_items=dataset_items[pipeline_name][collection_name],
                        logger=self.logger,
                        progress=progress,
                        tasks_by_pipeline_name=tasks_by_pipeline_name,
                    )  # type: ignore[call-arg]
                    self.logger.info(f'Completed populating data for pipeline "{pipeline_name}"')

        return dataset_items

    def _process_files_with_metadata(
        self,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]],
        max_workers: int | None = None,
    ) -> None:
        """
        Process files with their associated metadata types.

        Args:
            dataset_mapping: The dataset mapping containing source and destination paths.
            max_workers: Maximum number of worker processes to use. If None, uses all available CPU cores.
        """
        # Group files by metadata type
        files_by_type: dict[type, dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]]] = {}

        for pipeline_name, pipeline_data_mapping in dataset_mapping.items():
            for relative_dst, metadata_items, ancillary_data in pipeline_data_mapping.values():
                if not metadata_items:
                    continue

                dst = self.get_pipeline_data_dir(pipeline_name) / relative_dst

                # Group by the type of the first metadata item
                metadata_type = type(metadata_items[0])
                if metadata_type not in files_by_type:
                    files_by_type[metadata_type] = {}

                files_by_type[metadata_type][dst] = (metadata_items, ancillary_data)

        # Process files for each metadata type
        for metadata_type, files in files_by_type.items():
            metadata_type.process_files(
                dataset_mapping=files,
                max_workers=max_workers,
                dry_run=self.dry_run,
            )

        total_files = sum(len(files) for files in files_by_type.values())
        self.logger.info(f"Processed {total_files} files with metadata")

    def _update_metadata_hashes(
        self,
        file_path: str,
        metadata_items: list[BaseMetadata],
        progress: Progress | None = None,
        task: TaskID | None = None,
    ) -> None:
        """Update hash values for metadata items."""
        file_data_path = Path(file_path)
        if file_data_path.is_file():
            file_hash = compute_hash(file_data_path)
            for metadata_item in metadata_items:
                metadata_item.hash_sha256 = file_hash

        if progress and task is not None:
            progress.advance(task)

    def _process_items(
        self,
        dataset_items: dict[str, list[BaseMetadata]],
        progress: Progress | None = None,
        task: TaskID | None = None,
        max_workers: int | None = None,
    ) -> dict[str, list[BaseMetadata]]:
        """Process all items and return them sorted by path."""

        @multithreaded(max_workers=max_workers)
        def process_items_with_hashes(
            self: DatasetWrapper,
            thread_num: str,  # noqa: ARG001
            item: tuple[str, list[BaseMetadata]],
            logger: logging.Logger | None = None,  # noqa: ARG001
            progress: Progress | None = None,
            task: TaskID | None = None,
        ) -> None:
            """Process items and calculate their hashes in parallel."""
            file_path, metadata_items = item
            self._update_metadata_hashes(file_path, metadata_items, progress, task)

        items = [
            (Path(self.root_dir) / file_path, metadata_items) for file_path, metadata_items in dataset_items.items()
        ]
        process_items_with_hashes(
            self,
            items=items,
            logger=self.logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]
        return OrderedDict(sorted(dataset_items.items(), key=lambda item: item[0]))

    def _group_by_metadata_type(
        self,
        items: dict[str, list[BaseMetadata]],
    ) -> dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]:
        """Group dataset items by their metadata type."""
        grouped_items: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {}

        for path, metadata_items in items.items():
            for metadata_item in metadata_items:
                metadata_type = type(metadata_item)
                if metadata_type not in grouped_items:
                    grouped_items[metadata_type] = {}
                if path not in grouped_items[metadata_type]:
                    grouped_items[metadata_type][path] = []
                grouped_items[metadata_type][path].append(metadata_item)

        return grouped_items

    def _create_metadata_files(
        self,
        dataset_name: str,
        grouped_items: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]],
        collection_name: str | None = None,
    ) -> None:
        """Create metadata files for each type."""
        for metadata_type, type_items in grouped_items.items():
            metadata_type.create_dataset_metadata(
                dataset_name=dataset_name,
                root_dir=self.root_dir,
                items=type_items,
                metadata_name=collection_name,
                dry_run=self.dry_run,
                saver_overwrite=self._metadata_saver_overwrite,
            )

    def _log_metadata_summary(
        self,
        grouped_items: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]],
    ) -> None:
        """Log a summary of the metadata generation."""
        type_counts = [f"{len(items)} {metadata_type.__name__}" for metadata_type, items in grouped_items.items()]
        self.logger.info(
            f"Generated metadata file containing {', '.join(type_counts)} items",
        )

    def generate_metadata(
        self,
        dataset_name: str,
        dataset_items: MAPPED_DATASET_ITEMS,
        mapping_processor_decorator: list[DECORATOR_TYPE],
        max_workers: int | None = None,
        *,
        progress: bool = True,
    ) -> None:
        """
        Generate metadata for a dataset.

        This function processes a dataset, calculates file hashes, groups items by metadata type,
        and creates dataset metadata files. It can optionally display a progress bar during execution.

        Args:
            dataset_name: The name of the dataset.
            dataset_items: A dictionary mapping file paths to lists of BaseMetadata objects.
            mapping_processor_decorator: Dataset mapping processor decorator.
            progress: Whether to display a progress bar. Defaults to True.
            max_workers: Maximum number of worker processes to use. If None, uses all available CPU cores.

        Raises:
            FileNotFoundError: If a file specified in dataset_items is not found in the data directory.
            PermissionError: If there are insufficient permissions to read files or write metadata.
            IOError: If there are issues reading files or writing metadata.
        """
        if progress:
            with Progress(SpinnerColumn(), *get_default_columns()) as progress_bar:
                total_tasks = len(flatten_mapping(flatten_middle_mapping(dataset_items))) + 1
                task = progress_bar.add_task("[green]Generating dataset metadata (5/12)", total=total_tasks)

                processed_items = execute_on_mapping(
                    dataset_items,
                    lambda x: self._process_items(x, progress_bar, task, max_workers),
                )
                grouped_items = execute_on_mapping(processed_items, self._group_by_metadata_type)

                progress_bar.update(task, description="[green]Writing dataset metadata (5/12)")
                for decorator in mapping_processor_decorator:
                    decorator(lambda x, y: self._create_metadata_files(dataset_name, x, y), grouped_items)

                progress_bar.advance(task)
        else:
            processed_items = execute_on_mapping(dataset_items, lambda x: self._process_items(x))
            grouped_items = execute_on_mapping(processed_items, self._group_by_metadata_type)
            for decorator in mapping_processor_decorator:
                decorator(lambda x, y: self._create_metadata_files(dataset_name, x, y), grouped_items)

        self._log_metadata_summary(flatten_mapping(flatten_middle_mapping(grouped_items)))

    def _run_post_package_processors(self, post_package_processors: list[Callable[[Path], set[Path]]]) -> set[Path]:
        changed_files = set()
        with Progress(SpinnerColumn(), *get_default_columns()) as progress_bar:
            task = progress_bar.add_task("[green]Running post package hooks (6/12)", total=len(post_package_processors))
            for post_package_processor in post_package_processors:
                changed_files.update(post_package_processor(self.root_dir))
                progress_bar.advance(task)

        return changed_files

    def _update_manifest(self, changed_files: set[Path], max_worker: int | None = None) -> None:
        manifest = Manifest.load(self.manifest_path)
        manifest.update(
            changed_files,
            self.data_dir,
            {self.manifest_path, self.log_path},
            logger=self.logger,
            max_workers=max_worker,
        )

        if not self.dry_run:
            manifest.save(self.manifest_path)

    def generate_dataset_summary(
        self,
        dataset_items: dict[str, list[BaseMetadata]],
        *,
        progress: bool = True,
    ) -> None:
        """
        Generate a summary of the dataset.

        Args:
            dataset_items: The dictionary of dataset items to summarize.
            progress: A flag to indicate whether to show a progress bar.
        """

        def generate_summary() -> None:
            summary = self.summarise(dataset_items)
            if not self.dry_run:
                self.summary_path.write_text(str(summary))
            self.logger.info(
                f"Generated dataset summary at {format_path_for_logging(self.summary_path, self._project_dir)}",
            )

        if progress:
            with Progress(SpinnerColumn(), *get_default_columns()) as progress_bar:
                task = progress_bar.add_task("[green]Generating dataset summary (7/12)", total=1)
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

    def _generate_dataset_map(self, image_set_items: dict[str, list[BaseMetadata]], zoom: int | None = None) -> None:
        """
        Generate a summary of the dataset, including a map of geolocations if available.

        Args:
            image_set_items: The dictionary of image set items to summarize.
            zoom: Optional zoom level for the map.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Generating dataset map (8/12)", total=1)

            # Check for geolocations
            geolocations = [
                (image_data.latitude, image_data.longitude)
                for image_data_list in image_set_items.values()
                for image_data in image_data_list
                if self._validate_geolocations(image_data.latitude, image_data.longitude)
            ]
            if geolocations:
                summary_map = make_summary_map(geolocations, zoom=zoom)
                if summary_map is not None:
                    map_path = self.root_dir / "map.png"
                    if not self.dry_run:
                        summary_map.save(map_path)
                    coordinate_label = "spatial coordinate" if len(geolocations) == 1 else "spatial coordinates"
                    self.logger.info(
                        f"Generated summary map containing {len(geolocations)} "
                        f"{coordinate_label} at {format_path_for_logging(map_path, self._project_dir)}",
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
            task = progress.add_task("[green]Copying logs (10/12)", total=1)
            if not self.dry_run:
                copy2(project_log_path, self.logs_dir)
                for pipeline_log_path in pipeline_log_paths:
                    copy2(pipeline_log_path, self.pipeline_logs_dir)
            self.logger.info(f"Copied project logs to {format_path_for_logging(self.logs_dir, self._project_dir)}")
            progress.advance(task)

    def _copy_pipelines(self, project_pipelines_dir: Path) -> None:
        """
        Copy project pipelines to the appropriate directory, ignoring unnecessary files.

        Args:
            project_pipelines_dir: The path to the project pipelines directory.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Copying pipelines (9/12)", total=1)
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
            self.logger.info(
                f"Copied project pipelines to {format_path_for_logging(self.pipelines_dir, self._project_dir)}",
            )
            progress.advance(task)

    def _generate_manifest(
        self,
        dataset_items: dict[str, list[BaseMetadata]],
        max_workers: int | None = None,
    ) -> None:
        """
        Generate and save the manifest for the dataset, excluding certain paths.

        The manifest provides a comprehensive list of files and their hashes for verification.
        """
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            globbed_files = list(self.root_dir.glob("**/*"))
            task = progress.add_task("[green]Generating manifest (11/12)", total=len(globbed_files))
            manifest = Manifest.from_dir(
                self.root_dir,
                exclude_paths=[self.manifest_path, self.log_path],
                dataset_items=dataset_items,
                progress=progress,
                task=task,
                logger=self.logger,
                max_workers=max_workers,
            )
            if not self.dry_run:
                manifest.save(self.manifest_path, logger=self.logger)
            self.logger.info(
                f"Generated manifest for {len(globbed_files)} "
                f"files and paths at {format_path_for_logging(self.manifest_path, self._project_dir)}",
            )

    def summarise(self, dataset_items: dict[str, list[BaseMetadata]]) -> ImagerySummary:
        """
        Create an imagery summary for this dataset.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_dataset(self, dataset_items)

    def check_dataset_mapping(
        self,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]],
        max_workers: int | None = None,
    ) -> None:
        """
        Verify that the given dataset mapping is valid.

        Args:
            dataset_mapping: A mapping from source paths to destination paths and metadata.
            max_workers: Maximum number of worker processes to use. If None, uses all available CPU cores.

        Raises:
            DatasetWrapper.InvalidDatasetMappingError: If the path mapping is invalid.
        """
        total_tasks = 0
        for pipeline_data_mapping in dataset_mapping.values():
            total_tasks += len(pipeline_data_mapping) * 4

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            task = progress.add_task("[green]Checking dataset mapping (2/12)", total=total_tasks)

            for pipeline_data_mapping in dataset_mapping.values():
                self._verify_source_paths_exist(pipeline_data_mapping, progress, task, max_workers)
                self._verify_unique_source_resolutions(pipeline_data_mapping, progress, task, max_workers)
                self._verify_relative_destination_paths(pipeline_data_mapping, progress, task, max_workers)
                self._verify_no_destination_collisions(pipeline_data_mapping, progress, task, max_workers)

        self.logger.info("Dataset mapping is valid")

    def _verify_source_paths_exist(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
        max_workers: int | None = None,
    ) -> None:
        @multithreaded(max_workers=max_workers)
        def verify_path(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            logger: logging.Logger | None = None,  # noqa: ARG001
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
            logger=self.logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_unique_source_resolutions(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
        max_workers: int | None = None,
    ) -> None:
        reverse_src_resolution: dict[Path, Path] = {}

        @multithreaded(max_workers=max_workers)
        def verify_resolution(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            reverse_src_resolution: dict[Path, Path],
            logger: logging.Logger | None = None,  # noqa: ARG001
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
            logger=self.logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_relative_destination_paths(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
        max_workers: int | None = None,
    ) -> None:
        destinations = [dst for dst, _, _ in pipeline_data_mapping.values()]

        @multithreaded(max_workers=max_workers)
        def verify_destination_path(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: Path,
            logger: logging.Logger | None = None,  # noqa: ARG001
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
            logger=self.logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]

    def _verify_no_destination_collisions(
        self,
        pipeline_data_mapping: dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]],
        progress: Progress,
        task: TaskID,
        max_workers: int | None = None,
    ) -> None:
        reverse_mapping: dict[Path, Path] = {
            dst.resolve(): src for src, (dst, _, _) in pipeline_data_mapping.items() if dst is not None
        }

        @multithreaded(max_workers=max_workers)
        def verify_no_collision(
            self: DatasetWrapper,  # noqa: ARG001
            thread_num: str,  # noqa: ARG001
            item: tuple[Path, Path],
            reverse_mapping: dict[Path, Path],
            logger: logging.Logger | None = None,  # noqa: ARG001
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
            logger=self.logger,
            progress=progress,
            task=task,
        )  # type: ignore[call-arg]
