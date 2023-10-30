import logging
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from typing import Dict, List, Tuple, Union
from uuid import uuid4

from ifdo.models import ImageData, ImageSetHeader, iFDO

from marimba.utils.log import LogMixin, get_file_handler


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
        return f"""Dataset summary:
    {self.num_images} images ({sizeof_fmt(self.size_images_bytes)})
    {self.num_videos} videos ({sizeof_fmt(self.size_videos_bytes)})
    {self.num_other} other files ({sizeof_fmt(self.size_other_bytes)})
    {self.num_files} total files ({sizeof_fmt(self.size_files_bytes)})"""


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

        # Check that the root directory doesn't already exist
        if root_dir.is_dir():
            raise FileExistsError(root_dir)

        # Create the file structure
        root_dir.mkdir(parents=True)
        data_dir.mkdir()

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

    def _setup_logging(self):
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

    def populate(self, dataset_name: str, dataset_mapping: Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]], copy: bool = True):
        """
        Populate the dataset with files from the given dataset mapping.

        Args:
            dataset_name: The name of the dataset.
            dataset_mapping: A dict mapping pipeline name -> { output file path -> (input file path, image data) }
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

        # Update the dataset summary
        summary = self.summarize()
        if not self.dry_run:
            self.summary_path.write_text(str(summary))

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
                src_other = reverse_mapping.get(dst)
                if src.resolve() != src_other.resolve():
                    raise DatasetWrapper.InvalidDatasetMappingError(
                        f"Resolved destination path {dst.resolve()} is the same for source paths {src} and {src_other}."
                    )
