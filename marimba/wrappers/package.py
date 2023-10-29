import logging
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from typing import Dict, Union

from ifdo import iFDO

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
    def from_package(cls, package_wrapper: "PackageWrapper") -> "ImagerySummary":
        """
        Create an imagery summary from a package wrapper.

        Args:
            package_wrapper: The package wrapper.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_dir(package_wrapper.data_dir)

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

        for path in directory.iterdir():
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
        return f"""Package summary:
    {self.num_images} images ({sizeof_fmt(self.size_images_bytes)})
    {self.num_videos} videos ({sizeof_fmt(self.size_videos_bytes)})
    {self.num_other} other files ({sizeof_fmt(self.size_other_bytes)})
    {self.num_files} total files ({sizeof_fmt(self.size_files_bytes)})"""


class PackageWrapper(LogMixin):
    """
    Package directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the package directory structure is invalid.
        """

        pass

    class InvalidPathMappingError(Exception):
        """
        Raised when a path mapping dictionary is invalid.
        """

        pass

    def __init__(self, root_dir: Union[str, Path]):
        self._root_dir = Path(root_dir)

        self._check_file_structure()
        self._setup_logging()

    @classmethod
    def create(cls, root_dir: Union[str, Path], ifdo: iFDO) -> "PackageWrapper":
        """
        Create a new package from an iFDO.

        Args:
            root_dir: The root directory of the package.
            ifdo: The iFDO to package.

        Returns:
            A package wrapper instance.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the package directory structure
        root_dir = Path(root_dir)
        data_dir = root_dir / "data"
        metadata_path = root_dir / "metadata.yml"

        # Check that the root directory doesn't already exist
        if root_dir.is_dir():
            raise FileExistsError(f"Package directory {root_dir} already exists.")

        # Create the file structure and write the iFDO
        root_dir.mkdir(parents=True)
        data_dir.mkdir()
        ifdo.save(metadata_path)

        return cls(root_dir)

    def _check_file_structure(self):
        """
        Check that the file structure of the project directory is valid. If not, raise an InvalidStructureError with details.

        Raises:
            PackageWrapper.InvalidStructureError: If the file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise PackageWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path):
            if not path.is_file():
                raise PackageWrapper.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.data_dir)
        check_file_exists(self.metadata_path)

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `package.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.root_dir, "package", False, level=logging.DEBUG)

        # Add the file handler to the logger
        self.logger.addHandler(self._file_handler)

    def populate(self, path_mapping: Dict[Path, Path], copy: bool = True):
        """
        Populate the package with files from the given path mapping.

        Args:
            path_mapping: A mapping from source paths to destination paths.
            copy: Whether to copy (True) or move (False) the files.

        Raises:
            PackageWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        # Verify that the path mapping is valid
        PackageWrapper.check_path_mapping(path_mapping)

        # Copy or move the files
        for src, relative_dst in path_mapping.items():
            # Compute the absolute destination path
            dst = self.data_dir / relative_dst

            # Create the parent directory if it doesn't exist
            dst.parent.mkdir(parents=True, exist_ok=True)

            # Copy or move the file
            if copy:
                copy2(src, dst)  # use copy2 to preserve metadata
            else:
                src.rename(dst)  # use rename to move the file
            self.logger.info(f"{src.absolute()} -> {dst} ({copy=})")

        # Update the package summary
        summary = self.summarize()
        self.summary_path.write_text(str(summary))

    def summarize(self) -> ImagerySummary:
        """
        Create an imagery summary for this package.

        Returns:
            An imagery summary.
        """
        return ImagerySummary.from_package(self)

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
        The path to the package summary.
        """
        return self._root_dir / "summary.txt"

    @staticmethod
    def check_path_mapping(path_mapping: Dict[Path, Path]):
        """
        Verify that the given path mapping is valid.

        Args:
            path_mapping: A mapping from source paths to destination paths.

        Raises:
            PackageWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        # Verify that all source paths exist
        for src in path_mapping:
            if not src.exists():
                raise PackageWrapper.InvalidPathMappingError(f"Source path {src} does not exist.")

        # Verify that all source paths resolve to unique paths
        reverse_src_resolution = {}
        for src in path_mapping:
            resolved = src.resolve().absolute()
            if resolved in reverse_src_resolution:
                raise PackageWrapper.InvalidPathMappingError(f"Source paths {src} and {reverse_src_resolution[resolved]} both resolve to {resolved}.")
            reverse_src_resolution[resolved] = src

        # Verify that all destination paths are relative
        for dst in path_mapping.values():
            if dst.is_absolute():
                raise PackageWrapper.InvalidPathMappingError(f"Destination path {dst} must be relative.")

        # Verify that there are no collisions in destination paths
        reverse_mapping = {dst.resolve(): src for src, dst in path_mapping.items()}
        for src, dst in path_mapping.items():
            src_other = reverse_mapping.get(dst)
            if src.resolve() != src_other.resolve():
                raise PackageWrapper.InvalidPathMappingError(
                    f"Resolved destination path {dst.resolve()} is the same for source paths {src} and {src_other}."
                )
