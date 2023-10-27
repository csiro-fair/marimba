import logging
from pathlib import Path
from shutil import copy2
from typing import Dict, Union

from ifdo import iFDO

from marimba.utils.log import LogMixin, get_file_handler


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
            resolved = src.resolve()
            if resolved in reverse_src_resolution:
                raise PackageWrapper.InvalidPathMappingError(f"Source paths {src} and {reverse_src_resolution[resolved]} both resolve to {resolved}.")
            reverse_src_resolution[resolved] = src

        # Verify that all destination paths are relative
        for dst in path_mapping.values():
            if dst.is_absolute():
                raise PackageWrapper.InvalidPathMappingError(f"Destination path {dst} must be relative.")

        # Verify that there are no collisions in destination paths
        reverse_mapping = {dst: src for src, dst in path_mapping.items()}
        for src, dst in path_mapping.items():
            src_other = reverse_mapping.get(dst)
            dst_other = path_mapping[src_other]
            if dst.resolve() != path_mapping[src_other].resolve():
                raise PackageWrapper.InvalidPathMappingError(f"Destination path {dst} collides with {dst_other} for source path {src}.")
