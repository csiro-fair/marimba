"""
Marimba Core Collection Wrapper Module.

This module provides the CollectionWrapper class, which serves as a wrapper for managing collection directories
within the Marimba project. It includes functionality for creating and validating the directory structure,
handling configuration files, and managing pipeline data directories.

Imports:
    - Path from pathlib: For handling filesystem paths.
    - Any, Dict, Union from typing: For type hints.
    - load_config, save_config from marimba.core.utils.config: For loading and saving configuration files.

Classes:
    - CollectionWrapper: A class that provides methods for creating, validating, and managing collection directories.
        - InvalidStructureError: Raised when the collection directory structure is invalid.
        - NoSuchPipelineError: Raised when a pipeline is not found.
"""

from pathlib import Path
from typing import Any

from marimba.core.utils.config import load_config, save_config


class CollectionWrapper:
    """
    Collection directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the collection directory structure is invalid.
        """

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline is not found.
        """

    def __init__(self, root_dir: str | Path) -> None:
        """
        Initialise the class instance.

        Args:
            root_dir (Union[str, Path]): The root directory for the file structure.
        """
        self._root_dir = Path(root_dir)

        self._check_file_structure()

    @classmethod
    def create(cls, root_dir: str | Path, config: dict[str, Any]) -> "CollectionWrapper":
        """
        Create a new collection directory.

        Args:
            root_dir: The collection root directory.
            config: The collection configuration.

        Returns:
            A collection.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the collection directory structure
        root_dir = Path(root_dir)
        config_path = root_dir / "collection.yml"

        # Check that the root directory doesn't already exist
        if root_dir.is_dir():
            raise FileExistsError(f"Collection directory {root_dir} already exists.")

        # Create the file structure and write the config
        root_dir.mkdir(parents=True)
        save_config(config_path, config)

        return cls(root_dir)

    @property
    def root_dir(self) -> Path:
        """
        The collection root directory.
        """
        return self._root_dir

    @property
    def config_path(self) -> Path:
        """
        The path to the collection configuration file.
        """
        return self.root_dir / "collection.yml"

    def _check_file_structure(self) -> None:
        """
        Check that the collection directory structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            CollectionDirectory.InvalidStructureError: If the collection directory structure is invalid.
        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise CollectionWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path) -> None:
            if not path.is_file():
                raise CollectionWrapper.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self.root_dir)
        check_file_exists(self.config_path)

    def load_config(self) -> dict[str, Any]:
        """
        Load the collection configuration. Reads `collection.yml` from the collection root directory.
        """
        return load_config(self.config_path)

    def save_config(self, config: dict[str, Any]) -> None:
        """
        Save a new collection configuration to `collection.yml` in the collection root directory.
        """
        save_config(self.config_path, config)

    def create_pipeline_data_dir(self, pipeline_name: str) -> Path:
        """
        Create a new data directory for a pipeline.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The path to the pipeline data directory.
        """
        pipeline_data_dir = self._get_pipeline_data_dir(pipeline_name)
        if pipeline_data_dir.is_dir():
            raise FileExistsError(f'Pipeline data directory "{pipeline_data_dir}" already exists.')

        pipeline_data_dir.mkdir(parents=True)
        return pipeline_data_dir

    def _get_pipeline_data_dir(self, pipeline_name: str) -> Path:
        """
        Get the path to the pipeline directory.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The path to the pipeline directory.
        """
        return self.root_dir / pipeline_name

    def get_pipeline_data_dir(self, pipeline_name: str) -> Path:
        """
        Get the path to the pipeline data directory.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The path to the pipeline data directory.

        Raises:
            CollectionWrapper.NoSuchPipelineError: If the pipeline does not exist.
        """
        pipeline_data_dir = self._get_pipeline_data_dir(pipeline_name)
        if not pipeline_data_dir.is_dir():
            raise CollectionWrapper.NoSuchPipelineError(f'Pipeline "{pipeline_name}" does not exist.')
        return pipeline_data_dir
