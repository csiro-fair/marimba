from pathlib import Path
from typing import Union

from marimba.core.utils.config import load_config, save_config


class CollectionWrapper:
    """
    Collection directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the collection directory structure is invalid.
        """

        pass

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline is not found.
        """

        pass

    def __init__(self, root_dir: Union[str, Path]):
        self._root_dir = Path(root_dir)

        self._check_file_structure()

    @classmethod
    def create(cls, root_dir: Union[str, Path], config: dict) -> "CollectionWrapper":
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

    def _check_file_structure(self):
        """
        Check that the collection directory structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            CollectionDirectory.InvalidStructureError: If the collection directory structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise CollectionWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path):
            if not path.is_file():
                raise CollectionWrapper.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self._root_dir)
        check_file_exists(self.config_path)

    def load_config(self) -> dict:
        """
        Load the collection configuration. Reads `collection.yml` from the collection root directory.
        """
        return load_config(self.config_path)

    def save_config(self, config: dict):
        """
        Save a new collection configuration to `collection.yml` in the collection root directory.
        """
        save_config(self.config_path, config)

    def get_pipeline_data_dir(self, pipeline_name: str) -> Path:
        """
        Get the path to the pipeline data directory.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The path to the pipeline data directory.
        """
        return self.root_dir / pipeline_name
