from pathlib import Path
from typing import Union

from marimba.utils.config import load_config, save_config


class Deployment:
    """
    Deployment directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the deployment directory structure is invalid.
        """

        pass

    class NoSuchInstrumentError(Exception):
        """
        Raised when an instrument is not found.
        """

        pass

    def __init__(self, root_dir: Union[str, Path]):
        self._root_dir = Path(root_dir)

        self._check_file_structure()

    @classmethod
    def create(cls, root_dir: Union[str, Path], config: dict) -> "Deployment":
        """
        Create a new deployment directory.

        Args:
            root_dir: The deployment root directory.
            config: The deployment configuration.

        Returns:
            A deployment.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the deployment directory structure
        root_dir = Path(root_dir)
        config_path = root_dir / "deployment.yml"

        # Check that the root directory doesn't already exist
        if root_dir.is_dir():
            raise FileExistsError(f"Deployment directory {root_dir} already exists.")

        # Create the file structure and write the config
        root_dir.mkdir(parents=True)
        save_config(config_path, config)

        return cls(root_dir)

    @property
    def root_dir(self) -> Path:
        """
        The deployment root directory.
        """
        return self._root_dir

    @property
    def config_path(self) -> Path:
        """
        The path to the deployment configuration file.
        """
        return self.root_dir / "deployment.yml"

    def _check_file_structure(self):
        """
        Check that the deployment directory structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            InvalidStructureError: If the deployment directory structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise Deployment.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path):
            if not path.is_file():
                raise Deployment.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self._root_dir)
        check_file_exists(self.config_path)

    def load_config(self) -> dict:
        """
        Load the deployment configuration. Reads `deployment.yml` from the deployment root directory.
        """
        return load_config(self.config_path)

    def save_config(self, config: dict):
        """
        Save a new deployment configuration to `deployment.yml` in the deployment root directory.
        """
        save_config(self.config_path, config)

    def get_instrument_data_dir(self, instrument_name: str) -> Path:
        """
        Get the path to the instrument data directory.

        Args:
            instrument_name: The name of the instrument.

        Returns:
            The path to the instrument data directory.
        """
        return self.root_dir / instrument_name

    def get_instrument_config(self, instrument_name: str) -> dict:
        """
        Get the instrument-specific configuration.

        If no instrument-specific configuration is found, an empty dictionary is returned.

        Args:
            instrument_name: The name of the instrument.

        Returns:
            The instrument-specific configuration.
        """
        config = self.load_config()

        if instrument_name not in config:
            raise self.NoSuchInstrumentError(f"Instrument {instrument_name} not found in the configuration file at {self.config_path}.")

        return config[instrument_name]
