from pathlib import Path
from typing import Union
from yaml import safe_load,YAMLError
from uuid import uuid4
from subprocess import Popen
import shlex

from marimba.core.utils.config import load_config, save_config


class CardWrapper:
    """
    sdcard wrapper.
    """

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline is not found.
        """

        pass

    class RootNotDirectoryError(Exception):
        """
        Raised if root path is not a directory
        """
        pass
    def __init__(self, root_dir: Union[str, Path],config: dict):
        self._root_dir = Path(root_dir)
        self._config = config

    @classmethod
    def create(cls, root_dir: Union[str, Path],project:str,pipeline:str,importdate:str,overwrite=False) -> "CardWrapper":
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
        if not root_dir.is_dir():
            raise FileExistsError(f"Collection directory {root_dir} already exists.")
        configPath = root_dir /  "import.yml"
        if configPath.exists() and not overwrite:
            config = load_config(configPath)
        else:
            config = dict()
            config['Project'] = project
            config['PipeLine'] = pipeline
            config['ImportDate'] = importdate
            config['ImportToken'] = str(uuid4())[0:8]
            save_config(configPath,config)

        # Check that the root directory doesn't already exist

        return cls(root_dir,config)

    def import_data(self,destination:Path,copy:bool,move:bool):
        destination = destination / self.config['ImportDate'] / f"{self.config['Project']}_{self.config['PipeLine']}_{self.config['ImportDate']}_{self.config['ImportToken']}"
        destination.mkdir(parents=True,exist_ok=True)
        command =f"rclone copy {self.card_dir.absolute()} {destination.absolute()} --progress --low-level-retries 1 "
        if copy:
            process = Popen(shlex.split(command))
            process.wait()
        if move:
            command =f"rclone move {self.root_dir.absolute()} {destination} --progress --delete-empty-src-dirs"
            process = Popen(shlex.split(command))
            process.wait()


    @property
    def card_dir(self) -> Path:
        """
        The collection root directory.
        """
        return self._root_dir

    @property
    def config_path(self) -> Path:
        """
        The path to the collection configuration file.
        """
        return self._root_dir / "import.yml"
    
    @property
    def config(self) -> Path:
        """
        Get the config dictionary
        """
        return self._config

    def load_config(self) -> dict:
        """
        Load the collection configuration. Reads `collection.yml` from the collection root directory.
        """
        return load_config(self.config_path)
    
    def save_config(self, config: dict,overwrite:bool):
        """
        Save a new collection configuration to `collection.yml` in the collection root directory.
        """
        if self.config_path.exists() and not overwrite:
            raise FileExistsError(f"Card all ready  initialised {self.config_path} already exists.")
        else:
            save_config(self.config_path, config)




