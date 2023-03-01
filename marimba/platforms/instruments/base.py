from abc import ABC, abstractmethod
from typing import Iterable, Tuple


class Instrument(ABC):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """
    
    def __init__(self, config: dict):

        # Get info from config
        image_set_header = config.get("image-set-header")
        self.platform = image_set_header.get("image-platform")
        self.sensor = image_set_header.get("image-sensor")
        self.filetype = image_set_header.get("image-acquisition")

    @abstractmethod
    def get_output_file_name(self, file_path: str) -> str:
        """
        Get the output file name for a given file path.
        
        Args:
            file_path: The path to the file.
        
        Returns:
            The file base name.
        """
        file_name = file_path.split("/")[-1].uppercase()
        return file_name

    @abstractmethod
    def get_output_file_directory(self, directory_path: str, destination_path: str) -> str:
        """
        Get the output file directory for a given directory path and destination path.
        
        Args:
            directory_path: The path to the directory.
            destination_path: The path to the destination directory.
        
        Returns:
            The output directory path.
        """
        if destination_path:
            return destination_path
        else:
            return directory_path

    @abstractmethod
    def is_target_rename_directory(self, directory_path: str) -> bool:
        """
        Check if a given directory path is a target for renaming.
        
        Args:
            directory_path: The path to the directory.
        
        Returns:
            True if the directory is a target for renaming, False otherwise.
        """
        return True

    @abstractmethod
    def get_manual_metadata_fields(self) -> bool:
        """
        Prompt for any manual metadata fields for the instrument.
        
        Returns:
            True on success, False on failure/bad input.
        """
        return True

    @classmethod
    @abstractmethod
    def prompt_config(cls) -> Iterable[Tuple[str, str]]:
        """
        Get the configuration key/prompt pairs for the instrument.
        
        Returns:
            An iterable of key/prompt pairs.
        """
        raise NotImplemented
