from abc import ABC, abstractmethod


class Instrument(ABC):
    def __init__(self, config: dict):

        # Get info from config
        image_set_header = config.get("image-set-header")
        self.platform = image_set_header.get("image-platform")
        self.sensor = image_set_header.get("image-sensor")
        self.filetype = image_set_header.get("image-acquisition")

    @abstractmethod
    def get_output_file_name(self, file_path) -> str:
        file_name = file_path.split("/")[-1].uppercase()
        return file_name

    @abstractmethod
    def get_output_file_directory(self, directory_path, destination_path) -> str:
        if destination_path:
            return destination_path
        else:
            return directory_path

    @abstractmethod
    def is_target_rename_directory(self, directory_path) -> bool:
        return True

    @abstractmethod
    def get_manual_metadata_fields(self) -> bool:
        return True
