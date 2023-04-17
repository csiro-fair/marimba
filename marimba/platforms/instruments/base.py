import os.path
from abc import ABC

from marimba.utils.log import LogMixin, get_collection_logger, get_instrument_file_handler

collection_logger = get_collection_logger()


class Instrument(ABC, LogMixin):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        # Add the instrument file handler to the logger
        try:
            self.logger.addHandler(get_instrument_file_handler(os.path.basename(root_path)))
            self.logger.info(f'Initialising instrument-level logging for {instrument_config.get("id")}')
        except Exception as e:
            collection_logger.error(f"Failed to add instrument file handler: {e}")

        # Root and work paths for the instrument
        self.root_path = root_path
        self.work_path = os.path.join(self.root_path, "work")

        # Collection and instrument configuration
        self.collection_config = collection_config
        self.instrument_config = instrument_config
