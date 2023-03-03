from abc import ABC, abstractmethod
from typing import Iterable, Tuple


class Instrument(ABC):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, instrument_path: str, collection_config: dict, instrument_config: dict):

        self.instrument_path = instrument_path
        self.collection_config = collection_config
        self.instrument_config = instrument_config


    @abstractmethod
    def rename(
            self,
            dry_run: bool,
    ):
        raise NotImplemented

    @classmethod
    @abstractmethod
    def prompt_config(cls) -> Iterable[Tuple[str, str]]:
        """
        Get the configuration key/prompt pairs for the instrument.

        Returns:
            An iterable of key/prompt pairs.
        """
        raise NotImplemented
