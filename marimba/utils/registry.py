from typing import List

from marimba.platforms.instruments.base import Instrument
from marimba.platforms.instruments.zeiss_axio_observer import ZeissAxioObserver


class Registry:
    """
    Registry of instrument name -> Instrument implementation class mappings. Supports adding mappings at runtime.
    """
    
    # Define the default instrument mappings here. These are the instruments that are supported out of the box.
    CLASS_MAP = {
        "ZAO": ZeissAxioObserver
    }
    
    @staticmethod
    def get(name: str) -> Instrument:
        """
        Get the instrument class for the given name.
        
        Args:
            name: The name of the instrument.
        
        Returns:
            The instrument implementation class.
        """
        if name not in Registry.CLASS_MAP:
            raise ValueError(f"No instrument with name {name} found.")
        return Registry.CLASS_MAP[name]
    
    @staticmethod
    def add(name: str, instrument: Instrument):
        """
        Add a new instrument to the registry.
        
        Args:
            name: The name of the instrument.
            instrument: The instrument implementation class. Must be a subclass of Instrument.
        """
        if not isinstance(name, str):
            raise ValueError("Provided name must be a string.")
        if not issubclass(instrument, Instrument):
            raise ValueError("Provided class must be a subclass of Instrument.")
        Registry.CLASS_MAP[name] = instrument
    
    @staticmethod
    def get_names() -> List[str]:
        """
        Get the names of all instruments in the registry.
        
        Returns:
            A list of all instrument names.
        """
        return list(Registry.CLASS_MAP.keys())
