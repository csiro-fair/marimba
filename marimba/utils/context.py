"""
Global context for MarImBA CLI.
"""
from pathlib import Path
from typing import Optional, Union

COLLECTION_PATH: Optional[Path] = None


def get_collection_path() -> Optional[Path]:
    """
    Get the collection directory.

    Returns:
        The collection directory.
    """
    return COLLECTION_PATH


def set_collection_path(collection_path: Union[str, Path]):
    """
    Set the collection directory.

    Args:
        collection_path: The collection directory.
    """
    collection_path = Path(collection_path)
    global COLLECTION_PATH
    COLLECTION_PATH = collection_path


def get_instrument_path(instrument_name: str) -> Optional[Path]:
    """
    Get the instrument directory.

    Args:
        instrument_name: The name of the instrument.

    Returns:
        The instrument directory.
    """
    # Check that the collection path has been set
    collection_path = get_collection_path()
    if collection_path is None:
        return None

    return collection_path / "instruments" / instrument_name
