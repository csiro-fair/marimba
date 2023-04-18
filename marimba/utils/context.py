"""
Global context for MarImBA CLI.
"""
import os

COLLECTION_PATH = None


def get_collection_path() -> str:
    """
    Get the collection directory.

    Returns:
        The collection directory.
    """
    return COLLECTION_PATH


def set_collection_path(collection_path: str):
    """
    Set the collection directory.

    Args:
        collection_path: The collection directory.
    """
    global COLLECTION_PATH
    COLLECTION_PATH = collection_path


def get_instrument_path(instrument_name: str) -> str:
    """
    Get the instrument directory.

    Args:
        instrument_name: The name of the instrument.

    Returns:
        The instrument directory.
    """
    return os.path.join(get_collection_path(), "instruments", instrument_name)
