"""
Global context for MarImBA CLI.
"""
import os

COLLECTION_DIR = None


def get_collection_dir() -> str:
    """
    Get the collection directory.

    Returns:
        The collection directory.
    """
    return COLLECTION_DIR


def set_collection_dir(collection_dir: str):
    """
    Set the collection directory.

    Args:
        collection_dir: The collection directory.
    """
    global COLLECTION_DIR
    COLLECTION_DIR = collection_dir


def get_instrument_dir(instrument_name: str) -> str:
    """
    Get the instrument directory.

    Args:
        instrument_name: The name of the instrument.

    Returns:
        The instrument directory.
    """
    return os.path.join(get_collection_dir(), "instruments", instrument_name)
