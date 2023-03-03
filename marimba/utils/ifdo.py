"""
iFDO utilities.
"""
from ifdo import iFDO


def load_ifdo(path: str) -> iFDO:
    """
    Load an iFDO file from a path.
    
    Args:
        path: The path to the iFDO file.

    Returns:
        The parsed iFDO object.
    """
    return iFDO.load(path)


def save_ifdo(ifdo: iFDO, path: str):
    """
    Save an iFDO file to a path.
    
    Args:
        ifdo: The iFDO object to save.
        path: The path to save the iFDO file to. Should end in .yaml.
    """
    ifdo.save(path)
