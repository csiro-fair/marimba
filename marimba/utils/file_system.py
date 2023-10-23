"""
File system utils
"""

import os
from pathlib import Path
from typing import Union

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def create_directory_if_necessary(path: Union[str, Path]):
    """
    Create a directory if it doesn't already exist.

    Args:
        path: The path to the directory.
    """
    path = Path(path)  # convert to Path object if necessary
    if not path.is_dir():
        try:
            logger.info(f"Creating new directory path: {path}")
            path.mkdir(parents=True)
        except Exception as error:
            logger.error(error)
