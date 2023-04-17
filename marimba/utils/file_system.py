"""
File system utils
"""

import os

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def create_directory_if_necessary(path: str):
    """
    Create a directory if it doesn't already exist.

    Args:
        path: The path to the directory.
    """
    if not os.path.isdir(path):
        try:
            logger.info(f"Creating new directory path: {path}")
            os.makedirs(path)
        except OSError as error:
            logger.error(error)
