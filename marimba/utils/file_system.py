"""
File system utils
"""

import os

from marimba.utils.log import get_collection_logger

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2021, National Collections and Marine Infrastructure, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"

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
