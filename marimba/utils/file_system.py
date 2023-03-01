"""
File system utils
"""

import logging
import os
from logging.config import dictConfig

from marimba.utils.logger_config import LoggerConfig

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2021, National Collections and Marine Infrastructure, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"

dictConfig(LoggerConfig.standardConfig)
logger = logging.getLogger(__name__)


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
