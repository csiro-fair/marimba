#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File system utils
"""

import logging
import os
from logging.config import dictConfig

from utils.logger_config import LoggerConfig

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


def is_directory_path_valid(path):
    if os.path.isdir(path):
        return True
    else:
        return False


def create_directory_path_if_not_existing(path):
    if not os.path.isdir(path):
        try:
            logger.info(f"Creating new directory path: {path}")
            os.makedirs(path)
        except OSError as error:
            logger.error(error)


def check_directory_level(path: str):
    logger.info(f"Checking directory path is a bottom-level directory...")
    subdirectory_list = [f.path for f in os.scandir(path) if f.is_dir()]
    if len(subdirectory_list) > 0:
        logger.error(f"Directory path is not a bottom-level directory - exiting")
        exit()
    else:
        logger.info(f"Directory path is a bottom-level directory!")
