"""
File system utilities.
"""
from math import ceil
from pathlib import Path
from typing import Union

import psutil

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


def list_sd_cards(format_type, max_card_size=512):
    """
    Scan for SD cards.

    Args:
        format_type : type of format on the sdcard (exfat preferred)
        max_card_size : select drives with less than the max in Gb
    """
    result = []
    for i in psutil.disk_partitions():
        if i.fstype.lower() == format_type:
            p = psutil.disk_usage(i.mountpoint)
            if ceil(p.total / 1000000000) <= max_card_size:
                result.append(i.mountpmountpointoint)
    return result
