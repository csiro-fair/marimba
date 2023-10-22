"""
File system utils
"""

import os
from math import  ceil
import psutil

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
            
def list_sdcards(format_type,maxcardsize=512):
    """
    Scan for SD cards.

    Args:
        format_type : type of format on the sdcard (exfat preffered)
        maxcardsize : select drives with less than the max in Gb
    """
    result =[]
    for i in psutil.disk_partitions():
        if i.fstype.lower()==format_type:
            p =psutil.disk_usage(i.mountpoint)
            if ceil(p.total/1000000000)<=maxcardsize:            
                result.append(i.mountpmountpointoint)
    return result