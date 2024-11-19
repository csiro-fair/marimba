"""
Marimba Constants.

This module contains constants and helper functions used for configuring and managing Marimba projects. It provides
default values and help text for various project-related settings.

Constants:
    - PROJECT_DIR_HELP: Help text for specifying the Marimba project root directory.
"""

from enum import Enum

PROJECT_DIR_HELP = (
    "Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current "
    "working directory and its parents."
)


class Operation(str, Enum):
    """
    Define an enumeration of file operations.

    This class represents different types of file operations as an enumeration. It inherits from both str and Enum,
    allowing for string comparison and enumeration functionality. The class provides three predefined operations:
    copy, move, and link.

    Attributes:
        - copy: Represents the copy operation.
        - move: Represents the move operation.
        - link: Represents the link operation.
    """

    copy = "copy"
    move = "move"
    link = "link"
