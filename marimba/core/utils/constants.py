"""
Marimba Constants.

This module contains constants and helper functions used for configuring and managing Marimba projects. It provides
default values and help text for various project-related settings.

Constants:
    - PROJECT_DIR_HELP: Help text for specifying the Marimba project root directory.
    - EXIF_SUPPORTED_EXTENSIONS: Set of file extensions that support EXIF metadata writing.
"""

from enum import Enum

PROJECT_DIR_HELP = (
    "Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current "
    "working directory and its parents."
)

# File extensions that support EXIF metadata writing
EXIF_SUPPORTED_EXTENSIONS = {
    # Standard formats with native EXIF support
    ".jpg",
    ".jpeg",
    ".tiff",
    ".tif",
    # Common RAW formats that support EXIF
    ".cr2",  # Canon
    ".cr3",  # Canon
    ".nef",  # Nikon
    ".arw",  # Sony
    ".dng",  # Adobe Digital Negative
    ".raf",  # Fujifilm
    ".orf",  # Olympus
    ".pef",  # Pentax
    ".rw2",  # Panasonic
}


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


class MetadataGenerationLevelOptions(str, Enum):
    """
    Defines an enumeration of metadata generation levels options.

    Attributes:
        - project: One metadata file is generated for a project.
        - pipeline: One metadata file is generated per pipeline.
        - collection: One metadata file is generated per pipeline and per collection.
    """

    project = "project"
    pipeline = "pipeline"
    collection = "collection"
