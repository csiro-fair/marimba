"""
Vulture whitelist for Marimba project.

This file contains patterns that should not be flagged as dead code by vulture.
Vulture whitelist files must be valid Python code that references the items
you want to whitelist.

See: https://github.com/jendrikseipp/vulture#whitelisting-false-positives
"""

# All imports at top
import logging

import exiftool
from PIL import Image

import marimba.main
from marimba.core.schemas.base import BaseMetadata

# CLI command patterns - Typer automatically discovers these
marimba_cli = marimba.main.marimba_cli  # Main CLI function

# Common patterns that may trigger false positives
logger = logging.getLogger()
logger.debug  # noqa: B018
logger.info  # noqa: B018
logger.warning  # noqa: B018
logger.error  # noqa: B018

# BaseMetadata interface methods (may be called polymorphically)
BaseMetadata.process_files  # noqa: B018

# PIL and Image processing patterns
Image.open  # noqa: B018

# ExifTool patterns
exiftool.ExifToolHelper  # noqa: B018

# Processing method patterns that might be used dynamically
# These are common patterns that vulture might flag incorrectly
_batch_processing = None  # Batch processing methods
_chunk_processing = None  # Chunk processing methods
_process_files = None  # File processing methods
_extract_properties = None  # Property extraction

# Pydantic model methods
model_dump = None  # Pydantic model serialization
model_validate = None  # Pydantic validation

# Common attribute patterns
__version__ = None
__all__: list[str] = []

# Metadata patterns
metadata = None
_metadata = None
