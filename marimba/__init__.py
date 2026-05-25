"""
Marimba - A Python framework for structuring, processing, packaging and distributing FAIR scientific image datasets.

Public API: the names re-exported below are the supported surface for pipeline authors and other downstream
consumers. Internal modules (``marimba.core.*``) may be refactored without notice; downstream code should import
from this top-level package wherever possible.
"""

import importlib.metadata

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.generic import GenericMetadata
from marimba.core.schemas.ifdo import iFDOMetadata
from marimba.core.utils.constants import Operation

__version__ = "unknown"
__author__ = "Marimba Development Team"
__email__ = "chris.jackett@csiro.au"

try:
    __version__ = importlib.metadata.version("marimba")
except (importlib.metadata.PackageNotFoundError, Exception):  # noqa: BLE001
    __version__ = "unknown"

__all__ = [
    "BaseMetadata",
    "BasePipeline",
    "GenericMetadata",
    "Operation",
    "__author__",
    "__email__",
    "__version__",
    "iFDOMetadata",
]
