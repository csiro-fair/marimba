"""
Marimba - A Python framework for structuring, processing, packaging and distributing FAIR scientific image datasets.
"""

import importlib.metadata

__version__ = "unknown"
__author__ = "Marimba Development Team"
__email__ = "chris.jackett@csiro.au"

try:
    __version__ = importlib.metadata.version("marimba")
except (importlib.metadata.PackageNotFoundError, Exception):  # noqa: BLE001
    __version__ = "unknown"

__all__ = ["__author__", "__email__", "__version__"]
