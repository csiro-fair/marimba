"""
Marimba Metadata Abstract Base Class Module.
"""

from abc import ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ifdo.models import ImageData
else:
    from ifdo.models import ImageData


class BaseMetadata(ABC):  # noqa: B024
    """
    Base metadata class. All metadata classes should inherit from this class.
    """

    # TODO(@cjackett): Determine the required interface


class iFDOMetadata(BaseMetadata, ImageData):  # type: ignore[misc]  # noqa: N801
    """
    iFDO image metadata class. Inherits from `ifdo.models.ImageData` and `BaseMetadata` to unify the interfaces.
    """


class EXIFMetadata(BaseMetadata):
    """
    EXIF metadata class.
    """


class DarwinCoreMetadata(BaseMetadata):
    """
    Darwin Core metadata class.
    """
