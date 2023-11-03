from abc import ABC

from ifdo.models import ImageData


class BaseMetadata(ABC):
    """
    Base metadata class. All metadata classes should inherit from this class.
    """

    pass  # TODO: Determine the required interface


class iFDOMetadata(BaseMetadata, ImageData):
    """
    iFDO image metadata class. Inherits from `ifdo.models.ImageData` and `BaseMetadata` to unify the interfaces.
    """

    pass


class EXIFMetadata(BaseMetadata):
    """
    EXIF metadata class.
    """

    pass


class DarwinCoreMetadata(BaseMetadata):
    """
    Darwin Core metadata class.
    """

    pass
