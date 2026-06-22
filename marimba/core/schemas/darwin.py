"""
Marimba Darwin Core Metadata Implementation.
"""

from marimba.core.schemas.base import BaseMetadata


class DarwinCoreMetadata(BaseMetadata):
    """
    Darwin Core metadata implementation for biological data interchange.

    This abstract class extends BaseMetadata to provide a specialized interface
    for Darwin Core metadata standard, commonly used in biodiversity and
    biological data management. Implementations should provide concrete
    behavior for all inherited abstract methods.
    """

    # TODO @<cjackett>: implement. Empty stub kept as roadmap placeholder for Darwin Core support; all BaseMetadata
    # abstract methods (datetime/latitude/longitude/altitude/context/license/creators/hash_sha256, plus
    # create_dataset_metadata and process_files) need concrete implementations before this can be instantiated.
