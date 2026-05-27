"""Marimba core package — framework spine.

Defines the common ``MarimbaError`` base from which every marimba-domain exception inherits, so CLI handlers
and downstream consumers can catch the broad case with ``except MarimbaError`` while still using typed
``except`` for specific failure modes.
"""


class MarimbaError(Exception):
    """Base class for every marimba-domain exception.

    Subclassed by the nested exception types on each wrapper (``ProjectWrapper``, ``DatasetWrapper``,
    ``CollectionWrapper``, ``PipelineWrapper``, ``DistributionTargetWrapper``) as well as the standalone
    exceptions in ``marimba.core.distribution.base``, ``marimba.core.installer.*``, and
    ``marimba.core.utils.map``. Catch ``MarimbaError`` for catch-all error handling; catch specific
    subclasses when the handler needs to react differently to different failure modes.
    """


class NetworkConnectionError(MarimbaError):
    """Raised when a network operation fails (e.g. the static-map tile server is unreachable).

    Lives on the core package so CLI handlers can ``except NetworkConnectionError`` without pulling in
    the heavy ``staticmap`` / ``requests`` / ``PIL`` chain that the map-rendering implementation needs.
    """
