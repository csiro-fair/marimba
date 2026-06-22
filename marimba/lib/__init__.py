"""
Marimba standard-library helpers for pipeline implementations.

Pipeline authors are encouraged to import from these submodules
(``marimba.lib.image``, ``marimba.lib.video``, etc.) rather than reaching into
``marimba.core.*``.

Submodules are not eagerly imported: ``import marimba.lib`` is cheap. Each
submodule loads on first explicit reference (``from marimba.lib import image``
or ``marimba.lib.image`` after the eager fallback below). This keeps the
CLI-startup path from pulling cv2 / av / numpy / PIL just to print --help.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

__all__ = [
    "concurrency",
    "decorators",
    "exif",
    "gps",
    "image",
    "video",
]


def __getattr__(name: str) -> ModuleType:
    """Lazy-load submodules on attribute access (PEP 562).

    Supports ``marimba.lib.image`` after a plain ``import marimba.lib``;
    ``from marimba.lib import image`` resolves via the standard submodule
    machinery before this helper fires.
    """
    if name in __all__:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
