"""
Marimba Standard Library - EXIF.

Wraps :mod:`exiftool` so pipeline authors don't have to manage the underlying
``ExifToolHelper`` lifecycle directly.

ExifTool is a Perl process: starting it costs ~100-300 ms on Linux. For
batched workflows (anything that reads metadata from more than a handful of
files) hold a single helper open via ``exif.session()`` and reuse it across
calls. The one-shot ``get_dict(path)`` form remains available for ergonomics
but pays the full Perl-startup cost on every call.

Example::

    from marimba.lib import exif

    with exif.session() as et:
        for path in image_paths:
            metadata = et.get_dict(path)
            ...
"""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import exiftool
from exiftool.exceptions import ExifToolException

from marimba.core.utils.dependencies import ToolDependency, show_dependency_error_and_exit


class ExifSession:
    """Reusable ExifTool session over a single Perl subprocess.

    Constructed via :func:`session`; not intended to be instantiated directly.
    """

    def __init__(self, helper: exiftool.ExifToolHelper) -> None:
        """Wrap an already-open ``ExifToolHelper``; lifecycle owned by :func:`session`."""
        self._helper = helper

    def get_dict(self, path: str | Path) -> Any:  # noqa: ANN401
        """Return the EXIF dictionary for ``path`` or ``None`` on missing/unreadable.

        Errors specific to ExifTool's Perl interpreter being unavailable propagate
        through :func:`show_dependency_error_and_exit`; per-file failures (corrupt
        EXIF, unsupported format, file-not-found, missing/invalid GPS structure)
        resolve to ``None``.
        """
        try:
            metadata = self._helper.get_metadata(str(path))
        except FileNotFoundError as e:
            if "exiftool" in str(e).lower():
                show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
            return None
        except (ExifToolException, KeyError, ValueError, TypeError, AttributeError, IndexError):
            return None

        if metadata:
            return metadata[0]
        return None

    def get_dicts(self, paths: list[str | Path]) -> list[Any]:
        """Batch-read metadata for multiple paths in a single ExifTool call.

        Returns a list aligned 1:1 with ``paths``; entries are dicts or ``None``
        for unreadable files. One ExifTool invocation services the whole batch.
        """
        if not paths:
            return []
        try:
            results = self._helper.get_metadata([str(p) for p in paths])
        except FileNotFoundError as e:
            if "exiftool" in str(e).lower():
                show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
            return [None] * len(paths)
        except ExifToolException:
            return [None] * len(paths)
        return list(results) if results else [None] * len(paths)


@contextmanager
def session() -> Iterator[ExifSession]:
    """Open a reusable :class:`ExifSession` over a single ExifTool subprocess.

    Prefer this over repeated ``get_dict(path)`` calls when reading metadata
    from multiple files — one Perl startup serves the whole batch.
    """
    try:
        with exiftool.ExifToolHelper() as helper:
            yield ExifSession(helper)
    except FileNotFoundError as e:
        if "exiftool" in str(e).lower():
            show_dependency_error_and_exit(ToolDependency.EXIFTOOL, str(e))
        raise


def get_dict(path: str | Path) -> Any:  # noqa: ANN401
    """One-shot EXIF read.

    Spawns a fresh ExifTool subprocess (~100-300 ms of Perl startup) per call.
    Prefer :func:`session` for batches:

        with exif.session() as et:
            for path in paths:
                meta = et.get_dict(path)
    """
    with session() as et:
        return et.get_dict(path)
