"""
Module containing the implementation of the metadata header.

Classes:
    HeaderMergeConflictError: Custom Error-Type for signaling that two header cannot be merged.
    MetadataHeader: Metadata header class wrapping mergeable header data.
"""

from __future__ import annotations

import inspect
from copy import copy
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class HeaderMergeConflictError(Exception):
    """
    Custom Error-Type for signaling that two header cannot be merged.
    """

    def __init__(self, conflict_attr: str, *args: object) -> None:
        """
        Initializes a HeaderMergeConflictError instance.

        Args:
            conflict_attr: The name of the attribute responsible for the merge conflict.
            *args: Error
        """
        super().__init__(*args)
        self._conflict_attr = conflict_attr

    def __str__(self) -> str:
        return f"Conflicting header information in field: {self._conflict_attr}"


class MetadataHeader(Generic[T]):
    """
    Metadata header class wrapping mergeable header data.

    For this the data has to be able to be parsed into a Python dictionary.
    """

    def __init__(self, header: T) -> None:
        """
        Initializes a MetadataHeader instance.

        Args:
            header: Header data.
        """
        self._header = header

    @property
    def header(self) -> T:
        """
        Returns inner header data.
        """
        return self._header

    def __add__(self, other: MetadataHeader[T]) -> MetadataHeader[T]:
        result_data = self.header.model_dump(mode="python")
        other_data = other.header.model_dump(mode="python")

        for attr_name, own_value in result_data.items():
            other_value = other_data.get(attr_name, None)
            if own_value == other_value:
                continue

            if other_value is None:
                continue

            if own_value is not None:
                raise HeaderMergeConflictError(attr_name)

            result_data[attr_name] = other_value

        return MetadataHeader(type(self.header).model_validate(result_data))

    def merge(self, other: MetadataHeader[T] | None) -> MetadataHeader[T]:
        """
        Merge a metadata header with this header.

        Args:
            other: Other metadata header.

        Returns:
           Merged header of the this and the other header.
        """
        if other is None:
            return copy(self)
        return self + other

    @staticmethod
    def _get_attributes(value: object) -> list[tuple[str, Any]]:
        members = inspect.getmembers(value)
        return [
            (name, value) for name, value in members if (not name.startswith("_")) and (not inspect.ismethod(value))
        ]
