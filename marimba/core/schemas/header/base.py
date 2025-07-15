from __future__ import annotations

import inspect
from copy import copy
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class HeaderMergeconflictError(Exception):
    def __init__(self, conflict_attr: str, *args: object) -> None:
        super().__init__(*args)
        self._conflict_attr = conflict_attr

    def __str__(self) -> str:
        return f"Conflicting header information in field: {self._conflict_attr}"


class BaseMetadataHeader(Generic[T]):
    def __init__(self, header: T) -> None:
        self._header = header

    @property
    def header(self) -> T:
        return self._header

    def __add__(self, other: BaseMetadataHeader[T]) -> BaseMetadataHeader[T]:
        result = copy(self)
        own_attr = self._get_attributes(result.header)

        for attr_name, own_value in own_attr:
            other_value = getattr(other.header, attr_name)
            if own_value == other_value:
                continue

            if other_value is None:
                continue

            if own_value is not None:
                raise HeaderMergeconflictError(attr_name)

            setattr(result.header, attr_name, other_value)

        return result

    def merge(self, other: BaseMetadataHeader[T] | None) -> BaseMetadataHeader[T]:
        if other is None:
            return copy(self)
        return self + other

    @staticmethod
    def _get_attributes(value: object) -> list[tuple[str, Any]]:
        members = inspect.getmembers(value)
        public_attr = [
            (name, value) for name, value in members if (not name.startswith("_")) and (not inspect.ismethod(value))
        ]

        return public_attr
