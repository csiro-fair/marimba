from __future__ import annotations

import inspect
from copy import copy
from typing import Any, Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


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
        result_data = self.header.model_dump(mode="python")
        other_data = other.header.model_dump(mode="python")

        for attr_name, own_value in result_data.items():
            other_value = other_data.get(attr_name, None)
            if own_value == other_value:
                continue

            if other_value is None:
                continue

            if own_value is not None:
                raise HeaderMergeconflictError(attr_name)

            result_data[attr_name] = other_value

        return BaseMetadataHeader(type(self.header).model_validate(result_data))

    def merge(self, other: BaseMetadataHeader[T] | None) -> BaseMetadataHeader[T]:
        if other is None:
            return copy(self)
        return self + other

    @staticmethod
    def _get_attributes(value: object) -> list[tuple[str, Any]]:
        members = inspect.getmembers(value)
        public_attr = [
            (name, value)
            for name, value in members
            if (not name.startswith("_")) and (not inspect.ismethod(value))
        ]

        return public_attr
