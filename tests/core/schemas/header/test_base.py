import pytest
from dataclasses import dataclass
from marimba.core.schemas.header import BaseMetadataHeader, HeaderMergeconflictError


@dataclass
class TestHeader:
    a: str | None
    b: int | None


def test_valid_merge() -> None:
    first_header = BaseMetadataHeader(TestHeader(None, 0))
    second_header = BaseMetadataHeader(TestHeader("test", 0))

    result = first_header + second_header

    assert result.header.a == "test"
    assert result.header.b == 0


def test_invalid_merge() -> None:
    first_header = BaseMetadataHeader(TestHeader("other", 0))
    second_header = BaseMetadataHeader(TestHeader("test", 0))

    with pytest.raises(Exception) as e:
        assert isinstance(e, HeaderMergeconflictError)
        assert e._conflict_attr == "a"

        first_header + second_header
