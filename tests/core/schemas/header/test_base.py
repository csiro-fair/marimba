from functools import reduce
from ifdo import ImageContext, ImageSetHeader
from pydantic import BaseModel
import pytest
from dataclasses import dataclass
from marimba.core.schemas.header import BaseMetadataHeader, HeaderMergeconflictError


class TestHeader(BaseModel):
    a: str | None
    b: int | None


def test_valid_merge() -> None:
    first_header = BaseMetadataHeader(TestHeader(a=None, b=0))
    second_header = BaseMetadataHeader(TestHeader(a="test", b=0))

    result = reduce(BaseMetadataHeader.__add__, [first_header, second_header])

    assert result.header.a == "test"
    assert result.header.b == 0


def test_invalid_merge() -> None:
    first_header = BaseMetadataHeader(TestHeader(a="other", b=0))
    second_header = BaseMetadataHeader(TestHeader(a="test", b=0))

    with pytest.raises(Exception) as e:
        assert isinstance(e, HeaderMergeconflictError)
        assert e._conflict_attr == "a"

        first_header + second_header


def test_ifdo_Header() -> None:
    first_header = BaseMetadataHeader(
        ImageSetHeader(image_set_name="", image_set_uuid="", image_set_handle="")
    )
    first_header.header.image_context = ImageContext(name="Test")
    second_header = BaseMetadataHeader(
        ImageSetHeader(image_set_name="", image_set_uuid="", image_set_handle="")
    )

    result = first_header + second_header
    assert result.header.image_context is not None
    assert result.header.image_context.name == "Test"
