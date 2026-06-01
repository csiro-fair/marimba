"""Unit tests for the regression-harness scrubbers.

Fast (no cache, no network, no marimba bootstrap) checks that the scrubbers
normalise exactly the volatile fields they target and leave everything else
intact. The load-bearing property here is CPU-invariance: the entropy-coded
JPEG scan and the pixel-derived iFDO fields must not contribute to the scrubbed
hash, so two functionally-identical runs on different CPU microarchitectures
produce the same scrubbed manifest (see scrub.py drift source 5).
"""

from __future__ import annotations

import pytest

from tests.e2e.regression import scrub

pytestmark = pytest.mark.unit

# A real UUID-v4-shape string and the fixed placeholder it scrubs to.
_REAL_UUID = b"3f2504e0-4f89-41d3-9a0c-0305e82c3301"


def _fake_jpeg(scan: bytes, *, header_extra: bytes = b"") -> bytes:
    """Minimal JPEG-shaped byte string: SOI + header + SOS + scan + EOI.

    Not a decodable image — just carries the markers scrub_jpeg_bytes keys on.
    """
    return b"\xff\xd8" + header_extra + b"\xff\xda\x00\x0c" + scan + b"\xff\xd9"


def test_jpeg_scrub_ignores_scan_bytes() -> None:
    """Mutating the entropy-coded scan must not change the scrubbed bytes.

    This is the CPU-invariance guarantee: libjpeg-turbo emits different scan
    bytes per CPU generation, and the scrubber drops them all.
    """
    a = _fake_jpeg(b"\x01\x02\x03 pixel data A")
    b = _fake_jpeg(b"\xaa\xbb\xcc completely different scan B of other length")
    assert scrub.scrub_jpeg_bytes(a) == scrub.scrub_jpeg_bytes(b)


def test_jpeg_scrub_reflects_header_changes() -> None:
    """A change in a header segment (pre-SOS) must change the scrubbed bytes."""
    a = _fake_jpeg(b"scan", header_extra=b"\xff\xe0\x00\x06JFIF")
    b = _fake_jpeg(b"scan", header_extra=b"\xff\xe0\x00\x06EXIF")
    assert scrub.scrub_jpeg_bytes(a) != scrub.scrub_jpeg_bytes(b)


def test_jpeg_scrub_normalises_header_uuid() -> None:
    """A UUID embedded in the header (EXIF) is replaced by the placeholder."""
    raw = _fake_jpeg(b"scan", header_extra=b"\xff\xe1\x00\x2cExif\x00\x00" + _REAL_UUID)
    out = scrub.scrub_jpeg_bytes(raw)
    assert _REAL_UUID not in out
    assert scrub.UUID_PLACEHOLDER.encode("ascii") in out


def test_jpeg_scrub_no_sos_returns_uuid_scrubbed_whole() -> None:
    """With no SOS marker, the whole input is retained (UUIDs still scrubbed)."""
    raw = b"\xff\xd8 header only " + _REAL_UUID
    out = scrub.scrub_jpeg_bytes(raw)
    assert out.startswith(b"\xff\xd8 header only ")
    assert _REAL_UUID not in out


def test_jpeg_scrub_idempotent() -> None:
    raw = _fake_jpeg(b"scan", header_extra=b"\xff\xe1\x00\x2cExif" + _REAL_UUID)
    once = scrub.scrub_jpeg_bytes(raw)
    assert scrub.scrub_jpeg_bytes(once) == once


def test_jpeg_scrub_normalises_exif_usercomment_entropy() -> None:
    """Pixel-derived image-entropy in the EXIF:UserComment JSON is neutralised.

    marimba embeds the iFDO ImageData as JSON in the header; its image-entropy
    is a numpy float that jitters in the low-order digits across runner CPUs.
    Two headers differing only in that value must scrub identically.
    """
    a = _fake_jpeg(b"scan", header_extra=b'\xff\xe1{"image-entropy": 6.257750034332275, "x": 1}')
    b = _fake_jpeg(b"scan", header_extra=b'\xff\xe1{"image-entropy": 6.257750034332999, "x": 1}')
    assert scrub.scrub_jpeg_bytes(a) == scrub.scrub_jpeg_bytes(b)
    assert b'"image-entropy": 0.0' in scrub.scrub_jpeg_bytes(a)


def test_jpeg_scrub_normalises_exif_usercomment_average_color() -> None:
    """Pixel-derived image-average-color in the EXIF:UserComment JSON is neutralised."""
    a = _fake_jpeg(b"scan", header_extra=b'\xff\xe1{"image-average-color": [16, 52, 68], "x": 1}')
    b = _fake_jpeg(b"scan", header_extra=b'\xff\xe1{"image-average-color": [17, 51, 69], "x": 1}')
    assert scrub.scrub_jpeg_bytes(a) == scrub.scrub_jpeg_bytes(b)
    assert b'"image-average-color": [0, 0, 0]' in scrub.scrub_jpeg_bytes(a)


def test_yaml_scrub_normalises_entropy() -> None:
    """Differing image-entropy values collapse to the same scrubbed text."""
    a = "    image-entropy: 5.15189790725708\n"
    b = "    image-entropy: 5.15189790725712\n"
    assert scrub.scrub_yaml_text(a) == scrub.scrub_yaml_text(b)
    assert f"image-entropy: {scrub.ENTROPY_PLACEHOLDER}" in scrub.scrub_yaml_text(a)


def test_yaml_scrub_normalises_average_color_block() -> None:
    """Differing image-average-color block sequences collapse identically."""
    a = "    image-average-color:\n    - 11\n    - 36\n    - 60\n    next-field: x\n"
    b = "    image-average-color:\n    - 12\n    - 35\n    - 61\n    next-field: x\n"
    sa = scrub.scrub_yaml_text(a)
    assert sa == scrub.scrub_yaml_text(b)
    assert f"image-average-color: {scrub.AVERAGE_COLOR_PLACEHOLDER}" in sa
    # The following field is untouched.
    assert "next-field: x" in sa


def test_yaml_scrub_preserves_unrelated_fields() -> None:
    text = "    image-entropy: 6.5\n    image-altitude-meters: 1234.5\n"
    out = scrub.scrub_yaml_text(text)
    assert "image-altitude-meters: 1234.5" in out


def test_yaml_scrub_idempotent() -> None:
    text = "    image-average-color:\n    - 11\n    - 36\n    - 60\n    image-entropy: 6.5\n"
    once = scrub.scrub_yaml_text(text)
    assert scrub.scrub_yaml_text(once) == once
