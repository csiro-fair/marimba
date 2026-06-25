"""Unit tests for the regression-harness scrubbers.

Fast (no cache, no network, no marimba bootstrap) checks that the scrubbers
normalise exactly the volatile fields they target and leave everything else
intact. The load-bearing property here is CPU-invariance: the pixel-derived
iFDO fields must not contribute to the scrubbed manifest, and JPEG content
(whose bytes vary across CPU microarchitectures) is excluded outright, so two
functionally-identical runs on different runners produce the same scrubbed
manifest (see scrub.py drift source 5).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.regression import scrub

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("name", ["a.jpg", "a.JPG", "x/y/img.jpeg", "data/MRITC/c/images/f.JPG"])
def test_jpeg_content_excluded_from_comparison(name: str) -> None:
    """JPEG bytes vary across runner CPUs, so they are treated as volatile."""
    assert scrub.has_volatile_content(Path(name)) is True


@pytest.mark.parametrize("name", ["a.yml", "a.csv", "video.mp4", "metadata.yaml"])
def test_non_jpeg_non_log_content_is_compared(name: str) -> None:
    """Deterministic file types are still byte-compared (not volatile)."""
    assert scrub.has_volatile_content(Path(name)) is False


def test_map_png_and_logs_remain_volatile() -> None:
    """The pre-existing carve-outs still hold alongside the JPEG exclusion."""
    assert scrub.has_volatile_content(Path("map.png")) is True
    assert scrub.has_volatile_content(Path("logs/run.log")) is True


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


def test_marimba_version_scrub_normalises_curation_sentence() -> None:
    """Differing Marimba versions in the iFDO curation sentence collapse identically."""
    a = '{"image-set-header": {"image-curation-protocol": "Packaged with Marimba v1.2.0 (https://x)."}}'
    b = '{"image-set-header": {"image-curation-protocol": "Packaged with Marimba v1.3.1 (https://x)."}}'
    assert scrub.scrub_json_text(a) == scrub.scrub_json_text(b)
    assert scrub.MARIMBA_VERSION_PLACEHOLDER in scrub.scrub_json_text(a)
    # A pre-release/build suffix is also normalised.
    c = '{"image-set-header": {"image-curation-protocol": "Packaged with Marimba v1.2.0-rc1 (https://x)."}}'
    assert scrub.scrub_json_text(c) == scrub.scrub_json_text(a)


def test_marimba_version_scrub_normalises_summary_row() -> None:
    """Differing Marimba versions in the summary.md row collapse identically."""
    a = "| Marimba Version | 1.2.0 |\n"
    b = "| Marimba Version | 1.3.1 |\n"
    assert scrub.scrub_markdown_text(a) == scrub.scrub_markdown_text(b)
    assert "1.2.0" not in scrub.scrub_markdown_text(a)


def test_marimba_version_scrub_idempotent() -> None:
    text = '{"x": "built with Marimba v9.9.9 (https://x)."}'
    once = scrub.scrub_json_text(text)
    assert scrub.scrub_json_text(once) == once
