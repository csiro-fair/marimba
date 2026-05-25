"""Scrubbers that normalise non-deterministic fields in marimba's packaged output.

Phase 2 characterisation (see temp/phase2-determinism/REPORT.md, throwaway)
identified three drift sources between two identical-input runs:

1. Per-image UUID generated fresh by `marimba process`. Embedded in each JPEG
   in two EXIF locations: the standalone `ImageUniqueID` tag and inside the
   JSON blob written to `UserComment`. Same UUID in both places.
2. Per-dataset `image-set-uuid` generated fresh by `marimba package`. Appears
   once per per-collection ifdo YAML and once in the dataset-level rollup.
3. `image-hash-sha256` field in each per-image YAML entry: an indirect
   consequence of (1) — the image bytes differ, so their SHA differs.

Every other dataset file is byte-deterministic, including videos, CSVs, the
top-level summary.md / metadata.yml / MRITC.ifdo.yml, and the pipeline source
copy under `pipelines/`. Logs are excluded from comparison entirely (timestamps,
durations).

Scrubber strategy:

- `scrub_yaml_text` replaces each of the three known volatile field values with
  fixed-shape placeholders.
- `scrub_jpeg_bytes` regex-replaces every UUID-shaped substring with a fixed
  placeholder, preserving byte offsets (UUIDs are fixed-width 36 chars so EXIF
  length fields stay valid).
- `scrubbed_hash` dispatches by suffix and returns the SHA256 of the scrubbed
  content (or of the raw bytes if no scrubber applies).
- `rebuild_manifest` parses a marimba-generated manifest, re-hashes each
  referenced file via `scrubbed_hash`, and returns the rebuilt manifest text
  with the same line ordering as the original (so a regression in marimba's
  manifest sort order surfaces as a byte diff).
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

# Fixed-width 36-char placeholder UUID. Same shape as a real UUID v4 so byte
# offsets in EXIF / YAML stay stable.
UUID_PLACEHOLDER = "00000000-0000-4000-8000-000000000000"

# Fixed-width 64-char placeholder for SHA256 hex.
SHA256_PLACEHOLDER = "0" * 64

# Regex for a canonical hyphenated v4-shape UUID (8-4-4-4-12 lowercase hex).
# Marimba uses lowercase hex consistently in both YAML and embedded EXIF JSON.
_UUID_RE = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_UUID_RE_TEXT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# YAML scrubber operates on whole-field matches so we don't accidentally
# touch a UUID that happens to appear in (say) a free-form description.
# Each pattern captures `<lead><field>: <value>` and replaces only the value.
_YAML_FIELD_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?P<lead>^\s*)image-uuid:\s*(?P<value>[0-9a-f-]+)\s*$", re.MULTILINE),
        f"\\g<lead>image-uuid: {UUID_PLACEHOLDER}",
    ),
    (
        re.compile(r"(?P<lead>^\s*)image-set-uuid:\s*(?P<value>[0-9a-f-]+)\s*$", re.MULTILINE),
        f"\\g<lead>image-set-uuid: {UUID_PLACEHOLDER}",
    ),
    (
        re.compile(r"(?P<lead>^\s*)image-hash-sha256:\s*(?P<value>[0-9a-f]+)\s*$", re.MULTILINE),
        f"\\g<lead>image-hash-sha256: {SHA256_PLACEHOLDER}",
    ),
)

# Suffixes for which we apply text-based YAML scrubbing.
_YAML_SUFFIXES = frozenset({".yml", ".yaml"})

# Suffixes for which we apply JPEG byte scrubbing.
_JPEG_SUFFIXES = frozenset({".jpg", ".jpeg"})

# Files whose name (case-insensitive) we treat as logs and exclude from any
# comparison entirely. The dataset puts them all under `logs/` so the path
# prefix is sufficient, but the suffix check is a belt-and-braces guard.
_LOG_SUFFIXES = frozenset({".log"})


def scrub_yaml_text(text: str) -> str:
    """Replace the three known volatile YAML field values with placeholders.

    Idempotent: scrubbing already-scrubbed text returns the same text.
    """
    for pattern, replacement in _YAML_FIELD_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def scrub_jpeg_bytes(raw: bytes) -> bytes:
    """Replace every UUID-shaped substring in `raw` with UUID_PLACEHOLDER.

    Operates at byte level (no JPEG decode) so EXIF byte offsets stay valid.
    UUIDs and the placeholder are both exactly 36 ASCII chars.

    False-positive risk is negligible: a 36-byte sequence matching the v4-shape
    UUID regex requires hyphens at exactly positions 8/13/18/23 and lowercase
    hex chars everywhere else. Vanishingly unlikely to occur in JPEG entropy-
    coded image data by accident, and on the few occasions it does the rewrite
    is byte-length-preserving so the file stays a valid JPEG.
    """
    return _UUID_RE.sub(UUID_PLACEHOLDER.encode("ascii"), raw)


def is_log_path(rel_path: Path) -> bool:
    """True if this dataset-relative path should be excluded from comparison."""
    if rel_path.suffix.lower() in _LOG_SUFFIXES:
        return True
    return bool(rel_path.parts) and rel_path.parts[0] == "logs"


def scrubbed_hash(path: Path) -> str:
    """SHA256 of the file at `path` after suffix-appropriate scrubbing.

    Returns the unscrubbed SHA if no scrubber applies to this suffix. Caller
    is responsible for skipping log files via `is_log_path` before invoking.
    """
    suffix = path.suffix.lower()
    raw = path.read_bytes()

    if suffix in _YAML_SUFFIXES:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return hashlib.sha256(raw).hexdigest()
        return hashlib.sha256(scrub_yaml_text(text).encode("utf-8")).hexdigest()

    if suffix in _JPEG_SUFFIXES:
        return hashlib.sha256(scrub_jpeg_bytes(raw)).hexdigest()

    return hashlib.sha256(raw).hexdigest()


# Marimba's manifest line format: "<relative-path>:<sha256-hex>\n". The
# splitter must be conservative: paths can in principle contain `:` (they
# don't here, but rsplit guards against it).
_MANIFEST_LINE_RE = re.compile(r"^(?P<path>.+):(?P<hash>[0-9a-f]{64})\s*$")


def rebuild_manifest(manifest_text: str, dataset_root: Path) -> str:
    """Re-hash every file referenced by `manifest_text` using `scrubbed_hash`.

    Preserves the line ordering of the input manifest, so a regression in
    marimba's manifest sort order surfaces as a byte diff against the golden.
    Log entries (per `is_log_path`) keep their relative-path slot but have
    their hash replaced with `SHA256_PLACEHOLDER`: log content is inherently
    non-deterministic, but the presence + path + ordering of a log entry in
    the manifest is still load-bearing. Malformed lines and lines pointing at
    a missing file are passed through unchanged so the comparison fails
    loudly rather than silently swallowing the drift.
    """
    out: list[str] = []
    for line in manifest_text.splitlines():
        m = _MANIFEST_LINE_RE.match(line)
        if not m:
            out.append(line)
            continue
        rel = Path(m.group("path"))
        target = dataset_root / rel
        if not target.is_file():
            out.append(line)
            continue
        if is_log_path(rel):
            out.append(f"{rel}:{SHA256_PLACEHOLDER}")
        else:
            out.append(f"{rel}:{scrubbed_hash(target)}")
    return "\n".join(out) + ("\n" if manifest_text.endswith("\n") else "")
