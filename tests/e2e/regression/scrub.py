"""Scrubbers that normalise non-deterministic fields in marimba's packaged output.

A determinism characterisation (two independent identical-input bootstraps,
per-file diff; method preserved in golden/README.md §"Scrubber correctness")
identified three drift sources between two identical-input runs:

1. Per-image UUID generated fresh by `marimba process`. Embedded in each JPEG
   in two EXIF locations: the standalone `ImageUniqueID` tag and inside the
   JSON blob written to `UserComment`. Same UUID in both places.
2. Per-dataset `image-set-uuid` generated fresh by `marimba package`. Appears
   once per per-collection ifdo YAML and once in the dataset-level rollup.
3. `image-hash-sha256` field in each per-image YAML entry: an indirect
   consequence of (1) — the image bytes differ, so their SHA differs.

Plus two further drift sources identified after the initial harness landed:

4. `Creation Date` row in `summary.md`, populated via `datetime.now()` by
   `marimba/core/utils/summary.py`. Drifts across runs on different days
   even with identical inputs.
5. Pixel-derived values that vary across CPU microarchitectures, not across
   runs. numpy's SIMD reductions and libjpeg's encoder round differently
   between CPU generations, so on GitHub's mixed hosted-runner fleet two
   functionally identical runs produce different bytes for a handful of
   borderline frames. This surfaces in several places inside each JPEG — the
   main entropy-coded scan, the embedded EXIF thumbnail's own scan, and the
   `image-entropy` (Shannon entropy of the pixel histogram) and
   `image-average-color` (pixel mean) values that `marimba/core/schemas/ifdo.py`
   computes and bakes into both the iFDO YAML and the EXIF:UserComment JSON.

JPEG content has no stable byte subset to hash, so it is excluded from byte
comparison entirely (see `has_volatile_content`); the image's metadata is still
validated through its scrubbed per-image YAML. Every other dataset file is
byte-deterministic, including videos, CSVs, `metadata.yml`, and the pipeline
source copy under `pipelines/`. Logs are excluded from comparison entirely
(timestamps, durations).

Scrubber strategy:

- `scrub_yaml_text` replaces each known volatile field value (the UUIDs, the
  cascaded image hash, and the pixel-derived `image-entropy` /
  `image-average-color`) with fixed-shape placeholders.
- `scrubbed_hash` dispatches by suffix and returns the SHA256 of the scrubbed
  content (or of the raw bytes if no scrubber applies). JPEGs never reach it —
  `rebuild_manifest` placeholders them first via `has_volatile_content`.
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

# Placeholders for the pixel-derived iFDO fields. Their low-order digits drift
# across CPU microarchitectures (see drift source 5 in the module docstring),
# so the values are normalised away rather than compared.
ENTROPY_PLACEHOLDER = "0.0"
AVERAGE_COLOR_PLACEHOLDER = "[0, 0, 0]"

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
    (
        re.compile(r"(?P<lead>^[ \t]*)image-entropy:[ \t]*(?P<value>[-+0-9.eE]+)[ \t]*$", re.MULTILINE),
        f"\\g<lead>image-entropy: {ENTROPY_PLACEHOLDER}",
    ),
    # image-average-color serialises as a block sequence whose `- <int>` items
    # share the key's indentation (yaml.safe_dump default). Match the key line
    # plus its same-indent items and collapse to a fixed flow-style placeholder.
    (
        re.compile(
            r"(?P<lead>^[ \t]*)image-average-color:[ \t]*\n(?:(?P=lead)-[ \t]+-?\d+[ \t]*\n)+",
            re.MULTILINE,
        ),
        f"\\g<lead>image-average-color: {AVERAGE_COLOR_PLACEHOLDER}\n",
    ),
)

# Suffixes for which we apply text-based YAML scrubbing.
_YAML_SUFFIXES = frozenset({".yml", ".yaml"})

# JPEG suffixes. Their content is excluded from byte comparison entirely (see
# has_volatile_content) because the encoded pixels — main scan, the embedded
# EXIF thumbnail's own scan, and the numpy-derived image-entropy / image-
# average-color baked into EXIF:UserComment — all drift across CPU
# microarchitectures on GitHub's mixed runner fleet (drift source 5).
_JPEG_SUFFIXES = frozenset({".jpg", ".jpeg"})

# Suffixes for which we apply markdown scrubbing. summary.md is the only
# dataset-level .md file and embeds today's date in the Creation Date row
# (marimba/core/utils/summary.py uses datetime.now() to populate it), so its
# bytes drift across runs on different days even with identical inputs.
_MARKDOWN_SUFFIXES = frozenset({".md"})

# Patterns for markdown scrubbing. Each match's value is replaced by a fixed
# placeholder. Length-preserving is not required — the test compares hashes.
_MARKDOWN_FIELD_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?P<lead>^\|\s*Creation Date\s*\|)[^|]*(?P<trail>\|)", re.MULTILINE),
        r"\g<lead> CREATION_DATE_PLACEHOLDER \g<trail>",
    ),
)

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


def scrub_markdown_text(text: str) -> str:
    """Replace volatile markdown field values with placeholders.

    Currently normalises the `Creation Date` row in summary.md, which marimba
    populates with `datetime.now()` at packaging time.

    Idempotent: scrubbing already-scrubbed text returns the same text.
    """
    for pattern, replacement in _MARKDOWN_FIELD_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def is_log_path(rel_path: Path) -> bool:
    """True if this dataset-relative path is a log file."""
    if rel_path.suffix.lower() in _LOG_SUFFIXES:
        return True
    return bool(rel_path.parts) and rel_path.parts[0] == "logs"


def has_volatile_content(rel_path: Path) -> bool:
    """True if this file's bytes vary unpredictably across runs.

    Used by `rebuild_manifest` to placeholder the hash slot for files whose
    content can't be reproduced from project state alone. Currently:

    - Log files (`logs/...` and `.log` suffix): timestamps, durations,
      ordering under parallelism.
    - `map.png` at the dataset root: staticmap renders by fetching tiles
      from a remote OSM-backed server, so the bytes change over wall-clock
      time as upstream tile data updates. Same project state -> different
      bytes a day later. Tier A still asserts the file exists and is a
      valid non-empty PNG; this just excludes it from byte-level comparison.
    - JPEG images (`.jpg` / `.jpeg`): their encoded bytes drift across CPU
      microarchitectures on GitHub's mixed hosted-runner fleet — the main
      entropy-coded scan, the embedded EXIF thumbnail's own scan, and the
      numpy-derived image-entropy / image-average-color baked into the
      EXIF:UserComment JSON all vary in their low-order bits. There is no
      stable byte subset to hash, so the content is excluded. The image's
      metadata is still byte-validated via its per-image iFDO YAML (which is
      scrubbed for the same volatile fields), and Tier A/B assert presence,
      validity, and counts.

    Distinct from `is_log_path` — the latter is also consumed by the
    inventory classifier in `golden/regenerate.py` to bucket logs as a file
    class, where `map.png` is correctly classified as an image.
    """
    if is_log_path(rel_path):
        return True
    if rel_path.suffix.lower() in _JPEG_SUFFIXES:
        return True
    return rel_path == Path("map.png")


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

    if suffix in _MARKDOWN_SUFFIXES:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return hashlib.sha256(raw).hexdigest()
        return hashlib.sha256(scrub_markdown_text(text).encode("utf-8")).hexdigest()

    return hashlib.sha256(raw).hexdigest()


# Marimba's manifest line format: "<relative-path>:<sha256-hex>\n". The
# splitter must be conservative: paths can in principle contain `:` (they
# don't here, but rsplit guards against it).
_MANIFEST_LINE_RE = re.compile(r"^(?P<path>.+):(?P<hash>[0-9a-f]{64})\s*$")


def rebuild_manifest(manifest_text: str, dataset_root: Path) -> str:
    """Re-hash every file referenced by `manifest_text` using `scrubbed_hash`.

    Preserves the line ordering of the input manifest, so a regression in
    marimba's manifest sort order surfaces as a byte diff against the golden.
    Entries with volatile content (per `has_volatile_content`: log files
    and `map.png`) keep their relative-path slot but have their hash
    replaced with `SHA256_PLACEHOLDER` — those files' bytes can't be
    reproduced from project state alone, but the presence + path +
    ordering of the manifest entry is still load-bearing. Malformed lines
    and lines pointing at a missing file are passed through unchanged so
    the comparison fails loudly rather than silently swallowing the drift.
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
        if has_volatile_content(rel):
            out.append(f"{rel}:{SHA256_PLACEHOLDER}")
        else:
            out.append(f"{rel}:{scrubbed_hash(target)}")
    return "\n".join(out) + ("\n" if manifest_text.endswith("\n") else "")
