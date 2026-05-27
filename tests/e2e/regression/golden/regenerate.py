#!/usr/bin/env python3
# ruff: noqa: T201, EXE001, PLR0911
r"""Regenerate the golden snapshot from a packaged marimba dataset.

Run this whenever a deliberate marimba or pipeline change rotates the
expected output. Surface the diff in your commit so reviewers can confirm
the rotation is intended.

Usage:
    uv run python tests/e2e/regression/golden/regenerate.py <dataset-dir>

    # Typical: regenerate from a fresh fixture run
    uv run pytest --rootdir . -c config/pytest.ini --no-cov \
        tests/e2e/regression/test_smoke.py
    # ... then drive a full bootstrap (Phase 4+ will provide a fixture for this)
    uv run python tests/e2e/regression/golden/regenerate.py \
        /path/to/project/datasets/IN2018_V06

Writes:
    tests/e2e/regression/golden/invariants.json
    tests/e2e/regression/golden/manifest.scrubbed.txt
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from tests.e2e.regression.scrub import (  # noqa: E402
    is_log_path,
    rebuild_manifest,
)

GOLDEN_DIR = Path(__file__).resolve().parent

# Suffix classification for the inventory section of invariants.json.
_MEDIA_IMAGE = frozenset({".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"})
_MEDIA_VIDEO = frozenset({".mp4", ".mov", ".avi", ".mkv", ".webm"})


def classify(rel: Path) -> str:
    suffix = rel.suffix.lower()
    if suffix in _MEDIA_IMAGE:
        return "image"
    if suffix in _MEDIA_VIDEO:
        return "video"
    if is_log_path(rel):
        return "log"
    if suffix in {".yml", ".yaml"}:
        return "yaml"
    if suffix == ".json":
        return "json"
    if suffix == ".md":
        return "markdown"
    if suffix == ".txt":
        return "text"
    if suffix == ".csv":
        return "csv"
    if suffix == ".py":
        return "python"
    if suffix == ".png":
        return "image"
    return "other"


def collect_inventory(dataset_root: Path) -> dict[str, Any]:
    """Walk the dataset; produce structural counts (no per-file detail).

    Per-collection counts are keyed `<pipeline>/<collection>/<kind>` (e.g.
    `MRITC/IN2018_V06_025/images`) so adding a new pipeline output or a
    new per-collection subdirectory surfaces as a missing key rather than
    a silently-pooled total.
    """
    files = [p for p in dataset_root.rglob("*") if p.is_file()]
    rels = [p.relative_to(dataset_root) for p in files]

    by_class: dict[str, int] = {}
    files_at_root: dict[str, str] = {}  # path -> class, for the small handful of top-level files
    dir_file_count: dict[str, int] = {}  # top-level dir name -> file count under it
    per_pipeline_collection_kind: dict[str, int] = {}  # "<pipeline>/<collection>/<kind>" -> count

    for rel in rels:
        klass = classify(rel)
        by_class[klass] = by_class.get(klass, 0) + 1

        if len(rel.parts) == 1:
            files_at_root[str(rel)] = klass
        else:
            top = rel.parts[0]
            dir_file_count[top] = dir_file_count.get(top, 0) + 1

        # data/<pipeline>/<collection>/<kind>/...
        if len(rel.parts) >= 4 and rel.parts[0] == "data":
            _, pipeline, collection, kind, *_ = rel.parts
            key = f"{pipeline}/{collection}/{kind}"
            per_pipeline_collection_kind[key] = per_pipeline_collection_kind.get(key, 0) + 1

    return {
        "total_files": len(files),
        "by_class": dict(sorted(by_class.items())),
        "top_level_files": dict(sorted(files_at_root.items())),
        "top_level_dir_file_counts": dict(sorted(dir_file_count.items())),
        "per_pipeline_collection_kind_counts": dict(sorted(per_pipeline_collection_kind.items())),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dataset_dir",
        type=Path,
        help="Path to the packaged dataset directory (e.g. .../datasets/IN2018_V06).",
    )
    args = parser.parse_args()

    dataset_root: Path = args.dataset_dir.resolve()
    if not dataset_root.is_dir():
        print(f"ERROR: not a directory: {dataset_root}", file=sys.stderr)
        return 1

    manifest_path = dataset_root / "manifest.txt"
    if not manifest_path.is_file():
        print(f"ERROR: missing manifest.txt under {dataset_root}", file=sys.stderr)
        return 1

    scrubbed = rebuild_manifest(manifest_path.read_text(encoding="utf-8"), dataset_root)
    scrubbed_path = GOLDEN_DIR / "manifest.scrubbed.txt"
    scrubbed_path.write_text(scrubbed, encoding="utf-8")
    print(f"wrote {scrubbed_path} ({len(scrubbed.splitlines())} lines)")

    inventory = collect_inventory(dataset_root)
    invariants = {
        "dataset_name": dataset_root.name,
        "inventory": inventory,
        "manifest_scrubbed_sha256": hashlib.sha256(scrubbed.encode("utf-8")).hexdigest(),
        "manifest_line_count": len(scrubbed.splitlines()),
    }
    invariants_path = GOLDEN_DIR / "invariants.json"
    invariants_path.write_text(json.dumps(invariants, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {invariants_path}")

    # Print a one-line summary the operator can sanity-check against the run log.
    inv = inventory
    print(
        f"summary: total={inv['total_files']} "
        f"images={inv['by_class'].get('image', 0)} "
        f"videos={inv['by_class'].get('video', 0)} "
        f"yamls={inv['by_class'].get('yaml', 0)} "
        f"pipeline-collection-kinds={len(inv['per_pipeline_collection_kind_counts'])}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
