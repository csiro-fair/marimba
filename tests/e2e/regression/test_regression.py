"""End-to-end regression tests for the mritc-demo dataset processing pipeline.

Three independently-runnable tiers (cheap -> expensive):

- `test_tier_a_structural`  — the dataset's directory layout, file presence,
  and basic parsability match expectations. Fast feedback on gross breakage.
- `test_tier_b_inventory`   — structural totals (file counts by class, per-
  pipeline/collection/kind counts, manifest line count) match
  `golden/invariants.json`. Catches drift in counts without requiring byte
  equality.
- `test_tier_c_scrubbed_manifest_byte_equality` — the per-file content of
  every dataset file matches the golden `manifest.scrubbed.txt` after
  scrub.py normalises the known volatile fields (per-image UUIDs, dataset
  UUID, cascaded image hashes). The strongest check; if this passes nothing
  in the dataset has drifted.

Each test drives a fresh `packaged_dataset` fixture (~30s warm-cache,
~60s cold-cache), so running all three from a clean tree is ~90s. To debug a
single failure, invoke pytest with `-k tier_a` / `-k tier_b` / `-k tier_c`.

When a test fails legitimately (a deliberate marimba or pipeline change
shifted the output), regenerate the goldens per
`tests/e2e/regression/golden/README.md`.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.e2e.regression.conftest import PACKAGED_DATASET_NAME
from tests.e2e.regression.golden.regenerate import collect_inventory
from tests.e2e.regression.scrub import rebuild_manifest

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
INVARIANTS_GOLDEN = GOLDEN_DIR / "invariants.json"
MANIFEST_GOLDEN = GOLDEN_DIR / "manifest.scrubbed.txt"


pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.network]


@pytest.mark.e2e
def test_tier_a_structural(packaged_dataset: tuple[Path, dict[str, float]]) -> None:
    """Dataset layout, top-level files, manifest + summary parsable, every YAML loads.

    Cheapest tier: catches missing files, malformed YAML, and gross layout
    regressions without comparing values to a golden.
    """
    dataset_dir, _timings = packaged_dataset

    assert dataset_dir.is_dir(), f"packaged dataset directory missing: {dataset_dir}"
    assert dataset_dir.name == PACKAGED_DATASET_NAME, f"unexpected dataset name: {dataset_dir.name}"

    # Required top-level files / directories.
    for required_file in ("manifest.txt", "summary.md", "ifdo.yml", "MRITC.ifdo.yml", "map.png", "provenance.json"):
        target = dataset_dir / required_file
        assert target.is_file(), f"missing required dataset file: {required_file}"
        assert target.stat().st_size > 0, f"required dataset file is empty: {required_file}"

    for required_dir in ("data", "logs", "pipelines"):
        target = dataset_dir / required_dir
        assert target.is_dir(), f"missing required dataset directory: {required_dir}"

    # data/MRITC/ is the canonical pipeline-output subtree.
    mritc_data = dataset_dir / "data" / "MRITC"
    assert mritc_data.is_dir(), "data/MRITC/ output subtree missing"

    # manifest.txt is parsable as <path>:<sha256> lines. Marimba's manifest
    # covers both files and directories — the directory hash is a stable
    # signature of the subtree's contents (~56 of 1680 entries are dirs at
    # current dataset shape).
    manifest_text = (dataset_dir / "manifest.txt").read_text(encoding="utf-8")
    manifest_lines = [ln for ln in manifest_text.splitlines() if ln.strip() and not ln.startswith("#")]
    assert manifest_lines, "manifest.txt is empty"
    for line in manifest_lines:
        path_part, _, hash_part = line.rpartition(":")
        assert path_part, f"manifest line missing path: {line!r}"
        assert len(hash_part) == 64, f"manifest line hash not 64 chars: {line!r}"
        assert all(c in "0123456789abcdef" for c in hash_part), f"manifest line hash not lowercase hex: {line!r}"
        # Manifest paths are dataset-relative; entry must resolve to a file or directory.
        target = dataset_dir / path_part
        assert target.exists(), f"manifest references missing path: {path_part}"

    # Every YAML at the dataset root must be valid YAML.
    for yml in dataset_dir.glob("*.yml"):
        try:
            yaml.safe_load(yml.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:  # pragma: no cover - regression-only
            pytest.fail(f"YAML file failed to parse: {yml.name} -> {exc}")

    # Per-collection YAML files exist for the 10 source collections.
    expected_yaml_stems = {
        f"IN2018_V06_{n}.MRITC" for n in ("025", "026", "045", "057", "060", "064", "114", "119", "128", "168")
    }
    actual_yaml_stems = {p.stem for p in dataset_dir.glob("*.MRITC.ifdo.yml")}
    actual_yaml_stems = {s.removesuffix(".ifdo") for s in actual_yaml_stems}
    missing = expected_yaml_stems - actual_yaml_stems
    assert not missing, f"missing per-collection MRITC ifdo YAMLs: {sorted(missing)}"

    # summary.md is non-trivial — minimum sanity check on length.
    summary_text = (dataset_dir / "summary.md").read_text(encoding="utf-8")
    assert "Dataset Summary" in summary_text or "Summary" in summary_text, "summary.md has no recognisable heading"
    assert len(summary_text) > 1000, f"summary.md is suspiciously short: {len(summary_text)} chars"

    # provenance.json is valid PROV-O JSON-LD with the dataset entity, the packaging activity, and Marimba.
    provenance = json.loads((dataset_dir / "provenance.json").read_text(encoding="utf-8"))
    assert "@context" in provenance, "provenance.json missing JSON-LD @context"
    assert "@graph" in provenance, "provenance.json missing JSON-LD @graph"
    graph = provenance["@graph"]
    assert any(n.get("@type") == "prov:Activity" for n in graph), "provenance.json has no prov:Activity"
    assert any(
        n.get("@id") == "#marimba" and "schema:softwareVersion" in n for n in graph
    ), "provenance.json has no Marimba SoftwareAgent with a version"

    # Each pipeline software agent records a 40-hex git commit (guards the git-provenance capture).
    pipeline_agents = [n for n in graph if str(n.get("@id", "")).startswith("#pipeline-")]
    assert pipeline_agents, "provenance.json has no pipeline SoftwareAgent"
    for agent in pipeline_agents:
        commit = agent.get("marimba:commit", "")
        assert len(commit) == 40, f"pipeline agent commit is not a 40-char sha: {commit!r}"
        assert all(c in "0123456789abcdef" for c in commit), f"pipeline commit not lowercase hex: {commit!r}"

    # The dataset entity is present and identified by its image-set UUID.
    assert any(
        n.get("@type") == "prov:Entity" and str(n.get("@id", "")).startswith("urn:uuid:") for n in graph
    ), "provenance.json has no dataset prov:Entity"


@pytest.mark.e2e
def test_tier_b_inventory(packaged_dataset: tuple[Path, dict[str, float]]) -> None:
    """Structural totals match golden/invariants.json (counts only, not content)."""
    dataset_dir, _timings = packaged_dataset

    expected: dict[str, Any] = json.loads(INVARIANTS_GOLDEN.read_text(encoding="utf-8"))
    actual_inventory = collect_inventory(dataset_dir)

    assert (
        expected["dataset_name"] == dataset_dir.name
    ), f"dataset name drift: golden={expected['dataset_name']} actual={dataset_dir.name}"

    expected_inv = expected["inventory"]
    # Walk the inventory sub-dicts independently so a failure points at the
    # exact category that drifted rather than dumping the full mismatch.
    for key in (
        "total_files",
        "by_class",
        "top_level_files",
        "top_level_dir_file_counts",
        "per_pipeline_collection_kind_counts",
    ):
        assert key in expected_inv, f"golden inventory missing key: {key}"
        assert key in actual_inventory, f"actual inventory missing key: {key}"
        assert (
            actual_inventory[key] == expected_inv[key]
        ), f"inventory drift at '{key}'\n  expected: {expected_inv[key]}\n  actual:   {actual_inventory[key]}"

    # Manifest line count is part of invariants — a regression in marimba's
    # manifest emission (e.g. dropping a file) surfaces here even if every
    # remaining line still hashes correctly.
    manifest_lines = (dataset_dir / "manifest.txt").read_text(encoding="utf-8").splitlines()
    assert (
        len(manifest_lines) == expected["manifest_line_count"]
    ), f"manifest line count drift: golden={expected['manifest_line_count']} actual={len(manifest_lines)}"


@pytest.mark.e2e
@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Tier C byte-equality golden is generated on Linux; exiftool/ffmpeg embed "
    "platform- and version-specific EXIF bytes, so the JPEG bytes and the iFDO "
    "metadata extracted from them diverge on other platforms even after scrubbing. "
    "Tier A (structure) and Tier B (inventory) remain cross-platform.",
)
def test_tier_c_scrubbed_manifest_byte_equality(packaged_dataset: tuple[Path, dict[str, float]]) -> None:
    """Scrubbed manifest of the fresh run byte-matches golden/manifest.scrubbed.txt.

    This is the load-bearing regression assertion: every non-log dataset file
    contributes its scrubbed sha256 to the manifest, so byte-equality of the
    scrubbed manifest implies byte-equality of every non-log dataset file
    after the known volatile fields are normalised. Drift surfaces line-by-
    line so failures point at the exact file(s) that changed.
    """
    dataset_dir, _timings = packaged_dataset

    expected_text = MANIFEST_GOLDEN.read_text(encoding="utf-8")
    actual_text = rebuild_manifest(
        (dataset_dir / "manifest.txt").read_text(encoding="utf-8"),
        dataset_dir,
    )

    expected_sha = hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
    actual_sha = hashlib.sha256(actual_text.encode("utf-8")).hexdigest()

    if expected_sha != actual_sha:
        # Produce a focused diff: report the first N lines that differ rather
        # than dumping the full 1680-line manifest into pytest's output.
        expected_lines = expected_text.splitlines()
        actual_lines = actual_text.splitlines()
        diffs: list[str] = []
        for i, (a, b) in enumerate(zip(expected_lines, actual_lines, strict=False)):
            if a != b:
                diffs.append(f"  line {i + 1}:\n    expected: {a}\n    actual:   {b}")
            if len(diffs) >= 20:
                total_differing = sum(1 for a, b in zip(expected_lines, actual_lines, strict=False) if a != b)
                diffs.append(f"  ... ({total_differing} total differing lines)")
                break
        len_msg = ""
        if len(expected_lines) != len(actual_lines):
            len_msg = f"\nline-count differs: expected={len(expected_lines)} actual={len(actual_lines)}"
        pytest.fail(
            f"scrubbed manifest byte-mismatch:\n"
            f"  expected sha: {expected_sha}\n"
            f"  actual sha:   {actual_sha}{len_msg}\n"
            f"first differing lines:\n" + "\n".join(diffs),
        )
