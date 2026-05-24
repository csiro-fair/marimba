"""Phase-1 smoke test for the mritc-demo regression harness.

Confirms the cache + import path works end-to-end before any tiered comparison
logic lands. Runs once on a fresh project per pytest invocation; subsequent
phases will add process + package + tiered verification on top.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.e2e.regression.conftest import (
    COLLECTION_PREFIX,
    COLLECTION_SOURCE_DIRS,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.e2e.regression.conftest import MarimbaRunner


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.network
def test_smoke_cache_and_import(
    marimba_run: MarimbaRunner,
    cached_data: Path,
    cached_pipeline: Path,
) -> None:
    """`new project` + `new pipeline` + 10x `import` succeeds against cached data.

    Phase-1 acceptance: every step exits zero, every collection lands its
    pipeline-named subdirectory under collections/<name>/MRITC/. Validates
    the cache plumbing and import-path before later phases add process /
    package / tiered verification.
    """
    project_dir = marimba_run.project_dir

    # Sanity: pinned caches resolved to real directories with the expected
    # top-level entries (10 collection dirs in data; pipeline.py in pipeline).
    assert cached_data.is_dir(), "cached_data fixture must materialise a directory"
    assert cached_pipeline.is_dir(), "cached_pipeline fixture must materialise a directory"
    for src in COLLECTION_SOURCE_DIRS:
        assert (cached_data / src).is_dir(), f"missing source collection dir: {src}"

    marimba_run.new_project()
    assert (project_dir / ".marimba").is_dir(), "marimba new project did not populate .marimba/"
    assert (project_dir / "collections").is_dir()
    assert (project_dir / "pipelines").is_dir()
    assert (project_dir / "datasets").is_dir()

    marimba_run.new_pipeline("MRITC", cached_pipeline)
    pipeline_dir = project_dir / "pipelines" / "MRITC"
    assert pipeline_dir.is_dir(), "MRITC pipeline did not install"
    assert (pipeline_dir / "repo").is_dir(), "pipeline repo subdirectory missing"

    for src in COLLECTION_SOURCE_DIRS:
        name = f"{COLLECTION_PREFIX}{src}"
        marimba_run.import_collection(name=name, source=cached_data / src)
        coll_dir = project_dir / "collections" / name
        assert coll_dir.is_dir(), f"collection {name} did not land"
        # Pipeline-named subdir is the marimba per-pipeline import target.
        assert (coll_dir / "MRITC").is_dir(), f"collection {name} missing MRITC/ pipeline-target subdir"
