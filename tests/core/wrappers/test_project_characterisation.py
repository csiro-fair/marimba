"""Characterisation-of-absence tests for ProjectWrapper.

These tests pin gaps surfaced by the testing-review cycle 01 audit (P1-10
concurrent-import collision, P2-9 corrupted-project recovery). They are
xfailed today because the production paths they exercise either lack the
behaviour the test asserts (concurrent-import locking) or surface
ambiguous error states (partial-.marimba teardown). When the production
fixes land, remove the `pytest.mark.xfail` markers so the tests become
regular pinning tests.

Marker reason format follows docs/prompts/testing-review.md:
`characterisation-of-absence: <one-line description>`.
"""

import threading
from pathlib import Path

import pytest

from marimba.core.wrappers.project import ProjectWrapper


class TestProjectWrapperConcurrentImport:
    """P1-10 characterisation.

    Concurrent `marimba import` invocations may corrupt project state because
    ProjectWrapper takes no inter-process lock around its mutating operations.
    """

    @pytest.mark.unit
    @pytest.mark.xfail(
        reason="characterisation-of-absence: P1-10 — ProjectWrapper has no lock against "
        "concurrent imports/creates; this test pins the lack of locking. Flip from xfail "
        "to xpass-then-pass when locking lands.",
        strict=False,
    )
    def test_concurrent_create_collection_does_not_race(self, tmp_path: Path) -> None:
        """Two threads each creating distinct collections must not race-corrupt project state."""
        project = ProjectWrapper.create(tmp_path / "proj")

        errors: list[Exception] = []

        def create_collection(name: str) -> None:
            try:
                project.create_collection(name, config={"name": name})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=create_collection, args=(f"col{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent collection creation raised: {errors}"
        # All four collections should have landed and be visible after load.
        reloaded = ProjectWrapper(project.root_dir)
        assert len(reloaded.collection_wrappers) == 4


class TestProjectWrapperPartialMarimbaDir:
    """P2-9 characterisation.

    A project whose .marimba/ directory is partially deleted should raise a clear,
    actionable error on load — not silently load with incomplete state.
    """

    @pytest.mark.unit
    @pytest.mark.xfail(
        reason="characterisation-of-absence: P2-9 — ProjectWrapper._check_file_structure "
        "only verifies the root dir exists; a missing or partial .marimba/ subdirectory "
        "is not caught at load time and surfaces as a confusing failure later. Flip when "
        "structure validation tightens.",
        strict=False,
    )
    def test_load_raises_clear_error_when_marimba_dir_missing(self, tmp_path: Path) -> None:
        """Loading a project whose .marimba/ dir was deleted should raise InvalidStructureError."""
        project = ProjectWrapper.create(tmp_path / "proj")
        # Simulate corruption: delete the .marimba directory.
        marimba_dir = project.root_dir / ".marimba"
        if marimba_dir.exists():
            import shutil

            shutil.rmtree(marimba_dir)

        with pytest.raises(ProjectWrapper.InvalidStructureError, match=r"\.marimba"):
            ProjectWrapper(project.root_dir)
