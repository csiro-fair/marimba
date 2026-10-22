"""
Integration tests for resumable packaging.

Each test packages a small dataset of real JPEGs with iFDO metadata (so EXIF embedding, hashing and validation all run
for real), interrupts it inside one stage by making that stage raise, resumes it, and checks the result against an
uninterrupted reference run of the same inputs. Two runs of the same inputs differ only in ``logs/project.log`` and
``provenance.json``, which carry run timestamps, so those entries are excluded from the comparison.
"""

import random
import signal
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ifdo.models import ImageData
from PIL import Image

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.ifdo import iFDOMetadata
from marimba.core.utils import manifest as manifest_module
from marimba.core.utils import package_state as package_state_module
from marimba.core.utils.constants import MetadataGenerationLevelOptions, Operation
from marimba.core.utils.dataset import get_mapping_processor_decorator
from marimba.core.utils.hash import compute_hash
from marimba.core.utils.package_state import PackageStage, PackageState, mapping_destinations_digest
from marimba.core.utils.summary import ImagerySummary
from marimba.core.wrappers.dataset import DatasetWrapper
from marimba.core.wrappers.project import ProjectWrapper
from marimba.main import _close_packaging, _close_state_on_sigterm, _open_package_state

DATASET = "RESUME_TEST"
PIPELINE = "pipe"
IMAGE_COUNT = 6
FINGERPRINT = {"dataset": DATASET, "operation": "copy"}
VOLATILE_ENTRIES = {"logs/project.log", "provenance.json"}
FIRST_STAGE = PackageStage.POPULATE


class StageInterruptedError(RuntimeError):
    """Raised by a test to interrupt a packaging stage."""


@pytest.fixture(autouse=True)
def _no_map_tiles(monkeypatch: pytest.MonkeyPatch) -> None:
    # The dataset map fetches map tiles over the network; the stage itself still runs and is checkpointed.
    monkeypatch.setattr(DatasetWrapper, "_generate_dataset_map", lambda self, items, zoom: None)  # noqa: ARG005


@pytest.fixture
def source_dir(tmp_path: Path) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    rng = random.Random(0)  # noqa: S311 - reproducible test images, not cryptography
    for i in range(IMAGE_COUNT):
        image = Image.new("RGB", (64, 48))
        image.putdata([(rng.randrange(256), rng.randrange(256), rng.randrange(256)) for _ in range(64 * 48)])
        image.save(src / f"img_{i}.jpg")
    (src / "notes.txt").write_text("ancillary file without metadata")
    return src


def _mapping(
    source_dir: Path,
) -> dict[str, dict[str, dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]]]:
    """A fresh dataset mapping, as compose would build one (metadata objects are mutated while packaging)."""
    collection: dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]] = {}
    for i in range(IMAGE_COUNT):
        image_data = ImageData(
            image_datetime=datetime(2025, 1, 1, 0, 0, i, tzinfo=UTC),
            image_latitude=-27.0 - i / 1000,
            image_longitude=153.0,
        )
        collection[source_dir / f"img_{i}.jpg"] = (Path(f"C1/stills/img_{i}.jpg"), [iFDOMetadata(image_data)], None)
    collection[source_dir / "notes.txt"] = (Path("C1/notes.txt"), None, None)
    return {PIPELINE: {"C1": collection}}


def _package(
    project_dir: Path,
    source_dir: Path,
    *,
    checkpoint: bool = True,
    resume: bool = False,
    resume_from_stage: int | None = None,
    operation: Operation = Operation.copy,
) -> None:
    """Package the test dataset, checkpointed like the CLI unless ``checkpoint`` is False."""
    project = ProjectWrapper(project_dir)
    mapping = _mapping(source_dir)
    state = None
    if checkpoint:
        state = project.open_package_state(
            DATASET,
            FINGERPRINT,
            mapping_destinations_digest(mapping),
            resume=resume,
            resume_from_stage=resume_from_stage,
        )
    try:
        dataset = project.create_dataset(
            DATASET,
            mapping,
            [get_mapping_processor_decorator(MetadataGenerationLevelOptions.project)],
            [],
            operation=operation,
            max_workers=1,
            exif_chunk_size=2,
            package_state=state,
        )
        dataset.close()
    finally:
        if state is not None:
            state.close()


def _comparable_manifest(project_dir: Path) -> dict[str, str]:
    lines = (project_dir / "datasets" / DATASET / "manifest.txt").read_text().splitlines()
    entries = dict(line.rsplit(":", 1) for line in lines if line and not line.startswith("#"))
    return {path: digest for path, digest in entries.items() if path not in VOLATILE_ENTRIES}


@pytest.fixture
def reference(tmp_path: Path, source_dir: Path) -> dict[str, str]:
    """Comparable manifest of an uninterrupted, uncheckpointed run."""
    project_dir = tmp_path / "reference"
    ProjectWrapper.create(project_dir)
    _package(project_dir, source_dir, checkpoint=False)
    return _comparable_manifest(project_dir)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    path = tmp_path / "project"
    ProjectWrapper.create(path)
    return path


def _fail_after(monkeypatch: pytest.MonkeyPatch, owner: Any, name: str, successes: int) -> list[int]:
    """Make ``owner.name`` raise StageInterruptedError after ``successes`` successful calls; returns the call count."""
    original = getattr(owner, name)
    calls = [0]

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        calls[0] += 1
        if calls[0] > successes:
            raise StageInterruptedError(name)
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, name, wrapper)
    return calls


def _state_dir(project_dir: Path) -> Path:
    return project_dir / ".marimba" / "package" / DATASET


def _completed_stages(project_dir: Path) -> list[PackageStage]:
    state = PackageState.load(_state_dir(project_dir))
    try:
        return state.completed_stages
    finally:
        state.close()


INTERRUPTIONS: dict[str, Callable[[pytest.MonkeyPatch], object]] = {
    "populate": lambda mp: _fail_after(mp, DatasetWrapper, "_place_file", successes=3),
    "metadata_files": lambda mp: _fail_after(mp, iFDOMetadata, "_write_exif_batch", successes=1),
    "dataset_metadata": lambda mp: _fail_after(mp, package_state_module, "compute_hash", successes=3),
    "summary": lambda mp: _fail_after(mp, ImagerySummary, "from_dataset", successes=0),
    "manifest": lambda mp: _fail_after(mp, package_state_module, "compute_hash", successes=IMAGE_COUNT + 1),
}
INTERRUPTED_STAGE = {
    "populate": PackageStage.POPULATE,
    "metadata_files": PackageStage.PROCESS_METADATA_FILES,
    "dataset_metadata": PackageStage.DATASET_METADATA,
    "summary": PackageStage.SUMMARY,
    "manifest": PackageStage.MANIFEST,
}


@pytest.mark.integration
class TestResumeAfterInterruption:
    """A run interrupted inside a stage resumes to the same dataset as an uninterrupted run."""

    @pytest.mark.parametrize("where", list(INTERRUPTIONS))
    def test_resume_matches_uninterrupted_run(
        self,
        where: str,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            INTERRUPTIONS[where](mp)
            # The interruption surfaces directly or wrapped by the stage's worker pool
            with pytest.raises((StageInterruptedError, RuntimeError)):
                _package(project_dir, source_dir)

        # The state survives, records the stages before the interrupted one, and not the interrupted one
        completed = _completed_stages(project_dir)
        assert PackageStage(INTERRUPTED_STAGE[where]) not in completed
        assert all(stage in completed for stage in PackageStage if FIRST_STAGE <= stage < INTERRUPTED_STAGE[where])

        _package(project_dir, source_dir, resume=True)

        assert _comparable_manifest(project_dir) == reference
        assert not _state_dir(project_dir).exists()

    def test_linked_dataset_resumes_without_hard_link_prompt(
        self,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # With --operation link the interrupted run's own links make every source look hard-linked; the resume must
        # not stop to ask about them again (a non-interactive run cannot answer the prompt).
        with monkeypatch.context() as mp:
            _fail_after(mp, iFDOMetadata, "_write_exif_batch", successes=1)
            with pytest.raises((StageInterruptedError, RuntimeError)):
                _package(project_dir, source_dir, operation=Operation.link)
        assert (source_dir / "img_0.jpg").stat().st_nlink == 2

        def no_prompt(*args: object, **kwargs: object) -> None:  # noqa: ARG001
            pytest.fail("the hard-link prompt was shown on resume")

        monkeypatch.setattr("marimba.core.wrappers._safety.prompt_user_for_hard_link_continuation", no_prompt)
        _package(project_dir, source_dir, resume=True, operation=Operation.link)

        assert _comparable_manifest(project_dir) == reference

    def test_exiftool_leftovers_are_removed_before_resuming(
        self,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, iFDOMetadata, "_write_exif_batch", successes=1)
            with pytest.raises((StageInterruptedError, RuntimeError)):
                _package(project_dir, source_dir)

        # A kill during an ExifTool write leaves its temporary file, which blocks the next write to that image
        stills = project_dir / "datasets" / DATASET / "data" / PIPELINE / "C1" / "stills"
        (stills / "img_5.jpg_exiftool_tmp").write_bytes(b"partial")

        _package(project_dir, source_dir, resume=True)

        assert not (stills / "img_5.jpg_exiftool_tmp").exists()
        assert _comparable_manifest(project_dir) == reference

    def test_interrupted_metadata_stage_resumes_after_last_batch(
        self,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, iFDOMetadata, "_write_exif_batch", successes=2)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir)

        # Two batches of two images were embedded and journalled before the interruption
        state = PackageState.load(_state_dir(project_dir))
        try:
            assert len(state.metadata_files) == 4
        finally:
            state.close()

        calls = _fail_after(monkeypatch, iFDOMetadata, "_write_exif_batch", successes=100)
        _package(project_dir, source_dir, resume=True)

        assert calls[0] == 1, "only the one remaining batch is embedded on resume"
        assert _comparable_manifest(project_dir) == reference


@pytest.mark.integration
class TestHashReuse:
    """Hashes journalled before an interruption are reused, unless the file has changed."""

    def _hashed_paths(self, monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], list[str]]:
        """Record the files hashed while packaging, split into before and during validation."""
        before: list[str] = []
        during: list[str] = []
        validating = [False]
        real_compute_hash = compute_hash
        real_validate = DatasetWrapper.validate

        def recording_compute_hash(path: Path, root_dir: Path | None = None) -> str:
            (during if validating[0] else before).append(path.name)
            return real_compute_hash(path, root_dir)

        def flagging_validate(self: DatasetWrapper, *args: Any, **kwargs: Any) -> None:
            validating[0] = True
            real_validate(self, *args, **kwargs)

        monkeypatch.setattr(package_state_module, "compute_hash", recording_compute_hash)
        monkeypatch.setattr(DatasetWrapper, "validate", flagging_validate)
        return before, during

    def test_journalled_hashes_are_not_recomputed(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, ImagerySummary, "from_dataset", successes=0)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir)

        before, during = self._hashed_paths(monkeypatch)
        _package(project_dir, source_dir, resume=True)

        images = {f"img_{i}.jpg" for i in range(IMAGE_COUNT)}
        assert not images & set(before), "images hashed in stage 5 must come from the journal in stage 10"
        assert "notes.txt" in before
        assert images <= set(during), "validation reads every file regardless of the journal"

    def test_modified_file_is_rehashed(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, ImagerySummary, "from_dataset", successes=0)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir)

        touched = project_dir / "datasets" / DATASET / "data" / PIPELINE / "C1" / "stills" / "img_0.jpg"
        stat = touched.stat()
        touched.touch()
        assert touched.stat().st_mtime_ns != stat.st_mtime_ns

        before, _ = self._hashed_paths(monkeypatch)
        _package(project_dir, source_dir, resume=True)

        assert "img_0.jpg" in before
        assert "img_1.jpg" not in before

    def test_manifest_reuses_stage_five_hashes_without_a_journal(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[str] = []
        real = compute_hash

        def recording(path: Path, root_dir: Path | None = None) -> str:
            calls.append(path.name)
            return real(path, root_dir)

        monkeypatch.setattr(manifest_module, "compute_hash", recording)
        _package(project_dir, source_dir, checkpoint=False)

        # Manifest generation hashes only files without metadata; validation then hashes everything.
        images = [name for name in calls if name.startswith("img_")]
        assert len(images) == IMAGE_COUNT, "each image is hashed once, by validation, not again by the manifest"


@pytest.mark.integration
class TestResumeGuards:
    """Refusals that stop a resume from producing a dataset that mixes two different runs."""

    def _interrupt_at_summary(self, project_dir: Path, source_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, ImagerySummary, "from_dataset", successes=0)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir)

    def test_changed_inputs_are_refused(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._interrupt_at_summary(project_dir, source_dir, monkeypatch)
        project = ProjectWrapper(project_dir)
        mapping = _mapping(source_dir)
        del mapping[PIPELINE]["C1"][source_dir / "notes.txt"]

        with pytest.raises(ProjectWrapper.PackageResumeError) as excinfo:
            project.open_package_state(
                DATASET,
                {**FINGERPRINT, "operation": "link", "zoom": 9},
                mapping_destinations_digest(mapping),
                resume=True,
            )
        message = str(excinfo.value)
        assert "operation: 'copy' -> 'link'" in message
        assert "zoom: None -> 9" in message
        assert "destinations: 7 files -> 6 files" in message

        # The refused attempt released the lock, so a correct resume still works
        _package(project_dir, source_dir, resume=True)

    def test_fresh_run_refused_while_interrupted_run_waits(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._interrupt_at_summary(project_dir, source_dir, monkeypatch)
        with pytest.raises(ProjectWrapper.PackageResumeError, match="Resume it with --resume"):
            _package(project_dir, source_dir)

    def test_resume_without_state_is_refused(self, project_dir: Path, source_dir: Path) -> None:
        with pytest.raises(ProjectWrapper.PackageResumeError, match="No interrupted packaging run"):
            _package(project_dir, source_dir, resume=True)

    def test_concurrent_run_is_refused(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._interrupt_at_summary(project_dir, source_dir, monkeypatch)
        holder = PackageState.load(_state_dir(project_dir))
        try:
            with pytest.raises(ProjectWrapper.PackageResumeError, match="Another packaging run holds"):
                _package(project_dir, source_dir, resume=True)
        finally:
            holder.close()

    def test_stale_state_without_dataset_is_discarded(self, project_dir: Path, source_dir: Path) -> None:
        stale = PackageState.create(_state_dir(project_dir), {"dataset": "old"}, {})
        stale.close()
        _package(project_dir, source_dir)
        assert (project_dir / "datasets" / DATASET / "manifest.txt").is_file()
        assert not _state_dir(project_dir).exists()

    def test_successful_run_is_reported_complete(self, project_dir: Path, source_dir: Path) -> None:
        project = ProjectWrapper(project_dir)
        assert not project.is_dataset_packaged(DATASET)
        _package(project_dir, source_dir)
        assert project.is_dataset_packaged(DATASET)

    def test_dry_run_opens_no_state(self, project_dir: Path, source_dir: Path) -> None:
        project = ProjectWrapper(project_dir, dry_run=True)
        state = _open_package_state(
            project,
            DATASET,
            _mapping(source_dir),
            [],
            [PIPELINE],
            {},
            resume=False,
            resume_from_stage=None,
        )
        assert state is None
        assert not (project_dir / ".marimba" / "package").exists()


@pytest.mark.integration
class TestResumeFromStage:
    """Adopting runs whose progress was not recorded."""

    def _package_without_checkpoints_until_manifest(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An uncheckpointed run killed in stage 10, like one packaged before resumable packaging existed."""
        with monkeypatch.context() as mp:
            _fail_after(mp, DatasetWrapper, "_generate_manifest", successes=0)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir, checkpoint=False)
        assert not _state_dir(project_dir).exists()

    def test_adopts_uncheckpointed_run_and_finishes_it(
        self,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._package_without_checkpoints_until_manifest(project_dir, source_dir, monkeypatch)
        calls = _fail_after(monkeypatch, iFDOMetadata, "_write_exif_batch", successes=100)

        _package(project_dir, source_dir, resume_from_stage=int(PackageStage.MANIFEST))

        assert calls[0] == 0, "stages before the manifest are not redone"
        assert _comparable_manifest(project_dir) == reference
        assert not _state_dir(project_dir).exists()

    @pytest.mark.parametrize("stage", [1, 2, 13])
    def test_out_of_range_stage_is_refused(self, stage: int, project_dir: Path, source_dir: Path) -> None:
        with pytest.raises(ProjectWrapper.PackageResumeError, match="must be between 3 and 12"):
            _package(project_dir, source_dir, resume_from_stage=stage)

    def test_missing_dataset_is_refused(self, project_dir: Path, source_dir: Path) -> None:
        with pytest.raises(ProjectWrapper.PackageResumeError, match="There is no dataset"):
            _package(project_dir, source_dir, resume_from_stage=10)

    def test_missing_earlier_outputs_are_refused(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._package_without_checkpoints_until_manifest(project_dir, source_dir, monkeypatch)
        (project_dir / "datasets" / DATASET / "summary.md").unlink()

        with pytest.raises(ProjectWrapper.PackageResumeError, match=r"summary\.md \(stage 6\)"):
            _package(project_dir, source_dir, resume_from_stage=11)

    def test_stage_five_needs_a_checkpointed_stage_four(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._package_without_checkpoints_until_manifest(project_dir, source_dir, monkeypatch)
        with pytest.raises(ProjectWrapper.PackageResumeError, match="resume from stage 4 instead"):
            _package(project_dir, source_dir, resume_from_stage=5)

    def test_from_stage_on_checkpointed_run_redoes_later_stages(
        self,
        project_dir: Path,
        source_dir: Path,
        reference: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as mp:
            _fail_after(mp, DatasetWrapper, "_generate_manifest", successes=0)
            with pytest.raises(StageInterruptedError):
                _package(project_dir, source_dir)

        _package(project_dir, source_dir, resume_from_stage=int(PackageStage.SUMMARY))

        assert _comparable_manifest(project_dir) == reference


@pytest.mark.integration
class TestSigterm:
    """A terminated run releases its lock and keeps its journals, so the next job can resume at once."""

    def test_sigterm_flushes_journals_and_releases_lock(
        self,
        project_dir: Path,
        source_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = ProjectWrapper(project_dir)
        mapping = _mapping(source_dir)
        state = project.open_package_state(DATASET, FINGERPRINT, mapping_destinations_digest(mapping))
        assert state is not None
        hashed = source_dir / "notes.txt"
        state.hashes.record("notes.txt", hashed.stat(), "H")

        exit_codes: list[int] = []

        def fake_exit(code: int) -> None:
            exit_codes.append(code)

        monkeypatch.setattr("marimba.main.os._exit", fake_exit)
        _close_state_on_sigterm(state)
        try:
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        finally:
            _close_packaging(state, None)

        assert exit_codes == [128 + signal.SIGTERM]
        assert not (_state_dir(project_dir) / "lock").exists()
        reloaded = PackageState.load(_state_dir(project_dir))
        try:
            assert reloaded.hashes.lookup("notes.txt", hashed.stat()) == "H"
        finally:
            reloaded.close()
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
