"""
End-to-end tests for the ``marimba package`` resume options.

The behaviour of a resume inside each stage is covered in ``tests/core/wrappers/test_dataset_resume.py``; these tests
cover the CLI surface: the options, their error panels and exit codes, and the state directory's lifecycle.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from marimba.core.utils.summary import ImagerySummary
from marimba.main import marimba_cli as app
from tests.conftest import TestDataFactory, assert_cli_failure, assert_cli_success

DATASET = "test_dataset"


@pytest.fixture
def project_dir(cli_runner: CliRunner, tmp_path: Path) -> Path:
    project = tmp_path / "project"
    data_dir = tmp_path / "data"
    TestDataFactory.create_test_images(data_dir, image_count=2, image_size="minimal")
    assert_cli_success(cli_runner.invoke(app, ["new", "project", str(project)]), context="Project creation")
    assert_cli_success(
        cli_runner.invoke(app, ["import", "collection", str(data_dir), "--project-dir", str(project)]),
        context="Collection import",
    )
    return project


def _package(cli_runner: CliRunner, project_dir: Path, *args: str):  # type: ignore[no-untyped-def]
    return cli_runner.invoke(app, ["package", DATASET, "--project-dir", str(project_dir), *args])


def _state_dir(project_dir: Path) -> Path:
    return project_dir / ".marimba" / "package" / DATASET


@pytest.mark.e2e
class TestPackageResumeCli:
    """The resume options, their errors and exit codes, and the state directory lifecycle."""

    def test_successful_package_leaves_no_state(self, cli_runner: CliRunner, project_dir: Path) -> None:
        assert_cli_success(_package(cli_runner, project_dir), context="Package")
        assert (project_dir / "datasets" / DATASET / "manifest.txt").is_file()
        assert not _state_dir(project_dir).exists()

    def test_resume_of_packaged_dataset_is_a_no_op(self, cli_runner: CliRunner, project_dir: Path) -> None:
        assert_cli_success(_package(cli_runner, project_dir), context="Package")
        result = _package(cli_runner, project_dir, "--resume")
        assert_cli_success(result, expected_message="already packaged", context="Resume of finished dataset")

    def test_resume_without_interrupted_run_fails(self, cli_runner: CliRunner, project_dir: Path) -> None:
        result = _package(cli_runner, project_dir, "--resume")
        assert_cli_failure(result, expected_error="No interrupted packaging run", context="Resume without state")

    def test_resume_cannot_be_combined_with_dry_run(self, cli_runner: CliRunner, project_dir: Path) -> None:
        result = _package(cli_runner, project_dir, "--resume", "--dry-run")
        assert_cli_failure(result, expected_error="cannot be combined with --dry-run", context="Resume dry-run")

    def test_dry_run_writes_no_state(self, cli_runner: CliRunner, project_dir: Path) -> None:
        assert_cli_success(_package(cli_runner, project_dir, "--dry-run"), context="Dry-run package")
        assert not (project_dir / ".marimba" / "package").exists()

    def test_resume_from_invalid_stage_fails(self, cli_runner: CliRunner, project_dir: Path) -> None:
        assert_cli_success(_package(cli_runner, project_dir), context="Package")
        result = _package(cli_runner, project_dir, "--resume-from-stage", "2")
        assert_cli_failure(result, expected_error="must be between 3 and 12", context="Invalid stage")

    def test_interrupted_package_resumes(
        self,
        cli_runner: CliRunner,
        project_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def interrupted(*args: object, **kwargs: object) -> None:  # noqa: ARG001
            msg = "simulated interruption"
            raise OSError(msg)

        with monkeypatch.context() as mp:
            mp.setattr(ImagerySummary, "from_dataset", interrupted)
            result = _package(cli_runner, project_dir)
        assert result.exit_code != 0
        assert (_state_dir(project_dir) / "run.json").is_file()
        assert not (_state_dir(project_dir) / "lock").exists(), "a failed run releases its lock"

        result = _package(cli_runner, project_dir)
        assert_cli_failure(result, expected_error="Resume it with --resume", context="Fresh run over interrupted run")

        assert_cli_success(_package(cli_runner, project_dir, "--resume"), context="Resume")
        assert (project_dir / "datasets" / DATASET / "manifest.txt").is_file()
        assert not _state_dir(project_dir).exists()

    def test_resume_with_changed_options_fails(
        self,
        cli_runner: CliRunner,
        project_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def interrupted(*args: object, **kwargs: object) -> None:  # noqa: ARG001
            msg = "simulated interruption"
            raise OSError(msg)

        with monkeypatch.context() as mp:
            mp.setattr(ImagerySummary, "from_dataset", interrupted)
            _package(cli_runner, project_dir)

        result = _package(cli_runner, project_dir, "--resume", "--version", "2.0")
        assert_cli_failure(result, expected_error="version: '1.0' -> '2.0'", context="Changed options")
