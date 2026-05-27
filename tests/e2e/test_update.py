"""End-to-end tests for the `marimba update` CLI subcommand."""

from pathlib import Path

import pytest
import pytest_mock
from typer.testing import CliRunner

from marimba.core.wrappers.project import ProjectWrapper
from marimba.main import marimba_cli as app
from tests.conftest import assert_cli_success, run_cli_command


@pytest.mark.e2e
class TestUpdateWorkflow:
    """Cover the update happy path and error handling."""

    def test_update_invokes_project_wrapper_update_pipelines(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """`marimba update` calls ProjectWrapper.update_pipelines and reports success."""
        project_dir = tmp_path / "proj"
        run_cli_command(runner, ["new", "project", str(project_dir)], context="Project creation")

        mock_update = mocker.patch("marimba.main.ProjectWrapper.update_pipelines")

        result = runner.invoke(app, ["update", "--project-dir", str(project_dir)])

        assert_cli_success(result, expected_message="Successfully updated", context="update happy path")
        mock_update.assert_called_once()

    def test_update_failure_reports_error_text(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """An exception inside update_pipelines surfaces the error text and exits non-zero."""
        project_dir = tmp_path / "proj"
        run_cli_command(runner, ["new", "project", str(project_dir)], context="Project creation")

        mocker.patch(
            "marimba.main.ProjectWrapper.update_pipelines",
            side_effect=ProjectWrapper.UpdatePipelinesError("network unreachable"),
        )

        result = runner.invoke(app, ["update", "--project-dir", str(project_dir)])

        assert "Could not update pipelines" in result.output, f"Expected error panel text; got: {result.output}"
        assert result.exit_code == 1, f"Expected exit code 1 on update failure; got: {result.exit_code}"

    def test_update_without_project_dir_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        """Running update outside a project directory exits non-zero with a clear message."""
        # Use a clean tmp directory that does not contain a .marimba project.
        result = runner.invoke(app, ["update", "--project-dir", str(tmp_path / "no_such_proj")])

        assert result.exit_code != 0
