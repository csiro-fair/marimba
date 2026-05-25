"""End-to-end tests for the `marimba install` CLI subcommand."""

from pathlib import Path

import pytest
import pytest_mock
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import assert_cli_success, run_cli_command


@pytest.mark.e2e
class TestInstallWorkflow:
    """Cover the install happy path and error handling."""

    def test_install_invokes_project_wrapper_install_pipelines(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """`marimba install` calls ProjectWrapper.install_pipelines and reports success."""
        project_dir = tmp_path / "proj"
        run_cli_command(runner, ["new", "project", str(project_dir)], context="Project creation")

        mock_install = mocker.patch("marimba.main.ProjectWrapper.install_pipelines")

        result = runner.invoke(app, ["install", "--project-dir", str(project_dir)])

        assert_cli_success(result, expected_message="Successfully installed", context="install happy path")
        mock_install.assert_called_once()

    def test_install_failure_reports_error_and_exits_with_code_one(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """An exception inside install_pipelines surfaces as exit code 1 with the error text."""
        project_dir = tmp_path / "proj"
        run_cli_command(runner, ["new", "project", str(project_dir)], context="Project creation")

        mocker.patch(
            "marimba.main.ProjectWrapper.install_pipelines",
            side_effect=RuntimeError("uv binary missing"),
        )

        result = runner.invoke(app, ["install", "--project-dir", str(project_dir)])

        assert result.exit_code == 1, f"install_command raises typer.Exit(1); got {result.exit_code}\n{result.output}"
        assert "Could not install pipelines" in result.output, f"Expected error panel text; got: {result.output}"

    def test_install_without_project_dir_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        """Running install outside a project directory exits non-zero."""
        result = runner.invoke(app, ["install", "--project-dir", str(tmp_path / "no_such_proj")])

        assert result.exit_code != 0
