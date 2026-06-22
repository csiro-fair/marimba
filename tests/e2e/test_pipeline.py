"""
End-to-End tests for pipeline operations.

These tests validate complete pipeline workflows from CLI invocation to final outcomes,
including pipeline creation, deletion, processing, and management operations.
"""

from pathlib import Path
from typing import Any

import pytest
import pytest_mock
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import (
    TestDataFactory,
    assert_cli_failure,
    assert_cli_success,
    assert_project_structure_complete,
    create_mock_pipeline_structure,
    run_cli_command,
)


class TestPipelineManagement:
    """Test pipeline creation and management workflows."""

    @pytest.mark.e2e
    def test_pipeline_creation_success_workflow(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test successful end-to-end pipeline creation workflow.

        Verifies that the complete pipeline creation process works from CLI invocation
        to final directory structure, including Git clone operations and file creation.
        """
        # Arrange: Create project and mock Git operations
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation for pipeline test",
        )

        def mock_clone_from(_url: str, to_path: str, **_kwargs: Any) -> Any:
            repo_path = Path(to_path)
            repo_path.mkdir(parents=True, exist_ok=True)
            config = TestDataFactory.create_pipeline_config(name="test_pipeline")
            (repo_path / "pipeline.yml").write_text(
                f"name: {config['name']}\nversion: {config['version']}\ndescription: {config['description']}",
            )
            (repo_path / "main.py").write_text("# Pipeline implementation")
            return mocker.Mock()

        mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_clone_from)

        # Act: Create pipeline
        result = runner.invoke(
            app,
            [
                "new",
                "pipeline",
                "test_pipeline",
                "https://github.com/example/test-pipeline.git",
                "--project-dir",
                str(temp_project_dir),
            ],
        )

        # Assert: Success and correct Git operations
        assert_cli_success(result, "test_pipeline", context="Pipeline creation")

        mock_clone.assert_called_once()
        clone_args = mock_clone.call_args[0]
        assert clone_args[0] == "https://github.com/example/test-pipeline.git"
        assert "test_pipeline" in str(clone_args[1])

        # Assert: Directory structure created correctly
        pipeline_dir = temp_project_dir / "pipelines" / "test_pipeline"
        repo_dir = pipeline_dir / "repo"
        config_file = repo_dir / "pipeline.yml"
        main_script = repo_dir / "main.py"

        assert pipeline_dir.exists()
        assert pipeline_dir.is_dir()
        assert repo_dir.exists()
        assert repo_dir.is_dir()
        assert config_file.exists()
        assert config_file.is_file()
        assert main_script.exists()
        assert main_script.is_file()

        assert_project_structure_complete(temp_project_dir)

    @pytest.mark.e2e
    def test_pipeline_creation_invalid_repository_failure(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline creation fails gracefully with invalid repository.

        Verifies error handling when attempting to create a pipeline from a
        non-existent Git repository URL and ensures proper cleanup after failure.
        """
        # Arrange: Create project and verify initial state
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation for invalid repository test",
        )

        pipelines_dir = temp_project_dir / "pipelines"
        assert pipelines_dir.exists(), "Pipelines directory should exist after project creation"

        # Arrange: Mock Git clone to simulate repository not found error
        def mock_clone_failure(_url: str, _to_path: str, **_kwargs: Any) -> None:
            from git.exc import GitCommandError

            raise GitCommandError(
                command="git clone",
                status=128,
                stderr="fatal: repository 'https://github.com/nonexistent/nonexistent-repo.git' not found",
            )

        mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_clone_failure)

        # Act: Attempt pipeline creation with invalid repository
        result = runner.invoke(
            app,
            [
                "new",
                "pipeline",
                "test_pipeline",
                "https://github.com/nonexistent/nonexistent-repo.git",
                "--project-dir",
                str(temp_project_dir),
            ],
        )

        # Assert: Command fails with expected exit code
        assert_cli_failure(result, context="Invalid repository pipeline creation")

        # Assert: Git clone was attempted
        mock_clone.assert_called_once_with(
            "https://github.com/nonexistent/nonexistent-repo.git",
            mocker.ANY,
        )

        # Assert: Error message contains Git-related information
        error_output = result.output.lower()
        git_error_patterns = ["git", "repository", "clone", "fatal", "not found"]
        assert any(
            pattern in error_output for pattern in git_error_patterns
        ), f"Expected Git-related error message containing one of {git_error_patterns}, got: {result.output}"

        # Assert: Repository directory not created due to clone failure
        pipeline_dir = temp_project_dir / "pipelines" / "test_pipeline"
        pipeline_repo = pipeline_dir / "repo"
        assert not pipeline_repo.exists(), "Repository directory should not exist after clone failure"

        # Assert: Project structure remains intact
        assert_project_structure_complete(temp_project_dir)

    @pytest.mark.e2e
    def test_new_pipeline_workflow_error_handling(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline creation error handling in complete workflow.

        Verifies that Git clone failures are properly handled with specific error
        messages and that no partial artifacts remain after failure.
        """
        # Arrange: Create project
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation",
        )

        def mock_clone_failure(_url: str, _to_path: str, **_kwargs: Any) -> None:
            from git.exc import GitCommandError

            raise GitCommandError(
                command="git clone",
                status=128,
                stderr="fatal: repository not found",
            )

        mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_clone_failure)

        # Act: Attempt pipeline creation with failing clone
        result = runner.invoke(
            app,
            [
                "new",
                "pipeline",
                "error_pipeline",
                "https://github.com/example/nonexistent.git",
                "--project-dir",
                str(temp_project_dir),
            ],
        )

        # Assert: Command fails with expected exit code
        assert_cli_failure(result, context="Git clone failure")

        # Assert: Git clone was attempted
        mock_clone.assert_called_once_with(
            "https://github.com/example/nonexistent.git",
            mocker.ANY,
        )

        # Assert: Error message contains Git-related information
        error_output = result.output.lower()
        git_error_patterns = ["git", "repository", "clone", "fatal"]
        assert any(
            pattern in error_output for pattern in git_error_patterns
        ), f"Expected Git-related error message containing one of {git_error_patterns}, got: {result.output}"

        # Assert: Repository directory not created due to clone failure
        pipeline_dir = temp_project_dir / "pipelines" / "error_pipeline"
        repo_dir = pipeline_dir / "repo"
        assert not repo_dir.exists(), "Repository directory should not exist after clone failure"

        assert_project_structure_complete(temp_project_dir)

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_pipeline_creation_with_config_validation(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline creation handles configuration validation correctly.

        Verifies that valid configuration JSON is parsed correctly and that
        repository errors are properly reported when config is valid but Git clone fails.
        """
        # Arrange: Create project
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation",
        )

        valid_config = '{"test_param": "test_value", "threshold": 0.8}'

        # Arrange: Mock Git clone to fail with repository error
        def mock_clone_failure(_url: str, _to_path: str, **_kwargs: Any) -> None:
            from git.exc import GitCommandError

            raise GitCommandError(
                command="git clone",
                status=128,
                stderr="fatal: repository 'https://github.com/example/nonexistent.git' not found",
            )

        mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_clone_failure)

        # Act: Create pipeline with valid config but failing Git clone
        result = runner.invoke(
            app,
            [
                "new",
                "pipeline",
                "config_pipeline",
                "https://github.com/example/nonexistent.git",
                "--project-dir",
                str(temp_project_dir),
                "--config",
                valid_config,
            ],
        )

        # Assert: Git clone was attempted with correct URL
        mock_clone.assert_called_once_with(
            "https://github.com/example/nonexistent.git",
            mocker.ANY,
        )

        # Assert: Command fails due to repository issue, not config
        assert_cli_failure(result, context="Repository failure with valid config")

        error_output = result.output.lower()
        repository_error_patterns = ["git", "repository", "clone", "fatal", "not found"]
        assert any(
            pattern in error_output for pattern in repository_error_patterns
        ), f"Expected repository-related error containing one of {repository_error_patterns}, got: {result.output}"

        # Assert: Config was parsed successfully (no config parsing errors)
        config_error_patterns = ["invalid config", "json error", "parse error", "config validation"]
        assert not any(
            pattern in error_output for pattern in config_error_patterns
        ), f"Config should parse without errors (error should be repository-related), got: {result.output}"

        # Assert: No partial directory artifacts
        repo_dir = temp_project_dir / "pipelines" / "config_pipeline" / "repo"
        assert not repo_dir.exists(), "Repository directory should not exist after clone failure"

    @pytest.mark.e2e
    def test_delete_pipeline_nonexistent_failure(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test pipeline deletion fails appropriately for non-existent pipeline.

        Verifies proper error handling when attempting to delete a pipeline
        that does not exist in the project.
        """
        # Arrange: Create project and verify empty pipelines directory
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation",
        )

        pipelines_dir = temp_project_dir / "pipelines"
        assert pipelines_dir.exists()
        assert len(list(pipelines_dir.iterdir())) == 0

        # Act: Attempt to delete non-existent pipeline
        result = runner.invoke(
            app,
            ["delete", "pipeline", "nonexistent_pipeline", "--project-dir", str(temp_project_dir)],
        )

        # Assert: Command fails with expected exit code
        assert_cli_failure(result, context="Non-existent pipeline deletion")

        # Assert: Error message indicates pipeline not found
        error_output = result.output.lower()
        not_found_patterns = ["not found", "does not exist", "nonexistent_pipeline", "pipeline not found"]
        assert any(
            pattern in error_output for pattern in not_found_patterns
        ), f"Expected pipeline not-found error containing one of {not_found_patterns}, got: {result.output}"

        # Assert: Directory state unchanged
        assert len(list(pipelines_dir.iterdir())) == 0, "Pipelines directory should remain empty"
        assert_project_structure_complete(temp_project_dir)

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_delete_pipeline_existing_success(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test successful deletion of an existing pipeline.

        Verifies that pipeline deletion removes all associated files and
        directories while preserving overall project structure.
        """
        # Arrange: Create project and mock pipeline
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation",
        )

        pipeline_config = TestDataFactory.create_pipeline_config(name="test_pipeline")
        pipeline_dir = create_mock_pipeline_structure(temp_project_dir, "test_pipeline", pipeline_config)

        # Arrange: Verify pipeline exists before deletion
        repo_dir = pipeline_dir / "repo"
        config_file = pipeline_dir / "pipeline.yml"
        assert pipeline_dir.exists(), f"Pipeline directory should exist before deletion: {pipeline_dir}"
        assert pipeline_dir.is_dir(), f"Pipeline path should be a directory: {pipeline_dir}"
        assert repo_dir.exists(), f"Repository directory should exist: {repo_dir}"
        assert config_file.exists(), f"Pipeline config file should exist: {config_file}"

        # Act: Delete the pipeline
        result = runner.invoke(
            app,
            ["delete", "pipeline", "test_pipeline", "--project-dir", str(temp_project_dir)],
        )

        # Assert: Successful deletion
        assert_cli_success(result, "test_pipeline", context="Pipeline deletion")

        # Assert: Pipeline completely removed
        assert not pipeline_dir.exists(), f"Pipeline directory should be removed: {pipeline_dir}"
        assert not repo_dir.exists(), f"Repository directory should be removed: {repo_dir}"
        assert not config_file.exists(), f"Config file should be removed: {config_file}"

        assert_project_structure_complete(temp_project_dir)

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_pipeline_workflow_integration_success(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test complete successful pipeline workflow integration.

        Verifies end-to-end pipeline creation with Git clone mock and proper
        directory structure validation after successful installation.
        This is an e2e test that validates the entire workflow from CLI to output.
        """
        # Arrange: Create project and prepare test data
        temp_project_dir = tmp_path / "test_project"
        run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation",
        )

        pipeline_name = "integration_pipeline"
        repo_url = "https://github.com/example/integration-pipeline.git"

        def mock_successful_clone(_url: str, to_path: str, **_kwargs: Any) -> Any:
            """Mock Git clone that creates realistic pipeline structure."""
            repo_path = Path(to_path)
            repo_path.mkdir(parents=True, exist_ok=True)

            # Create realistic pipeline files
            config = TestDataFactory.create_pipeline_config(name=pipeline_name)
            (repo_path / "pipeline.yml").write_text(
                f"name: {config['name']}\n"
                f"version: {config['version']}\n"
                f"description: {config['description']}\n"
                f"requirements:\n  - python>=3.8",
            )
            (repo_path / "main.py").write_text(
                "#!/usr/bin/env python3\n"
                "# Integration test pipeline\n"
                "def main():\n    print('Pipeline executed successfully')",
            )
            (repo_path / "README.md").write_text("# Integration Pipeline\nTest pipeline")
            return mocker.Mock()

        mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_successful_clone)

        # Act: Create pipeline
        result = runner.invoke(
            app,
            [
                "new",
                "pipeline",
                pipeline_name,
                repo_url,
                "--project-dir",
                str(temp_project_dir),
            ],
        )

        # Assert: Command succeeds with correct Git operations
        assert_cli_success(result, pipeline_name, context="Pipeline creation")

        mock_clone.assert_called_once_with(repo_url, mocker.ANY)

        # Assert: Complete directory structure created correctly
        pipeline_dir = temp_project_dir / "pipelines" / pipeline_name
        repo_dir = pipeline_dir / "repo"

        assert pipeline_dir.exists(), f"Pipeline directory should exist: {pipeline_dir}"
        assert pipeline_dir.is_dir(), f"Pipeline path should be a directory: {pipeline_dir}"
        assert repo_dir.exists(), f"Repository directory should exist: {repo_dir}"
        assert repo_dir.is_dir(), f"Repository path should be a directory: {repo_dir}"

        # Assert: Required pipeline files exist
        config_file = repo_dir / "pipeline.yml"
        main_script = repo_dir / "main.py"
        readme_file = repo_dir / "README.md"

        assert config_file.exists(), "Pipeline configuration should exist"
        assert config_file.is_file(), "Pipeline configuration should be a file"
        assert main_script.exists(), "Main script should exist"
        assert main_script.is_file(), "Main script should be a file"
        assert readme_file.exists(), "README should exist"
        assert readme_file.is_file(), "README should be a file"

        # Assert: File content validation
        config_content = config_file.read_text()
        assert pipeline_name in config_content, f"Config should contain pipeline name '{pipeline_name}'"
        assert "requirements:" in config_content, "Config should contain requirements section"

        main_content = main_script.read_text()
        assert "def main():" in main_content, "Main script should contain main function definition"

        assert_project_structure_complete(temp_project_dir)


class TestProcessWorkflows:
    """Test processing workflows and pipeline operations."""

    @pytest.mark.e2e
    def test_process_workflow_no_pipelines_available(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test process command gracefully handles empty pipeline scenarios.

        Validates that the process command executes successfully when no pipelines
        are available in the project, demonstrating proper workflow recognition
        and graceful no-op completion without errors.
        """
        # Arrange: Create project with no pipelines
        temp_project_dir = tmp_path / "test_project"
        result = run_cli_command(
            runner,
            ["new", "project", str(temp_project_dir)],
            context="Project creation for process workflow test",
        )

        # Arrange: Verify no pipelines exist
        pipelines_dir = temp_project_dir / "pipelines"
        assert pipelines_dir.exists(), "Pipelines directory should exist after project creation"
        assert pipelines_dir.is_dir(), "Pipelines path should be a directory"
        pipeline_contents = list(pipelines_dir.iterdir())
        assert len(pipeline_contents) == 0, f"No pipelines should be present initially, found: {pipeline_contents}"

        # Act: Run process command
        result = runner.invoke(app, ["process", "--project-dir", str(temp_project_dir)])

        # Assert: Command succeeds with exit code 0
        assert (
            result.exit_code == 0
        ), f"Process command should succeed with no pipelines, got exit code {result.exit_code}: {result.output}"

        output_lower = result.output.lower()

        # Assert: No unhandled exceptions or critical errors
        error_indicators = ["traceback", "fatal", "error:", "exception:", "failed"]
        found_errors = [indicator for indicator in error_indicators if indicator in output_lower]
        assert not found_errors, f"Should not have errors, found: {found_errors} in output: {result.output}"

        # Assert: Output should indicate workflow was recognized and handled appropriately
        # The process command should either show it's processing or indicate no work needed
        workflow_indicators = ["processing", "processed", "pipeline", "no pipelines", "complete"]
        found_indicators = [indicator for indicator in workflow_indicators if indicator in output_lower]
        assert found_indicators, (
            f"Expected process workflow output to contain at least one workflow indicator from {workflow_indicators}, "
            f"but found none in: {result.output}"
        )

        # Assert: Project structure remains intact
        assert_project_structure_complete(temp_project_dir)
