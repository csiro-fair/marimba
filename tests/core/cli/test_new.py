from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from marimba.core.wrappers.project import ProjectWrapper
from marimba.main import marimba_cli
from tests.conftest import (
    assert_project_structure_complete,
    run_cli_command,
)

runner = CliRunner()

# ---------------------------------------------------------------------------------------------------------------------#
# Testing project()
# ---------------------------------------------------------------------------------------------------------------------#


class TestProjectCommand:
    """Test class for the project CLI command."""

    @pytest.mark.integration
    def test_project_creates_new_project_successfully(self, tmp_path: Path) -> None:
        """
        Test project command creates a new Marimba project successfully.

        This integration test verifies that the CLI command properly orchestrates
        project creation by testing the real interaction between CLI argument parsing,
        ProjectWrapper.create(), and filesystem operations.
        """
        # Arrange - Set up test directory path
        project_dir = tmp_path / "new_project"

        # Verify precondition - directory should not exist initially
        assert not project_dir.exists(), "Project directory should not exist before creation"

        # Act - Execute CLI command to create new project
        run_cli_command(
            runner,
            ["new", "project", str(project_dir)],
            expected_success=True,
            expected_message="Created new Marimba project at",
            context="Project creation",
        )

        # Assert - Verify project was created with correct structure
        assert project_dir.exists(), "Project directory should exist after successful creation"
        assert_project_structure_complete(project_dir, "New project creation should have complete structure")

    @pytest.mark.integration
    def test_project_exits_if_project_exists(self, tmp_path: Path) -> None:
        """
        Test project command exits with error when project directory already exists.

        This integration test verifies that when attempting to create a project in a directory
        where a Marimba project already exists, the CLI command exits with code 1 and displays
        the error message "A Marimba project already exists at:" followed by the project directory path.
        Tests the real interaction between CLI, ProjectWrapper.create(), and filesystem operations.
        """
        # Arrange - Create an existing Marimba project directory with real project structure
        existing_project_dir = tmp_path / "existing_project"
        ProjectWrapper.create(existing_project_dir)

        # Verify precondition - project should exist with proper structure
        assert existing_project_dir.exists(), "Project directory should exist after creation"
        assert (existing_project_dir / ".marimba").exists(), "Marimba project marker should exist"

        # Act & Assert - Using shared CLI failure helper for consistent error checking
        run_cli_command(
            runner,
            ["new", "project", str(existing_project_dir)],
            expected_success=False,
            expected_message="A Marimba project already exists at:",
            context="Project already exists",
        )

    @pytest.mark.integration
    def test_project_exit_code_on_success(self, tmp_path: Path) -> None:
        """
        Test project command exits with code 0 on successful project creation.

        This integration test verifies that the CLI command returns the correct
        exit code (0) when project creation succeeds, testing the real interaction
        between CLI command execution and success handling.
        """
        # Arrange
        project_dir = tmp_path / "success_test_project"

        # Act
        result = runner.invoke(marimba_cli, ["new", "project", str(project_dir)])

        # Assert
        assert result.exit_code == 0, f"Command should exit with code 0 on success, got {result.exit_code}"
        assert "Created new Marimba project at" in result.output, "Success message should be displayed"

        # Verify project was actually created with proper structure
        assert_project_structure_complete(project_dir, "Project creation with success message")

    @pytest.mark.integration
    def test_project_handles_existing_directory_path(self, tmp_path: Path) -> None:
        """
        Test project command handles existing directory appropriately.

        This integration test verifies that the CLI command properly handles cases where a
        directory already exists at the specified path. The ProjectWrapper.create() method
        raises FileExistsError for any existing directory, and the CLI displays the standard
        error message and exits with code 1, testing real component interaction.
        """
        # Arrange - Create an existing directory (non-Marimba)
        existing_dir = tmp_path / "existing_directory"
        existing_dir.mkdir()

        # Add some content to verify it's a real directory
        (existing_dir / "some_file.txt").write_text("existing content")

        # Verify precondition - directory exists but is not a Marimba project
        assert existing_dir.exists(), "Directory should exist before test"
        assert not (existing_dir / ".marimba").exists(), "Should not be a Marimba project"

        # Act - Try to create project in existing directory
        result = runner.invoke(marimba_cli, ["new", "project", str(existing_dir)])

        # Assert - Should fail with specific error message and exit code
        assert result.exit_code == 1, f"Command should fail with exit code 1, got {result.exit_code}"
        assert (
            "A Marimba project already exists at:" in result.output
        ), f"Should show directory exists error message, got: {result.output}"
        # Rich formatting may wrap long paths, so check for parts that should appear
        assert "existing" in result.output, f"Directory name part should be in error message, got: {result.output}"
        assert "irectory" in result.output, f"Directory name part should be in error message, got: {result.output}"

    @pytest.mark.unit
    def test_project_handles_creation_failure_with_proper_exit_code(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """
        Test project command behavior when ProjectWrapper.create() raises general exception.

        This unit test verifies the current behavior when project creation fails with
        an unexpected exception (not FileExistsError). The current implementation does
        not catch general exceptions, so they propagate as unhandled exceptions with
        exit code 1. Tests the actual error handling behavior for general creation failures.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        expected_error_message = "Permission denied"

        # Mock ProjectWrapper.create to raise a general exception
        mock_create = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create",
            side_effect=Exception(expected_error_message),
        )

        # Act
        result = runner.invoke(marimba_cli, ["new", "project", str(project_dir)])

        # Assert - Current implementation allows general exceptions to propagate
        # This results in exit code 1 but with a traceback instead of clean error message
        assert result.exit_code == 1, f"Command should exit with code 1 on creation failure, got {result.exit_code}"

        # Verify ProjectWrapper.create was called with correct arguments
        mock_create.assert_called_once_with(project_dir)


# ---------------------------------------------------------------------------------------------------------------------#
# Testing pipeline()
# ---------------------------------------------------------------------------------------------------------------------#


class TestPipelineCommand:
    """Test class for the pipeline CLI command."""

    @pytest.mark.integration
    def test_pipeline_creates_new_pipeline_successfully(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command creates a new pipeline successfully.

        This integration test verifies that the CLI command properly handles pipeline creation
        by testing real CLI argument parsing and project wrapper interactions while only
        mocking external Git operations that require network access.
        """
        # Arrange
        project_dir = tmp_path / "project"
        pipeline_name = "test_pipeline"
        url = "https://example.com/repo.git"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name

        # Mock only external Git operations that require network access
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir)],
        )

        # Assert - Verify CLI behavior and proper method calls
        assert (
            result.exit_code == 0
        ), f"Command should succeed with exit code 0, got {result.exit_code} with output: {result.output}"
        assert (
            "Created new Marimba pipeline" in result.output
        ), f"Success message should be displayed in output: {result.output}"
        assert (
            f'"{pipeline_name}"' in result.output
        ), f"Pipeline name should be displayed in success message: {result.output}"

        # Verify the create_pipeline method was called with correct arguments
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=False)

    @pytest.mark.integration
    def test_pipeline_creates_with_empty_config_dict_parsing(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command correctly parses empty config parameter and passes empty dict.

        This integration test verifies that when no --config parameter is provided,
        the CLI correctly parses it as None, converts it to an empty dictionary,
        and passes this empty config dict to ProjectWrapper.create_pipeline().
        Tests the real config parsing logic with actual project structure.
        """
        # Arrange
        project_dir = tmp_path / "config_test_project"
        pipeline_name = "config_test_pipeline"
        url = "https://example.com/config-test.git"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Verify precondition - project should exist with proper structure
        assert project_dir.exists(), "Project directory should exist after creation"
        assert (project_dir / ".marimba").exists(), "Marimba project marker should exist"

        # Mock only external Git operations that require network access
        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act - Execute CLI command WITHOUT --config parameter to test empty config parsing
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir)],
        )

        # Assert - Verify successful command execution with specific exit code
        assert (
            result.exit_code == 0
        ), f"Command should succeed with exit code 0, got {result.exit_code} with output: {result.output}"

        # Assert - Verify success message components are present
        assert (
            "Created new Marimba pipeline" in result.output
        ), f"Success message should be displayed, got: {result.output}"
        assert (
            f'"{pipeline_name}"' in result.output
        ), f"Pipeline name '{pipeline_name}' should be in success message, got: {result.output}"

        # Assert - Verify create_pipeline was called with exact expected arguments (key integration test)
        # This tests that CLI correctly parses None config parameter and converts to empty dict
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=False)

        # Assert - Verify the mock was called exactly once with no additional calls
        assert mock_create_pipeline.call_count == 1, "create_pipeline should be called exactly once"

        # Assert - Verify real project structure remains intact after operation
        assert project_dir.exists(), "Project directory should exist after pipeline creation"
        assert (project_dir / ".marimba").exists(), "Marimba project marker should exist after pipeline creation"
        assert (project_dir / "pipelines").exists(), "Pipelines directory should exist in project structure"

    @pytest.mark.unit
    def test_pipeline_invalid_name_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command exits with error for invalid pipeline name.

        This unit test verifies proper error handling when an invalid pipeline
        name causes ProjectWrapper.create_pipeline() to raise InvalidNameError.
        Tests the CLI's error handling path for invalid names while mocking
        only the external dependencies that would prevent testing the real logic.
        """
        # Arrange
        project_dir = tmp_path / "project"
        ProjectWrapper.create(project_dir)  # Create real project structure
        pipeline_name = "invalid/name"
        url = "https://example.com/repo.git"
        expected_error_message = "Invalid pipeline name contains invalid characters"

        # Mock only the create_pipeline method to simulate InvalidNameError
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            side_effect=ProjectWrapper.InvalidNameError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir)],
        )

        # Assert - Verify CLI behavior
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert "Invalid pipeline name:" in result.output, f"Should show invalid name error, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Assert - Verify the correct method was called with expected arguments
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=False)

    @pytest.mark.unit
    def test_pipeline_creation_failure(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command exits with error when pipeline creation fails with general exception.

        This unit test verifies proper error handling when ProjectWrapper.create_pipeline()
        raises a general Exception (not InvalidNameError), ensuring the CLI displays
        "Could not create pipeline:" followed by the specific error message and exits with code 1.
        Tests the error handling path for unexpected failures during pipeline creation.
        """
        # Arrange
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        pipeline_name = "test_pipeline"
        url = "https://example.com/repo.git"
        expected_error_message = "Network connection failed"

        # Mock project directory detection to return our test directory
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)

        # Mock the create_pipeline method to raise an exception
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            side_effect=Exception(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir)],
        )

        # Assert - Verify failure behavior
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "Could not create pipeline:" in result.output
        ), f"Should show creation error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Assert - Verify the correct method was called with expected arguments
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=False)

    @pytest.mark.integration
    def test_pipeline_creates_with_explicit_project_dir_argument(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command creates successfully when explicit --project-dir argument is provided.

        This integration test verifies that the CLI command properly handles the --project-dir
        argument by creating a pipeline in the explicitly specified project directory rather than
        searching from the current directory. Tests real CLI argument parsing, project directory
        resolution, and ProjectWrapper instantiation with only external Git operations mocked.
        """
        # Arrange
        project_dir = tmp_path / "explicit_project"
        pipeline_name = "explicit_pipeline"
        url = "https://example.com/explicit.git"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Mock pipeline wrapper and external Git operations only
        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name

        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 0, f"Command should succeed with exit code 0, got {result.exit_code}"
        assert "Created new Marimba pipeline" in result.output, "Success message should be displayed"
        assert f'"{pipeline_name}"' in result.output, "Pipeline name should be in success message"
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=False)

    @pytest.mark.integration
    def test_pipeline_integration_with_real_project(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command creates successfully with real project structure and component interactions.

        This integration test verifies the real interaction between CLI argument parsing,
        project wrapper initialization, and pipeline creation workflow. It tests the actual
        ProjectWrapper instantiation and project directory validation while only mocking
        external Git operations that require network connectivity.
        """
        # Arrange
        project_dir = tmp_path / "integration_project"
        pipeline_name = "test_integration_pipeline"
        url = "https://github.com/example/test-pipeline.git"
        config = '{"key": "value"}'

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Verify precondition - project should exist with proper structure
        assert project_dir.exists(), "Project directory should exist after creation"
        assert (project_dir / ".marimba").exists(), "Marimba project marker should exist"
        assert (project_dir / "pipelines").exists(), "Pipelines directory should exist"

        # Mock only external Git operations that require network access
        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act - Execute CLI command with config parameter
        result = runner.invoke(
            marimba_cli,
            [
                "new",
                "pipeline",
                pipeline_name,
                url,
                "--project-dir",
                str(project_dir),
                "--config",
                config,
            ],
        )

        # Assert - Verify successful command execution
        assert (
            result.exit_code == 0
        ), f"Command should succeed with exit code 0, got {result.exit_code} with output: {result.output}"

        # Assert - Verify success message contains expected components
        assert (
            "Created new Marimba pipeline" in result.output
        ), f"Success message should be displayed in output: {result.output}"
        assert (
            f'"{pipeline_name}"' in result.output
        ), f"Pipeline name '{pipeline_name}' should be displayed in success message: {result.output}"

        # Assert - Verify create_pipeline was called with correct parsed config
        expected_config = {"key": "value"}
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, expected_config, accept_defaults=False)

        # Assert - Verify real project structure remains intact
        assert (project_dir / ".marimba").exists(), "Project marker should still exist"
        assert (project_dir / "pipelines").exists(), "Pipelines directory should still exist"

    @pytest.mark.integration
    def test_pipeline_with_accept_defaults_flag(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command with --accept-defaults flag passes accept_defaults=True to create_pipeline.

        This integration test verifies that when the --accept-defaults/-y flag is provided,
        the CLI correctly passes accept_defaults=True to ProjectWrapper.create_pipeline(),
        allowing the pipeline to be created without user prompts.
        """
        # Arrange
        project_dir = tmp_path / "accept_defaults_project"
        pipeline_name = "test_pipeline"
        url = "https://example.com/repo.git"

        # Create a real Marimba project structure
        ProjectWrapper.create(project_dir)

        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name

        # Mock create_pipeline to verify it's called with accept_defaults=True
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act - Execute CLI command with --accept-defaults flag
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir), "--accept-defaults"],
        )

        # Assert
        assert result.exit_code == 0, f"Command should succeed, got {result.exit_code} with output: {result.output}"
        assert "Created new Marimba pipeline" in result.output, f"Success message should be displayed: {result.output}"

        # Verify create_pipeline was called with accept_defaults=True
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=True)

    @pytest.mark.integration
    def test_pipeline_with_accept_defaults_short_flag(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test pipeline command with -y short flag passes accept_defaults=True to create_pipeline.

        This integration test verifies that the short form -y flag works identically to --accept-defaults.
        """
        # Arrange
        project_dir = tmp_path / "accept_defaults_short_project"
        pipeline_name = "test_pipeline"
        url = "https://example.com/repo.git"

        # Create a real Marimba project structure
        ProjectWrapper.create(project_dir)

        mock_pipeline_wrapper = mocker.MagicMock()
        mock_pipeline_wrapper.root_dir = project_dir / "pipelines" / pipeline_name

        # Mock create_pipeline to verify it's called with accept_defaults=True
        mock_create_pipeline = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_pipeline",
            return_value=mock_pipeline_wrapper,
        )

        # Act - Execute CLI command with -y short flag
        result = runner.invoke(
            marimba_cli,
            ["new", "pipeline", pipeline_name, url, "--project-dir", str(project_dir), "-y"],
        )

        # Assert
        assert result.exit_code == 0, f"Command should succeed, got {result.exit_code} with output: {result.output}"
        assert "Created new Marimba pipeline" in result.output, f"Success message should be displayed: {result.output}"

        # Verify create_pipeline was called with accept_defaults=True
        mock_create_pipeline.assert_called_once_with(pipeline_name, url, {}, accept_defaults=True)


# ---------------------------------------------------------------------------------------------------------------------#
# Testing collection()
# ---------------------------------------------------------------------------------------------------------------------#


class TestCollectionCommand:
    """Test class for the collection CLI command."""

    @pytest.mark.integration
    def test_collection_creates_new_collection(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test collection command creates a new collection successfully.

        This integration test verifies that the CLI command properly orchestrates collection creation
        by testing the real interaction between CLI argument parsing, project wrapper initialization,
        and collection configuration with minimal mocking of external dependencies only.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        collection_name = "test_collection"
        parent_collection_name = "parent_collection"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Mock only external dependencies that involve user interaction
        mock_collection_config = {"parent": parent_collection_name, "metadata_schema": "ifdo"}
        mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )

        # Mock the create_collection method to avoid actual filesystem operations but test real wrapper creation
        mock_collection_wrapper = mocker.MagicMock()
        mock_collection_wrapper.root_dir = project_dir / "collections" / collection_name
        mock_create = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            return_value=mock_collection_wrapper,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "new",
                "collection",
                collection_name,
                parent_collection_name,
                "--project-dir",
                str(project_dir),
            ],
        )

        # Assert
        assert (
            result.exit_code == 0
        ), f"Collection creation should succeed with exit code 0, got {result.exit_code} with output: {result.output}"
        assert (
            "Created new Marimba collection" in result.output
        ), f"Success message should be displayed in output, got: {result.output}"
        assert (
            f'"{collection_name}"' in result.output
        ), f"Collection name '{collection_name}' should appear in success message, got: {result.output}"

        # Verify the create_collection was called with correct arguments from CLI parsing
        mock_create.assert_called_once_with(
            collection_name,
            mock_collection_config,
        ), "ProjectWrapper.create_collection should be called with parsed CLI arguments and prompted config"

    @pytest.mark.unit
    def test_collection_invalid_name_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test collection command exits with error for invalid collection name.

        This unit test verifies proper error handling when an invalid collection name
        causes ProjectWrapper.create_collection() to raise InvalidNameError, ensuring
        the CLI displays appropriate error messages and exits with code 1.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        collection_name = "invalid/name"
        expected_error_message = "Invalid name"
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}

        # Mock external dependencies - project location, configuration prompt, and collection creation
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )
        mock_create_collection = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            side_effect=ProjectWrapper.InvalidNameError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "Invalid collection name:" in result.output
        ), f"Should show invalid name error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the correct methods were called with expected arguments
        mock_prompt_config.assert_called_once_with(parent_collection_name=None, config={}, accept_defaults=False)
        mock_create_collection.assert_called_once_with(collection_name, mock_collection_config)

    @pytest.mark.unit
    def test_collection_no_such_parent_collection_error(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """
        Test collection command exits with error when parent collection does not exist.

        This unit test verifies proper error handling when ProjectWrapper.create_collection()
        raises NoSuchCollectionError for a non-existent parent collection, ensuring the CLI
        displays appropriate error messages and exits with code 1. Uses mocking to isolate
        the error handling logic from external dependencies.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        collection_name = "test_collection"
        parent_collection_name = "non_existent_parent"
        expected_error_message = "No such parent collection"
        mock_collection_config = {"parent": parent_collection_name, "metadata_schema": "ifdo"}

        # Mock external dependencies - project location, configuration prompt, and collection creation
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )
        mock_create_collection = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            side_effect=ProjectWrapper.NoSuchCollectionError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "new",
                "collection",
                collection_name,
                parent_collection_name,
                "--project-dir",
                str(project_dir),
            ],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "No such parent collection:" in result.output
        ), f"Should display parent collection error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the correct methods were called with expected arguments
        mock_prompt_config.assert_called_once_with(
            parent_collection_name=parent_collection_name,
            config={},
            accept_defaults=False,
        )
        mock_create_collection.assert_called_once_with(collection_name, mock_collection_config)

    @pytest.mark.unit
    def test_collection_creation_specific_failure(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test collection command exits with error when collection creation fails with CreateCollectionError.

        This unit test verifies proper error handling for CreateCollectionError exceptions,
        ensuring the CLI displays appropriate error messages and exits with code 1.
        Uses mocking to isolate the error handling logic from external dependencies.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        collection_name = "test_collection"
        expected_error_message = "Creation failed"
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}

        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )
        mock_create_collection = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            side_effect=ProjectWrapper.CreateCollectionError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "Could not create collection:" in result.output
        ), f"Should show creation error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the correct methods were called with expected arguments
        mock_prompt_config.assert_called_once_with(parent_collection_name=None, config={}, accept_defaults=False)
        mock_create_collection.assert_called_once_with(collection_name, mock_collection_config)

    @pytest.mark.unit
    def test_collection_creation_general_failure(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test collection command exits with error when collection creation fails with general exception.

        This unit test verifies proper error handling for unexpected exceptions during collection creation,
        ensuring the CLI displays appropriate error messages and exits with code 1. Uses mocking to
        isolate the error handling logic from external dependencies.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        collection_name = "test_collection"
        expected_error_message = "Unexpected error occurred"
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}

        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )
        mock_create_collection = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            side_effect=Exception(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "Could not create collection:" in result.output
        ), f"Should show general error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the correct methods were called with expected arguments
        mock_prompt_config.assert_called_once_with(parent_collection_name=None, config={}, accept_defaults=False)
        mock_create_collection.assert_called_once_with(collection_name, mock_collection_config)

    @pytest.mark.integration
    def test_collection_creation_other_failure(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test collection command exits with error when collection creation fails with general exception.

        This integration test verifies proper error handling for unexpected exceptions during
        collection creation, testing real component interactions with minimal mocking of
        external dependencies only. Creates a real project structure and tests the integration
        between CLI parsing, project wrapper creation, and error handling.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        collection_name = "test_collection"
        expected_error_message = "Unexpected integration error"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Mock only the external user interaction dependency - the configuration prompt
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}
        mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )

        # Mock only the collection creation that would involve filesystem operations
        # to inject the failure, testing the integration between CLI and error handling
        mock_create_collection = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_collection",
            side_effect=Exception(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "Could not create collection:" in result.output
        ), f"Should show general error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the correct methods were called with expected arguments
        # Tests real component interaction - CLI parsing arguments correctly
        mock_create_collection.assert_called_once_with(collection_name, mock_collection_config)


# ---------------------------------------------------------------------------------------------------------------------#
# Testing target()
# ---------------------------------------------------------------------------------------------------------------------#


class TestTargetCommand:
    """Test class for the target CLI command."""

    @pytest.mark.integration
    def test_target_creates_new_target_successfully(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test target command creates a new distribution target successfully.

        This integration test verifies that the CLI command properly orchestrates
        distribution target creation by testing real CLI argument parsing, project
        directory detection, and ProjectWrapper.create_target() integration while only
        mocking external user interaction that requires prompting.
        """
        # Arrange - Create real project structure for integration testing
        project_dir = tmp_path / "project"
        target_name = "test_target"

        # Create a real Marimba project structure
        ProjectWrapper.create(project_dir)

        # Verify precondition - project should exist with proper structure
        assert project_dir.exists(), "Project directory should exist after creation"
        assert (project_dir / ".marimba").exists(), "Marimba project marker should exist"

        # Mock target wrapper and external user interaction only
        mock_target_wrapper = mocker.MagicMock()
        mock_target_wrapper.config_path = project_dir / "targets" / f"{target_name}.yml"

        mock_create_target = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_target",
            return_value=mock_target_wrapper,
        )
        mocker.patch(
            "marimba.core.wrappers.target.DistributionTargetWrapper.prompt_target",
            return_value=("s3", {"bucket": "test-bucket", "region": "us-east-1"}),
        )

        # Act - Execute CLI command with real project directory
        result = runner.invoke(
            marimba_cli,
            ["new", "target", target_name, "--project-dir", str(project_dir)],
        )

        # Assert - Verify successful command execution
        assert (
            result.exit_code == 0
        ), f"Command should succeed with exit code 0, got {result.exit_code} with output: {result.output}"

        # Assert - Verify create_target was called with correct parsed arguments
        mock_create_target.assert_called_once_with(target_name, "s3", {"bucket": "test-bucket", "region": "us-east-1"})

        # Assert - Verify exact success message format matches CLI output
        assert (
            "Created new Marimba target" in result.output
        ), f"Success message should be displayed, got: {result.output}"
        assert (
            f'"{target_name}"' in result.output
        ), f"Target name '{target_name}' should be displayed in success message, got: {result.output}"
        assert (
            f"{target_name}.yml" in result.output
        ), f"Config path '{target_name}.yml' should be displayed in success message, got: {result.output}"

    @pytest.mark.unit
    def test_target_invalid_name_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test target command exits with error for invalid target name.

        This unit test verifies proper CLI error handling when an invalid target
        name causes ProjectWrapper.create_target() to raise InvalidNameError,
        ensuring the CLI displays appropriate error messages and exits with code 1.
        """
        # Arrange
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        target_name = "invalid/name"
        expected_error_message = "Invalid name"

        mock_create_target = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_target",
            side_effect=ProjectWrapper.InvalidNameError(expected_error_message),
        )
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mocker.patch(
            "marimba.core.wrappers.target.DistributionTargetWrapper.prompt_target",
            return_value=("target_type", "target_config"),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "target", target_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert "Invalid target name:" in result.output, f"Should show invalid name error, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Verify the create_target method was called with correct arguments
        mock_create_target.assert_called_once_with(target_name, "target_type", "target_config")

    @pytest.mark.unit
    def test_target_already_exists_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test target command exits with error when target already exists.

        This unit test verifies proper error handling when FileExistsError
        is raised during target creation, ensuring the CLI displays the correct
        error message format including the target name and exits with code 1.
        Tests the specific error handling path for duplicate target creation.
        """
        # Arrange
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        target_name = "existing_target"
        expected_error_message = "Target already exists"

        # Mock external dependencies
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mocker.patch(
            "marimba.core.wrappers.target.DistributionTargetWrapper.prompt_target",
            return_value=("target_type", "target_config"),
        )
        mock_create_target = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_target",
            side_effect=FileExistsError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "target", target_name, "--project-dir", str(project_dir)],
        )

        # Assert - Verify exit code and specific error message format
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert (
            "A Marimba target already exists at:" in result.output
        ), f"Should show target exists error message, got: {result.output}"
        # Rich formatting may wrap paths across lines, so check for key components
        assert (
            target_name in result.output
        ), f"Should include target name '{target_name}' in error message, got: {result.output}"
        assert (
            str(project_dir) in result.output
        ), f"Should include project directory path in error message, got: {result.output}"

        # Assert - Verify the create_target method was called with correct arguments
        mock_create_target.assert_called_once_with(target_name, "target_type", "target_config")

    @pytest.mark.unit
    def test_target_creation_failure(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test target command exits with error when target creation fails with general exception.

        This unit test verifies proper error handling for unexpected exceptions during target creation,
        ensuring the CLI displays the error message "Could not create target:" followed by the specific
        error details and exits with code 1. Uses mocking to isolate the error handling logic from
        external dependencies while testing the real CLI error handling behavior.
        """
        # Arrange - Set up test data and mock external dependencies
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        target_name = "test_target"
        expected_error_message = "Creation failed"

        # Mock external dependencies to isolate the error handling logic
        mock_create_target = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.create_target",
            side_effect=Exception(expected_error_message),
        )
        mocker.patch("marimba.core.cli.new.find_project_dir_or_exit", return_value=project_dir)
        mocker.patch(
            "marimba.core.wrappers.target.DistributionTargetWrapper.prompt_target",
            return_value=("target_type", "target_config"),
        )

        # Act - Execute CLI command that should trigger error handling
        result = runner.invoke(
            marimba_cli,
            ["new", "target", target_name, "--project-dir", str(project_dir)],
        )

        # Assert - Verify error handling behavior and exit code
        assert result.exit_code == 1, f"Command should exit with error code 1, got {result.exit_code}"
        assert "Could not create target:" in result.output, f"Should show creation error message, got: {result.output}"
        assert (
            expected_error_message in result.output
        ), f"Should include specific error details '{expected_error_message}', got: {result.output}"

        # Assert - Verify the correct method was called with expected arguments
        mock_create_target.assert_called_once_with(target_name, "target_type", "target_config")


# ---------------------------------------------------------------------------------------------------------------------#
# Testing project directory detection from subdirectories
# ---------------------------------------------------------------------------------------------------------------------#


class TestProjectDirectoryDetection:
    """Test class for project directory detection functionality."""

    @pytest.mark.integration
    def test_find_project_dir_or_exit_with_marimba_as_file(self, tmp_path: Path) -> None:
        """
        Test CLI commands properly handle case where .marimba exists as file instead of directory.

        This integration test verifies that when .marimba exists as a file instead of a directory,
        find_project_dir_or_exit cannot locate a valid Marimba project directory and the CLI
        command exits with appropriate error code and message.
        """
        # Arrange
        project_dir = tmp_path / "invalid_project"
        project_dir.mkdir()

        # Create .marimba as a file instead of directory (invalid project structure)
        marimba_file = project_dir / ".marimba"
        marimba_file.write_text("invalid")

        collection_name = "test_collection"

        # Act
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(project_dir)],
        )

        # Assert
        assert result.exit_code == 1, (
            f"Command should exit with error code 1 when .marimba exists as file instead of directory, "
            f"got {result.exit_code} with output: {result.output}"
        )

        # Verify specific error message for project not found scenario
        expected_error_fragments = ["Could not find a", "project"]
        for fragment in expected_error_fragments:
            assert (
                fragment in result.output
            ), f"Error message should contain '{fragment}' when .marimba exists as file, got: {result.output}"

    @pytest.mark.unit
    def test_find_project_dir_or_exit_with_no_read_access(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test CLI commands properly handle failure to locate project directory.

        This unit test verifies that when find_project_dir returns None (simulating
        scenarios like permission issues or missing project structure), the CLI command
        exits with error code 1 and displays the exact error message from find_project_dir_or_exit.
        Uses mocking to isolate the error handling logic from filesystem dependencies.
        """
        # Arrange
        collection_name = "test_collection"

        # Mock find_project_dir within the paths module to return None
        # This simulates scenarios where project directory cannot be located
        # (permission issues, missing .marimba directory, etc.)
        mock_find_project_dir = mocker.patch("marimba.core.utils.paths.find_project_dir")
        mock_find_project_dir.return_value = None

        # Act - Don't specify project-dir to test find_project_dir_or_exit behavior
        # when it needs to search from current directory
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name],
        )

        # Assert - Verify CLI exits with error code 1
        assert (
            result.exit_code == 1
        ), f"Command should exit with error code 1 when project not found, got {result.exit_code}"

        # Assert - Verify specific error message components from find_project_dir_or_exit
        assert "Could not find a" in result.output, f"Should show project not found error, got: {result.output}"
        assert "project." in result.output, f"Should show complete error message, got: {result.output}"

        # Assert - Verify find_project_dir was called with current working directory
        # find_project_dir_or_exit should call find_project_dir with Path.cwd() when no project_dir specified
        mock_find_project_dir.assert_called_once()
        call_args = mock_find_project_dir.call_args[0][0]
        assert isinstance(call_args, Path), "find_project_dir should be called with a Path object"

    @pytest.mark.integration
    def test_find_project_dir_or_exit_with_invalid_project_dir(self, tmp_path: Path) -> None:
        """
        Test CLI commands properly handle invalid project directory detection.

        This integration test verifies that when find_project_dir_or_exit cannot locate
        a valid Marimba project directory, the CLI command exits with appropriate error
        code and message, testing the real interaction between project detection and CLI error handling.
        """
        # Arrange - Create directory without .marimba subdirectory (invalid project)
        invalid_project_dir = tmp_path / "not_a_project"
        invalid_project_dir.mkdir()
        collection_name = "test_collection"

        # Verify precondition - directory exists but has no .marimba subdirectory
        assert invalid_project_dir.exists(), "Directory should exist before test"
        assert not (invalid_project_dir / ".marimba").exists(), "Should not have .marimba subdirectory"

        # Act - Execute CLI command with invalid project directory
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(invalid_project_dir)],
        )

        # Assert - Verify CLI exits with error code 1 and displays specific error message
        assert (
            result.exit_code == 1
        ), f"Command should exit with error code 1 when project not found, got {result.exit_code}"
        assert "Could not find a" in result.output, f"Should show project not found error message, got: {result.output}"
        assert "project" in result.output, f"Should mention project in error message, got: {result.output}"

    @pytest.mark.integration
    def test_find_project_dir_from_subdir_executes_successfully(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test CLI commands can find project directory when executed from subdirectory.

        This integration test verifies that CLI commands properly locate the project
        root directory when invoked from within subdirectories of the project,
        testing the real interaction between CLI argument parsing, project detection,
        and the find_project_dir_or_exit utility function with minimal mocking of
        external dependencies only.
        """
        # Arrange - Create real project structure and subdirectory
        project_dir = tmp_path / "test_project"
        collection_name = "test_collection"

        # Create a real Marimba project structure for integration testing
        ProjectWrapper.create(project_dir)

        # Verify precondition - project structure exists
        assert (project_dir / ".marimba").exists(), "Project should have .marimba directory"

        # Create nested subdirectories to test project detection from subdirectory
        subdir = project_dir / "workspace" / "analysis" / "deep"
        subdir.mkdir(parents=True)

        # Mock only external dependencies that require user interaction
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )

        # Mock working directory to simulate running from subdirectory without affecting other tests
        mock_cwd = mocker.patch("marimba.core.utils.paths.Path.cwd", return_value=subdir)

        # Act - Execute CLI command without specifying --project-dir
        # This tests that find_project_dir_or_exit correctly locates the project root from subdirectory
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name],
        )

        # Assert - Command should succeed by finding project directory from subdirectory
        assert result.exit_code == 0, f"Command should succeed when run from subdirectory, got output: {result.output}"
        assert "Created new Marimba collection" in result.output, "Success message should be displayed"
        assert f'"{collection_name}"' in result.output, "Collection name should be in output"

        # Verify project directory detection was attempted from the subdirectory
        mock_cwd.assert_called(), "Project directory detection should check current working directory"

        # Verify the prompt configuration was called correctly
        mock_prompt_config.assert_called_once_with(parent_collection_name=None, config={}, accept_defaults=False)

        # Verify collection was actually created in the real project structure
        expected_collection_dir = project_dir / "collections" / collection_name
        assert expected_collection_dir.exists(), f"Collection directory should exist at {expected_collection_dir}"

        # Verify collection has proper structure (integration test for real collection creation)
        assert (expected_collection_dir / "collection.yml").exists(), "Collection should have configuration file"

    @pytest.mark.integration
    def test_find_project_dir_or_exit_with_symlink(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test CLI commands properly handle project directory accessed through symlink.

        This integration test verifies that when a Marimba project is accessed through
        a symbolic link, find_project_dir_or_exit correctly resolves the project
        directory and CLI commands execute successfully. Tests real filesystem operations
        and project detection logic with minimal mocking of user interaction dependencies.
        """
        # Arrange - Create real project structure and symlink
        real_project_dir = tmp_path / "real_project"
        ProjectWrapper.create(real_project_dir)

        # Verify precondition - real project exists with proper structure
        assert real_project_dir.exists(), "Real project directory should exist"
        assert (real_project_dir / ".marimba").exists(), "Real project should have .marimba directory"

        # Create symbolic link to the project directory
        symlink_project_dir = tmp_path / "symlinked_project"
        symlink_project_dir.symlink_to(real_project_dir)

        # Verify symlink was created correctly
        assert symlink_project_dir.exists(), "Symlinked project directory should exist"
        assert symlink_project_dir.is_symlink(), "Directory should be a symlink"
        assert symlink_project_dir.resolve() == real_project_dir.resolve(), "Symlink should resolve to real project"

        collection_name = "test_collection"

        # Mock only external user interaction dependency to ensure test stability
        mock_collection_config = {"parent": None, "metadata_schema": "ifdo"}
        mock_prompt_config = mocker.patch(
            "marimba.core.wrappers.project.ProjectWrapper.prompt_collection_config",
            return_value=mock_collection_config,
        )

        # Act - Execute CLI command using symlink path as project directory
        result = runner.invoke(
            marimba_cli,
            ["new", "collection", collection_name, "--project-dir", str(symlink_project_dir)],
        )

        # Assert - Verify successful command execution with symlinked project directory
        assert result.exit_code == 0, (
            f"Command should succeed with symlinked project directory, got exit code {result.exit_code} "
            f"with output: {result.output}"
        )
        assert (
            "Created new Marimba collection" in result.output
        ), f"Success message should be displayed, got: {result.output}"
        assert (
            f'"{collection_name}"' in result.output
        ), f"Collection name '{collection_name}' should appear in success message, got: {result.output}"

        # Assert - Verify real component interactions occurred correctly
        mock_prompt_config.assert_called_once_with(parent_collection_name=None, config={}, accept_defaults=False)

        # Assert - Verify collection was created in the real project directory through symlink resolution
        real_collections_dir = real_project_dir / "collections"
        assert (
            real_collections_dir.exists()
        ), "Collections directory should exist in real project directory after symlink resolution"

        real_collection_dir = real_collections_dir / collection_name
        assert (
            real_collection_dir.exists()
        ), f"Collection directory '{collection_name}' should be created in real project directory"

        # Assert - Verify collection has proper structure and configuration
        collection_config_file = real_collection_dir / "collection.yml"
        assert collection_config_file.exists(), "Collection should have configuration file in real project"

        # Assert - Verify collection is accessible through symlink path (filesystem behavior test)
        symlink_collections_dir = symlink_project_dir / "collections"
        symlink_collection_dir = symlink_collections_dir / collection_name
        assert (
            symlink_collection_dir.exists()
        ), "Collection should be accessible through symlink path due to filesystem symlink resolution"
        assert (
            symlink_collection_dir.resolve() == real_collection_dir.resolve()
        ), "Symlinked collection path should resolve to the same location as real collection path"
