"""Tests for marimba.main CLI module."""

from pathlib import Path
from typing import Any

import pytest
import pytest_mock
import typer
from _pytest.capture import CaptureFixture
from typer.testing import CliRunner

from marimba.core.utils.constants import Operation
from marimba.core.utils.log import LogLevel
from marimba.main import (
    global_options,
    marimba_cli,
    version_callback,
)
from tests.conftest import assert_cli_failure, assert_cli_success


class TestCLI:
    """Test CLI functionality."""

    @pytest.fixture
    def runner(self, cli_runner: CliRunner) -> CliRunner:
        return cli_runner

    @pytest.fixture
    def mock_project_dir(self, tmp_path):
        """Create a mock project directory."""
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        return project_dir

    @pytest.mark.unit
    def test_cli_initialization(self) -> None:
        """Test that the CLI app is created with correct Typer configuration.

        This test verifies that the marimba_cli object is properly initialized as a Typer instance
        with the expected name, help text, FAIR scientific datasets description, and other Typer
        configuration options. It ensures the CLI application is correctly configured for user
        interaction.
        """
        # Arrange - marimba_cli is already imported and initialized at module level

        # Act - No explicit action needed, testing the imported CLI object

        # Assert - Verify CLI object is a Typer instance
        assert isinstance(
            marimba_cli,
            typer.Typer,
        ), "marimba_cli must be an instance of typer.Typer"

        # Assert - Verify CLI name configuration
        assert marimba_cli.info.name == "Marimba", f"Expected CLI name 'Marimba', got '{marimba_cli.info.name}'"

        # Assert - Verify CLI has help text configured
        assert marimba_cli.info.help is not None, "CLI must have help text configured (info.help cannot be None)"

        # Assert - Verify help text contains expected FAIR scientific datasets description
        assert (
            "FAIR scientific image datasets" in marimba_cli.info.help
        ), f"CLI help must mention 'FAIR scientific image datasets', got: {marimba_cli.info.help}"

        # Assert - Verify no_args_is_help configuration
        assert (
            marimba_cli.info.no_args_is_help is True
        ), "CLI must be configured with no_args_is_help=True to show help when invoked without arguments"

    @pytest.mark.unit
    def test_version_callback_with_version(
        self,
        mocker: pytest_mock.MockerFixture,
        capsys: CaptureFixture[str],
    ) -> None:
        """Test version callback displays version string and exits when version is available.

        This test verifies that when importlib.metadata.version successfully returns a version
        string and the version_callback function is called with True, it prints the formatted
        version string "Marimba v{version}" to stdout and raises typer.Exit with the default
        exit code 0.
        """
        # Arrange - Mock the version lookup in the marimba.main module
        mocker.patch("marimba.main.importlib.metadata.version", return_value="1.0.0")

        # Act & Assert - Version callback should print version and raise typer.Exit
        with pytest.raises(typer.Exit) as exc_info:
            version_callback(True)

        # Assert - Verify the version message was printed to stdout
        captured = capsys.readouterr()
        assert "Marimba v1.0.0" in captured.out, f"Expected 'Marimba v1.0.0' in output, got: {captured.out}"

        # Assert - Verify typer.Exit was raised with default exit code 0
        assert exc_info.value.exit_code == 0, f"Expected exit code 0, got {exc_info.value.exit_code}"

    @pytest.mark.unit
    def test_version_callback_without_flag(self, capsys: CaptureFixture[str]) -> None:
        """Test version callback returns normally when flag is False without printing or exiting.

        This test verifies that when version_callback is called with False, it simply returns
        without printing version information, raising typer.Exit, or causing any side effects.
        This is the expected behavior when the --version flag is not provided by the user.
        """
        # Arrange - No setup needed, testing with False parameter

        # Act - Call version_callback with False (version flag not set)
        # This should complete without raising an exception or exiting
        version_callback(False)

        # Assert - Verify no output was printed to stdout or stderr
        captured = capsys.readouterr()
        assert captured.out == "", f"version_callback(False) should not print to stdout, got: {captured.out}"
        assert captured.err == "", f"version_callback(False) should not print to stderr, got: {captured.err}"

    @pytest.mark.unit
    def test_version_callback_with_exception(
        self,
        mocker: pytest_mock.MockerFixture,
        capsys: CaptureFixture[str],
    ) -> None:
        """Test version callback when metadata is not available and displays fallback message.

        This test verifies that when importlib.metadata.version raises PackageNotFoundError,
        the version_callback function handles it gracefully by printing a fallback
        message indicating the version is unknown (not installed as package) and
        then raises typer.Exit to terminate the CLI execution.
        """
        # Arrange
        import importlib.metadata

        mocker.patch(
            "marimba.main.importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError("marimba"),
        )

        # Act & Assert - version_callback should catch the exception and exit gracefully
        with pytest.raises(typer.Exit) as exc_info:
            version_callback(True)

        # Assert - Verify the fallback message was printed to stdout
        captured = capsys.readouterr()
        assert (
            "unknown (not installed as package)" in captured.out
        ), f"Expected 'unknown (not installed as package)' in output, got: {captured.out}"

        # Assert - Verify typer.Exit was raised with default exit code 0 (successful exit despite missing version)
        assert exc_info.value.exit_code == 0, f"Expected exit code 0, got {exc_info.value.exit_code}"

    @pytest.mark.unit
    def test_version_command_with_version(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
    ) -> None:
        """Test version command displays version string when metadata is available.

        This test verifies that when the CLI 'version' command is invoked and
        importlib.metadata.version successfully returns a version string, the command
        prints the formatted version string "Marimba v{version}" to stdout and exits
        successfully with code 0. This is a unit test because it tests the version
        command in isolation with mocked metadata lookup.
        """
        # Arrange - Mock the metadata version lookup
        mocker.patch("marimba.main.importlib.metadata.version", return_value="1.0.0")

        # Act - Execute the version command via CLI
        result = runner.invoke(marimba_cli, ["version"])

        # Assert - Verify successful execution with expected version message and exit code
        assert_cli_success(result, expected_message="Marimba v1.0.0", context="Version command")

    @pytest.mark.integration
    def test_version_command_with_exception(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
    ) -> None:
        """Test version command when package metadata is not available and displays fallback message.

        This test verifies that when importlib.metadata.version raises PackageNotFoundError during
        the version command execution, the CLI handles it gracefully by displaying the
        fallback message 'unknown (not installed as package)' and exits successfully
        with code 0. This validates that missing package metadata is handled gracefully when
        the package is not installed via pip/uv.
        """
        # Arrange - Mock the version lookup to raise PackageNotFoundError
        import importlib.metadata

        mocker.patch(
            "marimba.main.importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError("marimba"),
        )

        # Act - Execute the version command via CLI
        result = runner.invoke(marimba_cli, ["version"])

        # Assert - Verify successful execution with fallback message
        assert_cli_success(
            result,
            expected_message="unknown (not installed as package)",
            context="Version command without metadata",
        )

        # Assert - Verify the complete fallback message is displayed
        assert (
            "Marimba version: unknown (not installed as package)" in result.stdout
        ), f"Expected complete fallback message in output, got: {result.stdout}"

        # Assert - Verify successful exit code (0) despite missing version
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    @pytest.mark.unit
    def test_global_options_with_debug(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test global options configures logging handler with DEBUG level.

        This test verifies that when LogLevel.DEBUG is passed to global_options,
        the function retrieves the rich handler and sets its level to DEBUG (numeric value 10).
        It validates that the logging level conversion and handler configuration work correctly.
        """
        # Arrange
        mock_ctx = mocker.Mock()
        mock_ctx.invoked_subcommand = "import"  # Simulate subcommand invocation

        mock_log_handler = mocker.Mock()
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_get_rich_handler.return_value = mock_log_handler

        # Act
        global_options(ctx=mock_ctx, level=LogLevel.DEBUG, version=False)

        # Assert - Verify get_rich_handler was called to retrieve the handler
        mock_get_rich_handler.assert_called_once()

        # Assert - Verify setLevel was called with DEBUG level (numeric value 10)
        mock_log_handler.setLevel.assert_called_once_with(10)

    @pytest.mark.unit
    def test_global_options_with_quiet(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test global options configures logging handler with ERROR level (quiet mode).

        This test verifies that when LogLevel.ERROR is passed to global_options,
        the function retrieves the rich handler and sets its level to ERROR (numeric value 40).
        It validates that the logging level conversion and handler configuration work correctly
        for quiet mode, which suppresses all logging except errors.
        """
        # Arrange
        mock_ctx = mocker.Mock()
        mock_ctx.invoked_subcommand = "import"  # Simulate subcommand invocation

        mock_log_handler = mocker.Mock()
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_get_rich_handler.return_value = mock_log_handler

        # Act
        global_options(ctx=mock_ctx, level=LogLevel.ERROR, version=False)

        # Assert - Verify get_rich_handler was called to retrieve the handler
        mock_get_rich_handler.assert_called_once()

        # Assert - Verify setLevel was called with ERROR level (numeric value 40)
        mock_log_handler.setLevel.assert_called_once_with(40)

    @pytest.mark.unit
    def test_global_options_with_warning_level(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test global options configures logging handler with WARNING level.

        This test verifies that when LogLevel.WARNING is passed to global_options,
        the function retrieves the rich handler and sets its level to WARNING (numeric value 30).
        It validates that the logging level conversion and handler configuration work correctly
        for the WARNING level, which is commonly used for important messages without debug verbosity.
        """
        # Arrange
        mock_ctx = mocker.Mock()
        mock_ctx.invoked_subcommand = "import"  # Simulate subcommand invocation

        mock_log_handler = mocker.Mock()
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_get_rich_handler.return_value = mock_log_handler

        # Act
        global_options(ctx=mock_ctx, level=LogLevel.WARNING, version=False)

        # Assert - Verify get_rich_handler was called to retrieve the handler
        mock_get_rich_handler.assert_called_once()

        # Assert - Verify setLevel was called with WARNING level (numeric value 30)
        mock_log_handler.setLevel.assert_called_once_with(30)

    @pytest.mark.integration
    def test_import_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic import command functionality with default parameters.

        This test verifies that the import command correctly processes command line arguments,
        resolves the project directory, initializes ProjectWrapper with correct parameters,
        creates a new collection when it doesn't exist, and executes the import operation
        with proper parameter passing including the default copy operation and success messaging.
        """
        # Arrange
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Set up mock project instance with realistic behavior
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.collection_wrappers = {}  # No existing collections
        mock_project.pipeline_wrappers = {"test_pipeline": mocker.Mock()}
        mock_project.prompt_collection_config.return_value = {"schema": "ifdo", "version": "1.0"}
        mock_project.create_collection.return_value = None
        mock_project.run_import.return_value = None

        mock_find_project.return_value = mock_project_dir

        # Create source directory structure
        source_path = mock_project_dir / "source"
        source_path.mkdir()

        # Act
        result = runner.invoke(
            marimba_cli,
            ["import", "test_collection", str(source_path), "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify successful execution and output message
        assert_cli_success(
            result,
            expected_message="Imported data into collection",
            context="Basic import command",
        )
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert "test_collection" in result.stdout, f"Collection name should appear in output, got: {result.stdout}"

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=False)

        # Assert - Verify collection creation workflow
        mock_project.prompt_collection_config.assert_called_once_with(
            parent_collection_name=None,
            config={},
            accept_defaults=False,
        )
        mock_project.create_collection.assert_called_once_with(
            "test_collection",
            {"schema": "ifdo", "version": "1.0"},
        )

        # Assert - Verify import execution with correct parameters including exact operation
        import_call = mock_project.run_import.call_args
        assert import_call is not None, "run_import should have been called"
        assert import_call[0][0] == "test_collection", "Collection name should be 'test_collection'"
        assert import_call[0][1] == [source_path], f"Source paths should be [{source_path}]"
        assert import_call[0][2] == ["test_pipeline"], "Should use all available pipelines when none specified"
        assert import_call[1]["extra_args"] == [], "Extra args should be empty list"
        assert import_call[1]["operation"] == Operation.copy, "Default operation should be copy"
        assert import_call[1]["max_workers"] is None, "Max workers should be None by default"

    @pytest.mark.integration
    def test_import_command_with_options(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test import command with overwrite and dry-run options.

        This test verifies that the import command correctly processes additional command line options
        (--overwrite and --dry-run), properly configures the ProjectWrapper with dry-run mode,
        creates a new collection when overwrite is enabled, and executes the import operation
        with correct parameter passing while handling dry-run mode appropriately.
        """
        # Arrange
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Set up mock project instance with realistic behavior
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.collection_wrappers = {}  # No existing collections
        mock_project.pipeline_wrappers = {"pipeline1": mocker.Mock()}
        mock_project.prompt_collection_config.return_value = {"test": "config"}
        mock_project.create_collection.return_value = None
        mock_project.run_import.return_value = None

        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Create source directory structure
        source_path = mock_project_dir / "source"
        source_path.mkdir()

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "import",
                "test_collection",
                str(source_path),
                "--project-dir",
                str(mock_project_dir),
                "--overwrite",
                "--dry-run",
            ],
        )

        # Assert
        assert_cli_success(
            result,
            expected_message="Imported data into collection",
            context="Import command with overwrite and dry-run options",
        )

        # Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Verify ProjectWrapper instantiation with dry-run enabled
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=True)

        # Verify dry-run mode configuration
        mock_rich_handler.set_dry_run.assert_called_once_with(True)

        # Verify collection creation workflow with proper config
        mock_project.prompt_collection_config.assert_called_once_with(
            parent_collection_name=None,
            config={},
            accept_defaults=False,
        )
        mock_project.create_collection.assert_called_once_with(
            "test_collection",
            {"test": "config"},
        )

        # Verify import execution with correct parameters including operation and max_workers
        import_call = mock_project.run_import.call_args
        assert import_call is not None, "run_import should have been called"
        assert import_call[0][0] == "test_collection", "Collection name should be 'test_collection'"
        assert import_call[0][1] == [source_path], f"Source paths should be [{source_path}]"
        assert import_call[0][2] == ["pipeline1"], "Should use all available pipelines when none specified"
        assert import_call[1]["extra_args"] == [], "Extra args should be empty list"
        assert import_call[1]["operation"] == Operation.copy, "Default operation should be copy"
        assert import_call[1]["max_workers"] is None, "Max workers should be None by default"

    @pytest.mark.integration
    def test_process_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic process command functionality.

        This test verifies that the process command correctly processes command line arguments,
        resolves the project directory, initializes ProjectWrapper with correct parameters,
        executes the run_process operation with specified collection and pipeline names,
        and displays the appropriate success message upon completion. It validates the core
        workflow for processing a single collection with a single pipeline.
        """
        # Arrange
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Set up mock project instance with realistic behavior
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.run_process.return_value = None
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"test_pipeline": mocker.Mock()}
        mock_find_project.return_value = mock_project_dir

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "process",
                "--collection-name",
                "test_collection",
                "--pipeline-name",
                "test_pipeline",
                "--project-dir",
                str(mock_project_dir),
            ],
        )

        # Assert - Verify successful execution with expected message
        assert_cli_success(
            result,
            expected_message="Processed data for pipeline",
            context="Basic process command",
        )
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert "test_pipeline" in result.stdout, f"Pipeline name should appear in output, got: {result.stdout}"
        assert "test_collection" in result.stdout, f"Collection name should appear in output, got: {result.stdout}"

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=False)

        # Assert - Verify run_process method called with correct parameters
        mock_project.run_process.assert_called_once_with(
            ["test_collection"],
            ["test_pipeline"],
            [],  # extra args
            max_workers=None,
        )

    @pytest.mark.integration
    def test_process_command_with_dry_run_option(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test process command with dry-run option.

        This test verifies that the process command correctly handles the --dry-run flag,
        configuring the ProjectWrapper with dry_run=True, setting up the rich handler
        for dry-run mode, executing the run_process operation without modifying files,
        and displaying the appropriate success message. It validates that dry-run mode
        is properly propagated through the CLI layer to the business logic.
        """
        # Arrange
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Set up mock project instance with realistic behavior
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.run_process.return_value = None
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"test_pipeline": mocker.Mock()}
        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "process",
                "--collection-name",
                "test_collection",
                "--pipeline-name",
                "test_pipeline",
                "--project-dir",
                str(mock_project_dir),
                "--dry-run",
            ],
        )

        # Assert
        assert_cli_success(
            result,
            expected_message="Processed data for pipeline",
            context="Process command with dry-run option",
        )

        # Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Verify ProjectWrapper instantiation with dry-run enabled
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=True)

        # Verify dry-run mode configuration
        mock_rich_handler.set_dry_run.assert_called_once_with(True)

        # Verify run_process method called with correct parameters
        mock_project.run_process.assert_called_once_with(
            ["test_collection"],
            ["test_pipeline"],
            [],  # extra args
            max_workers=None,
        )

    @pytest.mark.integration
    def test_package_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic package command functionality.

        This test verifies that the package command correctly processes command line arguments,
        resolves the project directory, initializes ProjectWrapper with correct parameters,
        composes collections into a dataset mapping, creates the dataset with appropriate
        configuration, verifies resource cleanup, and displays the appropriate success message.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project

        # Set up realistic mock behavior for package workflow
        mock_collection = mocker.Mock()
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"pipeline1": mocker.Mock()}
        mock_dataset_mapping = {"test_collection": mock_collection}
        mock_project.compose.return_value = mock_dataset_mapping
        mock_dataset = mocker.Mock()
        mock_dataset.root_dir = mock_project_dir / "datasets" / "test_dataset"
        mock_project.create_dataset.return_value = mock_dataset
        mock_project.get_pipeline_post_processors.return_value = []
        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Mock validate_dependencies to avoid external dependency checks
        mocker.patch("marimba.main.validate_dependencies")

        # Act
        result = runner.invoke(
            marimba_cli,
            ["package", "test_dataset", "--collection-name", "test_collection", "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify successful execution and output message
        assert_cli_success(
            result,
            expected_message="Packaged dataset",
            context="Basic package command",
        )
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert "Packaged dataset" in result.stdout, f"Expected success message in output, got: {result.stdout}"

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(
            mock_project_dir,
        ), "find_project_dir_or_exit should be called once with the project directory"

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(
            mock_project_dir,
            dry_run=False,
        ), "ProjectWrapper should be instantiated with project_dir and dry_run=False"

        # Assert - Verify dry-run mode setup
        mock_rich_handler.set_dry_run.assert_called_once_with(
            False,
        ), "Rich handler should be configured with dry_run=False"

        # Assert - Verify compose method called with correct parameters
        mock_project.compose.assert_called_once_with(
            "test_dataset",
            ["test_collection"],
            ["pipeline1"],  # Should use all available pipelines when none specified
            [],  # extra args
            max_workers=None,
        ), "compose should be called with correct dataset name, collections, pipelines, and parameters"

        # Assert - Verify get_pipeline_post_processors called with correct pipeline names
        mock_project.get_pipeline_post_processors.assert_called_once_with(
            ["pipeline1"],
        ), "get_pipeline_post_processors should be called with correct pipeline names"

        # Assert - Verify create_dataset method called with correct parameters
        create_dataset_call = mock_project.create_dataset.call_args
        assert create_dataset_call is not None, "create_dataset should have been called"
        assert create_dataset_call[0][0] == "test_dataset", "Dataset name should be 'test_dataset'"
        assert (
            create_dataset_call[0][1] == mock_dataset_mapping
        ), f"Dataset mapping should be the exact mapping returned from compose, got {create_dataset_call[0][1]}"

        # Assert - Verify dataset wrapper cleanup
        mock_dataset.close.assert_called_once(), "Dataset wrapper close() should be called for resource cleanup"

    @pytest.mark.integration
    def test_package_command_with_options(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test package command with version, contact, and zoom options.

        This test verifies that the package command correctly processes additional command line options
        (--version, --contact-name, --contact-email, --zoom), properly passes these options through to
        the create_dataset method, and executes the complete packaging workflow with custom metadata.
        It ensures that optional configuration parameters are correctly propagated through the CLI layer
        to the underlying business logic.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project

        # Set up realistic mock behavior for package workflow with options
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"pipeline1": mocker.Mock()}
        mock_project.compose.return_value = {"test_collection": mocker.Mock()}
        mock_dataset = mocker.Mock()
        mock_dataset.root_dir = mock_project_dir / "datasets" / "test_dataset"
        mock_project.create_dataset.return_value = mock_dataset
        mock_project.get_pipeline_post_processors.return_value = []
        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Mock validate_dependencies to avoid external dependency checks
        mocker.patch("marimba.main.validate_dependencies")

        # Act
        result = runner.invoke(
            marimba_cli,
            [
                "package",
                "test_dataset",
                "--collection-name",
                "test_collection",
                "--project-dir",
                str(mock_project_dir),
                "--version",
                "2.0",
                "--contact-name",
                "Test User",
                "--contact-email",
                "test@example.com",
                "--zoom",
                "5",
            ],
        )

        # Assert
        assert_cli_success(
            result,
            expected_message="Packaged dataset",
            context="Package command with version, contact, and zoom options",
        )

        # Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=False)

        # Verify dry-run mode setup
        mock_rich_handler.set_dry_run.assert_called_once_with(False)

        # Verify compose method called with correct parameters
        mock_project.compose.assert_called_once_with(
            "test_dataset",
            ["test_collection"],
            ["pipeline1"],  # Should use all available pipelines when none specified
            [],  # extra args
            max_workers=None,
        )

        # Verify create_dataset method called with correct parameters including custom options
        create_dataset_call = mock_project.create_dataset.call_args
        assert create_dataset_call is not None, "create_dataset should have been called"

        # Assert - Verify positional arguments
        assert create_dataset_call[0][0] == "test_dataset", "Dataset name should be 'test_dataset'"

        # Assert - Verify dataset mapping contains the expected collection
        dataset_mapping = create_dataset_call[0][1]
        assert (
            "test_collection" in dataset_mapping
        ), f"Dataset mapping should contain 'test_collection', got keys: {list(dataset_mapping.keys())}"

        # Assert - Verify optional parameters are passed through correctly with exact values
        assert (
            create_dataset_call[1]["version"] == "2.0"
        ), f"Version parameter should be '2.0', got '{create_dataset_call[1].get('version')}'"
        assert (
            create_dataset_call[1]["contact_name"] == "Test User"
        ), f"Contact name should be 'Test User', got '{create_dataset_call[1].get('contact_name')}'"
        assert (
            create_dataset_call[1]["contact_email"] == "test@example.com"
        ), f"Contact email should be 'test@example.com', got '{create_dataset_call[1].get('contact_email')}'"
        assert create_dataset_call[1]["zoom"] == 5, f"Zoom level should be 5, got {create_dataset_call[1].get('zoom')}"

        # Verify dataset wrapper cleanup
        mock_dataset.close.assert_called_once()

    @pytest.mark.integration
    def test_distribute_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic distribute command functionality.

        This test verifies that the distribute command successfully invokes the ProjectWrapper.distribute
        method with correct parameters, handles project directory resolution, sets up dry-run mode,
        and displays appropriate success message upon completion.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.distribute.return_value = None
        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Act
        result = runner.invoke(
            marimba_cli,
            ["distribute", "test_dataset", "test_target", "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify successful execution
        assert_cli_success(
            result,
            expected_message="Successfully distributed dataset test_dataset",
            context="Basic distribute command",
        )

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(
            mock_project_dir,
        ), "find_project_dir_or_exit should be called once with the project directory"

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(
            mock_project_dir,
            dry_run=False,
        ), "ProjectWrapper should be instantiated with project_dir and dry_run=False"

        # Assert - Verify dry-run mode setup
        mock_rich_handler.set_dry_run.assert_called_once_with(
            False,
        ), "Rich handler should be configured with dry_run=False"

        # Assert - Verify distribute method called with correct parameters
        mock_project.distribute.assert_called_once_with(
            "test_dataset",
            "test_target",
            True,
        ), "distribute method should be called with dataset_name, target_name, and validate=True (default)"

    @pytest.mark.integration
    def test_update_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic update command functionality.

        This test verifies that the update command correctly processes command line arguments,
        resolves the project directory, initializes ProjectWrapper with correct parameters,
        executes the update_pipelines operation to pull all pipeline repositories,
        and displays the appropriate success message upon completion.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.update_pipelines.return_value = None
        mock_find_project.return_value = mock_project_dir

        # Act
        result = runner.invoke(marimba_cli, ["update", "--project-dir", str(mock_project_dir)])

        # Assert - Verify successful execution and output message
        assert_cli_success(
            result,
            expected_message="Successfully updated (pulled) all pipeline repositories",
            context="Basic update command",
        )
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert (
            "Successfully updated (pulled) all pipeline repositories" in result.stdout
        ), f"Expected success message in output, got: {result.stdout}"

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir)

        # Assert - Verify update_pipelines method execution
        mock_project.update_pipelines.assert_called_once_with()

    @pytest.mark.integration
    def test_install_command_basic(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test basic install command functionality.

        This test verifies that the install command correctly processes command line arguments,
        resolves the project directory, initializes ProjectWrapper with correct parameters,
        executes the install_pipelines operation, and displays the appropriate success message
        upon completion. It validates the core CLI workflow for pipeline installation.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.install_pipelines.return_value = None
        mock_find_project.return_value = mock_project_dir

        # Act
        result = runner.invoke(marimba_cli, ["install", "--project-dir", str(mock_project_dir)])

        # Assert - Verify successful execution and output message
        assert_cli_success(
            result,
            expected_message="Successfully installed all pipeline dependencies",
            context="Basic install command",
        )
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert (
            "Successfully installed all pipeline dependencies" in result.stdout
        ), f"Expected success message in output, got: {result.stdout}"

        # Assert - Verify project directory resolution
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir)

        # Assert - Verify install_pipelines method execution
        mock_project.install_pipelines.assert_called_once_with()

    @pytest.mark.integration
    def test_install_command_error_handling(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test install command error handling when pipeline installation fails.

        This test verifies that when ProjectWrapper.install_pipelines() raises an exception,
        the CLI command properly handles the error, logs it, displays an appropriate
        error message to the user, and exits with code 1. The error message should contain
        details about the failure.
        """
        # Arrange
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.install_pipelines.side_effect = RuntimeError("Installation failed")
        mock_find_project.return_value = mock_project_dir

        # Act
        result = runner.invoke(marimba_cli, ["install", "--project-dir", str(mock_project_dir)])

        # Assert
        assert_cli_failure(
            result,
            expected_exit_code=1,
            context="Install command error handling",
        )

        # Verify error message contains failure details
        assert (
            "Could not install pipelines: Installation failed" in result.stdout
        ), "Error message should contain failure details"

        # Verify install_pipelines method was attempted
        mock_project.install_pipelines.assert_called_once_with()

        # Verify project directory resolution and wrapper instantiation
        mock_find_project.assert_called_once_with(mock_project_dir)
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir)

    @pytest.mark.unit
    def test_cli_help(self, runner: CliRunner) -> None:
        """Test CLI help output displays application name and FAIR scientific datasets description.

        This test verifies that when the --help flag is provided to the marimba_cli,
        the help output includes the application name "Marimba" and describes its purpose
        for managing FAIR scientific image datasets. This validates the CLI's user-facing
        documentation is correctly configured.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act - Invoke CLI with --help flag
        result = runner.invoke(marimba_cli, ["--help"])

        # Assert - Verify successful execution and help output contains expected information
        assert_cli_success(result, expected_message="Marimba", context="CLI help")

        # Assert - Verify help mentions FAIR scientific purpose
        assert (
            "FAIR scientific" in result.stdout
        ), f"Help should mention 'FAIR scientific' purpose, got: {result.stdout}"

        # Assert - Verify help mentions image datasets functionality
        assert (
            "image datasets" in result.stdout
        ), f"Help should mention 'image datasets' functionality, got: {result.stdout}"

    @pytest.mark.unit
    def test_cli_no_args(self, runner: CliRunner) -> None:
        """Test CLI with no arguments shows help due to no_args_is_help=True configuration.

        This test verifies that Typer's no_args_is_help configuration works correctly,
        displaying usage information and exiting with the expected code when no
        subcommand is provided. This is a unit test because it tests the CLI object's
        configuration behavior in isolation without involving external dependencies.
        """
        # Arrange - runner is already provided by fixture

        # Act
        result = runner.invoke(marimba_cli, [])

        # Assert - Verify exit code matches Typer's default behavior for no_args_is_help=True
        assert (
            result.exit_code == 2
        ), f"CLI should exit with code 2 when no arguments provided (Typer default behavior), got {result.exit_code}"

        # Assert - Verify usage information is displayed
        assert (
            "Usage:" in result.stdout
        ), f"CLI should display usage information when no arguments provided, got: {result.stdout}"

        # Assert - Verify application name is displayed
        assert "Marimba" in result.stdout, f"CLI help should display the application name, got: {result.stdout}"

    @pytest.mark.unit
    def test_import_command_help(self, runner: CliRunner) -> None:
        """Test import command help displays required arguments and usage information.

        This test verifies that the import command's help output includes the required
        collection_name and source-path arguments, and provides appropriate usage guidance
        to users for importing data into collections. This is a unit test because it tests
        the CLI help configuration in isolation without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["import", "--help"])

        # Assert - Verify successful help display
        assert_cli_success(result, context="Import command help")

        # Assert - Verify required arguments are shown
        assert (
            "source-path" in result.stdout or "SOURCE_PATH" in result.stdout
        ), f"Help should show source-path argument, got: {result.stdout}"
        assert "collection_name" in result.stdout, f"Help should show collection_name argument, got: {result.stdout}"

    @pytest.mark.unit
    def test_process_command_help(self, runner: CliRunner) -> None:
        """Test process command help displays required options and usage information.

        This test verifies that the process command's help output includes the collection-name
        and pipeline-name options, and provides appropriate usage guidance to users for processing
        collections with pipelines. This is a unit test because it tests the CLI help configuration
        in isolation without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["process", "--help"])

        # Assert - Verify successful help display
        assert_cli_success(result, context="Process command help")

        # Assert - Verify collection-name option is shown
        assert "collection-name" in result.stdout, f"Help should show collection-name option, got: {result.stdout}"

        # Assert - Verify pipeline-name option is shown
        assert "pipeline-name" in result.stdout, f"Help should show pipeline-name option, got: {result.stdout}"

    @pytest.mark.unit
    def test_package_command_help(self, runner: CliRunner) -> None:
        """Test package command help displays required arguments and usage information.

        This test verifies that the package command's help output includes the required
        dataset_name argument and collection-name option, and provides appropriate usage
        guidance to users for packaging collections into FAIR datasets. This is a unit test
        because it tests the CLI help configuration in isolation without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["package", "--help"])

        # Assert
        assert_cli_success(result, context="Package command help")
        assert "dataset_name" in result.stdout, "Help should show dataset_name argument"
        assert (
            "collection-name" in result.stdout or "COLLECTION_NAME" in result.stdout
        ), "Help should show collection-name option"

    @pytest.mark.unit
    def test_distribute_command_help(self, runner: CliRunner) -> None:
        """Test distribute command help displays required arguments and usage information.

        This test verifies that the distribute command's help output includes the required
        dataset-name argument and provides appropriate usage guidance to users. This is a unit
        test because it tests the CLI help configuration in isolation without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["distribute", "--help"])

        # Assert
        assert_cli_success(result, context="Distribute command help")
        assert "dataset_name" in result.stdout, "Help should show dataset_name argument"
        assert "target_name" in result.stdout, "Help should show target_name argument"

    @pytest.mark.unit
    def test_update_command_help(self, runner: CliRunner) -> None:
        """Test update command help displays usage information and options.

        This test verifies that the update command's help output provides appropriate
        usage guidance and shows available options for updating pipeline repositories.
        This is a unit test because it tests the CLI help configuration in isolation
        without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["update", "--help"])

        # Assert - Verify successful help display
        assert_cli_success(result, context="Update command help")

        # Assert - Verify help describes update functionality with exact docstring text
        assert (
            "Update (pull) all Marimba pipelines" in result.stdout
        ), f"Help should describe update functionality with exact text, got: {result.stdout}"

        # Assert - Verify project-dir option is shown (Typer formats it as --project-dir)
        assert "--project-dir" in result.stdout, f"Help should show --project-dir option, got: {result.stdout}"

    @pytest.mark.unit
    def test_install_command_help(self, runner: CliRunner) -> None:
        """Test install command help displays usage information and options.

        This test verifies that the install command's help output provides appropriate
        usage guidance and shows available options for installing pipeline dependencies.
        This is a unit test because it tests the CLI help configuration in isolation
        without external dependencies.
        """
        # Arrange - runner fixture provides CLI test runner

        # Act
        result = runner.invoke(marimba_cli, ["install", "--help"])

        # Assert
        assert_cli_success(result, context="Install command help")
        assert "Install Python dependencies" in result.stdout, "Help should describe install functionality"
        assert "project-dir" in result.stdout or "PROJECT_DIR" in result.stdout, "Help should show project-dir option"


class TestCommandErrorHandling:
    """Test CLI command error handling."""

    @pytest.fixture
    def mock_project_dir(self, tmp_path):
        """Create a mock project directory."""
        return tmp_path / "test_project"

    @pytest.fixture
    def runner(self, cli_runner: CliRunner) -> CliRunner:
        return cli_runner

    @pytest.mark.unit
    def test_import_command_project_wrapper_initialization_error(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test import command when ProjectWrapper initialization raises error.

        This test verifies that when ProjectWrapper.__init__ raises an exception during
        the import command execution, the CLI allows the exception to bubble up to Typer's
        default exception handler, which displays the error and exits with code 1. This
        validates that critical initialization errors are not silently swallowed. This is
        a unit test because it tests error handling in isolation with all dependencies
        mocked, not testing real component interactions.
        """
        # Arrange - Set up mocks and test data
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Configure ProjectWrapper to raise a specific error during initialization
        test_error_message = "Failed to initialize project wrapper"
        mock_project_wrapper_class.side_effect = RuntimeError(test_error_message)
        mock_find_project.return_value = mock_project_dir

        # Create source directory structure for valid input
        source_path = mock_project_dir / "source"
        source_path.mkdir(parents=True)

        # Act - Execute the import command with initialization error
        result = runner.invoke(
            marimba_cli,
            ["import", "test_collection", str(source_path), "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify error handling behavior
        # The exception should bubble up to Typer's exception handler, resulting in exit code 1
        assert_cli_failure(
            result,
            expected_exit_code=1,
            context="Import command with ProjectWrapper initialization error",
        )

        # Verify the exception was raised with the correct error message
        # Typer's CliRunner stores unhandled exceptions in result.exception
        assert result.exception is not None, "Exception should be stored in result.exception"
        assert isinstance(result.exception, RuntimeError), "Exception should be a RuntimeError"
        assert test_error_message in str(
            result.exception,
        ), f"Error message '{test_error_message}' should appear in exception message"

        # Verify the project directory resolution was attempted before the error
        mock_find_project.assert_called_once_with(mock_project_dir)

        # Verify ProjectWrapper instantiation was attempted with correct parameters
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=False)

    @pytest.mark.unit
    def test_process_command_processing_error(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test process command error handling when run_process raises exception.

        This unit test verifies that when ProjectWrapper.run_process() raises an exception,
        the CLI command properly handles the error by:
        1. Logging the exception details
        2. Displaying an appropriate error message to the user
        3. Exiting with code 0 (current behavior via typer.Exit without code)

        This is a unit test because it tests error handling in isolation with all dependencies
        mocked, not testing real component interactions.

        Note: The current implementation exits with code 0 due to `typer.Exit` without an
        exit code argument at main.py:444. This differs from install_command which uses
        `typer.Exit(1)` for consistency with error handling conventions.
        """
        # Arrange - Set up mocks for process command error scenario
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Create mock project instance
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project

        # Configure run_process to raise a generic exception
        test_error_message = "Processing error"
        mock_project.run_process.side_effect = Exception(test_error_message)

        # Set up required wrapper attributes
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"test_pipeline": mocker.Mock()}
        mock_find_project.return_value = mock_project_dir

        # Act - Execute process command with error condition
        result = runner.invoke(
            marimba_cli,
            [
                "process",
                "--collection-name",
                "test_collection",
                "--pipeline-name",
                "test_pipeline",
                "--project-dir",
                str(mock_project_dir),
            ],
        )

        # Assert - Verify error message is displayed with specific failure details
        output = result.output or result.stdout
        assert (
            "Error during processing: Processing error" in output
        ), f"Error message should contain 'Error during processing: Processing error', got: {output}"

        # Assert - Verify command exits with code 1 on processing failure
        assert result.exit_code == 1, f"Command should exit with code 1 on failure, got {result.exit_code}"

        # Assert - Verify run_process method was attempted with correct parameters
        mock_project.run_process.assert_called_once_with(
            ["test_collection"],
            ["test_pipeline"],
            [],  # extra args
            max_workers=None,
        ), "run_process should be called with correct collection, pipeline, args, and max_workers"

        # Assert - Verify project directory resolution occurred
        mock_find_project.assert_called_once_with(
            mock_project_dir,
        ), "find_project_dir_or_exit should be called with the project directory"

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(
            mock_project_dir,
            dry_run=False,
        ), "ProjectWrapper should be instantiated with project_dir and dry_run=False"

    @pytest.mark.unit
    def test_package_command_packaging_error(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test package command error handling when compose operation raises generic exception.

        This unit test verifies that when ProjectWrapper.compose() raises a generic Exception
        during the package command execution, the CLI command properly handles the error by:
        1. Logging the exception details
        2. Displaying an appropriate error message to the user
        3. Exiting gracefully (typer.Exit defaults to code 0)
        4. NOT proceeding to create_dataset since compose failed

        This is a unit test because it tests error handling in isolation with all dependencies
        mocked, not testing real component interactions.
        """
        # Arrange - Import real classes to preserve exception types for isinstance checks
        from marimba.core.wrappers.dataset import DatasetWrapper
        from marimba.core.wrappers.project import ProjectWrapper

        # Mock external dependencies
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_find_project.return_value = mock_project_dir

        # Mock ProjectWrapper class and preserve exception classes for except clauses
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project_wrapper_class.CompositionError = ProjectWrapper.CompositionError
        mock_project_wrapper_class.NoSuchPipelineError = ProjectWrapper.NoSuchPipelineError
        mock_project_wrapper_class.NoSuchCollectionError = ProjectWrapper.NoSuchCollectionError
        mock_project_wrapper_class.ReadOnlyFilesError = ProjectWrapper.ReadOnlyFilesError

        # Mock DatasetWrapper class and preserve exception classes
        mock_dataset_wrapper_class = mocker.patch("marimba.main.DatasetWrapper")
        mock_dataset_wrapper_class.ManifestError = DatasetWrapper.ManifestError

        # Create mock project instance with required attributes
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        mock_project.pipeline_wrappers = {"pipeline1": mocker.Mock()}

        # Configure compose to raise a generic error (not a specific exception type)
        test_error_message = "Compose operation failed"
        mock_project.compose.side_effect = Exception(test_error_message)

        # Mock the rich handler for dry-run configuration
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Act - Execute package command that will trigger the error
        result = runner.invoke(
            marimba_cli,
            ["package", "test_dataset", "--collection-name", "test_collection", "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify error message is displayed to user
        output = result.output or result.stdout
        assert (
            "Could not package collection" in output
        ), f"Error message should contain 'Could not package collection', got: {output}"
        assert (
            test_error_message in output
        ), f"Error message should contain specific error '{test_error_message}', got: {output}"

        # Assert - Verify command exits with code 1 on packaging failure
        assert result.exit_code == 1, f"Command should exit with code 1 on failure, got {result.exit_code}"

        # Assert - Verify project directory resolution occurred
        mock_find_project.assert_called_once_with(
            mock_project_dir,
        ), "find_project_dir_or_exit should be called with the project directory"

        # Assert - Verify ProjectWrapper instantiation with correct parameters
        mock_project_wrapper_class.assert_called_once_with(
            mock_project_dir,
            dry_run=False,
        ), "ProjectWrapper should be instantiated with dry_run=False"

        # Assert - Verify dry-run mode was configured
        mock_rich_handler.set_dry_run.assert_called_once_with(
            False,
        ), "Rich handler should be configured with dry_run=False"

        # Assert - Verify compose method was attempted with correct parameters
        mock_project.compose.assert_called_once_with(
            "test_dataset",
            ["test_collection"],
            ["pipeline1"],  # Should use all available pipelines when none specified
            [],  # extra args
            max_workers=None,
        ), "compose should be called with correct parameters before error occurred"

        # Assert - Verify create_dataset was NOT called since compose failed
        mock_project.create_dataset.assert_not_called(), (
            "create_dataset should NOT be called when compose raises an exception"
        )

    @pytest.mark.integration
    def test_distribute_command_distribution_error(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
        mock_project_dir: Path,
    ) -> None:
        """Test distribute command when distribution raises generic error.

        This test verifies that when ProjectWrapper.distribute() raises a generic Exception
        (not one of the specific exception types handled separately), the CLI command properly
        handles the error, logs it, displays an appropriate error message to the user including
        the error details, and exits with code 0 (typer.Exit default). This validates the
        catch-all exception handler at the end of the distribute_command.
        """
        # Arrange
        # Import the real classes to preserve exception classes
        from marimba.core.distribution.base import DistributionTargetBase
        from marimba.core.wrappers.dataset import DatasetWrapper
        from marimba.core.wrappers.project import ProjectWrapper

        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")

        # Preserve exception classes so except clauses work correctly
        mock_project_wrapper_class.NoSuchDatasetError = ProjectWrapper.NoSuchDatasetError
        mock_project_wrapper_class.NoSuchTargetError = ProjectWrapper.NoSuchTargetError

        # Preserve DatasetWrapper.ManifestError for the except clause
        mock_dataset_wrapper_class = mocker.patch("marimba.main.DatasetWrapper")
        mock_dataset_wrapper_class.ManifestError = DatasetWrapper.ManifestError

        # Preserve DistributionTargetBase.DistributionError for the except clause
        mock_distribution_target_base_class = mocker.patch("marimba.main.DistributionTargetBase")
        mock_distribution_target_base_class.DistributionError = DistributionTargetBase.DistributionError

        # Create mock project instance
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project

        # Configure distribute to raise a generic error (not one of the specific exception types)
        test_error_message = "Distribution error"
        mock_project.distribute.side_effect = RuntimeError(test_error_message)
        mock_find_project.return_value = mock_project_dir

        # Mock the rich handler for dry-run setup
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        # Act
        result = runner.invoke(
            marimba_cli,
            ["distribute", "test_dataset", "test_target", "--project-dir", str(mock_project_dir)],
        )

        # Assert - Verify command displays error message with details
        output = result.output or result.stdout
        assert (
            "Could not distribute dataset" in output
        ), f"Error message should contain 'Could not distribute dataset', got: {output}"
        assert test_error_message in output, f"Error message should contain '{test_error_message}', got: {output}"

        # Assert - Verify the command exits with code 1 on distribute failure
        assert result.exit_code == 1, f"Command should exit with code 1 on failure, got {result.exit_code}"

        # Assert - Verify project directory resolution and wrapper instantiation
        mock_find_project.assert_called_once_with(mock_project_dir)
        mock_project_wrapper_class.assert_called_once_with(mock_project_dir, dry_run=False)

        # Assert - Verify dry-run mode setup
        mock_rich_handler.set_dry_run.assert_called_once_with(False)

        # Assert - Verify distribute method was attempted with correct parameters
        mock_project.distribute.assert_called_once_with(
            "test_dataset",
            "test_target",
            True,  # validate=True (default)
        )


class TestCLIIntegration:
    """Test CLI integration scenarios."""

    @pytest.fixture
    def runner(self, cli_runner: CliRunner) -> CliRunner:
        return cli_runner

    @pytest.mark.integration
    def test_version_flag_in_global_options(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
    ) -> None:
        """Test --version flag in global options displays version and exits successfully.

        This test verifies that the --version flag can be used as a global option before any
        subcommand, triggering the version_callback function through the global_options callback.
        It ensures the CLI displays the formatted version string and exits with code 0, validating
        the integration between the global options callback and version handling.
        """
        # Arrange
        mocker.patch("marimba.main.importlib.metadata.version", return_value="1.0.0")

        # Act
        result = runner.invoke(marimba_cli, ["--version"])

        # Assert
        assert_cli_success(result, expected_message="Marimba v1.0.0", context="Version flag in global options")

        # Assert - Verify version string is displayed in output
        assert "Marimba v1.0.0" in result.stdout, f"Expected 'Marimba v1.0.0' in output, got: {result.stdout}"

        # Assert - Verify successful exit code
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    @pytest.mark.integration
    def test_debug_flag(
        self,
        mocker: pytest_mock.MockerFixture,
        runner: CliRunner,
    ) -> None:
        """Test --level DEBUG flag sets logging level to DEBUG and executes successfully.

        This test verifies that the --level DEBUG option properly configures the rich handler
        to use DEBUG logging level (numeric value 10) and that the version command executes
        without error. This is an integration test because it tests the interaction between
        the global options callback, logging configuration, and command execution.
        """
        # Arrange - Mock logging handler and version lookup
        mock_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_log_handler = mocker.Mock()
        mock_handler.return_value = mock_log_handler
        mocker.patch("marimba.main.importlib.metadata.version", return_value="1.0.0")

        # Act - Execute version command with DEBUG logging level
        result = runner.invoke(marimba_cli, ["--level", "DEBUG", "version"])

        # Assert - Verify successful execution with expected version message
        assert_cli_success(result, expected_message="Marimba v1.0.0", context="Debug level with version command")

        # Assert - Verify logging level was set to DEBUG (numeric value 10)
        mock_log_handler.setLevel.assert_called_once_with(
            10,
        ), f"Expected setLevel to be called with 10 (DEBUG), got {mock_log_handler.setLevel.call_args}"

        # Assert - Verify exit code is 0 for successful execution
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    @pytest.mark.integration
    def test_quiet_flag_sets_error_log_level(self, mocker: pytest_mock.MockerFixture, runner: CliRunner) -> None:
        """Test --level ERROR flag sets logging level to ERROR and executes successfully.

        This test verifies that the --level ERROR option (quiet mode) properly configures
        the rich handler to use ERROR logging level (numeric value 40) and that the version
        command executes without error. This is an integration test because it tests the
        interaction between the global options callback, logging configuration, and command
        execution.
        """
        # Arrange - Mock logging handler and version lookup
        mock_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_log_handler = mocker.Mock()
        mock_handler.return_value = mock_log_handler
        mocker.patch("marimba.main.importlib.metadata.version", return_value="1.0.0")

        # Act - Execute version command with ERROR logging level (quiet mode)
        result = runner.invoke(marimba_cli, ["--level", "ERROR", "version"])

        # Assert - Verify successful execution with expected version message
        assert_cli_success(result, expected_message="Marimba v1.0.0", context="Error level with version command")

        # Assert - Verify logging level was set to ERROR (numeric value 40)
        mock_log_handler.setLevel.assert_called_once_with(
            40,
        ), f"Expected setLevel to be called with 40 (ERROR), got {mock_log_handler.setLevel.call_args}"

        # Assert - Verify exit code is 0 for successful execution
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    @pytest.fixture
    def workflow_mocks(
        self,
        mocker: pytest_mock.MockerFixture,
        tmp_path: Path,
    ) -> tuple[Any, Path, Path]:
        """Set up mocks and directories for workflow testing."""
        mocker.patch("marimba.main.validate_dependencies")
        mock_find_project = mocker.patch("marimba.main.find_project_dir_or_exit")
        mock_project_wrapper_class = mocker.patch("marimba.main.ProjectWrapper")
        mock_project = mocker.Mock()
        mock_project_wrapper_class.return_value = mock_project

        # Configure mock project behavior
        mock_project.run_import.return_value = None
        mock_project.run_process.return_value = None
        mock_project.compose.return_value = {"test_collection": mocker.Mock()}
        mock_dataset = mocker.Mock()
        mock_dataset.root_dir = tmp_path / "datasets" / "test_dataset"
        mock_project.create_dataset.return_value = mock_dataset
        mock_project.pipeline_wrappers = {"test_pipeline": mocker.Mock()}
        mock_project.prompt_collection_config.return_value = {"schema": "ifdo", "version": "1.0"}
        mock_project.create_collection.return_value = None
        mock_project.get_pipeline_post_processors.return_value = []

        # Create directory structure
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        source_path = tmp_path / "source"
        source_path.mkdir()
        mock_find_project.return_value = project_dir

        # Mock rich handler
        mock_get_rich_handler = mocker.patch("marimba.main.get_rich_handler")
        mock_rich_handler = mocker.Mock()
        mock_get_rich_handler.return_value = mock_rich_handler

        return mock_project, project_dir, source_path

    @pytest.mark.integration
    def test_workflow_command_chaining_integration(
        self,
        workflow_mocks: tuple[Any, Path, Path],
        runner: CliRunner,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test CLI command integration across typical workflow sequence (import -> process -> package).

        This integration test verifies that CLI commands correctly pass parameters to underlying
        ProjectWrapper methods and that the CLI layer properly handles the sequence of operations
        in a typical workflow. It focuses on the integration between CLI argument parsing,
        command execution, and wrapper method invocation rather than the actual file system
        operations (which are mocked to isolate the CLI integration logic).

        The test validates:
        - CLI arguments are correctly parsed and passed to ProjectWrapper methods
        - Success messages are displayed for each command
        - Wrapper methods receive correct parameters in the expected format
        - Command sequencing works correctly from CLI perspective
        """
        # Arrange
        mock_project, project_dir, source_path = workflow_mocks

        # Act & Assert - Execute workflow commands in sequence

        # 1. Import command - Verify CLI correctly invokes import workflow
        mock_project.collection_wrappers = {}
        result_import = runner.invoke(
            marimba_cli,
            ["import", "test_collection", str(source_path), "--project-dir", str(project_dir)],
        )

        # Assert - Import command completed successfully
        assert_cli_success(
            result_import,
            expected_message="Imported data into collection",
            context="Workflow integration: import command",
        )

        # Assert - Import wrapper method was called with correct parameters
        mock_project.run_import.assert_called_once()
        import_args = mock_project.run_import.call_args
        assert (
            import_args[0][0] == "test_collection"
        ), f"Import should use collection name 'test_collection', got {import_args[0][0]}"
        assert import_args[0][1] == [
            source_path,
        ], f"Import should use source path {source_path}, got {import_args[0][1]}"
        assert import_args[0][2] == [
            "test_pipeline",
        ], f"Import should use pipeline 'test_pipeline', got {import_args[0][2]}"

        # 2. Process command - Verify CLI correctly invokes process workflow
        mock_project.collection_wrappers = {"test_collection": mocker.Mock()}
        result_process = runner.invoke(
            marimba_cli,
            [
                "process",
                "--collection-name",
                "test_collection",
                "--pipeline-name",
                "test_pipeline",
                "--project-dir",
                str(project_dir),
            ],
        )

        # Assert - Process command completed successfully
        assert_cli_success(
            result_process,
            expected_message="Processed data for pipeline",
            context="Workflow integration: process command",
        )

        # Assert - Process wrapper method was called with correct parameters
        mock_project.run_process.assert_called_once()
        process_args = mock_project.run_process.call_args
        assert process_args[0][0] == [
            "test_collection",
        ], f"Process should use collection names ['test_collection'], got {process_args[0][0]}"
        assert process_args[0][1] == [
            "test_pipeline",
        ], f"Process should use pipeline names ['test_pipeline'], got {process_args[0][1]}"
        assert process_args[0][2] == [], f"Process should have empty extra args, got {process_args[0][2]}"  # extra args
        assert (
            process_args[1]["max_workers"] is None
        ), f"Process should have max_workers=None, got {process_args[1]['max_workers']}"

        # 3. Package command - Verify CLI correctly invokes packaging workflow
        result_package = runner.invoke(
            marimba_cli,
            ["package", "test_dataset", "--collection-name", "test_collection", "--project-dir", str(project_dir)],
        )

        # Assert - Package command completed successfully
        assert_cli_success(
            result_package,
            expected_message="Packaged dataset",
            context="Workflow integration: package command",
        )

        # Assert - Compose wrapper method was called with correct parameters
        mock_project.compose.assert_called_once()
        compose_args = mock_project.compose.call_args
        assert (
            compose_args[0][0] == "test_dataset"
        ), f"Compose should use dataset name 'test_dataset', got {compose_args[0][0]}"
        assert compose_args[0][1] == [
            "test_collection",
        ], f"Compose should use collection names ['test_collection'], got {compose_args[0][1]}"
        assert compose_args[0][2] == [
            "test_pipeline",
        ], f"Compose should use pipeline names ['test_pipeline'], got {compose_args[0][2]}"
        assert compose_args[0][3] == [], f"Compose should have empty extra args, got {compose_args[0][3]}"  # extra args
        assert (
            compose_args[1]["max_workers"] is None
        ), f"Compose should have max_workers=None, got {compose_args[1]['max_workers']}"

        # Assert - Create dataset wrapper method was called
        mock_project.create_dataset.assert_called_once()
        create_dataset_args = mock_project.create_dataset.call_args
        assert (
            create_dataset_args[0][0] == "test_dataset"
        ), f"Create dataset should use name 'test_dataset', got {create_dataset_args[0][0]}"

        # Assert - Dataset cleanup was performed
        mock_dataset = mock_project.create_dataset.return_value
        mock_dataset.close.assert_called_once(), "Dataset wrapper close() should be called for resource cleanup"

    @pytest.mark.unit
    def test_subcommand_structure_has_all_required_commands(self) -> None:
        """Test that all required subcommands are properly registered in the CLI.

        This test verifies that the marimba_cli Typer application has all expected direct
        commands (import, package, process, distribute, update, install, version) and
        command groups (new, delete) registered. It ensures the CLI exposes the complete
        command structure necessary for user interaction with the Marimba framework.

        This is a unit test because it tests the CLI object's command registration structure
        in isolation without invoking any commands or accessing external dependencies.
        """
        # Arrange - Define expected command structure
        expected_commands = {"import", "package", "process", "distribute", "update", "install", "version"}
        expected_groups = {"new", "delete"}

        # Act - Extract actual registered commands and groups from marimba_cli
        actual_commands = {cmd.name for cmd in marimba_cli.registered_commands}
        actual_groups = {grp.name for grp in marimba_cli.registered_groups}

        # Assert - Verify all expected commands are registered
        missing_commands = expected_commands - actual_commands
        assert expected_commands.issubset(actual_commands), (
            f"Missing direct commands: {missing_commands}. "
            f"Expected commands: {expected_commands}, "
            f"Registered commands: {actual_commands}"
        )

        # Assert - Verify all expected command groups are registered
        missing_groups = expected_groups - actual_groups
        assert expected_groups.issubset(actual_groups), (
            f"Missing command groups: {missing_groups}. "
            f"Expected groups: {expected_groups}, "
            f"Registered groups: {actual_groups}"
        )
