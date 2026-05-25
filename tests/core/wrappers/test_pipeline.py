"""
Tests for the PipelineWrapper class in marimba.core.wrappers.pipeline.

This module provides comprehensive tests for the pipeline wrapper functionality including:
- Initialization and property access
- Configuration loading and saving
- Pipeline class discovery and instantiation
- Repository management (creation, updates)
- File structure validation
- Error handling scenarios
- Logging configuration
"""

import logging
import re
from pathlib import Path
from typing import Any

import pytest
import pytest_mock
from git.exc import GitError

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from marimba.core.wrappers.pipeline import PipelineWrapper


class MockTestPipeline(BasePipeline):
    """Mock test pipeline class for testing purposes (renamed to avoid pytest collection)."""

    def __init__(
        self,
        root_path: Any,
        config: Any = None,
        metadata_class: Any = BaseMetadata,
        *,
        dry_run: bool = False,
    ) -> None:
        super().__init__(root_path, config, metadata_class, dry_run=dry_run)

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {"test_param": "default_value", "test_int": 42}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {"collection_param": "default_collection"}

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {Path("test.txt"): (Path("relative/test.txt"), None, None)}


class TestPipelineWrapperInitialization:
    """Tests for PipelineWrapper initialization and basic properties."""

    @pytest.mark.integration
    def test_init_success(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful PipelineWrapper initialization with required file structure.

        Verifies that PipelineWrapper.__init__() properly:
        - Sets up all expected properties from the root directory path
        - Validates required file structure (repo dir and config file exist)
        - Initializes with the correct dry_run setting
        - Creates proper directory and file path references
        """
        # Arrange: Set up required file structure and mocks
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_installer_create = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")
        mock_installer = mocker.Mock()
        mock_installer_create.return_value = mock_installer

        # Act: Create PipelineWrapper instance
        wrapper = PipelineWrapper(tmp_path, dry_run=False)

        # Assert: Verify all properties are set correctly
        assert wrapper.root_dir == tmp_path, "Root directory should match input path"
        assert wrapper.repo_dir == tmp_path / "repo", "Repo directory should be root_dir/repo"
        assert wrapper.config_path == tmp_path / "pipeline.yml", "Config path should be root_dir/pipeline.yml"
        assert wrapper.log_path == tmp_path / f"{tmp_path.name}.log", "Log path should be root_dir/{name}.log"
        assert wrapper.name == tmp_path.name, "Name should match directory name"
        assert not wrapper.dry_run, "Dry run should be False as specified"

    @pytest.mark.unit
    def test_init_with_dry_run(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test PipelineWrapper initialization with dry_run=True properly sets dry_run mode.

        Verifies that PipelineWrapper.__init__() with dry_run=True:
        - Properly sets the dry_run property to True
        - Maintains all other standard initialization behavior
        - Creates the same directory structure and properties as non-dry-run mode
        """
        # Arrange: Set up required file structure and mock external dependencies
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        # Mock external dependencies to isolate unit under test
        mock_setup_logging = mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_installer_create = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")
        mock_installer = mocker.Mock()
        mock_installer_create.return_value = mock_installer

        # Act: Create PipelineWrapper instance with dry_run=True
        wrapper = PipelineWrapper(tmp_path, dry_run=True)

        # Assert: Verify dry_run mode is properly enabled
        assert wrapper.dry_run is True

        # Assert: Verify all other properties are set correctly
        assert wrapper.root_dir == tmp_path
        assert wrapper.repo_dir == tmp_path / "repo"
        assert wrapper.config_path == tmp_path / "pipeline.yml"
        assert wrapper.log_path == tmp_path / f"{tmp_path.name}.log"
        assert wrapper.name == tmp_path.name

        # Assert: Verify logging setup was called during initialization
        mock_setup_logging.assert_called_once()

        # Assert: Verify installer was created with correct parameters
        mock_installer_create.assert_called_once_with(wrapper.repo_dir, wrapper.logger)

    @pytest.mark.integration
    def test_init_with_string_path(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test PipelineWrapper initialization with string path properly converts and initializes.

        Verifies that PipelineWrapper.__init__() properly:
        - Accepts string path inputs and converts them to Path objects internally
        - Sets up all expected properties from the string path parameter
        - Validates required file structure (repo dir and config file exist)
        - Initializes with the correct dry_run setting
        - Creates proper directory and file path references from string input

        This integration test validates string-to-Path conversion behavior while testing
        real component interactions with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up required file structure and mock external dependencies
        - Act: Initialize PipelineWrapper with string path parameter
        - Assert: Verify all properties are set correctly and string path is converted properly
        """
        # Arrange: Set up required file structure and mocks
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_installer_create = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")
        mock_installer = mocker.Mock()
        mock_installer_create.return_value = mock_installer

        # Act: Create PipelineWrapper instance with string path parameter
        wrapper = PipelineWrapper(str(tmp_path), dry_run=False)

        # Assert: Verify all properties are set correctly from string path conversion
        assert wrapper.root_dir == tmp_path, "Root directory should match converted Path object from string input"
        assert wrapper.repo_dir == tmp_path / "repo", "Repo directory should be root_dir/repo"
        assert wrapper.config_path == tmp_path / "pipeline.yml", "Config path should be root_dir/pipeline.yml"
        assert wrapper.log_path == tmp_path / f"{tmp_path.name}.log", "Log path should be root_dir/{name}.log"
        assert wrapper.name == tmp_path.name, "Name should match directory name from converted path"
        assert not wrapper.dry_run, "Dry run should be False as specified"

        # Assert: Verify installer was created with correct parameters
        mock_installer_create.assert_called_once_with(wrapper.repo_dir, wrapper.logger)


class TestPipelineWrapperFileStructureValidation:
    """Tests for file structure validation in PipelineWrapper."""

    @pytest.mark.integration
    def test_missing_root_directory(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test InvalidStructureError when root directory doesn't exist.

        Verifies that PipelineWrapper.__init__() raises InvalidStructureError
        when the specified root directory does not exist. This tests the integration
        between PipelineWrapper initialization and file structure validation with
        minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create path to non-existent directory and mock external dependencies
        - Act: Attempt to initialize PipelineWrapper with missing directory
        - Assert: Verify specific InvalidStructureError is raised with expected message
        """
        # Arrange: Create path to non-existent directory
        missing_dir = tmp_path / "nonexistent"
        # Ensure directory doesn't exist to trigger validation error
        assert not missing_dir.exists(), "Directory should not exist for test validity"

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act & Assert: Verify specific InvalidStructureError is raised with expected message
        expected_error_pattern = f'"{re.escape(str(missing_dir))}" does not exist or is not a directory'
        with pytest.raises(
            PipelineWrapper.InvalidStructureError,
            match=expected_error_pattern,
        ):
            PipelineWrapper(missing_dir)

    @pytest.mark.integration
    def test_missing_repo_directory(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test InvalidStructureError when repo directory doesn't exist.

        Verifies that PipelineWrapper.__init__() raises InvalidStructureError
        when the required repo subdirectory is missing from the pipeline
        directory structure. This tests the file structure validation logic
        in isolation with mocked external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create directory structure missing repo subdirectory and mock dependencies
        - Act: Attempt to initialize PipelineWrapper
        - Assert: Verify specific InvalidStructureError is raised with expected message
        """
        # Arrange: Create incomplete directory structure (missing repo directory)
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        # repo directory is intentionally missing to trigger validation error

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act & Assert: Verify specific InvalidStructureError is raised with expected message
        expected_error_pattern = f'"{re.escape(str(tmp_path / "repo"))}" does not exist or is not a directory'
        with pytest.raises(
            PipelineWrapper.InvalidStructureError,
            match=expected_error_pattern,
        ):
            PipelineWrapper(tmp_path)

    @pytest.mark.unit
    def test_missing_config_file(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test InvalidStructureError when config file doesn't exist.

        Verifies that PipelineWrapper.__init__() raises InvalidStructureError
        when the required pipeline.yml config file is missing from the pipeline
        directory structure. This tests the file structure validation logic
        in isolation with mocked external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create directory structure missing config file and mock dependencies
        - Act: Attempt to initialize PipelineWrapper
        - Assert: Verify specific InvalidStructureError is raised with expected message
        """
        # Arrange: Create incomplete directory structure (missing config file)
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        # config file is intentionally missing to trigger validation error

        # Mock external dependencies to isolate unit under test
        mock_setup_logging = mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_installer_create = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act & Assert: Verify specific InvalidStructureError is raised with expected message
        expected_error_pattern = f'"{re.escape(str(tmp_path / "pipeline.yml"))}" does not exist or is not a file'
        with pytest.raises(
            PipelineWrapper.InvalidStructureError,
            match=expected_error_pattern,
        ):
            PipelineWrapper(tmp_path)

        # Assert: Verify that external dependencies were not called due to early validation failure
        mock_setup_logging.assert_not_called()
        mock_installer_create.assert_not_called()

    @pytest.mark.integration
    def test_repo_is_file_not_directory(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test InvalidStructureError when repo exists as file instead of directory.

        Verifies that PipelineWrapper.__init__() raises InvalidStructureError
        when the required repo subdirectory exists as a file instead of a directory.
        This tests the integration between file structure validation and error reporting
        with minimal mocking of external dependencies to ensure proper validation
        of the repository structure.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create directory structure with repo as file and mock dependencies
        - Act: Attempt to initialize PipelineWrapper
        - Assert: Verify specific InvalidStructureError is raised with expected message
        """
        # Arrange: Create invalid directory structure (repo as file instead of directory)
        repo_file = tmp_path / "repo"
        repo_file.write_text("not a directory")
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        # Ensure the test setup is correct for validation
        assert repo_file.is_file(), "Repo should exist as a file for test validity"
        assert not repo_file.is_dir(), "Repo should not be a directory for test validity"
        assert config_file.is_file(), "Config file should exist for test setup"

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act & Assert: Verify specific InvalidStructureError is raised with expected message
        expected_error_pattern = f'"{re.escape(str(repo_file))}" does not exist or is not a directory'
        with pytest.raises(
            PipelineWrapper.InvalidStructureError,
            match=expected_error_pattern,
        ):
            PipelineWrapper(tmp_path)

    @pytest.mark.unit
    def test_config_is_directory_not_file(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test InvalidStructureError when config exists as directory instead of file.

        Verifies that PipelineWrapper.__init__() raises InvalidStructureError
        when the required pipeline.yml config file exists as a directory instead of a file.
        This tests the file structure validation logic in isolation with mocked
        external dependencies to ensure proper validation of the configuration file.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create directory structure with config as directory and mock dependencies
        - Act: Attempt to initialize PipelineWrapper
        - Assert: Verify specific InvalidStructureError is raised with expected message
        """
        # Arrange: Create invalid directory structure (config as directory instead of file)
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_dir = tmp_path / "pipeline.yml"
        config_dir.mkdir()

        # Ensure the test setup is correct for validation
        assert repo_dir.is_dir(), "Repo directory should exist for test setup"
        assert config_dir.is_dir(), "Config should exist as a directory for test validity"
        assert not config_dir.is_file(), "Config should not be a file for test validity"

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act & Assert: Verify specific InvalidStructureError is raised with expected message
        expected_error_pattern = f'"{re.escape(str(config_dir))}" does not exist or is not a file'
        with pytest.raises(
            PipelineWrapper.InvalidStructureError,
            match=expected_error_pattern,
        ):
            PipelineWrapper(tmp_path)


class TestPipelineWrapperLogging:
    """Tests for logging setup in PipelineWrapper."""

    @pytest.mark.unit
    def test_setup_logging(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test logging setup creates file handler and adds it to wrapper's logger.

        Verifies that PipelineWrapper._setup_logging() properly:
        - Creates a file handler using get_file_handler with correct parameters
        - Adds the created file handler to the wrapper's logger instance
        - Uses the wrapper's root directory, name, and dry_run setting correctly
        - Integrates properly with the logging system during initialization

        This is a unit test because it tests the logging setup behavior in isolation
        with mocked external dependencies to verify the file handler creation and
        logger configuration without side effects.
        """
        # Arrange: Set up required file structure and mock external dependencies
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        # Mock external dependencies to isolate unit under test
        mock_get_file_handler = mocker.patch("marimba.core.wrappers.pipeline.get_file_handler")
        mock_get_logger = mocker.patch("marimba.core.utils.log.get_logger")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Set up mock return values for controlled testing
        mock_file_handler = mocker.Mock()
        mock_get_file_handler.return_value = mock_file_handler

        mock_logger = mocker.Mock()
        mock_get_logger.return_value = mock_logger

        # Act: Create PipelineWrapper instance (triggers _setup_logging during initialization)
        wrapper = PipelineWrapper(tmp_path, dry_run=False)

        # Assert: Verify file handler was created with correct parameters
        mock_get_file_handler.assert_called_once_with(
            tmp_path,
            tmp_path.name,
            False,
        ), "get_file_handler should be called with root directory, pipeline name, and dry_run setting"

        # Assert: Verify file handler was added to the logger
        assert mock_logger.addHandler.call_count >= 1, "addHandler should be called at least once"
        # Verify our specific file handler was added (among potentially other handlers)
        mock_logger.addHandler.assert_any_call(
            mock_file_handler,
        ), "File handler should be added to the wrapper's logger instance"

        # Assert: Verify wrapper stores the file handler reference
        assert wrapper._file_handler is mock_file_handler, "Wrapper should store reference to the created file handler"

        # Assert: Verify logger property is accessible and returns the mocked logger
        assert wrapper.logger is mock_logger, "Wrapper logger property should return the mocked logger instance"

    @pytest.mark.unit
    def test_setup_logging_dry_run(
        self,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test logging setup in dry run mode passes correct dry_run parameter.

        Verifies that PipelineWrapper._setup_logging() calls get_file_handler with dry_run=True
        when wrapper is initialized with dry_run=True and properly adds the handler to the logger.
        """
        # Arrange: Set up required file structure and mock external dependencies
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")

        mock_get_file_handler = mocker.patch("marimba.core.wrappers.pipeline.get_file_handler")
        mock_get_logger = mocker.patch("marimba.core.utils.log.get_logger")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        mock_file_handler = mocker.Mock()
        mock_get_file_handler.return_value = mock_file_handler

        mock_logger = mocker.Mock()
        mock_get_logger.return_value = mock_logger

        # Act: Create PipelineWrapper instance with dry_run=True
        wrapper = PipelineWrapper(tmp_path, dry_run=True)

        # Assert: Verify get_file_handler was called with correct parameters including dry_run=True
        mock_get_file_handler.assert_called_once_with(tmp_path, tmp_path.name, True)

        # Assert: Verify file handler was added to the logger and wrapper stores reference
        mock_logger.addHandler.assert_any_call(mock_file_handler)
        assert wrapper._file_handler is mock_file_handler
        assert wrapper.dry_run is True


class TestPipelineWrapperCreate:
    """Tests for PipelineWrapper.create class method."""

    @pytest.mark.unit
    def test_create_success(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful pipeline creation from git repository.

        Verifies that PipelineWrapper.create() properly:
        - Creates the pipeline directory structure
        - Clones the git repository to the repo subdirectory
        - Creates an empty pipeline.yml configuration file
        - Returns a properly initialized PipelineWrapper instance

        This is a unit test because it tests the create method in isolation with
        mocked external dependencies to verify the method's behavior without
        side effects from actual git operations or file system modifications.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up test directory, git URL, and mock external dependencies
        - Act: Call PipelineWrapper.create() method
        - Assert: Verify directory structure, git operations, config creation, and wrapper properties
        """
        # Arrange: Set up test data and mock external dependencies
        pipeline_dir = tmp_path / "new_pipeline"
        git_url = "https://github.com/example/pipeline.git"

        # Mock external dependencies only (git operations, file operations, logging)
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_save_config = mocker.patch("marimba.core.wrappers.pipeline.save_config")

        # Mock logging and installer to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Mock clone_from to simulate real directory creation behavior
        def mock_clone_from(_url: str, repo_path: Path) -> None:
            repo_path.mkdir(parents=True, exist_ok=True)

        # Mock save_config to simulate real file creation behavior
        def mock_save_config_impl(config_path: Path, _config: dict[str, Any]) -> None:
            config_path.touch()

        mock_repo_class.clone_from.side_effect = mock_clone_from
        mock_save_config.side_effect = mock_save_config_impl

        # Act: Create the pipeline using the class method
        result = PipelineWrapper.create(pipeline_dir, git_url, dry_run=True)

        # Assert: Verify directory structure was created correctly
        assert pipeline_dir.exists(), "Pipeline directory should be created by the create method"
        assert pipeline_dir.is_dir(), "Pipeline directory should be a directory, not a file"
        assert (pipeline_dir / "repo").exists(), "Repo subdirectory should exist after git clone mock"
        assert (pipeline_dir / "pipeline.yml").exists(), "Config file should exist after save_config mock"

        # Assert: Verify git clone was called with correct parameters
        mock_repo_class.clone_from.assert_called_once_with(
            git_url,
            pipeline_dir / "repo",
        ), "Git clone should be called with the provided URL and repo subdirectory path"

        # Assert: Verify config file was created with empty configuration
        mock_save_config.assert_called_once_with(
            pipeline_dir / "pipeline.yml",
            {},
        ), "Config file should be saved with empty dictionary as initial configuration"

        # Assert: Verify returned PipelineWrapper instance has correct properties
        assert isinstance(result, PipelineWrapper), "Create method should return a PipelineWrapper instance"
        assert result.root_dir == pipeline_dir, "Root directory property should match the input path"
        assert result.repo_dir == pipeline_dir / "repo", "Repo directory should be correctly set to root_dir/repo"
        assert (
            result.config_path == pipeline_dir / "pipeline.yml"
        ), "Config path should be correctly set to root_dir/pipeline.yml"
        assert result.dry_run is True, "Dry run mode should be preserved from the create method call"
        assert result.name == "new_pipeline", "Pipeline name should match the directory name"

    @pytest.mark.unit
    def test_create_directory_exists_error(self, tmp_path: Path) -> None:
        """Test FileExistsError when target directory already exists.

        Verifies that PipelineWrapper.create() properly:
        - Checks if the target directory already exists before attempting to create the pipeline
        - Raises FileExistsError with a specific error message when directory exists
        - Does not attempt git clone or file creation when validation fails
        - Tests the directory existence validation logic in isolation

        This is a unit test because it tests error validation logic in isolation
        without external dependencies like git operations or file system modifications.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create existing directory to trigger validation error
        - Act: Attempt to create pipeline in existing directory
        - Assert: Verify specific FileExistsError is raised with expected message
        """
        # Arrange: Create an existing directory to trigger validation error
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        git_url = "https://github.com/example/pipeline.git"

        # Ensure directory exists for test validity
        assert existing_dir.exists(), "Directory should exist for test setup validation"
        assert existing_dir.is_dir(), "Path should be a directory for test setup validation"

        # Act & Assert: Verify specific FileExistsError is raised with expected message
        expected_error_pattern = f'Pipeline root directory "{re.escape(str(existing_dir))}" already exists'
        with pytest.raises(FileExistsError, match=expected_error_pattern):
            PipelineWrapper.create(existing_dir, git_url)

    @pytest.mark.integration
    def test_create_with_string_path(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test pipeline creation with string path input properly converts to Path and creates structure.

        Verifies that PipelineWrapper.create() properly:
        - Accepts string path inputs and converts them to Path objects internally
        - Creates the expected directory structure (root directory, repo subdirectory)
        - Executes git clone operation with correct parameters
        - Creates empty pipeline.yml configuration file
        - Returns properly initialized PipelineWrapper instance with correct properties
        - Preserves dry_run mode and sets correct pipeline name from directory

        This integration test validates string-to-Path conversion behavior while testing
        real component interactions with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up string path, git URL, and mock external dependencies
        - Act: Call PipelineWrapper.create() with string path
        - Assert: Verify directory structure, git operations, config creation, and wrapper properties
        """
        # Arrange: Set up test data with string path to test conversion behavior
        pipeline_dir = tmp_path / "string_path_test"
        git_url = "https://github.com/example/pipeline.git"

        # Mock external dependencies only (git operations, file operations, logging)
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_save_config = mocker.patch("marimba.core.wrappers.pipeline.save_config")

        # Mock logging and installer to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Mock clone_from to simulate real directory creation behavior
        def mock_clone_from(_url: str, repo_path: Path) -> None:
            repo_path.mkdir(parents=True, exist_ok=True)

        # Mock save_config to simulate real file creation behavior
        def mock_save_config_impl(config_path: Path, _config: dict[str, Any]) -> None:
            config_path.touch()

        mock_repo_class.clone_from.side_effect = mock_clone_from
        mock_save_config.side_effect = mock_save_config_impl

        # Act: Create pipeline using string path (key behavior being tested)
        result = PipelineWrapper.create(str(pipeline_dir), git_url, dry_run=True)

        # Assert: Verify directory structure was created correctly from string path
        assert pipeline_dir.exists(), "Pipeline directory should be created from string path input"
        assert pipeline_dir.is_dir(), "Pipeline directory should be a directory, not a file"
        assert (pipeline_dir / "repo").exists(), "Repo subdirectory should exist after git clone mock"
        assert (pipeline_dir / "pipeline.yml").exists(), "Config file should exist after save_config mock"

        # Assert: Verify git clone was called with correct parameters (Path objects internally)
        mock_repo_class.clone_from.assert_called_once_with(
            git_url,
            pipeline_dir / "repo",
        ), "Git clone should be called with the provided URL and repo subdirectory path (converted from string)"

        # Assert: Verify config file was created with empty configuration
        mock_save_config.assert_called_once_with(
            pipeline_dir / "pipeline.yml",
            {},
        ), "Config file should be saved with empty dictionary as initial configuration"

        # Assert: Verify returned PipelineWrapper instance has correct properties
        assert isinstance(result, PipelineWrapper), "Create method should return a PipelineWrapper instance"
        assert result.root_dir == pipeline_dir, "Root directory property should match the converted Path object"
        assert result.repo_dir == pipeline_dir / "repo", "Repo directory should be correctly set to root_dir/repo"
        assert (
            result.config_path == pipeline_dir / "pipeline.yml"
        ), "Config path should be correctly set to root_dir/pipeline.yml"
        assert result.dry_run is True, "Dry run mode should be preserved from the create method call"
        assert result.name == "string_path_test", "Pipeline name should match the directory name from string path"


class TestPipelineWrapperConfiguration:
    """Tests for configuration loading and saving."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for pipeline configuration tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.unit
    def test_load_config(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test configuration loading delegates to load_config utility function.

        Verifies that PipelineWrapper.load_config() properly:
        - Calls the load_config utility function with the correct config file path
        - Returns the configuration data from the utility function unchanged
        - Delegates the actual file loading to the utility function
        - Maintains proper isolation by testing delegation behavior

        This is a unit test because it tests the delegation behavior in isolation
        with mocked external dependencies to verify the method correctly delegates
        to the utility function.
        """
        # Arrange: Set up test configuration data and mock external dependencies
        expected_config = {"key": "value", "test_param": "test_value"}
        mock_load_config = mocker.patch("marimba.core.wrappers.pipeline.load_config")
        mock_load_config.return_value = expected_config

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Load configuration using wrapper method
        result = wrapper.load_config()

        # Assert: Verify load_config utility was called with correct config file path
        mock_load_config.assert_called_once_with(
            wrapper.config_path,
        ), "load_config utility should be called with the wrapper's config_path property"

        # Assert: Verify the correct configuration data is returned unchanged
        assert result == expected_config, "Should return the configuration data from load_config utility unchanged"
        assert isinstance(result, dict), "Configuration should be returned as a dictionary"

    @pytest.mark.unit
    def test_save_config_with_data_delegates_to_utility_function(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test configuration saving with data delegates to save_config utility function.

        Verifies that PipelineWrapper.save_config() properly:
        - Accepts a dictionary configuration parameter
        - Delegates the actual file saving to the save_config utility function
        - Passes the correct config file path and configuration data to the utility
        - Follows the single responsibility principle by delegating file operations

        This is a unit test because it tests a single method in isolation with
        mocked external dependencies to verify delegation behavior.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked dependencies and test configuration
        - Act: Call save_config with test data
        - Assert: Verify save_config utility was called with correct parameters
        """
        # Arrange: Set up test configuration data and mock external dependencies
        test_config = {"test": "data", "param1": "value1", "param2": 42}

        # Mock external dependencies to isolate unit under test
        mock_save_config = mocker.patch("marimba.core.wrappers.pipeline.save_config")
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call save_config with test configuration data
        wrapper.save_config(test_config)

        # Assert: Verify save_config utility was called with correct parameters
        mock_save_config.assert_called_once_with(
            wrapper.config_path,
            test_config,
        ), "save_config utility should be called with wrapper's config path and provided configuration data"

    @pytest.mark.unit
    def test_save_config_with_none_skips_utility_call(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test configuration saving with None does not call save_config utility function.

        Verifies that PipelineWrapper.save_config() properly:
        - Accepts None as a valid configuration parameter
        - Does not call the save_config utility function when config is None
        - Follows the early return pattern for falsy config values
        - Maintains proper method isolation by not performing file operations

        This is a unit test because it tests a single method in isolation with
        mocked external dependencies to verify the None handling path.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked dependencies
        - Act: Call save_config with None parameter
        - Assert: Verify save_config utility was not called and no side effects occurred
        """
        # Arrange: Mock external dependencies to isolate unit under test
        mock_save_config = mocker.patch("marimba.core.wrappers.pipeline.save_config")
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call save_config with None parameter
        wrapper.save_config(None)

        # Assert: Verify save_config utility was not called when config is None
        mock_save_config.assert_not_called(), (
            "save_config utility should not be called when config parameter is None (falsy value)"
        )

    @pytest.mark.unit
    def test_save_config_with_empty_dict_does_not_call_utility_function(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test configuration saving with empty dictionary does not call save_config utility function.

        Verifies that PipelineWrapper.save_config() properly:
        - Accepts an empty dictionary as a valid configuration parameter
        - Does not call the save_config utility function when config is an empty dict (falsy value)
        - Follows the early return pattern for falsy config values (empty dict is falsy in Python)
        - Maintains proper method isolation by not performing file operations for empty configurations

        This is a unit test because it tests a single method in isolation with
        mocked external dependencies to verify the empty dictionary handling path.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked dependencies
        - Act: Call save_config with empty dictionary parameter
        - Assert: Verify save_config utility was not called and no side effects occurred
        """
        # Arrange: Mock external dependencies to isolate unit under test
        mock_save_config_utility = mocker.patch("marimba.core.wrappers.pipeline.save_config")
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Verify test setup - empty dict is falsy
        empty_config: dict[str, Any] = {}
        assert not empty_config, "Empty dictionary should be falsy for test validity"

        # Act: Call save_config with empty dictionary parameter (falsy value)
        wrapper.save_config(empty_config)

        # Assert: Verify save_config utility was not called when config is empty dict (falsy)
        mock_save_config_utility.assert_not_called(), (
            "save_config utility should not be called when config parameter is an empty dictionary (falsy value)"
        )


class TestPipelineWrapperInstanceManagement:
    """Tests for pipeline instance retrieval."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for pipeline instance management tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.unit
    def test_get_instance_success(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful pipeline instance retrieval with default parameters.

        Verifies that PipelineWrapper.get_instance() properly delegates to load_pipeline_instance
        with correct parameters derived from wrapper properties and returns the exact pipeline
        instance returned by the utility function.

        This is a unit test because it tests a single method in isolation with mocked
        external dependencies to verify delegation behavior and parameter passing.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked dependencies and mock pipeline instance
        - Act: Call get_instance() with default parameters
        - Assert: Verify load_pipeline_instance was called with correct parameters and exact instance returned
        """
        # Arrange: Set up mocked dependencies and expected pipeline instance
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_pipeline = mocker.Mock(spec=BasePipeline)
        mock_load_pipeline_instance.return_value = mock_pipeline

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        wrapper = PipelineWrapper(pipeline_setup, dry_run=True)

        # Act: Call get_instance with default parameters
        result = wrapper.get_instance()

        # Assert: Verify load_pipeline_instance was called with correct parameters from wrapper properties
        mock_load_pipeline_instance.assert_called_once_with(
            pipeline_setup,  # root_dir from wrapper
            pipeline_setup / "repo",  # repo_dir from wrapper
            pipeline_setup.name,  # name from wrapper
            pipeline_setup / "pipeline.yml",  # config_path from wrapper
            True,  # dry_run should match wrapper's dry_run setting
            log_string_prefix=None,  # default value
            allow_empty=False,  # default value when not specified
        ), "load_pipeline_instance should be called with parameters derived from wrapper properties"

        # Assert: Verify the exact pipeline instance is returned unchanged
        assert result is mock_pipeline, "Should return the exact pipeline instance from load_pipeline_instance"
        assert isinstance(result, type(mock_pipeline)), "Result should maintain the same type as the mock pipeline"

    @pytest.mark.unit
    def test_get_instance_allow_empty_true(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test pipeline instance retrieval with allow_empty=True delegates correctly to load_pipeline_instance.

        Verifies that PipelineWrapper.get_instance() properly:
        - Delegates to load_pipeline_instance utility function with correct parameters
        - Passes the allow_empty=True parameter correctly through the delegation
        - Returns None when the utility function returns None (no pipeline found)
        - Uses wrapper properties for all other parameters (root_dir, repo_dir, etc.)

        This is a unit test because it tests the delegation behavior in isolation with
        mocked external dependencies to verify the method correctly delegates to the
        utility function with appropriate parameters.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked load_pipeline_instance returning None
        - Act: Call get_instance with allow_empty=True
        - Assert: Verify correct delegation and None return value
        """
        # Arrange: Mock the pipeline loading to return None (empty pipeline)
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_load_pipeline_instance.return_value = None

        # Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act: Create wrapper and get instance with allow_empty=True
        wrapper = PipelineWrapper(pipeline_setup)
        result = wrapper.get_instance(allow_empty=True)

        # Assert: Verify load_pipeline_instance was called with correct parameters from wrapper properties
        mock_load_pipeline_instance.assert_called_once_with(
            pipeline_setup,  # root_dir from wrapper
            pipeline_setup / "repo",  # repo_dir from wrapper
            pipeline_setup.name,  # name from wrapper
            pipeline_setup / "pipeline.yml",  # config_path from wrapper
            False,  # dry_run should be False by default
            log_string_prefix=None,  # default value
            allow_empty=True,  # allow_empty parameter passed through correctly
        ), (
            "load_pipeline_instance should be called with parameters derived from wrapper properties "
            "and allow_empty=True"
        )

        # Assert: Verify None is returned when utility function returns None
        assert result is None, "Should return None when load_pipeline_instance returns None for empty pipeline"


class TestPipelineWrapperPipelineClassDiscovery:
    """Tests for pipeline class discovery and caching."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for pipeline class discovery tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.integration
    def test_get_pipeline_class_no_files(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test FileNotFoundError when no .pipeline.py files exist in repository.

        Verifies that PipelineWrapper.get_pipeline_class() correctly raises a FileNotFoundError
        with a specific error message when no pipeline implementation files (*.pipeline.py)
        are found in the repository directory. This tests the integration between file system
        operations and error reporting with minimal mocking of external dependencies.

        The method under test uses glob pattern matching to discover pipeline files in the
        repository and should raise a descriptive error when no files are found. This validates
        the file discovery mechanism and error handling for empty repositories.

        This is an integration test because it creates real directory structures and tests
        the actual file discovery mechanism with real file system operations rather than
        mocking the glob behavior.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with empty repository directory and mock external dependencies
        - Act: Call get_pipeline_class() on empty repository
        - Assert: Verify specific FileNotFoundError is raised with expected message format
        """
        # Arrange: Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance with real file structure (empty repo directory)
        wrapper = PipelineWrapper(pipeline_setup)
        repo_dir = pipeline_setup / "repo"

        # Ensure test setup is correct for validation
        assert repo_dir.exists(), "Repo directory should exist but be empty for test setup"
        assert repo_dir.is_dir(), "Repo path should be a directory for test setup"
        assert len(list(repo_dir.glob("*.pipeline.py"))) == 0, "Repo should contain no pipeline files for test validity"

        # Act & Assert: Verify specific FileNotFoundError is raised with expected message
        expected_error_message = f'No pipeline implementation found in "{repo_dir}"'
        with pytest.raises(FileNotFoundError, match=re.escape(expected_error_message)):
            wrapper.get_pipeline_class()

    @pytest.mark.integration
    def test_get_pipeline_class_multiple_files(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test FileNotFoundError when multiple .pipeline.py files exist in repository.

        This test verifies that get_pipeline_class() correctly detects and raises a FileNotFoundError
        when multiple pipeline implementation files are found in the repository directory.
        The method should only accept repositories with exactly one .pipeline.py file to ensure
        unambiguous pipeline loading.

        This is an integration test because it creates real files in the filesystem and tests
        the actual file discovery mechanism with real file operations rather than mocking
        the glob behavior.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create multiple pipeline files in repository and mock external dependencies
        - Act: Create wrapper and attempt to get pipeline class
        - Assert: Verify specific FileNotFoundError is raised with complete error message
        """
        # Arrange: Create multiple pipeline files in repository directory
        repo_dir = pipeline_setup / "repo"
        pipeline_file1 = repo_dir / "first.pipeline.py"
        pipeline_file2 = repo_dir / "second.pipeline.py"
        pipeline_file1.write_text("# Pipeline 1")
        pipeline_file2.write_text("# Pipeline 2")

        # Ensure test setup is correct for validation
        assert pipeline_file1.exists(), "First pipeline file should exist for test setup"
        assert pipeline_file2.exists(), "Second pipeline file should exist for test setup"

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Act: Create wrapper and attempt to get pipeline class
        wrapper = PipelineWrapper(pipeline_setup)

        # Assert: Verify specific FileNotFoundError is raised on the stable prefix.
        # Match only the prefix because CPython renders list-of-Path repr differently
        # across Python versions (3.13 reformatted multi-line list rendering in
        # exception messages, breaking a stricter pattern). The follow-up
        # assertions below verify the message contains both file paths.
        expected_prefix = f'Multiple pipeline implementations found in "{repo_dir}"'
        with pytest.raises(FileNotFoundError, match=re.escape(expected_prefix)) as exc_info:
            wrapper.get_pipeline_class()

        # Assert: Verify error message contains both file paths
        error_message = str(exc_info.value)
        assert "first.pipeline.py" in error_message, "Error message should contain first pipeline file name"
        assert "second.pipeline.py" in error_message, "Error message should contain second pipeline file name"
        assert str(repo_dir) in error_message, "Error message should contain repository directory path"

    @pytest.mark.integration
    def test_get_pipeline_class_spec_none(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test ImportError when spec_from_file_location returns None.

        This test verifies that get_pipeline_class() correctly raises an ImportError
        with a specific error message when spec_from_file_location returns None,
        indicating that the Python module spec could not be created from the pipeline file.
        This is an integration test because it tests the interaction between file discovery,
        module loading attempts, and error handling with real file system operations and
        minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline file and mock spec_from_file_location to return None
        - Act: Call get_pipeline_class()
        - Assert: Verify specific ImportError is raised with expected message
        """
        # Arrange: Create pipeline file and set up mocks for integration testing
        pipeline_file = pipeline_setup / "repo" / "test.pipeline.py"
        pipeline_file.write_text("# Test pipeline")

        # Mock external dependencies to avoid side effects in integration test
        mock_spec_from_file_location = mocker.patch("marimba.core.wrappers.pipeline.spec_from_file_location")
        mock_spec_from_file_location.return_value = None
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act & Assert: Verify specific ImportError is raised with expected message format
        expected_error_pattern = f"Could not load spec for test.pipeline from {re.escape(str(pipeline_file))}"
        with pytest.raises(ImportError, match=expected_error_pattern):
            wrapper.get_pipeline_class()

    @pytest.mark.integration
    def test_get_pipeline_class_no_loader(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test ImportError when module spec has no loader.

        This test verifies that get_pipeline_class() correctly raises an ImportError
        with a specific error message when the module spec returned by spec_from_file_location
        has a None loader. This can occur when the module file exists but cannot be
        properly loaded by Python's import machinery.

        This is an integration test because it tests the interaction between file discovery,
        module loading, and error handling with real file system operations and minimal
        mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline file and mock spec_from_file_location to return spec with None loader
        - Act: Call get_pipeline_class()
        - Assert: Verify specific ImportError is raised with complete expected message
        """
        # Arrange: Create pipeline file and set up mocks for integration test
        pipeline_file = pipeline_setup / "repo" / "test.pipeline.py"
        pipeline_file.write_text("# Test pipeline")

        # Mock external dependencies to avoid side effects in integration test
        mock_spec_from_file_location = mocker.patch("marimba.core.wrappers.pipeline.spec_from_file_location")
        mocker.patch("marimba.core.wrappers.pipeline.module_from_spec")
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Set up mock spec with None loader to trigger the error condition
        mock_spec = mocker.Mock()
        mock_spec.loader = None
        mock_spec_from_file_location.return_value = mock_spec

        # Ensure test setup is correct for validation
        assert pipeline_file.exists(), "Pipeline file should exist for test setup"
        assert pipeline_file.is_file(), "Pipeline path should be a file for test setup"

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act & Assert: Verify specific ImportError is raised with complete expected message
        expected_error_pattern = f"Could not find loader for test.pipeline from {re.escape(str(pipeline_file))}"
        with pytest.raises(ImportError, match=expected_error_pattern) as exc_info:
            wrapper.get_pipeline_class()

        # Assert: Verify error message contains expected components
        error_message = str(exc_info.value)
        assert "test.pipeline" in error_message, "Error message should contain module name"
        assert str(pipeline_file) in error_message, "Error message should contain complete file path"
        assert "Could not find loader" in error_message, "Error message should indicate loader issue"

        # Assert: Verify spec_from_file_location was called with correct parameters
        mock_spec_from_file_location.assert_called_once_with(
            "test.pipeline",
            str(pipeline_file.absolute()),
        ), "spec_from_file_location should be called with module name and absolute file path"

    @pytest.mark.integration
    def test_get_pipeline_class_success(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful pipeline class discovery and caching behavior with real file system operations.

        This test verifies that get_pipeline_class() correctly:
        - Discovers pipeline files in the repository directory using real file system operations
        - Loads a real Python module containing a valid BasePipeline subclass
        - Caches the result to avoid reloading on subsequent calls
        - Ignores non-BasePipeline classes and the base class itself

        This is an integration test because it creates real files and tests the actual
        file discovery mechanism and module loading with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Create real pipeline file with valid Python code and mock only external dependencies
        - Act: Call get_pipeline_class() twice to test discovery and caching
        - Assert: Verify correct class is returned and caching works as expected
        """
        # Arrange: Create real pipeline file with valid Python code
        pipeline_file = pipeline_setup / "repo" / "test.pipeline.py"
        pipeline_file.write_text(
            """
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from pathlib import Path
from typing import Any

class TestPipeline(BasePipeline):
    '''Test pipeline implementation for testing purposes.'''

    def __init__(
        self, root_path: Any, config: Any = None, metadata_class: Any = BaseMetadata, *, dry_run: bool = False
    ) -> None:
        super().__init__(root_path, config, metadata_class, dry_run=dry_run)

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {"test_param": "default_value"}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {"collection_param": "default_collection"}

    def _package(
        self, data_dir: Path, config: dict[str, Any], **kwargs: dict[str, Any]
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {Path("test.txt"): (Path("relative/test.txt"), None, None)}

class SomeOtherClass:
    '''Not a BasePipeline subclass - should be ignored.'''
    pass
""",
        )

        # Mock only external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Ensure test setup is correct for validation
        assert pipeline_file.exists(), "Pipeline file should exist for test setup"
        assert pipeline_file.is_file(), "Pipeline path should be a file for test setup"

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Test pipeline class discovery and caching behavior
        result_first_call = wrapper.get_pipeline_class()
        result_second_call = wrapper.get_pipeline_class()

        # Assert: Verify correct pipeline class is discovered and returned
        assert result_first_call is not None, "Should discover and return a valid BasePipeline subclass"
        assert hasattr(result_first_call, "__name__"), "Result should be a class with a name attribute"
        assert result_first_call.__name__ == "TestPipeline", "Should discover the TestPipeline class specifically"
        assert issubclass(result_first_call, BasePipeline), "Result should be a subclass of BasePipeline"
        assert result_first_call is not BasePipeline, "Result should not be the base class itself"

        # Assert: Verify caching behavior
        assert result_second_call == result_first_call, "Should return the same class on subsequent calls"
        assert result_second_call is result_first_call, "Should return the exact same object instance (cached)"

        # Assert: Verify the discovered class has expected methods from the pipeline interface
        assert hasattr(
            result_first_call,
            "get_pipeline_config_schema",
        ), "Pipeline class should have get_pipeline_config_schema method"
        assert hasattr(
            result_first_call,
            "get_collection_config_schema",
        ), "Pipeline class should have get_collection_config_schema method"
        assert callable(result_first_call.get_pipeline_config_schema), "get_pipeline_config_schema should be callable"
        assert callable(
            result_first_call.get_collection_config_schema,
        ), "get_collection_config_schema should be callable"

    @pytest.mark.unit
    def test_get_pipeline_class_no_valid_class(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test that None is returned when no valid pipeline class is found.

        This test verifies that get_pipeline_class() correctly returns None when the
        pipeline module contains only classes that are not valid BasePipeline subclasses.
        This is a unit test because it tests a single method in isolation with mocked
        external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline file and mock module loading with invalid classes
        - Act: Call get_pipeline_class()
        - Assert: Verify None is returned when no valid pipeline class exists
        """
        # Arrange: Set up pipeline file and mocks for isolated unit testing
        pipeline_file = pipeline_setup / "repo" / "test.pipeline.py"
        pipeline_file.write_text("# Test pipeline")

        # Mock external dependencies to isolate unit under test
        mock_spec_from_file_location = mocker.patch("marimba.core.wrappers.pipeline.spec_from_file_location")
        mock_module_from_spec = mocker.patch("marimba.core.wrappers.pipeline.module_from_spec")
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Set up mock module loading to return a module with no valid pipeline classes
        mock_loader = mocker.Mock()
        mock_spec = mocker.Mock()
        mock_spec.loader = mock_loader
        mock_spec_from_file_location.return_value = mock_spec

        # Create a mock module containing only invalid classes
        mock_module = type("MockModule", (), {})()
        mock_module.__dict__ = {
            "SomeClass": str,  # Not a BasePipeline subclass
            "BasePipeline": BasePipeline,  # This should be ignored (it's the base class itself)
            "AnotherClass": int,  # Not a BasePipeline subclass
        }
        mock_module_from_spec.return_value = mock_module

        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call the method under test
        result = wrapper.get_pipeline_class()

        # Assert: Verify None is returned when no valid pipeline classes are found
        assert result is None, "Should return None when no valid BasePipeline subclasses are found in module"

        # Assert: Verify the module loading was called with correct parameters
        mock_spec_from_file_location.assert_called_once_with(
            "test.pipeline",
            str(pipeline_file.absolute()),
        ), "spec_from_file_location should be called with module name and absolute file path"

        # Assert: Verify module execution was performed
        mock_loader.exec_module.assert_called_once_with(
            mock_module,
        ), "Module should be executed to load classes for inspection"


class TestPipelineWrapperPipelineConfigPrompt:
    """Tests for pipeline configuration prompting."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for pipeline configuration prompting tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.integration
    def test_prompt_pipeline_config_success(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful pipeline configuration prompting with user input.

        Verifies that PipelineWrapper.prompt_pipeline_config() properly:
        - Retrieves the pipeline instance with correct allow_empty parameter
        - Gets the pipeline configuration schema from the pipeline instance
        - Prompts the user for configuration using the schema
        - Returns the user-provided configuration
        - Logs the final configuration using the wrapper's logger

        This is an integration test because it tests the interaction between
        pipeline instance loading, schema retrieval, user prompting, and logging
        with minimal mocking of business logic components.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline with mock dependencies and expected schema/input
        - Act: Call prompt_pipeline_config() to test the integration
        - Assert: Verify all interactions and final configuration are correct
        """
        # Arrange: Set up test data and mock external dependencies only
        expected_schema = {"param1": "default1", "param2": 42}
        expected_user_input = {"param1": "user_value"}
        expected_final_config = {"param1": "user_value"}

        # Mock external dependencies only (not internal business logic)
        mock_prompt_schema = mocker.patch("marimba.core.wrappers.pipeline.prompt_schema")
        mock_prompt_schema.return_value = expected_user_input

        # Mock load_pipeline_instance to return a real pipeline instance for integration testing
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_pipeline = MockTestPipeline(root_path=pipeline_setup)
        mocker.patch.object(mock_pipeline, "get_pipeline_config_schema", return_value=expected_schema)
        mock_load_pipeline_instance.return_value = mock_pipeline

        # Mock initialization dependencies to avoid side effects
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Mock the wrapper's logger property to verify logging behavior
        mock_wrapper_logger = mocker.Mock()
        mocker.patch.object(
            type(wrapper),
            "logger",
            new_callable=mocker.PropertyMock(return_value=mock_wrapper_logger),
        )

        # Act: Call the method under test
        result = wrapper.prompt_pipeline_config()

        # Assert: Verify load_pipeline_instance was called with correct parameters from wrapper properties
        mock_load_pipeline_instance.assert_called_once_with(
            wrapper.root_dir,
            wrapper.repo_dir,
            wrapper.name,
            wrapper.config_path,
            wrapper.dry_run,
            log_string_prefix=None,
            allow_empty=False,
        ), "load_pipeline_instance should be called with wrapper properties and allow_empty=False by default"

        # Assert: Verify schema was prompted with the correct schema from pipeline
        mock_prompt_schema.assert_called_once_with(expected_schema, accept_defaults=False), (
            "prompt_schema should be called with the schema returned by pipeline.get_pipeline_config_schema()"
        )

        # Assert: Verify the correct configuration is returned
        assert result == expected_final_config, "Should return the configuration provided by user through prompt_schema"

        # Assert: Verify logging behavior - final configuration should be logged using wrapper's logger
        mock_wrapper_logger.info.assert_called_once_with(
            f"Provided pipeline config={expected_final_config}",
        ), "Should log the final pipeline configuration using the wrapper's logger"

    @pytest.mark.unit
    def test_prompt_pipeline_config_with_existing_config(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline configuration prompting with partial existing configuration.

        Verifies that PipelineWrapper.prompt_pipeline_config() properly:
        - Uses existing config values when they exist in the provided config
        - Only prompts for missing configuration parameters not provided in existing config
        - Merges existing configuration with newly prompted values
        - Logs the final merged configuration appropriately
        - Returns the complete configuration combining existing and prompted values

        This is a unit test because it tests the configuration merging logic in isolation
        with mocked dependencies to verify the behavior without external side effects.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked dependencies and test configuration
        - Act: Call prompt_pipeline_config with existing config that satisfies some schema requirements
        - Assert: Verify only missing parameters are prompted and final config merges both sources
        """
        # Arrange: Set up test data for schema, existing config, and expected prompting
        pipeline_schema = {
            "param1": "default1",
            "param2": 42,
            "param3": "default3",
        }
        existing_config = {"param1": "existing_value"}
        user_prompted_input = {"param2": 100}  # User only provides param2, param3 uses default
        expected_final_config = {"param1": "existing_value", "param2": 100}

        # Arrange: Set up mock external dependencies to isolate unit under test
        mock_prompt_schema = mocker.patch("marimba.core.wrappers.pipeline.prompt_schema")
        mock_prompt_schema.return_value = user_prompted_input

        # Mock load_pipeline_instance to return a real pipeline instance for realistic testing
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_pipeline = MockTestPipeline(root_path=pipeline_setup)
        mocker.patch.object(mock_pipeline, "get_pipeline_config_schema", return_value=pipeline_schema)
        mock_load_pipeline_instance.return_value = mock_pipeline

        # Mock initialization dependencies to avoid side effects
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Mock the wrapper's logger property to verify logging behavior
        mock_wrapper_logger = mocker.Mock()
        mocker.patch.object(
            type(wrapper),
            "logger",
            new_callable=mocker.PropertyMock(return_value=mock_wrapper_logger),
        )

        # Act: Call prompt_pipeline_config with partial existing configuration
        result = wrapper.prompt_pipeline_config(config=existing_config)

        # Assert: Verify load_pipeline_instance was called with correct parameters from wrapper properties
        mock_load_pipeline_instance.assert_called_once_with(
            wrapper.root_dir,
            wrapper.repo_dir,
            wrapper.name,
            wrapper.config_path,
            wrapper.dry_run,
            log_string_prefix=None,
            allow_empty=False,
        ), "load_pipeline_instance should be called with wrapper properties and allow_empty=False by default"

        # Assert: Verify only missing parameters were prompted (schema without existing param1)
        expected_prompted_schema = {"param2": 42, "param3": "default3"}  # param1 removed since it exists
        mock_prompt_schema.assert_called_once_with(expected_prompted_schema, accept_defaults=False), (
            "prompt_schema should be called only with schema parameters not present in existing config"
        )

        # Assert: Verify the result properly merges existing and prompted values
        assert (
            result == expected_final_config
        ), "Should return merged configuration combining existing config values with user-prompted values"
        assert "param1" in result, "Result should contain existing config parameter param1"
        assert result["param1"] == "existing_value", "Existing param1 value should be preserved"
        assert "param2" in result, "Result should contain prompted parameter param2"
        assert result["param2"] == 100, "Prompted param2 value should be included"
        assert "param3" not in result, "param3 should not be in result since user didn't provide it and it was optional"

        # Assert: Verify logging behavior - final configuration should be logged using wrapper's logger
        mock_wrapper_logger.info.assert_called_once_with(
            f"Provided pipeline config={expected_final_config}",
        ), "Should log the final merged pipeline configuration using the wrapper's logger"

    @pytest.mark.integration
    def test_prompt_pipeline_config_empty_pipeline(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline configuration prompting with empty pipeline returns None.

        Verifies that prompt_pipeline_config() properly handles empty pipeline scenarios:
        - Calls get_instance() with correct allow_empty parameter
        - Returns None when no pipeline instance is found and allow_empty=True
        - Does not attempt to access pipeline schema or prompt for configuration
        - Tests integration between configuration prompting and instance management

        This is an integration test because it tests the interaction between
        prompt_pipeline_config() and get_instance() with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Mock dependencies and set up empty pipeline scenario
        - Act: Call prompt_pipeline_config with allow_empty=True
        - Assert: Verify None is returned and get_instance was called correctly
        """
        # Arrange: Mock external dependencies to isolate unit under test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Mock get_instance to return None (simulating empty pipeline)
        mock_get_instance = mocker.patch.object(PipelineWrapper, "get_instance")
        mock_get_instance.return_value = None

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call prompt_pipeline_config with allow_empty=True
        result = wrapper.prompt_pipeline_config(allow_empty=True)

        # Assert: Verify get_instance was called with correct parameter
        mock_get_instance.assert_called_once_with(allow_empty=True), (
            "get_instance should be called with allow_empty=True to handle empty pipelines"
        )

        # Assert: Verify None is returned for empty pipeline
        assert result is None, "Should return None when pipeline is empty and allow_empty=True is specified"

    @pytest.mark.integration
    def test_prompt_pipeline_config_no_additional_prompting(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline configuration when existing config satisfies all schema requirements.

        Verifies that prompt_pipeline_config() properly:
        - Uses existing config values when they match all schema parameters
        - Skips prompting when no additional configuration is needed
        - Returns the existing configuration unchanged
        - Logs the final configuration appropriately
        - Calls get_instance with correct default parameters

        This is an integration test because it tests the interaction between
        prompt_pipeline_config() and the pipeline's configuration schema handling
        with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline with schema and existing config that satisfies all requirements
        - Act: Call prompt_pipeline_config with complete existing configuration
        - Assert: Verify no prompting occurs and existing config is returned unchanged
        """
        # Arrange: Mock external dependencies only (not internal business logic)
        mock_prompt_schema = mocker.patch("marimba.core.wrappers.pipeline.prompt_schema")

        # Create mock pipeline with schema that will be satisfied by existing config
        mock_pipeline = mocker.Mock()
        mock_pipeline.get_pipeline_config_schema.return_value = {"param1": "default1"}

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_get_instance = mocker.patch.object(PipelineWrapper, "get_instance")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        mock_get_instance.return_value = mock_pipeline

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Mock the wrapper's logger to verify logging behavior
        mock_wrapper_logger = mocker.Mock()
        mocker.patch.object(
            type(wrapper),
            "logger",
            new_callable=mocker.PropertyMock(return_value=mock_wrapper_logger),
        )

        # Set up existing config that satisfies all schema requirements
        existing_config = {"param1": "existing_value"}

        # Act: Call prompt_pipeline_config with complete existing configuration
        result = wrapper.prompt_pipeline_config(config=existing_config)

        # Assert: Verify get_instance was called with correct default parameters
        mock_get_instance.assert_called_once_with(allow_empty=False), (
            "get_instance should be called with default allow_empty=False"
        )

        # Assert: Verify no prompting occurs since all parameters are satisfied
        mock_prompt_schema.assert_not_called(), (
            "prompt_schema should not be called when existing config satisfies all schema requirements"
        )

        # Assert: Verify the result contains the existing configuration unchanged
        assert result == {
            "param1": "existing_value",
        }, "Should return existing config unchanged when it satisfies all schema requirements"

        # Assert: Verify logging behavior - configuration should be logged using wrapper's logger
        mock_wrapper_logger.info.assert_called_once_with(
            "Provided pipeline config={'param1': 'existing_value'}",
        ), "Should log the final pipeline configuration using wrapper's logger"

    @pytest.mark.integration
    def test_prompt_pipeline_config_with_project_logger(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test pipeline configuration prompting uses custom project logger when provided.

        Verifies that PipelineWrapper.prompt_pipeline_config() properly:
        - Uses the provided project_logger parameter for logging instead of wrapper's logger
        - Logs the final configuration using the custom logger with correct format
        - Returns the expected configuration from the pipeline schema
        - Integrates correctly with pipeline instance retrieval and schema prompting

        This is an integration test because it tests the interaction between
        prompt_pipeline_config(), pipeline instance loading, schema handling, and logging
        with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with real pipeline instance and mock external dependencies
        - Act: Call prompt_pipeline_config() with custom project logger
        - Assert: Verify project logger was used and pipeline behavior is correct
        """
        # Arrange: Set up test data and mock external dependencies for integration test
        expected_schema = {"test_param": "default_value", "test_int": 42}
        expected_user_input = {"test_param": "user_value"}
        expected_config = {"test_param": "user_value"}

        # Mock external dependencies only (not internal business logic)
        mock_prompt_schema = mocker.patch("marimba.core.wrappers.pipeline.prompt_schema")
        mock_prompt_schema.return_value = expected_user_input

        # Mock load_pipeline_instance to return a real pipeline instance for integration testing
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_pipeline = MockTestPipeline(root_path=pipeline_setup)
        mocker.patch.object(mock_pipeline, "get_pipeline_config_schema", return_value=expected_schema)
        mock_load_pipeline_instance.return_value = mock_pipeline

        # Create mock project logger to verify it gets used
        mock_project_logger = mocker.Mock(spec=logging.Logger)

        # Mock initialization dependencies to avoid side effects
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call prompt_pipeline_config with custom project logger
        result = wrapper.prompt_pipeline_config(project_logger=mock_project_logger)

        # Assert: Verify load_pipeline_instance was called with correct parameters from wrapper properties
        mock_load_pipeline_instance.assert_called_once_with(
            wrapper.root_dir,
            wrapper.repo_dir,
            wrapper.name,
            wrapper.config_path,
            wrapper.dry_run,
            log_string_prefix=None,
            allow_empty=False,
        ), "load_pipeline_instance should be called with wrapper properties and allow_empty=False by default"

        # Assert: Verify schema was prompted with the correct schema from pipeline
        mock_prompt_schema.assert_called_once_with(expected_schema, accept_defaults=False), (
            "prompt_schema should be called with the schema returned by pipeline.get_pipeline_config_schema()"
        )

        # Assert: Verify the correct configuration is returned
        assert result == expected_config, "Should return the configuration provided by user through prompt_schema"

        # Assert: Verify project logger was used instead of wrapper's logger
        mock_project_logger.info.assert_called_once_with(
            f"Provided pipeline config={expected_config}",
        ), "Should use the provided project_logger for logging configuration instead of wrapper's logger"

    @pytest.mark.integration
    def test_prompt_pipeline_config_uses_pipeline_logger_by_default(
        self,
        pipeline_setup: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that pipeline wrapper's logger is used when no project logger is provided.

        This test verifies that when no project_logger parameter is passed to
        prompt_pipeline_config(), the method uses the wrapper's own logger (self.logger)
        for logging the final configuration. This is an integration test because it tests
        the interaction between prompt_pipeline_config(), pipeline instance loading,
        schema handling, and logging with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with real pipeline instance and mock only external dependencies
        - Act: Call prompt_pipeline_config() without project_logger parameter
        - Assert: Verify wrapper's logger was used and real pipeline behavior is tested
        """
        # Arrange: Set up mock pipeline instance with realistic behavior for integration testing
        mock_pipeline = MockTestPipeline(root_path=pipeline_setup)
        expected_schema = {"test_param": "default_value", "test_int": 42}
        expected_user_input = {"test_param": "user_value"}
        expected_final_config = {"test_param": "user_value"}

        # Mock only external dependencies (not internal business logic)
        mock_prompt_schema = mocker.patch("marimba.core.wrappers.pipeline.prompt_schema")
        mock_prompt_schema.return_value = expected_user_input

        # Mock load_pipeline_instance to return our test pipeline
        mock_load_pipeline_instance = mocker.patch("marimba.core.wrappers.pipeline.load_pipeline_instance")
        mock_load_pipeline_instance.return_value = mock_pipeline

        # Mock initialization dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Mock the logger property to verify logging behavior
        mock_wrapper_logger = mocker.Mock()
        mocker.patch.object(
            type(wrapper),
            "logger",
            new_callable=mocker.PropertyMock(return_value=mock_wrapper_logger),
        )

        # Act: Call prompt_pipeline_config without providing project_logger
        result = wrapper.prompt_pipeline_config()

        # Assert: Verify load_pipeline_instance was called with correct parameters
        mock_load_pipeline_instance.assert_called_once_with(
            wrapper.root_dir,
            wrapper.repo_dir,
            wrapper.name,
            wrapper.config_path,
            wrapper.dry_run,
            log_string_prefix=None,
            allow_empty=False,
        ), "load_pipeline_instance should be called with wrapper properties and default allow_empty=False"

        # Assert: Verify prompt_schema was called with the real pipeline schema
        mock_prompt_schema.assert_called_once_with(expected_schema, accept_defaults=False), (
            "prompt_schema should be called with the schema returned by real pipeline instance"
        )

        # Assert: Verify wrapper's logger was used for configuration logging
        mock_wrapper_logger.info.assert_called_once_with(
            f"Provided pipeline config={expected_final_config}",
        ), "Should use wrapper's logger when no project_logger is provided"

        # Assert: Verify correct configuration is returned from real pipeline interaction
        assert result == expected_final_config, "Should return the configuration from real pipeline schema prompting"


class TestPipelineWrapperRepositoryOperations:
    """Tests for repository update and installation operations."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for repository operations tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.integration
    def test_update_success(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful repository update via git pull operation.

        Verifies that PipelineWrapper.update() properly integrates with git operations:
        - Creates a Repo instance using the wrapper's repository directory path
        - Accesses the origin remote from the initialized repository
        - Executes a git pull operation to fetch and merge remote changes
        - Completes the update operation without errors or exceptions
        - Maintains proper integration between wrapper and git library components

        This is an integration test because it tests the interaction between
        PipelineWrapper and git operations (Repo creation and remote operations)
        with minimal mocking of external dependencies to ensure proper component
        interaction while avoiding side effects from actual git operations.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up mock git repository and remote operations with pipeline wrapper
        - Act: Execute the update operation to test git integration
        - Assert: Verify correct git operations were called with expected parameters
        """
        # Arrange: Set up mock git repository and remote operations
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_repo = mocker.Mock()
        mock_origin = mocker.Mock()
        mock_repo.remotes.origin = mock_origin
        mock_repo_class.return_value = mock_repo

        # Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance to test integration behavior
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Perform the repository update operation to test git integration
        wrapper.update()

        # Assert: Verify git repository was accessed with correct path from wrapper properties
        mock_repo_class.assert_called_once_with(
            pipeline_setup / "repo",
        ), "Repo constructor should be called with the wrapper's repository directory path (repo_dir property)"

        # Assert: Verify git pull operation was executed on origin remote to update repository
        mock_origin.pull.assert_called_once(), (
            "Git pull operation should be executed on the origin remote to fetch and merge latest changes"
        )

        # Assert: Verify the wrapper's repo_dir property points to the expected directory
        assert (
            wrapper.repo_dir == pipeline_setup / "repo"
        ), "Wrapper's repo_dir property should point to the 'repo' subdirectory of the pipeline setup"

    @pytest.mark.integration
    def test_install_success(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test successful pipeline dependency installation via PipelineInstaller.

        Verifies that PipelineWrapper.install() properly:
        - Uses the cached PipelineInstaller instance created during initialization
        - Calls the installer to install pipeline dependencies
        - Delegates dependency installation to the PipelineInstaller framework
        - Maintains consistent behavior with wrapper initialization

        This is an integration test because it tests the interaction between
        PipelineWrapper and PipelineInstaller with minimal mocking of external dependencies.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper with mocked external dependencies only
        - Act: Call the install method to test installer integration
        - Assert: Verify installer creation and invocation with correct parameters
        """
        # Arrange: Mock external dependencies to avoid side effects in integration test
        mocker.patch.object(PipelineWrapper, "_setup_logging")

        # Mock PipelineInstaller.create to return a mock installer for controlled testing
        mock_installer_create = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")
        mock_installer = mocker.Mock()
        mock_installer_create.return_value = mock_installer

        # Create wrapper instance (installer is created during initialization)
        wrapper = PipelineWrapper(pipeline_setup)

        # Act: Call the install method to test integration with installer
        wrapper.install()

        # Assert: Verify installer was created with correct parameters during initialization
        mock_installer_create.assert_called_once_with(
            wrapper.repo_dir,
            wrapper.logger,
        ), "PipelineInstaller should be created with repo directory and logger during wrapper initialization"

        # Assert: Verify the installer was invoked to perform the installation
        mock_installer.assert_called_once(), "Cached installer instance should be called to install dependencies"

        # Assert: Verify the wrapper maintains reference to the installer instance
        assert (
            wrapper._pipeline_installer is mock_installer
        ), "Wrapper should maintain reference to the installer instance"


class TestPipelineWrapperErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.fixture
    def pipeline_setup(self, tmp_path: Path) -> Path:
        """Set up test fixtures for error handling tests."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        config_file = tmp_path / "pipeline.yml"
        config_file.write_text("test: config")
        return tmp_path

    @pytest.mark.unit
    def test_update_git_repo_creation_error(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test repository update propagates GitError when Repo() constructor fails.

        Verifies that PipelineWrapper.update() properly:
        - Attempts to create a Repo instance with the correct repository directory path
        - Propagates GitError when repository initialization fails (e.g., corrupted .git, invalid repo)
        - Does not attempt git pull operation when repository creation fails
        - Maintains proper error handling for repository access issues

        This is a unit test because it tests error handling in isolation with mocked
        external dependencies to verify the repository creation failure path.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper and mock Repo constructor to fail with GitError
        - Act: Call wrapper.update() method
        - Assert: Verify GitError is propagated and Repo constructor was called correctly
        """
        # Arrange: Set up test data and mock external dependencies
        expected_error_message = "fatal: not a git repository (or any of the parent directories): .git"

        # Mock external dependencies to avoid side effects in unit test
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_repo_class.side_effect = GitError(expected_error_message)

        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Act & Assert: Verify GitError is raised with specific message when Repo() fails
        with pytest.raises(GitError, match=re.escape(expected_error_message)):
            wrapper.update()

        # Assert: Verify Repo was called with correct repository directory path
        mock_repo_class.assert_called_once_with(
            wrapper.repo_dir,
        ), "Repo constructor should be called with the wrapper's repo directory path"

    @pytest.mark.unit
    def test_update_git_pull_error(self, pipeline_setup: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test repository update propagates GitError when git pull operation fails.

        Verifies that PipelineWrapper.update() properly:
        - Successfully creates a Repo instance with the repository directory
        - Attempts to access the origin remote and perform a pull operation
        - Propagates GitError when the pull operation fails (e.g., network issues, merge conflicts)
        - Maintains proper error handling for git pull failures

        This is a unit test because it tests error handling in isolation with mocked
        external dependencies to verify the git pull failure path.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up pipeline wrapper and mock git pull to fail with GitError
        - Act: Call wrapper.update() method
        - Assert: Verify GitError is propagated and git operations were called correctly
        """
        # Arrange: Set up test data and mock git pull failure
        expected_error_message = (
            "fatal: unable to access 'https://github.com/example/repo.git/': Could not resolve host"
        )

        # Mock external dependencies to isolate unit under test
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_repo = mocker.Mock()
        mock_origin = mocker.Mock()
        mock_origin.pull.side_effect = GitError(expected_error_message)
        mock_repo.remotes.origin = mock_origin
        mock_repo_class.return_value = mock_repo

        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create wrapper instance
        wrapper = PipelineWrapper(pipeline_setup)

        # Verify test setup is correct before acting
        assert mock_repo_class.return_value is mock_repo, "Mock repo should be properly configured"
        assert mock_repo.remotes.origin is mock_origin, "Mock origin remote should be properly configured"

        # Act & Assert: Verify GitError is raised with specific message when pull fails
        with pytest.raises(GitError, match=re.escape(expected_error_message)) as exc_info:
            wrapper.update()

        # Assert: Verify the exact error message is preserved
        assert str(exc_info.value) == expected_error_message, "GitError message should be preserved exactly"

        # Assert: Verify Repo was created with correct repository directory path
        mock_repo_class.assert_called_once_with(
            wrapper.repo_dir,
        ), "Repo constructor should be called with the wrapper's repo directory path"

        # Assert: Verify pull operation was attempted on origin remote
        mock_origin.pull.assert_called_once(), "Should attempt to pull from origin remote before failing"

    @pytest.mark.unit
    def test_create_git_clone_error(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test that PipelineWrapper.create() propagates GitError when git clone fails.

        Verifies that PipelineWrapper.create() properly:
        - Creates the pipeline root directory before attempting git clone
        - Propagates GitError when git clone operation fails
        - Leaves directory structure in consistent state (directory exists but no repo or config)
        - Does not attempt to save config when git clone fails
        - Does not create PipelineWrapper instance when git clone fails

        This is a unit test because it tests error handling in isolation with mocked
        external dependencies to verify the git clone failure path.

        Follows Arrange-Act-Assert pattern:
        - Arrange: Set up test data and mock git clone to fail
        - Act: Attempt to create pipeline with failing git clone
        - Assert: Verify GitError is propagated and side effects are correct
        """
        # Arrange: Set up test data and mock external dependencies to isolate unit under test
        pipeline_dir = tmp_path / "test_pipeline"
        git_url = "https://github.com/example/invalid-repo.git"
        expected_error_message = "fatal: repository 'https://github.com/example/invalid-repo.git/' not found"

        # Mock external dependencies to isolate unit under test
        mock_repo_class = mocker.patch("marimba.core.wrappers.pipeline.Repo")
        mock_save_config = mocker.patch("marimba.core.wrappers.pipeline.save_config")
        mock_repo_class.clone_from.side_effect = GitError(expected_error_message)

        # Act & Assert: Verify GitError is raised with specific message when git clone fails
        with pytest.raises(GitError, match=r"fatal: repository .* not found"):
            PipelineWrapper.create(pipeline_dir, git_url)

        # Assert: Verify side effects - directory exists but incomplete structure
        assert pipeline_dir.exists(), "Pipeline directory should be created before clone attempt"
        assert pipeline_dir.is_dir(), "Pipeline directory should be a directory"
        assert not (pipeline_dir / "repo").exists(), "Repo directory should not exist after failed clone"
        assert not (pipeline_dir / "pipeline.yml").exists(), "Config file should not exist after failed clone"

        # Assert: Verify git clone was attempted with correct parameters
        mock_repo_class.clone_from.assert_called_once_with(
            git_url,
            pipeline_dir / "repo",
        ), "Git clone should be attempted with correct URL and repo directory path"

        # Assert: Verify save_config was not called when git clone fails
        mock_save_config.assert_not_called(), "save_config should not be called when git clone fails"
