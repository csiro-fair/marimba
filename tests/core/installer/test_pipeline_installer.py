import logging
from pathlib import Path
from unittest.mock import call

import pytest
import pytest_mock

from marimba.core.installer.pipeline_installer import PipelineInstaller
from marimba.core.installer.uv_executor import ExecutorResult, UvExecutor


@pytest.fixture
def pipeline_path_with_requirements(tmp_path: Path) -> Path:
    """Create a temporary pipeline directory with a requirements.txt file."""
    requirements_content = "numpy==1.24.0\npandas>=1.5.0\n"
    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text(requirements_content)
    return tmp_path


@pytest.fixture
def pipeline_path_with_pyproject(tmp_path: Path) -> Path:
    """Create a temporary pipeline directory with a pyproject.toml file."""
    pyproject_content = """[build-system]
requires = ["setuptools", "wheel"]

[project]
name = "test-pipeline"
version = "0.1.0"
"""
    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(pyproject_content)
    return tmp_path


class TestPipelineInstaller:
    """Test suite for PipelineInstaller class."""

    @pytest.mark.unit
    def test_installer_requirements_file_success(
        self,
        pipeline_path_with_requirements: Path,
        mocker: pytest_mock.MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test successful installation using requirements.txt file.

        Verifies that the installer correctly executes UV with the requirements file,
        handles successful installation results, and logs appropriate messages.
        """
        # Arrange
        logger = logging.getLogger("marimba.test.requirements")
        output_message = "Installation successful"
        error_message = ""
        mock_executor = mocker.Mock(
            side_effect=[
                ExecutorResult("", ""),  # freeze: empty environment
                ExecutorResult(output_message, error_message),  # install
            ],
        )
        installer = PipelineInstaller(pipeline_path_with_requirements, logger, mock_executor)

        with caplog.at_level(logging.DEBUG, logger="marimba.test.requirements"):
            # Act
            installer()

        # Assert - Verify executor was called twice: freeze then install
        assert mock_executor.call_count == 2
        assert mock_executor.call_args_list[0] == call("freeze")
        assert mock_executor.call_args_list[1] == call(
            "install",
            "-r",
            str(pipeline_path_with_requirements / "requirements.txt"),
        )

        # Assert - Verify logging behavior
        log_records = caplog.records
        assert len(log_records) == 3, f"Expected exactly 3 log messages, got {len(log_records)}"

        # Verify start installation log
        start_record = log_records[0]
        assert start_record.levelname == "INFO", "First message should be INFO level"
        assert "Started installing pipeline dependencies from" in start_record.message
        assert str(pipeline_path_with_requirements / "requirements.txt") in start_record.message

        # Verify executor output is logged as debug
        output_record = log_records[1]
        assert output_record.levelname == "DEBUG", "Second message should be DEBUG level"
        assert output_record.message == output_message, "Second message should contain executor output"

        # Verify completion log
        completion_record = log_records[2]
        assert completion_record.levelname == "INFO", "Third message should be INFO level"
        assert completion_record.message == "Pipeline dependencies installed", "Third message should confirm completion"

    @pytest.mark.unit
    def test_installer_pyproject_file_success(
        self,
        pipeline_path_with_pyproject: Path,
        mocker: pytest_mock.MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test successful installation using pyproject.toml file.

        Verifies that the installer correctly executes UV with the pyproject directory
        when no requirements.txt is present, and properly logs the installation process.
        Note: The source code always logs requirements.txt path regardless of which
        dependency file is actually used - this test verifies that actual behavior.
        """
        # Arrange
        logger = logging.getLogger("marimba.test.pyproject")
        output_message = "Successfully installed test-pipeline-0.1.0"
        error_message = "Some deprecation warnings"
        mock_executor = mocker.Mock(
            side_effect=[
                ExecutorResult("", ""),  # freeze: empty environment
                ExecutorResult(output_message, error_message),  # install
            ],
        )
        installer = PipelineInstaller(pipeline_path_with_pyproject, logger, mock_executor)

        with caplog.at_level(logging.DEBUG, logger="marimba.test.pyproject"):
            # Act
            installer()

        # Assert - Verify executor was called twice: freeze then install
        assert mock_executor.call_count == 2
        assert mock_executor.call_args_list[0] == call("freeze")
        assert mock_executor.call_args_list[1] == call(
            "install",
            str(pipeline_path_with_pyproject),
        )

        # Assert - Verify logging behavior
        log_records = caplog.records
        assert len(log_records) == 4, f"Expected exactly 4 log messages, got {len(log_records)}"

        # Verify start installation log (note: source code always logs requirements.txt path)
        start_record = log_records[0]
        assert start_record.levelname == "INFO", "First message should be INFO level"
        assert "Started installing pipeline dependencies from" in start_record.message
        assert str(pipeline_path_with_pyproject / "requirements.txt") in start_record.message

        # Verify executor output is logged as debug
        output_record = log_records[1]
        assert output_record.levelname == "DEBUG", "Second message should be DEBUG level"
        assert output_record.message == output_message, "Second message should contain executor output"

        # Verify executor error is logged as warning
        error_record = log_records[2]
        assert error_record.levelname == "WARNING", "Third message should be WARNING level"
        assert error_record.message == error_message, "Third message should contain executor error"

        # Verify completion log
        completion_record = log_records[3]
        assert completion_record.levelname == "INFO", "Fourth message should be INFO level"
        assert (
            completion_record.message == "Pipeline dependencies installed"
        ), "Fourth message should confirm completion"

    @pytest.mark.unit
    def test_installer_missing_dependency_files_error(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test error when no dependency files (requirements.txt or pyproject.toml) are present.

        Verifies that InstallError is raised with specific error message when pipeline
        has no dependency specification files. The exception should be chained since
        the __call__ method wraps _install() and re-raises as InstallError.
        """
        # Arrange
        logger = logging.getLogger("test")
        mock_executor = mocker.Mock(return_value=ExecutorResult("", ""))
        installer = PipelineInstaller(tmp_path, logger, mock_executor)

        expected_error_message = (
            f"Pipeline does not defines dependencies: {tmp_path / 'requirements.txt'} / {tmp_path / 'pyproject.toml'}"
        )

        # Act & Assert
        with pytest.raises(PipelineInstaller.InstallError) as exc_info:
            installer()

        # Verify the exception has the correct chaining structure
        # The outer exception is the re-raised InstallError from __call__
        # The inner exception (cause) is the original InstallError from _install()
        assert exc_info.value.__cause__ is not None, "Exception should have a __cause__ attribute"
        assert isinstance(exc_info.value.__cause__, PipelineInstaller.InstallError), "Cause should be InstallError"
        assert (
            str(exc_info.value.__cause__) == expected_error_message
        ), f"Expected '{expected_error_message}', got '{exc_info.value.__cause__}'"

        # Verify executor was not called since no dependency files were found
        mock_executor.assert_not_called()

    @pytest.mark.unit
    def test_installer_executor_error(
        self,
        pipeline_path_with_requirements: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test error handling when UV executor raises UvError.

        Verifies that UvError exceptions are properly caught and re-raised as
        InstallError exceptions for consistent error handling.
        """
        # Arrange
        logger = logging.getLogger("test")
        uv_error_message = "UV installation failed"
        mock_executor = mocker.Mock(
            side_effect=[
                ExecutorResult("", ""),  # freeze: empty environment
                UvExecutor.UvError(uv_error_message),  # install fails
            ],
        )
        installer = PipelineInstaller(pipeline_path_with_requirements, logger, mock_executor)

        # Act & Assert
        with pytest.raises(PipelineInstaller.InstallError) as exc_info:
            installer()

        # Verify the original UvError is chained as the cause
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, UvExecutor.UvError)
        assert str(exc_info.value.__cause__) == uv_error_message

        # Verify freeze was called first, then install was attempted
        assert mock_executor.call_count == 2
        assert mock_executor.call_args_list[0] == call("freeze")
        assert mock_executor.call_args_list[1] == call(
            "install",
            "-r",
            str(pipeline_path_with_requirements / "requirements.txt"),
        )

    @pytest.mark.unit
    def test_installer_logs_output_and_errors(
        self,
        pipeline_path_with_requirements: Path,
        mocker: pytest_mock.MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that installer properly logs output and error messages from executor.

        Verifies that successful execution results in info/debug/warning logs with
        correct message content and levels from the UV executor output.
        """
        # Arrange
        output_message = "Package installed successfully"
        error_message = "Warning: deprecated dependency"
        mock_executor = mocker.Mock(
            side_effect=[
                ExecutorResult("", ""),  # freeze: empty environment
                ExecutorResult(output_message, error_message),  # install
            ],
        )

        # Use a logger that will be captured by caplog
        logger = logging.getLogger("marimba.test")
        installer = PipelineInstaller(pipeline_path_with_requirements, logger, mock_executor)

        with caplog.at_level(logging.DEBUG, logger="marimba.test"):
            # Act
            installer()

        # Assert
        # Verify logging behavior
        log_records = caplog.records
        assert len(log_records) == 4, f"Expected exactly 4 log messages, got {len(log_records)}"

        # Check first info message (start installation)
        start_record = log_records[0]
        assert start_record.levelname == "INFO", "First message should be INFO level"
        assert (
            "Started installing pipeline dependencies from" in start_record.message
        ), "First message should contain start text"
        assert (
            str(pipeline_path_with_requirements / "requirements.txt") in start_record.message
        ), "First message should contain requirements path"

        # Check debug message (executor output)
        output_record = log_records[1]
        assert output_record.levelname == "DEBUG", "Second message should be DEBUG level"
        assert output_record.message == output_message, "Second message should be executor output"

        # Check warning message (executor error)
        error_record = log_records[2]
        assert error_record.levelname == "WARNING", "Third message should be WARNING level"
        assert error_record.message == error_message, "Third message should be executor error"

        # Check final info message (completion)
        completion_record = log_records[3]
        assert completion_record.levelname == "INFO", "Fourth message should be INFO level"
        assert (
            completion_record.message == "Pipeline dependencies installed"
        ), "Fourth message should be completion text"

        # Verify executor was called twice: freeze then install
        assert mock_executor.call_count == 2
        assert mock_executor.call_args_list[0] == call("freeze")
        assert mock_executor.call_args_list[1] == call(
            "install",
            "-r",
            str(pipeline_path_with_requirements / "requirements.txt"),
        )

    @pytest.mark.unit
    def test_create_factory_method(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test the create factory method creates a functional PipelineInstaller.

        Verifies that the factory method correctly instantiates PipelineInstaller
        with proper path configuration and UvExecutor integration.
        """
        # Arrange
        logger = logging.getLogger("test")
        mock_uv_executor = mocker.Mock(return_value=ExecutorResult("Installation successful", ""))
        mock_uv_executor_create = mocker.patch(
            "marimba.core.installer.pipeline_installer.UvExecutor.create",
            return_value=mock_uv_executor,
        )

        # Act
        installer = PipelineInstaller.create(tmp_path, logger)

        # Assert
        assert isinstance(installer, PipelineInstaller), "Factory method should return PipelineInstaller instance"
        assert installer.requirements_path == tmp_path / "requirements.txt", "Requirements path should be set correctly"
        assert installer.py_project_path == tmp_path / "pyproject.toml", "PyProject path should be set correctly"
        mock_uv_executor_create.assert_called_once(), "UvExecutor.create should be called exactly once"

    @pytest.mark.unit
    def test_properties(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """Test that properties return correct paths.

        Verifies that requirements_path and py_project_path properties
        return the expected file paths within the pipeline directory.
        """
        # Arrange
        logger = logging.getLogger("test")
        mock_executor = mocker.Mock(return_value=ExecutorResult("", ""))
        installer = PipelineInstaller(tmp_path, logger, mock_executor)

        # Act & Assert
        assert installer.requirements_path == tmp_path / "requirements.txt", (
            f"Expected requirements_path to be {tmp_path / 'requirements.txt'}, "
            f"but got {installer.requirements_path}"
        )
        assert (
            installer.py_project_path == tmp_path / "pyproject.toml"
        ), f"Expected py_project_path to be {tmp_path / 'pyproject.toml'}, but got {installer.py_project_path}"

    @pytest.mark.unit
    def test_installer_uses_freeze_as_constraints_to_prevent_downgrade(
        self,
        pipeline_path_with_requirements: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that existing packages are passed as constraints to prevent downgrades.

        When packages are already installed (non-empty freeze output), the installer
        must pass them as a constraints file so uv cannot select a lower compatible version.
        """
        # Arrange
        logger = logging.getLogger("test")
        frozen_packages = "ifdo-py==1.3.0\nnumpy==1.26.0\n"
        mock_executor = mocker.Mock(
            side_effect=[
                ExecutorResult(frozen_packages, ""),  # freeze: packages already installed
                ExecutorResult("", ""),  # install with constraints
            ],
        )
        installer = PipelineInstaller(pipeline_path_with_requirements, logger, mock_executor)

        # Act
        installer()

        # Assert - install was called with a -c constraints flag
        assert mock_executor.call_count == 2
        install_args = mock_executor.call_args_list[1][0]
        assert install_args[0] == "install"
        assert "-c" in install_args, "Constraints flag must be passed when packages are already installed"
        constraints_path = install_args[install_args.index("-c") + 1]
        assert constraints_path.endswith("constraints.txt"), "Constraints file should have expected name"

    @pytest.mark.unit
    def test_installer_validate_exists_error(self, tmp_path: Path) -> None:
        """Test _validate_exists static method raises error for non-existent file.

        Verifies that the _validate_exists static method correctly identifies
        and raises InstallError when a requirements file does not exist.
        """
        # Arrange
        non_existent_path = tmp_path / "non_existent_requirements.txt"

        # Act & Assert
        with pytest.raises(PipelineInstaller.InstallError) as exc_info:
            PipelineInstaller._validate_exists(non_existent_path)

        # Verify error message contains expected information
        expected_message = f"Requirements file not found: {non_existent_path}"
        assert (
            str(exc_info.value) == expected_message
        ), f"Expected exact message '{expected_message}', got '{exc_info.value}'"
