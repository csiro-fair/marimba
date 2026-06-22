import subprocess
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from marimba.core.installer.uv_executor import ExecutorResult, UvExecutor


class TestUvExecutor:
    """Test suite for UvExecutor class."""

    @pytest.mark.unit
    def test_uv_executor_call_success_returns_executor_result(self, mocker: MockerFixture) -> None:
        """Test UvExecutor successful call returns ExecutorResult with decoded output and error."""
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        expected_output = "Package list:\nrequests==2.31.0\nnumpy==1.24.0\n"
        expected_error = ""
        mock_process.communicate.return_value = (expected_output.encode(), expected_error.encode())
        mock_process.returncode = 0
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act
        result = uv_executor("list")

        # Assert
        assert isinstance(result, ExecutorResult)
        assert result.output == expected_output
        assert result.error == expected_error

        # Assert subprocess integration
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "list"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.integration
    def test_uv_executor_create_and_execute_integration(self, tmp_path: Path) -> None:
        """Test UvExecutor creation and execution integration with real uv executable.

        This integration test verifies the complete workflow of discovering uv in PATH,
        creating an executor, and executing a real uv command. Tests real component
        interactions with minimal mocking.
        """
        # Arrange - Test real uv discovery and creation
        try:
            uv_executor = UvExecutor.create()
        except UvExecutor.UvError:
            pytest.skip("uv executable not found in PATH - cannot run integration test")

        # Act - Execute a safe, environment-independent uv command that should work
        # Use 'uv pip list' which is safe and should work in any environment with uv
        result = uv_executor("list")

        # Assert - Verify successful integration between UvExecutor.create() and __call__()
        assert isinstance(result, ExecutorResult)
        assert isinstance(result.output, str)
        assert isinstance(result.error, str)

        # Assert that the command executed successfully (real uv behavior)
        # Output content varies by environment but should be valid string format
        assert result.output is not None
        assert result.error is not None

    @pytest.mark.unit
    def test_uv_executor_output(self, mocker: MockerFixture) -> None:
        """Test UvExecutor output decoding and proper ExecutorResult structure."""
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        raw_output = b"test-package==1.0.0\nanother-package==2.0.0\n"
        raw_error = b"Warning: deprecated option\n"
        mock_process.communicate.return_value = (raw_output, raw_error)
        mock_process.returncode = 0
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act
        result = uv_executor("list", "--format", "freeze")

        # Assert
        assert isinstance(result, ExecutorResult)
        assert result.output == "test-package==1.0.0\nanother-package==2.0.0\n"
        assert result.error == "Warning: deprecated option\n"

        # Assert subprocess integration
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "list", "--format", "freeze"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.unit
    def test_uv_executor_command_construction_with_multiple_args(self, mocker: MockerFixture) -> None:
        """Test UvExecutor correctly constructs subprocess commands with multiple arguments.

        This test verifies that UvExecutor properly builds the command list when given
        multiple arguments, ensuring the uv path, 'pip' subcommand, and all provided
        arguments are correctly assembled for subprocess execution.
        """
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act
        result = uv_executor("install", "--requirement", "requirements.txt", "--quiet")

        # Assert - Verify command construction and execution
        assert isinstance(result, ExecutorResult)
        assert result.output == "success output"
        assert result.error == ""

        # Assert subprocess called with correctly constructed command
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "install", "--requirement", "requirements.txt", "--quiet"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.integration
    def test_uv_executor_pipeline_directory_integration(self, tmp_path: Path) -> None:
        """Test UvExecutor integration with pipeline directory structure operations.

        This integration test verifies UvExecutor component interactions with real
        directory structures and file operations, testing the integration between
        UvExecutor creation, directory setup, and command execution with minimal mocking.
        """
        # Arrange - Set up pipeline directory structure for integration testing
        try:
            uv_executor = UvExecutor.create()
        except UvExecutor.UvError:
            pytest.skip("uv executable not found in PATH - cannot run integration test")

        project_root = tmp_path / "marimba_project"
        project_root.mkdir()
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        test_pipeline_dir = pipelines_dir / "test_pipeline"
        test_pipeline_dir.mkdir()

        requirements_file = test_pipeline_dir / "requirements.txt"
        requirements_file.write_text("# Test requirements file\n")

        pipeline_yml = test_pipeline_dir / "pipeline.yml"
        pipeline_yml.write_text(
            "name: test_pipeline\nversion: 1.0.0\ndescription: Integration test pipeline\n",
        )

        # Act - Execute UvExecutor operations in context of pipeline directory
        list_result = uv_executor("list")
        help_result = uv_executor("--help")

        # Assert - Verify integration between UvExecutor and pipeline directory operations
        assert isinstance(list_result, ExecutorResult)
        assert isinstance(list_result.output, str)
        assert isinstance(list_result.error, str)

        assert isinstance(help_result, ExecutorResult)
        assert isinstance(help_result.output, str)
        assert "usage" in help_result.output.lower() or "uv" in help_result.output.lower()

        # Verify pipeline directory structure remains intact after UvExecutor operations
        assert requirements_file.exists()
        assert pipeline_yml.exists()
        assert test_pipeline_dir.is_dir()

        # Verify UvExecutor operations completed without exceptions
        assert list_result.output is not None
        assert help_result.output is not None

    @pytest.mark.unit
    def test_uv_executor_invalid_package_install_raises_uv_error(self, mocker: MockerFixture) -> None:
        """Test UvExecutor raises UvError when attempting to install invalid package."""
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        invalid_package = "nonexistent-package-xyz123"
        expected_error_output = f"Error: package '{invalid_package}' not found"
        mock_process.communicate.return_value = (b"", expected_error_output.encode())
        mock_process.returncode = 1
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act & Assert
        with pytest.raises(UvExecutor.UvError, match=r"uv pip command failed \(return code 1\)"):
            uv_executor("install", invalid_package)

        # Assert subprocess integration
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "install", invalid_package],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.unit
    def test_create_uv_not_found_raises_uv_error(self, mocker: MockerFixture) -> None:
        """Test UvExecutor.create() raises UvError when uv executable not found in PATH.

        This test verifies that the factory method properly handles the case where
        the uv executable is not available in the system PATH, ensuring appropriate
        error handling for missing dependencies.
        """
        # Arrange
        mock_which = mocker.patch("shutil.which")
        mock_which.return_value = None

        # Act & Assert
        with pytest.raises(UvExecutor.UvError, match="uv executable not found in PATH"):
            UvExecutor.create()

        # Assert mock was called correctly
        mock_which.assert_called_once_with("uv")

    @pytest.mark.unit
    def test_uv_executor_successful_command_with_stderr_warnings(self, mocker: MockerFixture) -> None:
        """Test UvExecutor properly handles successful commands that produce stderr warnings.

        This test specifically verifies that when a uv command succeeds (returncode=0) but
        produces warning messages on stderr, both stdout content and stderr warnings are
        correctly decoded and returned in the ExecutorResult without raising exceptions.
        This scenario commonly occurs with deprecated options or configuration warnings.
        """
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        expected_stdout = "Successfully installed package-1.0.0\n"
        expected_stderr = "WARNING: package-1.0.0 contains deprecated features\nWARNING: Consider upgrading\n"
        mock_process.communicate.return_value = (expected_stdout.encode(), expected_stderr.encode())
        mock_process.returncode = 0  # Success despite warnings
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act
        result = uv_executor("install", "package-1.0.0")

        # Assert - Verify successful execution with warnings doesn't raise exceptions
        assert isinstance(result, ExecutorResult)
        assert result.output == expected_stdout
        assert result.error == expected_stderr

        # Assert - Verify both output and error fields contain properly decoded strings
        assert isinstance(result.output, str)
        assert isinstance(result.error, str)
        assert len(result.error) > 0

        # Assert - Verify subprocess integration was called correctly
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "install", "package-1.0.0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.unit
    def test_uv_executor_empty_output_returns_empty_strings(self, mocker: MockerFixture) -> None:
        """Test UvExecutor correctly handles subprocess returning empty byte strings.

        This test verifies that when the underlying subprocess returns empty byte strings
        for both stdout and stderr (b"", b""), the UvExecutor properly decodes them to
        empty string objects and constructs a valid ExecutorResult. This scenario can
        occur when uv commands execute successfully but produce no output (e.g., some
        configuration commands or when no packages are installed).
        """
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        mock_process.communicate.return_value = (b"", b"")
        mock_process.returncode = 0
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act
        result = uv_executor("list")

        # Assert - Verify ExecutorResult structure and empty string decoding
        assert isinstance(result, ExecutorResult)
        assert result.output == ""
        assert result.error == ""
        assert isinstance(result.output, str)
        assert isinstance(result.error, str)

        # Assert - Verify subprocess was called with correct parameters
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", "list"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()

    @pytest.mark.unit
    def test_uv_executor_invalid_command_raises_uv_error(self, mocker: MockerFixture) -> None:
        """Test UvExecutor raises UvError when subprocess execution fails with invalid command.

        This test verifies that UvExecutor properly handles and raises UvError when the
        underlying uv pip subprocess returns a non-zero exit code, specifically testing
        the error handling for invalid commands and flags.
        """
        # Arrange
        uv_executor = UvExecutor("/fake/uv/path")
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.MagicMock()
        invalid_command = "invalid-command"
        invalid_flag = "--bad-flag"
        expected_error_output = f"Error: unknown command '{invalid_command}'"
        mock_process.communicate.return_value = (b"", expected_error_output.encode())
        mock_process.returncode = 2
        mock_popen.return_value.__enter__.return_value = mock_process

        # Act & Assert - Verify UvError is raised with specific message
        with pytest.raises(UvExecutor.UvError, match=r"uv pip command failed \(return code 2\)"):
            uv_executor(invalid_command, invalid_flag)

        # Assert - Verify subprocess was called with correct parameters
        mock_popen.assert_called_once_with(
            ["/fake/uv/path", "pip", invalid_command, invalid_flag],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_process.communicate.assert_called_once()
