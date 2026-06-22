"""
End-to-End tests for project lifecycle operations.

These tests validate complete project workflows from CLI creation through
directory structure verification and error handling scenarios.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import (
    assert_cli_failure,
    assert_cli_success,
    assert_project_structure_complete,
)


@pytest.mark.e2e
class TestProjectLifecycle:
    """Test complete project lifecycle workflows from CLI commands to filesystem changes."""

    def test_new_project_workflow(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """
        Test end-to-end project creation workflow.

        Validates that 'marimba new project' command successfully creates a complete
        project directory structure with all required subdirectories and configuration.
        This test verifies the core project initialization functionality.
        """
        # Arrange
        temp_project_dir = tmp_path / "test_project"

        # Act
        result = cli_runner.invoke(app, ["new", "project", str(temp_project_dir)])

        # Assert
        assert_cli_success(result, context="Project creation workflow")

        # Verify complete project structure was created
        assert_project_structure_complete(temp_project_dir, "New project creation")

        # Verify specific directory writability (critical for subsequent operations)
        test_file = temp_project_dir / "collections" / "test_write.tmp"
        test_file.write_text("test")
        assert test_file.exists(), "Should be able to write to collections directory"
        test_file.unlink()

    def test_project_creation_existing_directory_fails(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """
        Test that project creation fails appropriately when target directory already exists.

        Validates error handling when attempting to create a project in an existing
        directory, ensuring the CLI provides clear feedback and doesn't corrupt existing data.
        """
        # Arrange
        existing_dir = tmp_path / "existing_project"
        existing_dir.mkdir()

        # Act
        result = cli_runner.invoke(app, ["new", "project", str(existing_dir)])

        # Assert
        assert_cli_failure(result, context="Project creation in existing directory")

        # Verify specific error message is displayed
        expected_error_message = "A Marimba project already exists"
        assert (
            expected_error_message in result.output
        ), f"Expected error message '{expected_error_message}' not found in output:\n{result.output}"

        # Verify no modifications to existing directory
        assert existing_dir.exists(), "Existing directory should remain unchanged"
        assert existing_dir.is_dir(), "Existing directory should still be a directory"

        # Verify no project structure was created
        marimba_dir = existing_dir / ".marimba"
        assert not marimba_dir.exists(), "No .marimba directory should be created in existing directory"

    def test_project_operation_nonexistent_project_fails(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """
        Test error handling for operations on non-existent projects.

        Validates that CLI commands provide appropriate error messages and exit codes
        when attempting operations on projects that don't exist, ensuring graceful
        degradation and helpful user feedback.
        """
        # Arrange
        valid_project_dir = tmp_path / "valid_project"
        nonexistent_project_dir = tmp_path / "nonexistent_project"

        # Create a valid project first for comparison
        create_result = cli_runner.invoke(app, ["new", "project", str(valid_project_dir)])
        assert_cli_success(create_result, context="Setup for error handling test")

        # Act
        result = cli_runner.invoke(
            app,
            ["delete", "collection", "test_collection", "--project-dir", str(nonexistent_project_dir)],
        )

        # Assert
        assert_cli_failure(result, context="Operation on non-existent project")

        # Verify specific error message for project not found
        assert "Could not find a" in result.output, f"Expected 'Could not find a' in output:\n{result.output}"
        assert "Marimba" in result.output, f"Expected 'Marimba' in output:\n{result.output}"
        assert "project" in result.output, f"Expected 'project' in output:\n{result.output}"

        # Verify no side effects
        assert_project_structure_complete(valid_project_dir, "Valid project should remain intact")
        assert not nonexistent_project_dir.exists(), "Nonexistent project should not be created"

    def test_project_creation_invalid_path_fails(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """
        Test project creation failure with invalid filesystem paths.

        Validates error handling when provided with invalid or inaccessible paths,
        ensuring the CLI fails gracefully with appropriate error messages.
        This test verifies behavior when attempting to create a project directory
        under a file path (filesystem constraint violation).
        """
        # Arrange
        file_path = tmp_path / "not_a_directory.txt"
        file_path.write_text("This is a file")
        invalid_project_path = file_path / "project"

        # Act
        result = cli_runner.invoke(app, ["new", "project", str(invalid_project_path)])

        # Assert
        assert_cli_failure(result, context="Project creation with invalid path")

        # Verify CLI failed due to filesystem constraints
        assert result.exception is not None, "CLI should raise exception for invalid path"

        # Verify specific filesystem error type
        assert isinstance(
            result.exception,
            (NotADirectoryError, OSError, FileNotFoundError, PermissionError),
        ), (
            f"Expected filesystem-related error (NotADirectoryError, OSError, FileNotFoundError, or PermissionError), "
            f"but got {type(result.exception).__name__}: {result.exception}"
        )

        # Verify no side effects
        assert not invalid_project_path.exists(), "No project directory should be created"
        assert file_path.exists(), "Parent file should still exist"
        assert file_path.is_file(), "Parent should still be a file"
        assert file_path.read_text() == "This is a file", "Parent file content should be unchanged"
