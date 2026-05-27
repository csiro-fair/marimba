"""Tests for marimba.core.utils.paths module."""

import os
from os import R_OK
from pathlib import Path

import pytest
import typer
from pytest_mock import MockerFixture

from marimba.core.utils.paths import (
    detect_hardlinked_files,
    detect_readonly_files,
    find_project_dir,
    find_project_dir_or_exit,
    format_path_for_logging,
    hardlink_path,
    remove_directory_tree,
)


class TestFindProjectDir:
    """Test find_project_dir function."""

    @pytest.fixture
    def temp_project_structure(self, tmp_path):
        """Create temporary project structure."""
        # Create project structure
        project_root = tmp_path / "project_root"
        project_root.mkdir()
        (project_root / ".marimba").mkdir()

        # Create subdirectory
        subdir = project_root / "subdir"
        subdir.mkdir()

        return project_root, subdir

    @pytest.mark.unit
    def test_find_project_dir_from_root(self, temp_project_structure):
        """Test finding project directory when called directly from project root.

        This unit test verifies that when find_project_dir is called with the
        project root directory path (containing a .marimba marker directory),
        it correctly identifies and returns the same directory as the project root.
        This ensures proper project detection when starting from the root location.
        """
        # Arrange
        project_root, _ = temp_project_structure

        # Act
        result = find_project_dir(project_root)

        # Assert
        assert result == project_root, f"Expected project root {project_root}, but got {result}"
        assert (result / ".marimba").is_dir(), "Returned project should contain .marimba directory"

    @pytest.mark.unit
    def test_find_project_dir_from_subdirectory(self, temp_project_structure):
        """Test finding project directory when called from a subdirectory.

        This unit test verifies that when find_project_dir is called with a subdirectory
        path inside a Marimba project, it correctly traverses up the directory tree to
        find and return the project root directory (containing the .marimba marker).
        This ensures proper project detection when starting from nested directories.
        """
        # Arrange
        project_root, subdir = temp_project_structure

        # Act
        result = find_project_dir(subdir)

        # Assert
        assert result == project_root, f"Expected project root {project_root}, but got {result}"
        assert (result / ".marimba").is_dir(), "Returned project should contain .marimba directory"
        assert result.is_dir(), f"Returned project root {result} should be a directory"

    @pytest.mark.unit
    def test_find_project_dir_string_path(self, temp_project_structure):
        """Test finding project directory when called with string path instead of Path object.

        This unit test verifies that find_project_dir correctly handles string paths
        by converting them internally to Path objects and successfully traversing up
        the directory tree to locate the project root. This ensures the function
        provides consistent behavior regardless of whether the input is a string or
        Path object, supporting both usage patterns in the codebase.
        """
        # Arrange
        project_root, subdir = temp_project_structure
        string_path = str(subdir)

        # Act
        result = find_project_dir(string_path)

        # Assert
        assert result == project_root, f"Expected project root {project_root}, but got {result}"
        assert (result / ".marimba").is_dir(), "Returned project should contain .marimba directory"
        assert isinstance(string_path, str), "Test validation: input should be string type"
        assert isinstance(result, Path), "Function should return Path object even with string input"
        assert result.is_dir(), f"Returned project root {result} should be a directory"

    @pytest.mark.unit
    def test_find_project_dir_non_existent_path(self, tmp_path: Path) -> None:
        """Test finding project directory when path does not exist.

        This test verifies that find_project_dir returns None when called
        with a path that doesn't exist on the filesystem, ensuring robust
        error handling for invalid paths.
        """
        # Arrange
        non_existent_path = tmp_path / "completely_non_existent_directory"

        # Act
        result = find_project_dir(non_existent_path)

        # Assert
        assert result is None, f"Expected None when path doesn't exist, but got {result}"
        assert not non_existent_path.exists(), f"Test setup: path {non_existent_path} should not exist on filesystem"

    @pytest.mark.unit
    def test_find_project_dir_not_found(self, tmp_path: Path) -> None:
        """Test finding project directory when not found in existing directory.

        This test verifies that find_project_dir returns None when called
        with a directory that exists but contains no .marimba marker directory,
        ensuring proper project detection logic.
        """
        # Arrange
        non_project = tmp_path / "not_a_project"
        non_project.mkdir()

        # Act
        result = find_project_dir(non_project)

        # Assert
        assert result is None, "Should return None when directory exists but is not a project"
        assert non_project.exists(), "Test setup: directory should exist on filesystem"
        assert not (non_project / ".marimba").exists(), "Test validation: directory should not contain .marimba marker"

    @pytest.mark.unit
    def test_find_project_dir_no_read_access(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test finding project directory when os.access denies read permission.

        This unit test verifies that when os.access returns False for a directory
        (simulating no read permission), the find_project_dir function correctly
        handles this condition by returning None instead of attempting to traverse
        the directory tree. This ensures robust permission handling in environments
        with restricted filesystem access.
        """
        # Arrange - Create a real directory structure but mock permission check
        test_dir = tmp_path / "restricted_dir"
        test_dir.mkdir()
        (test_dir / ".marimba").mkdir()  # Create what would be a valid project structure

        # Verify test setup: directory structure exists and would normally be valid
        assert test_dir.exists(), "Test setup: directory should exist"
        assert (test_dir / ".marimba").is_dir(), "Test setup: .marimba directory should exist"

        # Mock access function in the paths module to simulate permission denied
        mock_access = mocker.patch("marimba.core.utils.paths.access", return_value=False)

        # Act - Attempt to find project directory with simulated permission restriction
        result = find_project_dir(test_dir)

        # Assert - Function should return None due to access restriction
        assert result is None, "Should return None when access function denies read permission"

        # Verify access was called with correct parameters
        mock_access.assert_called_with(test_dir, R_OK), "Should check read access using access function with R_OK flag"

    @pytest.mark.unit
    def test_find_project_dir_marimba_is_file(self, tmp_path: Path) -> None:
        """Test finding project directory when .marimba exists as a file instead of directory.

        This unit test verifies that when find_project_dir encounters a .marimba
        file (instead of the expected .marimba directory), it correctly returns None
        since the project structure is invalid. This ensures robust validation of
        the required .marimba directory marker for Marimba projects.
        """
        # Arrange
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        marimba_file = project_dir / ".marimba"
        marimba_file.touch()  # Create as file, not directory

        # Act
        result = find_project_dir(project_dir)

        # Assert
        assert result is None, "Should return None when .marimba exists as a file instead of directory"
        assert marimba_file.is_file(), "Test setup: .marimba should exist as a file"
        assert not marimba_file.is_dir(), "Test validation: .marimba should not be a directory"


class TestFindProjectDirOrExit:
    """Test find_project_dir_or_exit function."""

    @pytest.fixture
    def temp_project(self, tmp_path):
        """Create temporary project."""
        project_root = tmp_path / "test_project"
        project_root.mkdir()
        (project_root / ".marimba").mkdir()
        return project_root

    @pytest.mark.unit
    def test_find_project_dir_or_exit_with_valid_dir(self, temp_project: Path) -> None:
        """Test find_project_dir_or_exit with valid project directory returns the same directory.

        This unit test verifies that when find_project_dir_or_exit is called with
        a valid project directory (containing a .marimba subdirectory), it returns
        the same directory path without modification or error. This ensures the
        function correctly identifies and returns valid project directories.
        """
        # Arrange
        project_path = temp_project

        # Act
        result = find_project_dir_or_exit(project_path)

        # Assert
        assert result == project_path, f"Expected {project_path}, but got {result}"
        assert (result / ".marimba").is_dir(), "Returned project should contain .marimba directory"

    @pytest.mark.integration
    def test_find_project_dir_or_exit_with_none_uses_cwd(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """
        Test find_project_dir_or_exit with None uses current working directory to find project.

        This integration test verifies that when no project_dir is provided (None),
        the function uses the current working directory as the starting point for
        project directory search and successfully locates a valid Marimba project.
        Tests the integration between find_project_dir_or_exit and find_project_dir
        with real filesystem operations and minimal mocking.
        """
        # Arrange - Create a real project structure in tmp_path
        project_root = tmp_path / "test_project"
        project_root.mkdir()
        marimba_dir = project_root / ".marimba"
        marimba_dir.mkdir()

        # Create a subdirectory to work from (simulating being in subdirectory)
        work_dir = project_root / "subdir"
        work_dir.mkdir()

        # Mock only the external dependency (current working directory)
        # but test real project directory finding logic
        mock_cwd = mocker.patch("marimba.core.utils.paths.Path.cwd")
        mock_cwd.return_value = work_dir

        # Act - Test real functionality with minimal mocking
        result = find_project_dir_or_exit(None)

        # Assert - Verify real behavior and outcomes
        assert (
            result == project_root
        ), f"Should find project root {project_root} from subdirectory {work_dir}, got {result}"
        mock_cwd.assert_called_once(), "Should call Path.cwd() exactly once when project_dir is None"

        # Verify the project structure exists as expected and is correct
        assert (result / ".marimba").is_dir(), f"Found project {result} should have .marimba directory"
        assert result.is_dir(), f"Found project root {result} should be a directory"

    @pytest.mark.unit
    def test_find_project_dir_or_exit_not_found_exits(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Test find_project_dir_or_exit exits when project not found in directory tree.

        This unit test verifies that when find_project_dir_or_exit is called
        with a directory that exists but contains no .marimba marker directory anywhere
        in its parent tree, it correctly exits with code 1 and displays an appropriate
        error message. This ensures proper error handling when no Marimba project
        can be located from the given starting point.
        """
        # Arrange - Create a directory structure without any .marimba marker
        non_project_dir = tmp_path / "not_a_project" / "subdir"
        non_project_dir.mkdir(parents=True)

        # Act & Assert - Test real functionality with minimal mocking
        with pytest.raises(typer.Exit) as exc_info:
            find_project_dir_or_exit(non_project_dir)

        # Assert - Verify exact exit code and error message behavior
        assert exc_info.value.exit_code == 1, "Should exit with error code 1 when project not found"

        # Verify error message was printed to stdout (error_panel output)
        captured = capsys.readouterr()
        assert "Could not find a" in captured.out, "Should display error message about project not found"
        assert "project" in captured.out.lower(), "Error message should mention 'project'"

        # Verify test directory structure is as expected (no .marimba anywhere)
        current_dir = non_project_dir
        while current_dir != current_dir.parent:
            assert not (current_dir / ".marimba").exists(), f"Should not find .marimba in {current_dir}"
            current_dir = current_dir.parent


class TestRemoveDirectoryTree:
    """Test remove_directory_tree function."""

    @pytest.mark.unit
    def test_remove_directory_tree_dry_run(self, tmp_path: Path) -> None:
        """Test remove_directory_tree with dry_run=True preserves directory structure.

        This unit test verifies that when remove_directory_tree is called with dry_run=True,
        it executes without raising errors but doesn't actually delete the directory or its
        contents. This ensures the dry-run functionality provides preview capability without
        modifying the filesystem, which is essential for user safety when previewing operations.
        """
        # Arrange - Create nested directory structure with multiple files
        test_directory = tmp_path / "test_directory"
        test_directory.mkdir()
        test_file = test_directory / "test_file.txt"
        test_file.write_text("test content")

        # Create subdirectory with nested file to verify complete structure preservation
        subdir = test_directory / "subdir"
        subdir.mkdir()
        nested_file = subdir / "nested.txt"
        nested_file.write_text("nested content")

        # Act - Execute dry-run operation
        remove_directory_tree(test_directory, "test entity", dry_run=True)

        # Assert - Verify complete directory structure and contents remain unchanged
        assert test_directory.exists(), "Root directory should exist after dry-run"
        assert test_directory.is_dir(), "Root directory should remain a directory after dry-run"
        assert test_file.exists(), "Test file should exist after dry-run"
        assert test_file.read_text() == "test content", "Test file content should remain unchanged after dry-run"
        assert subdir.exists(), "Subdirectory should exist after dry-run"
        assert subdir.is_dir(), "Subdirectory should remain a directory after dry-run"
        assert nested_file.exists(), "Nested file should exist after dry-run"
        assert nested_file.read_text() == "nested content", "Nested file content should remain unchanged after dry-run"

    @pytest.mark.unit
    def test_remove_directory_tree_actual_deletion(self, tmp_path: Path) -> None:
        """Test remove_directory_tree with actual deletion removes directory and contents.

        This unit test verifies that when remove_directory_tree is called with dry_run=False,
        it successfully removes the target directory and all its contents from the filesystem.
        This tests the real behavior of the function rather than mocked interactions, ensuring
        the directory deletion operation works correctly in practice.
        """
        # Arrange
        test_directory = tmp_path / "test_directory"
        test_directory.mkdir()

        # Create files and subdirectories to verify complete deletion
        test_file = test_directory / "test_file.txt"
        test_file.write_text("test content")

        test_subdir = test_directory / "subdir"
        test_subdir.mkdir()
        nested_file = test_subdir / "nested.txt"
        nested_file.write_text("nested content")

        # Verify setup: directory and contents exist before deletion
        assert test_directory.exists(), "Test setup: directory should exist before deletion"
        assert test_file.exists(), "Test setup: file should exist before deletion"
        assert test_subdir.exists(), "Test setup: subdirectory should exist before deletion"
        assert nested_file.exists(), "Test setup: nested file should exist before deletion"

        # Act
        remove_directory_tree(test_directory, "test entity", dry_run=False)

        # Assert - Verify complete removal of directory and all contents
        assert not test_directory.exists(), "Directory should be completely removed after deletion"
        assert not test_file.exists(), "Files within directory should be removed after deletion"
        assert not test_subdir.exists(), "Subdirectories should be removed after deletion"
        assert not nested_file.exists(), "Nested files should be removed after deletion"

    @pytest.mark.unit
    def test_remove_directory_tree_invalid_directory(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Test remove_directory_tree with invalid directory exits with error.

        This unit test verifies that when remove_directory_tree is called with a path
        that doesn't exist or isn't a directory, it correctly raises typer.Exit with
        exit code 1 and displays an appropriate error message. This ensures proper
        input validation and prevents attempts to remove non-existent or invalid
        directory paths.
        """
        # Arrange
        non_existent_dir = tmp_path / "non_existent_directory"

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            remove_directory_tree(non_existent_dir, "test entity", dry_run=False)

        # Assert specific exit code
        assert exc_info.value.exit_code == 1, "Should exit with code 1 when directory doesn't exist"

        # Assert error message was displayed to user. On macOS CI the rendered
        # path wraps mid-word inside the Rich logger panel (timestamp+level
        # prefix eats ~36 cols, leaving little room on an 80-col display), so
        # assert on a substring short enough to survive any wrap point.
        captured = capsys.readouterr()
        assert "Invalid directory:" in captured.out, "Should display error message about invalid directory"
        assert "non_exi" in captured.out, "Error message should include the directory name"

        # Verify test preconditions
        assert not non_existent_dir.exists(), "Test setup: directory should not exist"

    @pytest.mark.unit
    def test_remove_directory_tree_deletion_error(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test remove_directory_tree handles filesystem errors during deletion.

        This unit test verifies that when shutil.rmtree raises an OSError during directory
        deletion (simulating permission denied or disk full scenarios), the remove_directory_tree
        function correctly handles the exception by raising typer.Exit with exit code 1 and
        displaying an appropriate error message. This ensures robust error handling for
        filesystem operations that may fail due to system-level issues.
        """
        # Arrange - Create a valid directory and mock the deletion operation to fail
        test_directory = tmp_path / "test_directory"
        test_directory.mkdir()

        error_message = "Permission denied"
        mock_rmtree = mocker.patch("marimba.core.utils.paths.shutil.rmtree")
        mock_rmtree.side_effect = OSError(error_message)

        # Act & Assert - Function should raise typer.Exit due to deletion failure
        with pytest.raises(typer.Exit) as exc_info:
            remove_directory_tree(test_directory, "test entity", dry_run=False)

        # Assert - Verify proper error handling and exit behavior
        assert exc_info.value.exit_code == 1, "Should exit with code 1 when filesystem deletion fails"

        # Verify shutil.rmtree was called with the correct directory
        mock_rmtree.assert_called_once_with(test_directory), "Should attempt to delete the specified directory"

        # Verify error message was displayed to user (test real behavior, not implementation details)
        captured = capsys.readouterr()
        assert "Error occurred while deleting" in captured.out, "Should display error message to user"
        assert "Permission denied" in captured.out, "Error message should include the specific error details"

        # Verify the directory still exists after failed deletion attempt
        assert test_directory.exists(), "Directory should still exist after failed deletion"


class TestHardlinkPath:
    """Test hardlink_path function."""

    @pytest.mark.unit
    def test_hardlink_path_invalid_source(self, tmp_path: Path) -> None:
        """Test hardlink_path with invalid source directory exits with error.

        This unit test verifies that when hardlink_path is called with a source path
        that doesn't exist on the filesystem, it correctly raises typer.Exit with
        exit code 1. This ensures robust error handling for invalid source paths,
        preventing attempts to create hard links from non-existent directories.
        """
        # Arrange
        non_existent_source = tmp_path / "non_existent_directory"
        destination_dir = tmp_path / "destination"

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            hardlink_path(non_existent_source, destination_dir, dry_run=False)

        # Assert specific exit code
        assert exc_info.value.exit_code == 1, "Should exit with code 1 when source directory doesn't exist"

        # Verify test preconditions
        assert not non_existent_source.exists(), "Test setup: source directory should not exist"

    @pytest.mark.unit
    def test_hardlink_path_source_is_file(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Test hardlink_path when source is a file instead of directory exits with error.

        This unit test verifies that when hardlink_path is called with a source path
        that exists as a file instead of the expected directory, it correctly raises
        typer.Exit with exit code 1 and logs an appropriate error message. This ensures
        proper input validation, as the function expects a directory to traverse for
        creating hard links of its contents.
        """
        # Arrange
        source_file = tmp_path / "source_file.txt"
        source_file.touch()
        destination_dir = tmp_path / "destination"

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            hardlink_path(source_file, destination_dir, dry_run=False)

        # Assert specific exit code and error behavior
        assert exc_info.value.exit_code == 1, "Should exit with code 1 when source is a file instead of directory"

        # Verify test preconditions
        assert source_file.exists(), "Test setup: source file should exist"
        assert source_file.is_file(), "Test setup: source should be a file, not directory"
        assert not source_file.is_dir(), "Test setup: source should not be a directory"

    @pytest.mark.unit
    def test_hardlink_path_dry_run(self, tmp_path: Path) -> None:
        """Test hardlink_path with dry_run=True creates directory structure without actual hard links.

        This unit test verifies that when hardlink_path is called with dry_run=True,
        it creates the necessary destination directory structure but does not actually
        create any hard link files. This ensures the dry-run mode provides preview
        functionality without modifying the filesystem beyond directory creation,
        which is essential for user safety when previewing operations.
        """
        # Arrange - Create source directory structure with files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_file1 = src_dir / "file1.txt"
        src_file1.write_text("content1")

        src_subdir = src_dir / "subdir"
        src_subdir.mkdir()
        src_file2 = src_subdir / "file2.txt"
        src_file2.write_text("content2")

        dest_dir = tmp_path / "dest"

        # Act - Execute hardlink_path in dry-run mode
        hardlink_path(src_dir, dest_dir, dry_run=True)

        # Assert - Verify dry-run behavior: directory structure created but no files linked
        assert dest_dir.exists(), "Destination directory should be created during dry-run"
        assert dest_dir.is_dir(), "Destination should be a directory"

        # Verify directory structure is created but no actual files were hard linked
        dest_subdir = dest_dir / "subdir"
        dest_file1 = dest_dir / "file1.txt"
        dest_file2 = dest_subdir / "file2.txt"

        assert dest_subdir.exists(), "Dry-run should create necessary directory structure"
        assert dest_subdir.is_dir(), "Created subdirectory should be a directory"
        assert not dest_file1.exists(), "Dry-run should not create actual hard linked files"
        assert not dest_file2.exists(), "Dry-run should not create hard linked files in subdirectories"

        # Verify source files remain unchanged with single hard link count
        src_stat1 = src_file1.stat()
        src_stat2 = src_file2.stat()
        assert src_stat1.st_nlink == 1, "Source file should maintain single hard link count during dry-run"
        assert src_stat2.st_nlink == 1, "Source nested file should maintain single hard link count during dry-run"

    @pytest.mark.unit
    def test_hardlink_path_actual_linking(self, tmp_path: Path) -> None:
        """Test hardlink_path creates actual hard links preserving directory structure.

        This unit test verifies that hardlink_path correctly creates hard links for files
        in a directory structure, preserving the directory layout and ensuring that the
        destination files are actual hard links (same inode, increased st_nlink count)
        rather than copies. This ensures storage efficiency and data consistency.
        """
        # Arrange - Create source structure with nested directories
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_file = src_dir / "file1.txt"
        src_file.write_text("test content")

        # Create nested subdirectory with another file
        subdir = src_dir / "subdir"
        subdir.mkdir()
        nested_file = subdir / "nested.txt"
        nested_file.write_text("nested content")

        dest_dir = tmp_path / "dest"

        # Act - Create hard links
        hardlink_path(src_dir, dest_dir, dry_run=False)

        # Assert - Verify hard links were created correctly
        dest_file = dest_dir / "file1.txt"
        dest_nested = dest_dir / "subdir" / "nested.txt"

        # Verify files exist and have correct content
        assert dest_file.exists(), "Hard linked file should exist"
        assert dest_file.read_text() == "test content", "Hard linked file should have same content"
        assert dest_nested.exists(), "Hard linked nested file should exist"
        assert dest_nested.read_text() == "nested content", "Hard linked nested file should have same content"

        # Verify directory structure is preserved
        assert (dest_dir / "subdir").is_dir(), "Directory structure should be preserved"

        # Verify actual hard linking (same inode and increased link count)
        src_stat = src_file.stat()
        dest_stat = dest_file.stat()
        assert src_stat.st_ino == dest_stat.st_ino, "Hard linked files should share the same inode"
        assert src_stat.st_nlink == 2, "Source file should have 2 hard links after linking"
        assert dest_stat.st_nlink == 2, "Destination file should have 2 hard links after linking"

        # Verify nested file hard linking
        nested_src_stat = nested_file.stat()
        nested_dest_stat = dest_nested.stat()
        assert nested_src_stat.st_ino == nested_dest_stat.st_ino, "Nested hard linked files should share the same inode"
        assert nested_src_stat.st_nlink == 2, "Nested source file should have 2 hard links after linking"

    @pytest.mark.unit
    def test_hardlink_path_linking_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test hardlink_path handles hard link creation errors gracefully.

        This unit test verifies that when Path.hardlink_to() raises an OSError during
        hard link creation, the hardlink_path function handles the exception gracefully
        by logging the error and continuing processing rather than crashing. This ensures
        robust error handling when filesystem operations fail due to permissions, disk
        space, or cross-device linking restrictions.
        """
        # Arrange
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "test_file.txt"
        source_file.write_text("test content")

        destination_dir = tmp_path / "destination"

        # Mock hardlink_to to simulate failure and logger to verify error logging
        mock_hardlink = mocker.patch.object(Path, "hardlink_to", side_effect=OSError("Hard link creation failed"))
        mock_logger = mocker.patch("marimba.core.utils.paths.logger")

        # Act - Should not raise exception, just handle error gracefully
        hardlink_path(source_dir, destination_dir, dry_run=False)

        # Assert - Verify mock was called and function completed without crashing
        mock_hardlink.assert_called_once(), "Should attempt to create hard link"
        assert destination_dir.exists(), "Destination directory should be created despite hard link failure"

        # Verify error logging behavior
        mock_logger.exception.assert_called_once(), "Should log the hard link creation error"
        logged_message = mock_logger.exception.call_args[0][0]
        assert "Failed to create hard link" in logged_message, "Error log should mention hard link creation failure"

        # Verify directory structure was created properly despite hard link failure
        expected_dest_file = destination_dir / "test_file.txt"
        assert expected_dest_file.parent.exists(), "Parent directory should be created for destination file"
        assert not expected_dest_file.exists(), "Hard link file should not exist due to simulated failure"


class TestDetectHardlinkedFiles:
    """Test detect_hardlinked_files function."""

    @pytest.mark.unit
    def test_detect_hardlinked_files_empty_list(self) -> None:
        """Test detect_hardlinked_files with empty list returns empty list.

        This test verifies that when detect_hardlinked_files is called with
        an empty list of files, it correctly returns an empty list without
        raising any exceptions, ensuring proper handling of edge cases.
        """
        # Arrange
        empty_file_list: list[Path] = []

        # Act
        result = detect_hardlinked_files(empty_file_list)

        # Assert
        assert result == [], "Should return empty list when given empty input list"

    @pytest.mark.unit
    def test_detect_hardlinked_files_non_existent_file(self, tmp_path: Path) -> None:
        """Test detect_hardlinked_files with non-existent file returns empty list.

        This test verifies that when detect_hardlinked_files is called with a path
        that doesn't exist on the filesystem, it correctly handles the error condition
        by skipping the non-existent file and returns an empty list, ensuring robust
        error handling for invalid file paths.
        """
        # Arrange
        non_existent = tmp_path / "non_existent.txt"

        # Act
        result = detect_hardlinked_files([non_existent])

        # Assert
        assert result == [], "Should return empty list when file doesn't exist on filesystem"

    @pytest.mark.unit
    def test_detect_hardlinked_files_directory_returns_empty(self, tmp_path: Path) -> None:
        """Test detect_hardlinked_files ignores directories and returns empty list.

        This test verifies that when detect_hardlinked_files is called with a directory
        instead of a file, it correctly skips the directory (since the function is
        designed to process only files, not directories) and returns an empty list,
        ensuring proper input validation and type handling.
        """
        # Arrange
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()

        # Act
        result = detect_hardlinked_files([test_dir])

        # Assert
        assert result == [], "Should return empty list when given directories instead of files"

        # Verify test setup is correct (additional validation)
        assert test_dir.exists(), "Test setup: directory should exist on filesystem"
        assert test_dir.is_dir(), "Test setup: path should be a directory, not a file"

    @pytest.mark.unit
    def test_detect_hardlinked_files_single_link(self, tmp_path: Path) -> None:
        """Test detect_hardlinked_files correctly ignores files with single link.

        This test verifies that detect_hardlinked_files correctly identifies that files
        with only one link (st_nlink == 1) are not considered hardlinked files, since
        hardlinking requires multiple references to the same inode. This ensures the
        function only reports true hardlinked files where st_nlink > 1.
        """
        # Arrange - Create a file with single link
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        # Act - Check for hardlinked files
        result = detect_hardlinked_files([test_file])

        # Assert - Single link file should not be detected as hardlinked
        assert result == [], "Files with single link should not be detected as hardlinked"

        # Verify the file actually has only one link (additional validation)
        file_stat = test_file.stat()
        assert file_stat.st_nlink == 1, "Test file should have exactly one hard link"

    @pytest.mark.unit
    def test_detect_hardlinked_files_multiple_links(self, tmp_path: Path) -> None:
        """Test detect_hardlinked_files detects files with multiple hard links.

        This test verifies that when files have multiple hard links (st_nlink > 1),
        the detect_hardlinked_files function correctly identifies all hardlinked files
        in the provided list. This is critical for the system to understand when files
        share the same underlying data on the filesystem, which affects storage
        calculations and data integrity operations.
        """
        # Arrange - Create original file and its hard link
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        hard_link = tmp_path / "hard_link.txt"
        hard_link.hardlink_to(test_file)

        # Verify setup: both files exist and are hardlinked
        assert test_file.exists(), "Test setup: original file should exist"
        assert hard_link.exists(), "Test setup: hard link file should exist"
        assert test_file.stat().st_nlink == 2, "Test setup: original file should have 2 hard links"
        assert hard_link.stat().st_nlink == 2, "Test setup: hard link should have 2 hard links"

        # Act - Check for hardlinked files in the list
        result = detect_hardlinked_files([test_file, hard_link])

        # Assert - Both files should be detected as hardlinked
        assert len(result) == 2, f"Expected exactly 2 hardlinked files, but found {len(result)}: {result}"
        assert (
            test_file in result
        ), f"Original file {test_file} should be detected as hardlinked but was not in result: {result}"
        assert (
            hard_link in result
        ), f"Hard link {hard_link} should be detected as hardlinked but was not in result: {result}"

        # Verify both files actually point to the same inode (additional validation)
        original_stat = test_file.stat()
        hardlink_stat = hard_link.stat()
        assert (
            original_stat.st_ino == hardlink_stat.st_ino
        ), f"Files should share the same inode: original={original_stat.st_ino}, hardlink={hardlink_stat.st_ino}"
        assert (
            original_stat.st_nlink == 2
        ), f"Original file should have exactly 2 hard links, but has {original_stat.st_nlink}"
        assert (
            hardlink_stat.st_nlink == 2
        ), f"Hard link should have exactly 2 hard links, but has {hardlink_stat.st_nlink}"

    @pytest.mark.unit
    def test_detect_hardlinked_files_stat_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test detect_hardlinked_files handles stat() OSError gracefully.

        This test verifies that when Path.stat() raises an OSError during file
        stat checking, the function handles the exception gracefully by skipping
        the problematic file and continuing processing other files, ensuring
        robust error handling for filesystem permission issues or system errors.
        """
        # Arrange - Create valid files and one that will cause stat error
        valid_file = tmp_path / "valid_file.txt"
        valid_file.write_text("valid content")

        error_file = tmp_path / "error_file.txt"
        error_file.write_text("error content")

        # Create hardlinked files to verify they still get detected despite the error
        hardlinked_file = tmp_path / "hardlinked.txt"
        hardlinked_file.write_text("hardlinked content")
        hardlink_copy = tmp_path / "hardlink_copy.txt"
        hardlink_copy.hardlink_to(hardlinked_file)

        # Create a mock path object that will raise OSError on stat()
        # but still behave normally for exists() and is_file()
        class ErrorPath(Path):
            def stat(self, *, follow_symlinks=True):
                error_message = "Permission denied"
                raise OSError(error_message)

            def exists(self, *, follow_symlinks=True):
                return True

            def is_file(self):
                return True

        error_path = ErrorPath(error_file)

        # Act - Process list containing both valid and problematic files
        result = detect_hardlinked_files([valid_file, error_path, hardlinked_file, hardlink_copy])

        # Assert - Function should handle error gracefully and return valid hardlinked files
        assert len(result) == 2, f"Expected exactly 2 hardlinked files despite stat error, but found {len(result)}"
        assert hardlinked_file in result, "Hardlinked file should be detected despite other file's stat error"
        assert hardlink_copy in result, "Hardlink copy should be detected despite other file's stat error"
        assert valid_file not in result, "Single link file should not be detected as hardlinked"
        assert error_path not in result, "File with stat error should be skipped gracefully"

    @pytest.mark.unit
    def test_detect_hardlinked_files_mixed_files(self, tmp_path: Path) -> None:
        """Test detect_hardlinked_files correctly filters mixed file types and conditions.

        This test verifies that detect_hardlinked_files correctly handles a mixture
        of different file types and conditions including non-existent files, single
        linked files, and hardlinked files, returning only the files that are
        actually hardlinked (st_nlink > 1).
        """
        # Arrange - Create various file types and conditions
        hardlinked_file = tmp_path / "hardlinked.txt"
        hardlinked_file.write_text("hardlinked content")
        hardlink_copy = tmp_path / "hardlink_copy.txt"
        hardlink_copy.hardlink_to(hardlinked_file)

        single_file = tmp_path / "single.txt"
        single_file.write_text("single content")

        non_existent = tmp_path / "non_existent.txt"

        test_dir = tmp_path / "directory"
        test_dir.mkdir()

        # Verify test setup - ensure hardlinks were created properly and single file has only one link
        assert hardlinked_file.exists(), "Test setup: hardlinked file should exist"
        assert hardlink_copy.exists(), "Test setup: hardlink copy should exist"
        assert single_file.exists(), "Test setup: single file should exist"
        assert not non_existent.exists(), "Test setup: non-existent file should not exist"
        assert test_dir.is_dir(), "Test setup: test directory should be a directory"

        hardlinked_stat = hardlinked_file.stat()
        hardlink_stat = hardlink_copy.stat()
        single_stat = single_file.stat()
        assert hardlinked_stat.st_nlink == 2, "Test setup: hardlinked file should have 2 hard links"
        assert hardlink_stat.st_nlink == 2, "Test setup: hardlink copy should have 2 hard links"
        assert hardlinked_stat.st_ino == hardlink_stat.st_ino, "Test setup: hardlinked files should share same inode"
        assert single_stat.st_nlink == 1, "Test setup: single file should have exactly 1 hard link"

        # Act - Test with mixed file types
        result = detect_hardlinked_files([hardlinked_file, hardlink_copy, single_file, non_existent, test_dir])

        # Assert - Only hardlinked files should be returned
        assert len(result) == 2, f"Expected exactly 2 hardlinked files, but found {len(result)}: {result}"
        assert hardlinked_file in result, "Original hardlinked file should be detected"
        assert hardlink_copy in result, "Hardlink copy should be detected"
        assert single_file not in result, "Single linked file should not be detected as hardlinked"
        assert non_existent not in result, "Non-existent file should not be in results"
        assert test_dir not in result, "Directory should not be detected as hardlinked file"

        # Verify the returned files still have the expected hard link properties
        result_hardlinked_stat = hardlinked_file.stat()
        result_hardlink_stat = hardlink_copy.stat()
        assert result_hardlinked_stat.st_nlink == 2, "Returned hardlinked file should still have 2 hard links"
        assert result_hardlink_stat.st_nlink == 2, "Returned hardlink copy should still have 2 hard links"
        assert (
            result_hardlinked_stat.st_ino == result_hardlink_stat.st_ino
        ), "Returned hardlinked files should still share same inode"


class TestDetectReadonlyFiles:
    """Test detect_readonly_files function."""

    @pytest.mark.unit
    def test_detect_readonly_files_empty_list(self) -> None:
        """Test detect_readonly_files with empty list returns empty list.

        This test verifies that when detect_readonly_files is called with
        an empty list of files, it correctly returns an empty list without
        raising any exceptions, ensuring proper handling of edge cases.
        """
        # Arrange
        empty_file_list: list[Path] = []

        # Act
        result = detect_readonly_files(empty_file_list)

        # Assert
        assert result == [], "Should return empty list when given empty input list"

    @pytest.mark.unit
    def test_detect_readonly_files_non_existent_file(self, tmp_path: Path) -> None:
        """Test detect_readonly_files with non-existent file returns empty list.

        This test verifies that when detect_readonly_files is called with a path
        that doesn't exist on the filesystem, it correctly handles the error condition
        by skipping the non-existent file and returns an empty list, ensuring robust
        error handling for invalid file paths.
        """
        # Arrange
        non_existent = tmp_path / "non_existent.txt"

        # Act
        result = detect_readonly_files([non_existent])

        # Assert
        assert result == [], "Should return empty list when file doesn't exist on filesystem"

    @pytest.mark.unit
    def test_detect_readonly_files_directory(self, tmp_path: Path) -> None:
        """Test detect_readonly_files ignores directories and returns empty list.

        This test verifies that when detect_readonly_files is called with a directory
        instead of a file, it correctly skips the directory (since the function is
        designed to check file permissions, not directory permissions) and returns
        an empty list, ensuring proper input validation and type handling.
        """
        # Arrange - Create a test directory
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()

        # Act - Check for read-only files in list containing directory
        result = detect_readonly_files([test_dir])

        # Assert - Directory should be ignored and empty list returned
        assert result == [], "Should return empty list when given directories instead of files"

        # Verify the test directory actually is a directory (additional validation)
        assert test_dir.is_dir(), "Test directory should exist and be a directory"

    @pytest.mark.unit
    def test_detect_readonly_files_writable_file(self, tmp_path: Path) -> None:
        """Test detect_readonly_files correctly excludes writable files from results.

        This unit test verifies that when a file has normal write permissions,
        the detect_readonly_files function correctly excludes that file from the
        read-only files list, returning an empty list. This ensures the function
        only reports files with restricted write permissions by testing the real
        functionality rather than mocked behavior.
        """
        # Arrange - Create a test file with default (writable) permissions
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        # Act - Check for read-only files
        result = detect_readonly_files([test_file])

        # Assert - File should not be detected as read-only
        assert result == [], "Writable files should not be detected as read-only"

        # Verify test setup - file should actually be writable
        assert os.access(test_file, os.W_OK), "Test setup: file should have write permissions"
        assert test_file.exists(), "Test setup: file should exist on filesystem"
        assert test_file.is_file(), "Test setup: path should be a file, not directory"

    @pytest.mark.unit
    def test_detect_readonly_files_readonly_file(self, tmp_path: Path) -> None:
        """Test detect_readonly_files correctly identifies read-only files.

        This test verifies that detect_readonly_files correctly identifies files
        that have been made read-only using filesystem permissions. It creates
        an actual read-only file and tests the real functionality rather than
        mocking the permission check, ensuring the function works with real
        filesystem permission restrictions.
        """
        # Arrange - Create a test file and make it read-only
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        # Make file read-only by removing write permissions
        test_file.chmod(0o444)  # Read-only permissions for owner, group, and others

        # Act - Check for read-only files
        result = detect_readonly_files([test_file])

        # Assert - File should be detected as read-only
        assert len(result) == 1, f"Expected exactly 1 read-only file, but found {len(result)}"
        assert test_file in result, f"File {test_file} should be detected as read-only"

        # Verify the file is actually read-only using os.access
        assert not os.access(test_file, os.W_OK), "Test setup: file should actually be read-only"

    @pytest.mark.unit
    def test_detect_readonly_files_access_error(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test detect_readonly_files handles os.access() OSError gracefully.

        This test verifies that when os.access() raises an OSError during permission
        checking, the function handles the exception gracefully by treating the file
        as read-only (non-writable) and including it in the returned list, ensuring
        robust error handling for filesystem permission issues or system errors.
        """
        # Arrange
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        # Mock os.access to raise OSError (simulating permission/filesystem issues)
        mock_access = mocker.patch("marimba.core.utils.paths.os.access")
        mock_access.side_effect = OSError("Permission denied")

        # Act
        result = detect_readonly_files([test_file])

        # Assert - Verify file is treated as read-only when os.access() raises OSError
        assert len(result) == 1, f"Expected exactly 1 read-only file, but found {len(result)}"
        assert test_file in result, "File should be treated as read-only when os.access() raises OSError"

        # Verify os.access was called with correct parameters
        mock_access.assert_called_once_with(test_file, os.W_OK), "Should call os.access() to check write permissions"

        # Verify test setup - file exists and is actually a file
        assert test_file.exists(), "Test setup: file should exist on filesystem"
        assert test_file.is_file(), "Test setup: path should be a file, not directory"


class TestFormatPathForLogging:
    """Test format_path_for_logging function."""

    @pytest.mark.unit
    def test_format_path_for_logging_with_project_dir(self, tmp_path: Path) -> None:
        """Test format_path_for_logging converts absolute path to relative when project directory provided.

        This unit test verifies that when format_path_for_logging is called with an absolute
        file path and a project directory, it correctly calculates and returns the relative
        path from the project root. This ensures consistent, readable logging output that
        shows file locations relative to the project structure rather than full system paths.
        """
        # Arrange - Create project directory and nested file path with actual filesystem structure
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        subdir = project_dir / "subdir"
        subdir.mkdir()
        test_path = subdir / "file.txt"
        test_path.touch()  # Create the actual file to test with real filesystem operations

        # Act - Format the absolute path for logging
        result = format_path_for_logging(test_path, project_dir)

        # Assert - Should return relative path from project root
        assert result == "subdir/file.txt", f"Expected 'subdir/file.txt' but got '{result}'"

        # Verify test setup is correct and filesystem operations work
        assert project_dir.exists(), "Test setup: project directory should exist"
        assert test_path.exists(), "Test setup: test file should exist on filesystem"
        assert test_path.is_relative_to(
            project_dir,
        ), f"Test validation: {test_path} should be relative to {project_dir}"

    @pytest.mark.unit
    def test_format_path_for_logging_string_path(self, tmp_path: Path) -> None:
        """Test format_path_for_logging correctly handles string path input.

        This unit test verifies that format_path_for_logging correctly handles string path
        inputs by converting them internally to Path objects and successfully calculating
        the relative path from the project directory. This ensures the function provides
        consistent behavior regardless of whether the input is a string or Path object,
        supporting both usage patterns in the codebase.
        """
        # Arrange - Create project structure and file, then convert to string path
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        test_path = project_dir / "file.txt"
        test_path.touch()  # Create the actual file for comprehensive testing
        string_path = str(test_path)

        # Act - Format the string path for logging
        result = format_path_for_logging(string_path, project_dir)

        # Assert - Should return relative path as string
        assert result == "file.txt", f"Expected 'file.txt' but got '{result}'"

        # Verify test setup and input validation
        assert isinstance(string_path, str), "Test validation: input should be string type"
        assert isinstance(result, str), "Function should return string regardless of input type"
        assert project_dir.exists(), "Test setup: project directory should exist"
        assert test_path.exists(), "Test setup: test file should exist on filesystem"

        # Verify the function correctly converted string to Path and calculated relative path
        expected_path = Path(string_path)
        assert expected_path.is_relative_to(
            project_dir,
        ), f"Test validation: {expected_path} should be relative to {project_dir}"

    @pytest.mark.unit
    def test_format_path_for_logging_without_project_dir_project_found(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test format_path_for_logging without project_dir parameter, but project found via find_project_dir.

        This unit test verifies that when format_path_for_logging is called without providing
        a project_dir parameter, it correctly calls find_project_dir to locate the project root
        and then successfully formats the path as relative to the found project directory.
        This ensures proper integration between format_path_for_logging and find_project_dir
        when project_dir is not explicitly provided.
        """
        # Arrange - Create project structure and test path
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        test_path = project_dir / "file.txt"
        test_path.touch()  # Create the actual file to ensure resolve() works properly

        # Mock find_project_dir to simulate finding the project directory
        mock_find = mocker.patch("marimba.core.utils.paths.find_project_dir")
        mock_find.return_value = project_dir

        # Act - Call function without project_dir parameter
        result = format_path_for_logging(test_path)

        # Assert - Verify correct behavior and interactions
        assert result == "file.txt", f"Expected 'file.txt' but got '{result}'"
        mock_find.assert_called_once_with(test_path.resolve()), "Should call find_project_dir with resolved path"

        # Verify test setup - ensure file exists for proper test conditions
        assert test_path.exists(), "Test setup: test file should exist on filesystem"
        assert project_dir.exists(), "Test setup: project directory should exist"

    @pytest.mark.unit
    def test_format_path_for_logging_without_project_dir_not_found(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test format_path_for_logging without project_dir parameter when no project found.

        This unit test verifies that when format_path_for_logging is called without providing
        a project_dir parameter and find_project_dir returns None (no project found), the
        function correctly falls back to returning the absolute path as a string. This ensures
        robust error handling when no Marimba project can be located in the directory tree.
        """
        # Arrange - Create test path and mock find_project_dir to return None
        test_path = tmp_path / "file.txt"
        test_path.touch()  # Create the file to ensure resolve() works properly

        mock_find = mocker.patch("marimba.core.utils.paths.find_project_dir")
        mock_find.return_value = None

        # Act - Call function without project_dir parameter
        result = format_path_for_logging(test_path)

        # Assert - Verify correct fallback behavior and interactions
        expected_absolute_path = str(test_path.resolve())
        assert result == expected_absolute_path, f"Expected absolute path '{expected_absolute_path}' but got '{result}'"
        mock_find.assert_called_once_with(test_path.resolve()), "Should call find_project_dir with resolved path"

    @pytest.mark.unit
    def test_format_path_for_logging_path_outside_project(self, tmp_path: Path) -> None:
        """Test format_path_for_logging with path outside project directory returns absolute path.

        This unit test verifies that when format_path_for_logging is called with a file path
        that exists outside the project directory tree, it correctly handles the ValueError
        from path.relative_to() by returning the absolute path as a string. This ensures
        proper fallback behavior when paths cannot be made relative to the project root,
        which is essential for consistent logging output across different file locations.
        """
        # Arrange - Create project directory and a path outside the project structure
        project_dir = tmp_path / "project"
        project_dir.mkdir()

        # Create directory structure for the outside path to ensure resolve() works properly
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        outside_path = outside_dir / "file.txt"
        outside_path.touch()  # Create the file to make it a valid path

        # Act - Format the outside path for logging with the project directory
        result = format_path_for_logging(outside_path, project_dir)

        # Assert - Should return absolute path when path cannot be made relative to project
        expected_absolute_path = str(outside_path.resolve())
        assert (
            result == expected_absolute_path
        ), f"Expected absolute path '{expected_absolute_path}' for path outside project, but got '{result}'"

        # Verify test setup is correct - ensure paths are actually outside each other
        assert not outside_path.is_relative_to(
            project_dir,
        ), f"Test validation: outside_path {outside_path} should not be relative to project_dir {project_dir}"
        assert outside_path.exists(), "Test setup: outside_path should exist on filesystem"
        assert project_dir.exists(), "Test setup: project_dir should exist on filesystem"
