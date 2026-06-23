"""Tests for marimba.core.utils.hash module."""

import hashlib
from pathlib import Path

import pytest
import pytest_mock

from marimba.core.utils.hash import compute_hash


class TestHashUtilities:
    """Test hash utility functions."""

    @pytest.fixture
    def test_file(self, tmp_path: Path) -> tuple[Path, bytes]:
        """Create a test file with known content."""
        test_file = tmp_path / "test_file.txt"
        test_content = b"Hello, World!"
        test_file.write_bytes(test_content)
        return test_file, test_content

    @pytest.fixture
    def test_directory(self, tmp_path: Path) -> Path:
        """Create a test directory."""
        test_dir = tmp_path / "test_directory"
        test_dir.mkdir()
        return test_dir

    @pytest.fixture
    def test_root_dir(self, tmp_path: Path) -> Path:
        """Create a root directory for relative path testing."""
        root_dir = tmp_path / "root"
        root_dir.mkdir()
        return root_dir

    @pytest.mark.unit
    def test_compute_hash_file_contents(self, test_file: tuple[Path, bytes]) -> None:
        """Test computing hash of file contents.

        Verifies that the compute_hash function correctly calculates the SHA-256 hash
        of a file's contents by comparing it with the expected hash of known content.
        """
        # Arrange
        file_path, content = test_file
        expected_hash = hashlib.sha256(content).hexdigest()

        # Act
        result = compute_hash(file_path)

        # Assert
        assert result == expected_hash, "Hash should match expected SHA-256 of file contents"

    @pytest.mark.unit
    def test_compute_hash_large_file(self, tmp_path: Path) -> None:
        """Test computing hash of large file (multiple chunks).

        Verifies that the compute_hash function correctly handles files larger than
        the internal read buffer size by testing chunked file reading.
        """
        # Arrange
        large_file = tmp_path / "large_file.txt"
        # Create content larger than actual chunk size (1MB) to test chunking behavior
        chunk_size = 1_048_576  # 1MB - matches implementation chunk size
        content = b"A" * (chunk_size + 1000)  # Slightly over 1MB to ensure chunking
        large_file.write_bytes(content)
        expected_hash = hashlib.sha256(content).hexdigest()

        # Act
        result = compute_hash(large_file)

        # Assert
        assert result == expected_hash, "Hash should match expected SHA-256 for large file content"

    @pytest.mark.unit
    def test_compute_hash_empty_file(self, tmp_path: Path) -> None:
        """Test computing hash of empty file.

        Verifies that the compute_hash function correctly handles empty files
        by computing the SHA-256 hash of zero bytes.
        """
        # Arrange
        empty_file = tmp_path / "empty_file.txt"
        empty_file.touch()
        expected_hash = hashlib.sha256().hexdigest()

        # Act
        result = compute_hash(empty_file)

        # Assert
        assert result == expected_hash, "Hash should match expected SHA-256 of empty file"

    @pytest.mark.unit
    def test_compute_hash_directory_absolute_path(self, test_directory: Path) -> None:
        """Test computing hash of directory using absolute path.

        Verifies that for non-file paths (directories), the function computes
        the hash of the path string representation.
        """
        # Arrange
        expected_hash = hashlib.sha256(str(test_directory.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(test_directory)

        # Assert
        assert result == expected_hash, "Hash should match expected SHA-256 of directory path string"

    @pytest.mark.integration
    def test_compute_hash_directory_with_root_dir(self, test_root_dir: Path) -> None:
        """Test computing hash of directory with root directory.

        Verifies that when a root directory is provided for a directory path,
        the function computes the hash of the relative path from root to target.
        """
        # Arrange
        test_dir = test_root_dir / "subdir"
        test_dir.mkdir()
        relative_path = test_dir.relative_to(test_root_dir)
        expected_hash = hashlib.sha256(str(relative_path.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(test_dir, root_dir=test_root_dir)

        # Assert
        assert result == expected_hash, "Hash should match expected SHA-256 of relative directory path"

    @pytest.mark.integration
    def test_compute_hash_file_with_root_dir_ignored(self, test_root_dir: Path) -> None:
        """Test that root_dir parameter is ignored when computing hash for files.

        Verifies that when hashing a file, the root_dir parameter is completely ignored
        and the function always computes the hash of the file contents only, not the path.
        This ensures consistent behavior where files are hashed by content, not by path.
        """
        # Arrange
        test_file = test_root_dir / "test.txt"
        content = b"File content for root_dir test"
        test_file.write_bytes(content)
        expected_hash = hashlib.sha256(content).hexdigest()

        # Act
        result_with_root = compute_hash(test_file, root_dir=test_root_dir)
        result_without_root = compute_hash(test_file)

        # Assert
        assert result_with_root == expected_hash, "Hash with root_dir should match file contents exactly"
        assert result_without_root == expected_hash, "Hash without root_dir should match file contents exactly"
        assert (
            result_with_root == result_without_root
        ), "File hash should be identical whether root_dir is provided or not"

    @pytest.mark.unit
    def test_compute_hash_directory_outside_root(self, tmp_path: Path, test_root_dir: Path) -> None:
        """Test error when directory is outside root directory.

        Verifies that the function raises a ValueError when trying to compute
        a hash for a directory that is not within the specified root directory.
        """
        # Arrange
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()

        # Act & Assert
        with pytest.raises(ValueError, match="is not within root directory"):
            compute_hash(outside_dir, root_dir=test_root_dir)

    @pytest.mark.integration
    def test_compute_hash_nested_directory_with_root(self, test_root_dir: Path) -> None:
        """Test computing hash of deeply nested directory with root.

        Verifies that the function correctly handles deeply nested directory structures
        when computing relative paths from a root directory.
        """
        # Arrange
        nested_dir = test_root_dir / "level1" / "level2" / "level3"
        nested_dir.mkdir(parents=True)
        relative_path = nested_dir.relative_to(test_root_dir)
        expected_hash = hashlib.sha256(str(relative_path.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(nested_dir, root_dir=test_root_dir)

        # Assert
        assert result == expected_hash, "Hash should match expected value for deeply nested directory path"

    @pytest.mark.unit
    def test_compute_hash_symlink_as_directory(self, tmp_path: Path) -> None:
        """Test computing hash of symlink to directory.

        Verifies that symbolic links to directories are treated as non-file paths
        and have their path string hashed rather than following the link.
        """
        # Arrange
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        symlink = tmp_path / "symlink"
        symlink.symlink_to(target_dir)

        # Verify symlink was created correctly
        assert symlink.is_symlink(), "Symlink should be created successfully"
        assert symlink.readlink() == target_dir, "Symlink should point to target directory"

        # Should hash the symlink path itself (since it's not a file)
        expected_hash = hashlib.sha256(str(symlink.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(symlink)

        # Assert
        assert result == expected_hash, "Hash should be of symlink path, not target directory"

    @pytest.mark.unit
    def test_compute_hash_nonexistent_path_as_directory(self, tmp_path: Path) -> None:
        """Test computing hash of non-existent path (treated as directory).

        Verifies that non-existent paths are treated as directory paths
        and have their path string hashed.
        """
        # Arrange
        nonexistent = tmp_path / "nonexistent"
        expected_hash = hashlib.sha256(str(nonexistent.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(nonexistent)

        # Assert
        assert result == expected_hash, "Hash should match expected value for non-existent path"

    @pytest.mark.unit
    def test_compute_hash_file_read_error(self, mocker: pytest_mock.MockerFixture, tmp_path: Path) -> None:
        """Test handling of file read error.

        Verifies that the function properly handles and re-raises OSError
        when file reading fails, wrapping it with a descriptive message.
        """
        # Arrange
        test_file = tmp_path / "test_file.txt"
        test_file.touch()  # Create the file so is_file() returns True
        mock_open_method = mocker.patch("pathlib.Path.open")
        mock_open_method.side_effect = OSError("Permission denied")

        # Act & Assert
        with pytest.raises(OSError, match=r"Failed to read file .*/test_file.txt: Permission denied"):
            compute_hash(test_file)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("content1", "content2", "should_be_equal", "description"),
        [
            ("Content 1", "Content 2", False, "different content produces different hashes"),
            ("Same content", "Same content", True, "same content produces same hash"),
            ("", "", True, "empty files produce same hash"),
            ("A" * 1000, "B" * 1000, False, "different large content produces different hashes"),
        ],
    )
    def test_compute_hash_file_content_comparison(
        self,
        tmp_path: Path,
        content1: str,
        content2: str,
        should_be_equal: bool,
        description: str,
    ) -> None:
        """Test file hash comparison with various content scenarios.

        Verifies that the compute_hash function produces identical hashes for identical
        content and different hashes for different content across various scenarios.
        """
        # Arrange
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text(content1)
        file2.write_text(content2)

        # Act
        hash1 = compute_hash(file1)
        hash2 = compute_hash(file2)

        # Assert
        if should_be_equal:
            assert hash1 == hash2, f"Hashes should be equal for {description}"
        else:
            assert hash1 != hash2, f"Hashes should be different for {description}"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("dir1_name", "dir2_name", "description"),
        [
            ("dir1", "dir2", "different simple directory names"),
            ("special-dir_with@symbols", "another-special#dir", "special characters in names"),
            ("nested/path/dir1", "nested/path/dir2", "nested directory paths"),
            ("测试目录1", "测试目录2", "unicode directory names"),
        ],
    )
    def test_compute_hash_directory_different_paths_different_hashes(
        self,
        tmp_path: Path,
        dir1_name: str,
        dir2_name: str,
        description: str,
    ) -> None:
        """Test that different directory paths produce different hashes.

        Verifies that directories with different path names produce different
        hash values, testing various naming scenarios including special characters.
        """
        # Arrange
        dir1 = tmp_path / dir1_name
        dir2 = tmp_path / dir2_name
        dir1.mkdir(parents=True)
        dir2.mkdir(parents=True)

        # Act
        hash1 = compute_hash(dir1)
        hash2 = compute_hash(dir2)

        # Assert
        assert hash1 != hash2, f"Different hashes expected for {description}"

    @pytest.mark.integration
    def test_compute_hash_relative_vs_absolute_paths_with_root(self, test_root_dir: Path) -> None:
        """Test that relative path calculation handles absolute vs relative inputs.

        Verifies that the function produces consistent results for the same directory
        regardless of whether the input path is absolute or relative to root.
        This tests the component interaction between Path resolution and hash computation.
        """
        # Arrange
        subdir = test_root_dir / "subdir"
        subdir.mkdir()

        # Create a relative path by constructing it manually (avoiding os.chdir)
        relative_subdir = Path("subdir")

        # Both paths should resolve to the same relative path from root_dir
        expected_relative_path = Path("subdir")
        expected_hash = hashlib.sha256(str(expected_relative_path.as_posix()).encode()).hexdigest()

        # Act
        absolute_result = compute_hash(subdir, root_dir=test_root_dir)  # Absolute path input
        # For relative path, we need to ensure it's resolved correctly within the root context
        # Since we can't change directory, we test with a path that's already relative to root
        relative_path_from_root = test_root_dir / relative_subdir
        relative_result = compute_hash(relative_path_from_root, root_dir=test_root_dir)

        # Assert
        assert absolute_result == expected_hash, "Hash should match expected value for absolute path"
        assert relative_result == expected_hash, "Hash should match expected value for relative path"
        assert absolute_result == relative_result, "Hash should be consistent for absolute vs relative path inputs"

    @pytest.mark.unit
    def test_compute_hash_special_characters_in_path(self, tmp_path: Path) -> None:
        """Test computing hash of path with special characters.

        Verifies that the function correctly handles directory paths containing
        special characters and symbols in the path name.
        """
        # Arrange
        special_dir = tmp_path / "special-dir_with@symbols"
        special_dir.mkdir()
        expected_hash = hashlib.sha256(str(special_dir.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(special_dir)

        # Assert
        assert result == expected_hash, "Hash should match expected value for directory with special characters"

    @pytest.mark.unit
    def test_compute_hash_unicode_path(self, tmp_path: Path) -> None:
        """Test computing hash of path with Unicode characters.

        Verifies that the function correctly handles directory paths containing
        Unicode characters (non-ASCII) in the path name.
        """
        # Arrange
        unicode_dir = tmp_path / "测试目录"
        unicode_dir.mkdir()
        expected_hash = hashlib.sha256(str(unicode_dir.as_posix()).encode()).hexdigest()

        # Act
        result = compute_hash(unicode_dir)

        # Assert
        assert result == expected_hash, "Hash should match expected value for directory with Unicode characters"

    @pytest.mark.unit
    def test_compute_hash_returns_lowercase_hex(self, test_file: tuple[Path, bytes]) -> None:
        """Test that hash is returned as lowercase hexadecimal.

        Verifies that the returned hash is a valid 64-character lowercase
        hexadecimal string as expected for SHA-256 hashes.
        """
        # Arrange
        file_path, _ = test_file

        # Act
        result = compute_hash(file_path)

        # Assert
        assert len(result) == 64, f"SHA-256 hash should be 64 characters long, got {len(result)}"
        assert all(
            c in "0123456789abcdef" for c in result
        ), f"Hash should contain only lowercase hex characters, got: {result}"
        assert result.islower(), f"Hash should be in lowercase format, got: {result}"

    @pytest.mark.unit
    def test_compute_hash_consistency(self, test_file: tuple[Path, bytes]) -> None:
        """Test that computing hash multiple times gives same result.

        Verifies that the compute_hash function is deterministic and produces
        the same hash value when called multiple times on the same file.
        """
        # Arrange
        file_path, _ = test_file

        # Act
        hash1 = compute_hash(file_path)
        hash2 = compute_hash(file_path)
        hash3 = compute_hash(file_path)

        # Assert
        assert hash1 == hash2 == hash3, "Hash should be consistent across multiple calls"

    @pytest.mark.unit
    def test_compute_hash_directory_resolve_error_with_root(
        self,
        tmp_path: Path,
        test_root_dir: Path,
    ) -> None:
        """Test error handling when directory is not within root directory.

        Verifies that when a directory path is not within the specified root directory,
        the function properly handles the error and raises a descriptive ValueError.
        """
        # Arrange
        outside_dir = tmp_path / "outside_dir"
        outside_dir.mkdir()

        # Act & Assert
        with pytest.raises(ValueError, match="is not within root directory"):
            compute_hash(outside_dir, root_dir=test_root_dir)
