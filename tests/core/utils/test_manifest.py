from pathlib import Path

import pytest

from marimba.core.utils.manifest import Manifest


class TestManifest:
    """Test suite for Manifest utility class."""

    @pytest.mark.unit
    def test_get_subdirectories_nested_structure_returns_all_intermediate_directories(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories correctly identifies all intermediate directories.

        This test verifies the core algorithm that traverses upward from file paths
        to collect all unique parent directories until reaching the base directory.
        This functionality is critical for the manifest update process to ensure
        all directory paths are properly tracked for validation.
        """
        # Arrange
        base_dir = tmp_path / "project"

        # Use file paths directly without creating actual filesystem structure
        # since this is a unit test focusing on the algorithm logic
        files = {
            base_dir / "data" / "event" / "image.jpg",
            base_dir / "data" / "event" / "subdir" / "another.jpg",
            base_dir / "data" / "metadata.json",
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        expected_subdirs = {
            base_dir / "data",
            base_dir / "data" / "event",
            base_dir / "data" / "event" / "subdir",
        }
        assert result == expected_subdirs, (
            f"Expected subdirectories {expected_subdirs}, but got {result}. "
            f"The method should identify all intermediate directories between files and base directory."
        )

    @pytest.mark.unit
    def test_get_subdirectories_single_level_returns_parent_directory(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories correctly handles files one level deep.

        This validates the boundary case where files are only one directory level
        below the base directory, ensuring the algorithm correctly identifies
        just the immediate parent without over-traversing.
        """
        # Arrange
        base_dir = tmp_path / "root"

        files = {
            base_dir / "subfolder" / "file1.txt",
            base_dir / "subfolder" / "file2.txt",
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        expected_subdirs = {base_dir / "subfolder"}
        assert result == expected_subdirs, (
            f"Expected single subdirectory {expected_subdirs}, but got {result}. "
            f"Single-level files should only return their immediate parent."
        )

    @pytest.mark.unit
    def test_get_subdirectories_files_in_base_directory_returns_empty_set(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories returns empty set when files are in base directory.

        This tests the boundary condition where files exist directly in the base
        directory, verifying that no subdirectories are identified since the parent
        of these files is the base directory itself.
        """
        # Arrange
        base_dir = tmp_path / "base"

        files = {
            base_dir / "file1.txt",
            base_dir / "file2.txt",
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        assert result == set(), (
            f"Expected empty set for files in base directory, but got {result}. "
            f"Files directly in base directory should not generate subdirectories."
        )

    @pytest.mark.unit
    def test_get_subdirectories_empty_files_set_returns_empty_set(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories handles empty input gracefully.

        This tests the edge case where no files are provided, ensuring the
        algorithm handles empty input without errors and returns an empty result.
        This is important for robustness when working with empty collections.
        """
        # Arrange
        base_dir = tmp_path / "empty_base"
        files: set[Path] = set()

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        assert result == set(), (
            f"Expected empty set for empty input, but got {result}. "
            f"Empty file set should return empty subdirectories."
        )

    @pytest.mark.unit
    def test_get_subdirectories_mixed_depth_files_returns_all_unique_directories(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories handles mixed-depth files correctly.

        This test verifies the algorithm can handle files at varying directory depths,
        ensuring it correctly identifies all intermediate directories for complex
        directory structures. This is essential for real-world usage where datasets
        contain files at different organizational levels.
        """
        # Arrange
        base_dir = tmp_path / "mixed"

        files = {
            base_dir / "shallow" / "file1.txt",
            base_dir / "deep" / "file2.txt",
            base_dir / "deep" / "nested" / "file3.txt",
            base_dir / "deep" / "nested" / "very" / "deep" / "file4.txt",
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        expected_subdirs = {
            base_dir / "shallow",
            base_dir / "deep",
            base_dir / "deep" / "nested",
            base_dir / "deep" / "nested" / "very",
            base_dir / "deep" / "nested" / "very" / "deep",
        }
        assert result == expected_subdirs, (
            f"Expected all unique subdirectories {expected_subdirs}, but got {result}. "
            f"Mixed-depth files should generate all intermediate directories."
        )

    @pytest.mark.unit
    def test_get_subdirectories_duplicate_directories_returns_unique_set(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories returns unique directories when files share paths.

        This ensures the algorithm correctly deduplicates directories when multiple
        files exist in the same directory, which is critical for efficient manifest
        operations and avoiding redundant directory processing.
        """
        # Arrange
        base_dir = tmp_path / "shared"

        files = {
            base_dir / "common" / "dir" / "file1.txt",
            base_dir / "common" / "dir" / "file2.txt",
            base_dir / "common" / "dir" / "file3.txt",
            base_dir / "common" / "other.txt",
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir)

        # Assert
        expected_subdirs = {
            base_dir / "common",
            base_dir / "common" / "dir",
        }
        assert result == expected_subdirs, (
            f"Expected unique subdirectories {expected_subdirs}, but got {result}. "
            f"Multiple files in same directory should not create duplicate subdirectory entries."
        )

        # Additional assertion to verify set properties
        assert len(result) == len(expected_subdirs), (
            f"Expected {len(expected_subdirs)} unique directories, but got {len(result)}. "
            f"Result should contain exactly the unique set of intermediate directories."
        )

    @pytest.mark.unit
    def test_get_subdirectories_absolute_paths_with_common_base(self, tmp_path: Path) -> None:
        """
        Test that _get_sub_directories works correctly with absolute paths.

        This test verifies the algorithm handles absolute paths correctly and
        stops traversal when it reaches the base directory, ensuring consistent
        behavior regardless of how paths are constructed.
        """
        # Arrange
        base_dir = tmp_path / "project"

        # Use absolute paths to test path resolution
        files = {
            (base_dir / "data" / "file1.txt").resolve(),
            (base_dir / "docs" / "readme.txt").resolve(),
            (base_dir / "config" / "settings.json").resolve(),
        }

        # Act
        result = Manifest._get_sub_directories(files, base_dir.resolve())

        # Assert
        expected_subdirs = {
            base_dir.resolve() / "data",
            base_dir.resolve() / "docs",
            base_dir.resolve() / "config",
        }
        assert result == expected_subdirs, (
            f"Expected subdirectories {expected_subdirs}, but got {result}. "
            f"Absolute paths should be handled correctly and stop at base directory."
        )


class TestManifestIdempotence:
    """Pin the FAIR-load-bearing manifest invariants: stability under re-run + save/load round-trip."""

    @pytest.mark.integration
    def test_from_dir_idempotent_across_runs(self, tmp_path: Path) -> None:
        """Re-running from_dir on the same directory produces an equal Manifest."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "a.txt").write_text("alpha")
        (data / "b.txt").write_text("beta")
        (data / "sub").mkdir()
        (data / "sub" / "c.txt").write_text("gamma")

        first = Manifest.from_dir(data)
        second = Manifest.from_dir(data)

        assert first == second

    @pytest.mark.integration
    def test_from_dir_hashes_deterministic_across_runs(self, tmp_path: Path) -> None:
        """Per-file hashes are byte-identical across re-runs."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "fixed.bin").write_bytes(b"deterministic content \x00\x01\x02")

        first = Manifest.from_dir(data)
        second = Manifest.from_dir(data)

        assert sorted(first.hashes.items()) == sorted(second.hashes.items())

    @pytest.mark.integration
    def test_save_load_round_trip_preserves_equality(self, tmp_path: Path) -> None:
        """Save then load reconstructs an equal Manifest."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "x.txt").write_text("hello")
        (data / "y.txt").write_text("world")

        original = Manifest.from_dir(data)
        manifest_path = tmp_path / "manifest.txt"
        original.save(manifest_path)

        loaded = Manifest.load(manifest_path)

        assert original == loaded

    @pytest.mark.integration
    def test_save_byte_equal_across_writes(self, tmp_path: Path) -> None:
        """Writing the same Manifest twice produces byte-equal files."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "a.txt").write_text("alpha")
        (data / "b.txt").write_text("beta")

        manifest = Manifest.from_dir(data)
        path1 = tmp_path / "manifest1.txt"
        path2 = tmp_path / "manifest2.txt"
        manifest.save(path1)
        manifest.save(path2)

        assert path1.read_bytes() == path2.read_bytes()


class TestManifestHeader:
    """Pin the dataset-identity header behaviour (F3 linkage) and path-parse robustness."""

    @pytest.mark.integration
    def test_header_round_trips_through_save_load(self, tmp_path: Path) -> None:
        """A dataset-identity header survives save then load."""
        header = {"image-set-uuid": "110cae5b-2d06-5b65-9173-f9f47c93e3d0", "hash-algorithm": "SHA-256"}
        manifest = Manifest({"data/a.txt": "abc"}, header=header)
        path = tmp_path / "manifest.txt"
        manifest.save(path)

        loaded = Manifest.load(path)
        assert loaded.header == header
        assert loaded.hashes == {"data/a.txt": "abc"}

    @pytest.mark.unit
    def test_header_excluded_from_equality(self) -> None:
        """The header must not affect equality, since validate() compares against a header-less manifest."""
        with_header = Manifest({"x": "1"}, header={"image-set-uuid": "u1"})
        without_header = Manifest({"x": "1"})
        assert with_header == without_header

    @pytest.mark.unit
    def test_load_parses_path_containing_colon(self, tmp_path: Path) -> None:
        """A relative path containing a colon parses correctly via rsplit on the final colon."""
        path = tmp_path / "manifest.txt"
        path.write_text("# image-set-name: IN2018_V06\ndata/a:b.txt:deadbeef\n")

        loaded = Manifest.load(path)
        assert loaded.hashes == {"data/a:b.txt": "deadbeef"}
        assert loaded.header == {"image-set-name": "IN2018_V06"}

    @pytest.mark.integration
    def test_validate_succeeds_with_header(self, tmp_path: Path) -> None:
        """A manifest carrying a header still validates against its directory."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "a.txt").write_text("alpha")

        manifest = Manifest.from_dir(data)
        manifest.header = {"image-set-uuid": "u1"}
        assert manifest.validate(data)
