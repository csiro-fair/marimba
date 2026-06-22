"""Tests for marimba.core.wrappers.dataset module."""

import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import pytest_mock

from marimba.core.wrappers.dataset import DatasetWrapper, _PackagingWarningCollector


class TestPackagingWarningCollector:
    """The packaging warning collector counts warnings and captures iFDO completeness findings for the panel."""

    @staticmethod
    def _record(message: str, **extra: object) -> logging.LogRecord:
        record = logging.LogRecord("test", logging.WARNING, __file__, 0, message, None, None)
        for key, value in extra.items():
            setattr(record, key, value)
        return record

    @pytest.mark.unit
    def test_counts_warnings_and_captures_ifdo_fields(self) -> None:
        """Each warning increments the count; records carrying iFDO extras are collected per file."""
        collector = _PackagingWarningCollector()
        collector.emit(self._record("a generic warning"))
        collector.emit(self._record("incomplete", ifdo_name="x.ifdo", ifdo_unpopulated_fields=["image-context"]))
        assert collector.warning_count == 2
        assert collector.ifdo_unpopulated_fields == {"x.ifdo": ["image-context"]}

    @pytest.mark.unit
    def test_no_ifdo_findings_when_no_extras(self) -> None:
        """A plain warning is counted but contributes no iFDO completeness entry."""
        collector = _PackagingWarningCollector()
        collector.emit(self._record("plain warning"))
        assert collector.warning_count == 1
        assert collector.ifdo_unpopulated_fields == {}


class TestDatasetWrapper:
    """Test DatasetWrapper functionality."""

    @pytest.fixture
    def dataset_wrapper(self, tmp_path: Path) -> Generator[DatasetWrapper, None, None]:
        """Set up a DatasetWrapper instance for testing.

        Creates a test dataset wrapper with proper cleanup after use.
        """
        wrapper = DatasetWrapper.create(tmp_path / "test_dataset")
        try:
            yield wrapper
        finally:
            wrapper.close()

    @pytest.mark.unit
    def test_check_dataset_mapping_nonexistent_source_file(self, dataset_wrapper: DatasetWrapper) -> None:
        """
        Test that dataset mapping validation fails when source files don't exist.

        Verifies that InvalidDatasetMappingError is raised with specific error message
        when the mapping references files that don't exist on the filesystem.
        Tests the core validation logic for source file existence checking.
        """
        # Arrange: Create mapping with nonexistent source file
        nonexistent_path = Path("nonexistent_file.txt")
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {nonexistent_path: (Path("destination.txt"), None, None)},
        }

        # Act & Assert: Should raise exception with specific error message containing the path
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=rf"Source path {re.escape(str(nonexistent_path))} does not exist",
        ):
            dataset_wrapper.check_dataset_mapping(dataset_mapping)

    @pytest.mark.integration
    def test_check_dataset_mapping_valid_mapping(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """
        Test that dataset mapping validation succeeds with valid mapping and logs success message.

        Verifies that check_dataset_mapping completes validation successfully and logs
        the expected success message when provided with a valid mapping containing
        existing source files and relative destination paths. This integration test
        verifies real component interactions including logging behavior.
        """
        # Arrange: Create test files for valid mapping
        test_dir = tmp_path / "valid_mapping_test"
        test_dir.mkdir()
        source_file1 = test_dir / "source1.txt"
        source_file2 = test_dir / "source2.txt"
        source_file1.write_text("test content 1")
        source_file2.write_text("test content 2")

        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "pipeline1": {
                source_file1: (Path("output1.txt"), None, None),
                source_file2: (Path("subdir/output2.txt"), None, None),
            },
        }

        # Arrange: Mock logger to verify success message
        mock_info = mocker.patch.object(dataset_wrapper.logger, "info")

        # Act: Should complete without raising an exception
        dataset_wrapper.check_dataset_mapping(dataset_mapping)

        # Assert: Verify success was logged with expected message
        mock_info.assert_called_with("Dataset mapping is valid")

        # Assert: No exception was raised (implicit in reaching this point)
        # The validation completed successfully, testing real component integration

    @pytest.mark.integration
    def test_check_dataset_mapping_duplicate_source_paths(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test that dataset mapping validation fails when source paths resolve to the same file.

        Verifies that InvalidDatasetMappingError is raised when multiple source paths
        (including symlinks) resolve to the same filesystem location, and that the
        error message contains specific path information.
        """
        # Arrange: Create test files and symlink
        temp_path = tmp_path / "mapping_test"
        temp_path.mkdir()
        file1 = temp_path / "file1.txt"
        file2 = temp_path / "file2.txt"
        file1.write_text("content1")
        file2.write_text("content2")

        # Create symlink that resolves to same file as file1
        link_file = temp_path / "some_dir" / "link.txt"
        link_file.parent.mkdir(exist_ok=True)
        link_file.symlink_to(file1.absolute())

        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {
                file1: (Path("destination1.txt"), None, None),
                file2: (Path("destination2.txt"), None, None),
                link_file: (Path("destination3.txt"), None, None),  # This should conflict with file1
            },
        }

        # Act & Assert: Should raise exception for duplicate source resolution
        with pytest.raises(DatasetWrapper.InvalidDatasetMappingError, match=r"both resolve to") as exc_info:
            dataset_wrapper.check_dataset_mapping(dataset_mapping)

        # Assert: Verify error message contains expected path information
        error_message = str(exc_info.value)
        assert str(file1) in error_message, f"Expected file1 path '{file1}' in error message, got: {error_message}"
        assert (
            str(link_file) in error_message
        ), f"Expected link_file path '{link_file}' in error message, got: {error_message}"
        assert (
            str(file1.resolve()) in error_message
        ), f"Expected resolved path '{file1.resolve()}' in error message, got: {error_message}"

    @pytest.mark.integration
    def test_check_dataset_mapping_absolute_destination_paths(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test that dataset mapping validation fails when destination paths are absolute.

        This integration test verifies that the check_dataset_mapping method raises
        InvalidDatasetMappingError with a specific error message when destination paths
        are absolute rather than relative to the dataset root. Tests the validation
        logic that ensures all destination paths must be relative for proper dataset
        packaging.
        """
        # Arrange: Create test file with absolute destination path
        temp_file = tmp_path / "abs_test" / "file.txt"
        temp_file.parent.mkdir()
        temp_file.touch()

        absolute_destination = Path("/tmp/absolute_destination.txt")
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {temp_file: (absolute_destination, None, None)},
        }

        # Act & Assert: Should raise exception with specific error message for absolute destination path
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=rf"Destination path {re.escape(str(absolute_destination))} must be relative",
        ) as exc_info:
            dataset_wrapper.check_dataset_mapping(dataset_mapping)

        # Assert: Verify error message contains expected path information
        error_message = str(exc_info.value)
        assert (
            str(absolute_destination) in error_message
        ), f"Expected absolute destination path '{absolute_destination}' in error message, got: {error_message}"
        assert (
            "must be relative" in error_message
        ), f"Expected 'must be relative' validation message in error message, got: {error_message}"

    @pytest.mark.integration
    def test_check_dataset_mapping_destination_collisions_fail_by_default(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test that dataset mapping validation fails by default when multiple sources map to same destination.

        This integration test verifies that the check_dataset_mapping method raises
        InvalidDatasetMappingError with a specific error message when different source
        files are mapped to the same destination path, causing a collision. This tests
        the default behavior where destination collisions are not allowed.
        """
        # Arrange: Create test files with colliding destinations
        collision_path = tmp_path / "collision_test"
        collision_path.mkdir()
        file1 = collision_path / "file1.txt"
        file2 = collision_path / "file2.txt"
        file1.write_text("content1")
        file2.write_text("content2")

        destination_path = Path("destination.txt")
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {
                file1: (destination_path, None, None),
                file2: (destination_path, None, None),  # Same destination - should conflict
            },
        }

        # Act & Assert: Should raise exception with specific error message pattern
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=r"Resolved destination path .* is the same for source paths",
        ) as exc_info:
            dataset_wrapper.check_dataset_mapping(dataset_mapping)

        # Assert: Verify error message contains collision information
        error_message = str(exc_info.value)
        assert str(file1) in error_message, f"Expected source file '{file1}' in error message, got: {error_message}"
        assert str(file2) in error_message, f"Expected source file '{file2}' in error message, got: {error_message}"

    @pytest.mark.unit
    def test_check_dataset_mapping_empty_mapping_valid(
        self,
        dataset_wrapper: DatasetWrapper,
    ) -> None:
        """
        Test that dataset mapping validation succeeds with empty mapping.

        Verifies that an empty dataset mapping is considered valid
        and does not raise any validation errors. Tests the validation
        logic for edge case of empty input without mocking core functionality.
        """
        # Arrange: Create empty mapping
        empty_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {}

        # Act: Should complete without raising an exception
        dataset_wrapper.check_dataset_mapping(empty_mapping)

        # Assert: No exception was raised (implicit in reaching this point)
        # The validation completed successfully for empty mapping edge case

    @pytest.mark.integration
    def test_check_dataset_mapping_valid_simple_mapping(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """
        Test that dataset mapping validation succeeds with valid simple mapping and logs success message.

        Verifies that a straightforward mapping with existing source files
        and relative destination paths passes validation and logs the expected
        success message. This integration test focuses on real component behavior
        with minimal mocking only for external dependencies like logging.
        """
        # Arrange: Create test file and simple mapping
        temp_file = tmp_path / "simple_test" / "file.txt"
        temp_file.parent.mkdir()
        temp_file.write_text("test content")

        simple_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {
                temp_file: (Path("simple_destination.txt"), None, None),
            },
        }

        # Arrange: Mock logger to verify success message
        mock_info = mocker.patch.object(dataset_wrapper.logger, "info")

        # Act: Should complete without raising an exception
        dataset_wrapper.check_dataset_mapping(simple_mapping)

        # Assert: Verify success was logged with expected message
        mock_info.assert_called_with("Dataset mapping is valid")

        # Assert: No exception was raised (implicit in reaching this point)
        # The validation completed successfully for simple valid mapping

    @pytest.mark.integration
    def test_check_dataset_mapping_symlink_to_same_file_fails(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test that dataset mapping validation fails when symlinks resolve to same file.

        Verifies that when a target file and a symlink pointing to it are both
        included in the mapping, a validation error is raised for duplicate source resolution.
        This integration test validates real symlink resolution behavior without mocking
        core business logic.
        """
        # Arrange: Create target file and symlink
        symlink_dir = tmp_path / "symlink_test"
        symlink_dir.mkdir()

        target_file = symlink_dir / "target.txt"
        target_file.touch()

        link_file = symlink_dir / "link_to_target.txt"
        link_file.symlink_to(target_file)

        symlink_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {
                target_file: (Path("dest1.txt"), None, None),
                link_file: (Path("dest2.txt"), None, None),  # Same resolved source
            },
        }

        # Act & Assert: Should raise exception for duplicate source resolution
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=r"Source paths .* and .* both resolve to",
        ) as exc_info:
            dataset_wrapper.check_dataset_mapping(symlink_mapping)

        # Assert: Verify error message contains expected path information
        error_message = str(exc_info.value)
        assert (
            str(target_file) in error_message
        ), f"Expected target file path '{target_file}' in error message, got: {error_message}"
        assert (
            str(link_file) in error_message
        ), f"Expected symlink path '{link_file}' in error message, got: {error_message}"
        assert (
            str(target_file.resolve()) in error_message
        ), f"Expected resolved path '{target_file.resolve()}' in error message, got: {error_message}"

    @pytest.mark.integration
    def test_allow_destination_collisions_flag_permits_collisions_and_logs_warning(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """
        Test that allow_destination_collisions=True permits collisions and logs warnings.

        This integration test verifies that when allow_destination_collisions=True:
        1. No exception is raised for destination path collisions
        2. Warning messages are logged about the collision with specific details
        3. The mapping validation completes successfully
        Tests the collision override behavior in dataset mapping validation.
        Minimal mocking is used only to verify warning logging behavior.
        """
        # Arrange: Create test files with collision mapping
        test_dir = tmp_path / "collision_test"
        test_dir.mkdir()
        file1 = test_dir / "file1.txt"
        file2 = test_dir / "file2.txt"
        file1.write_text("content1")
        file2.write_text("content2")

        collision_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "test": {
                file1: (Path("same_destination.txt"), None, None),
                file2: (Path("same_destination.txt"), None, None),  # Same destination - collision
            },
        }

        # Arrange: Mock only the logger warning method to verify logging (external dependency)
        mock_warning = mocker.patch.object(dataset_wrapper.logger, "warning")

        # Act: Should succeed with the flag and not raise an exception
        dataset_wrapper.check_dataset_mapping(collision_mapping, allow_destination_collisions=True)

        # Assert: Verify warning was logged exactly once with expected content
        mock_warning.assert_called_once()
        warning_call = mock_warning.call_args[0][0]

        # Verify the warning message contains expected elements from source implementation
        expected_resolved_dst = Path("same_destination.txt").resolve()
        expected_warning_start = (
            f"Destination path collision detected: {expected_resolved_dst} is the same for source paths"
        )
        expected_warning_end = f"Using first source file: {file1}"

        assert warning_call.startswith(
            expected_warning_start,
        ), f"Expected warning to start with '{expected_warning_start}', got: {warning_call}"
        assert warning_call.endswith(
            expected_warning_end,
        ), f"Expected warning to end with '{expected_warning_end}', got: {warning_call}"
        assert str(file1) in warning_call, f"Expected source file '{file1}' in warning, got: {warning_call}"
        assert str(file2) in warning_call, f"Expected source file '{file2}' in warning, got: {warning_call}"

        # Assert: Verify that without the flag, the same mapping would fail
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=r"Resolved destination path .* is the same for source paths",
        ):
            dataset_wrapper.check_dataset_mapping(collision_mapping, allow_destination_collisions=False)

    @pytest.mark.integration
    def test_check_dataset_mapping_aggregates_multiple_errors(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test that dataset mapping validation aggregates multiple validation errors.

        Verifies that when multiple different validation errors occur simultaneously
        (missing source files, absolute destination paths), all errors are collected
        and reported in a single exception message with proper error aggregation format
        rather than failing on the first error encountered.
        """
        # Arrange: Create mapping with multiple different validation errors
        test_dir = tmp_path / "multi_error_test"
        test_dir.mkdir()
        existing_file = test_dir / "existing.txt"
        existing_file.touch()

        missing_file1 = Path("missing1.txt")
        missing_file2 = Path("missing2.txt")
        absolute_dest = Path("/absolute/destination.txt")

        multi_error_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "pipeline1": {
                # Error type 1: Missing source files
                missing_file1: (Path("output1.txt"), None, None),
                missing_file2: (Path("output2.txt"), None, None),
                # Error type 2: Absolute destination paths
                existing_file: (absolute_dest, None, None),
            },
        }

        # Act & Assert: Should raise exception with all aggregated error messages
        with pytest.raises(DatasetWrapper.InvalidDatasetMappingError) as exc_info:
            dataset_wrapper.check_dataset_mapping(multi_error_mapping)

        # Assert: Error message should contain all validation failures with proper aggregation
        error_message = str(exc_info.value)

        # Verify error message follows expected format from source code
        assert error_message.startswith(
            "Dataset mapping validation failed:",
        ), f"Expected error message to start with validation prefix, got: {error_message}"

        # Check for missing source file errors with exact format
        expected_missing1_error = f"Source path {missing_file1} does not exist"
        expected_missing2_error = f"Source path {missing_file2} does not exist"
        assert (
            expected_missing1_error in error_message
        ), f"Expected exact missing1.txt error '{expected_missing1_error}' in aggregated message, got: {error_message}"
        assert (
            expected_missing2_error in error_message
        ), f"Expected exact missing2.txt error '{expected_missing2_error}' in aggregated message, got: {error_message}"

        # Check for absolute destination path error with exact format
        expected_absolute_error = f"Destination path {absolute_dest} must be relative"
        assert (
            expected_absolute_error in error_message
        ), f"Expected exact relative path error '{expected_absolute_error}' in aggregated message, got: {error_message}"

        # Verify error aggregation uses semicolon separator as per source implementation
        assert ";" in error_message, f"Expected semicolon separator in aggregated error message, got: {error_message}"

        # Count the number of errors to ensure all are captured (3 expected)
        error_count = (
            error_message.count(expected_missing1_error)
            + error_message.count(
                expected_missing2_error,
            )
            + error_message.count(expected_absolute_error)
        )
        assert error_count == 3, f"Expected exactly 3 validation errors to be aggregated, found {error_count}"

    @pytest.mark.integration
    def test_check_dataset_mapping_error_aggregation_integration(
        self,
        dataset_wrapper: DatasetWrapper,
        tmp_path: Path,
    ) -> None:
        """
        Test dataset mapping validation aggregates multiple simultaneous validation errors.

        Verifies that when multiple different validation errors occur simultaneously
        (missing source files, absolute destination paths, duplicate source resolution,
        destination collisions), all errors are collected and reported in a single
        InvalidDatasetMappingError exception with proper aggregation format.

        Tests the comprehensive error aggregation behavior across all validation
        components without mocking core business logic, ensuring real integration
        behavior where all validation failures are detected in a single pass.
        """
        # Arrange: Create mapping with multiple different validation errors
        test_dir = tmp_path / "error_aggregation_test"
        test_dir.mkdir()
        existing_file = test_dir / "existing.txt"
        existing_file.write_text("content")

        # Create additional files for collision and duplicate source testing
        existing_file2 = test_dir / "existing2.txt"
        existing_file2.write_text("content2")

        existing_file3 = test_dir / "existing3.txt"
        existing_file3.write_text("content3")

        # Create symlink to test duplicate source resolution
        link_file = test_dir / "link_to_existing.txt"
        link_file.symlink_to(existing_file)

        nonexistent_file1 = Path("missing1.txt")
        nonexistent_file2 = Path("missing2.txt")
        absolute_dest = Path("/absolute/path.txt")
        collision_dest = Path("collision.txt")

        multi_error_mapping: dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]] = {
            "pipeline1": {
                # Error type 1: Missing source files
                nonexistent_file1: (Path("output1.txt"), None, None),
                nonexistent_file2: (Path("output2.txt"), None, None),
                # Error type 2: Absolute destination paths
                existing_file: (absolute_dest, None, None),
                # Error type 3: Duplicate source resolution (symlink to existing_file)
                link_file: (Path("output3.txt"), None, None),
                # Error type 4: Destination collision (multiple sources to same destination)
                existing_file2: (collision_dest, None, None),
                existing_file3: (collision_dest, None, None),
            },
        }

        # Act & Assert: Multiple errors are aggregated into single exception
        with pytest.raises(
            DatasetWrapper.InvalidDatasetMappingError,
            match=r"Dataset mapping validation failed:",
        ) as exc_info:
            dataset_wrapper.check_dataset_mapping(multi_error_mapping)

        # Assert: All validation errors are present in aggregated message
        error_message = str(exc_info.value)
        expected_errors = [
            f"Source path {nonexistent_file1} does not exist",
            f"Source path {nonexistent_file2} does not exist",
            f"Destination path {absolute_dest} must be relative",
        ]

        for expected_error in expected_errors:
            assert (
                expected_error in error_message
            ), f"Expected error '{expected_error}' in aggregated message, got: {error_message}"

        # Assert: Duplicate source resolution error is present (order may vary)
        assert (
            "both resolve to" in error_message
        ), f"Expected duplicate source resolution error in aggregated message, got: {error_message}"
        assert (
            str(existing_file) in error_message
        ), f"Expected existing_file path in error message, got: {error_message}"
        assert str(link_file) in error_message, f"Expected link_file path in error message, got: {error_message}"

        # Assert: Error aggregation format follows expected pattern
        assert ";" in error_message, f"Expected semicolon separator in aggregated error message, got: {error_message}"
        assert error_message.startswith(
            "Dataset mapping validation failed:",
        ), f"Expected validation failure prefix, got: {error_message}"

        # Assert: Verify specific error counts for comprehensive validation coverage
        missing_file_errors = error_message.count("does not exist")
        absolute_path_errors = error_message.count("must be relative")
        duplicate_source_errors = error_message.count("both resolve to")
        destination_collision_errors = error_message.count("is the same for source paths")

        assert missing_file_errors == 2, f"Expected exactly 2 missing file errors, found {missing_file_errors}"
        assert absolute_path_errors == 1, f"Expected exactly 1 absolute path error, found {absolute_path_errors}"
        assert (
            duplicate_source_errors == 1
        ), f"Expected exactly 1 duplicate source error, found {duplicate_source_errors}"
        assert (
            destination_collision_errors == 1
        ), f"Expected exactly 1 destination collision error, found {destination_collision_errors}"

        total_expected_errors = 5
        total_found_errors = (
            missing_file_errors + absolute_path_errors + duplicate_source_errors + destination_collision_errors
        )
        assert total_found_errors == total_expected_errors, (
            f"Expected exactly {total_expected_errors} validation errors total, "
            f"found {total_found_errors} in: {error_message}"
        )
