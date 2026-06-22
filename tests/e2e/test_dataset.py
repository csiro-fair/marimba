"""
End-to-End tests for dataset operations.

These tests validate dataset packaging, metadata handling, and dataset management workflows.
This module tests the complete CLI workflows from project creation through dataset packaging
and deletion, ensuring proper filesystem operations and user feedback.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import (
    TestDataFactory,
    assert_cli_failure,
    assert_cli_success,
)


@pytest.mark.e2e
class TestDatasetPackaging:
    """Test dataset packaging and creation workflows."""

    def test_package_basic_workflow_success(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test dataset packaging workflow creates expected structure.

        Verifies that package command with required parameters successfully
        creates dataset directory structure and metadata files.
        """
        # Arrange: Set up project and data directories
        project_dir = tmp_path / "test_project"
        data_dir = tmp_path / "sample_data"
        data_dir.mkdir()

        # Create sample test data
        TestDataFactory.create_test_images(data_dir, image_count=2, image_size="minimal")
        (data_dir / "metadata.txt").write_text("sample metadata")

        # Arrange: Create project infrastructure
        result = cli_runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(
            result,
            expected_message="Created new Marimba project",
            context="Project creation for dataset package workflow",
        )

        # Arrange: Import data to create collection for packaging
        result = cli_runner.invoke(
            app,
            ["import", "test_collection", str(data_dir), "--project-dir", str(project_dir)],
        )
        # Import should succeed for basic test data
        assert_cli_success(result, context="Collection import for dataset packaging")

        # Act: Package the dataset with required parameters
        result = cli_runner.invoke(
            app,
            [
                "package",
                "test_dataset",
                "--collection-name",
                "test_collection",
                "--project-dir",
                str(project_dir),
                "--version",
                "1.0",
                "--contact-name",
                "Test User",
                "--contact-email",
                "test@example.com",
            ],
        )

        # Assert: Package command should succeed
        assert_cli_success(result, context="Dataset packaging")

        # Assert: Dataset directory structure is created
        dataset_dir = project_dir / "datasets" / "test_dataset"
        assert dataset_dir.exists(), "Dataset directory should be created"
        assert dataset_dir.is_dir(), "Dataset path should be a directory"

        # Assert: Dataset files are created with proper structure
        manifest_file = dataset_dir / "manifest.txt"
        summary_file = dataset_dir / "summary.md"
        assert manifest_file.exists(), "Dataset manifest file should be created"
        assert manifest_file.is_file(), "Manifest should be a file, not a directory"
        assert summary_file.exists(), "Dataset summary file should be created"
        assert summary_file.is_file(), "Summary should be a file, not a directory"

        # Assert: Manifest contains expected content structure
        manifest_content = manifest_file.read_text()
        assert len(manifest_content.strip()) > 0, "Manifest file should not be empty"
        assert "summary.md" in manifest_content, "Manifest should reference dataset files"
        assert ":" in manifest_content, "Manifest should contain hash entries with colons"

        # Assert: Summary file contains meaningful content
        summary_content = summary_file.read_text()
        assert len(summary_content.strip()) > 0, "Summary file should not be empty"

    def test_package_workflow_with_metadata_options(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test dataset packaging accepts comprehensive metadata options.

        Tests the package command's ability to handle complex metadata configurations
        including YAML output format, multiple metadata levels (project and collection),
        and collision handling flags. This ensures the CLI properly validates and
        processes all supported metadata options without errors.
        """
        # Arrange: Set up project and data directories
        project_dir = tmp_path / "test_project"
        data_dir = tmp_path / "sample_data"
        data_dir.mkdir()

        # Create sample test data
        TestDataFactory.create_test_images(data_dir, image_count=2, image_size="minimal")
        (data_dir / "metadata.txt").write_text("sample metadata")

        # Arrange: Create project infrastructure
        result = cli_runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(
            result,
            expected_message="Created new Marimba project",
            context="Project creation for metadata options test",
        )

        # Arrange: Import data to create collection for packaging
        result = cli_runner.invoke(
            app,
            ["import", "test_collection", str(data_dir), "--project-dir", str(project_dir)],
        )
        assert_cli_success(result, context="Collection import for metadata options test")

        # Act: Execute package command with comprehensive metadata options
        result = cli_runner.invoke(
            app,
            [
                "package",
                "test_dataset_meta",
                "--collection-name",
                "test_collection",
                "--project-dir",
                str(project_dir),
                "--version",
                "1.0",
                "--contact-name",
                "Test User",
                "--contact-email",
                "test@example.com",
                "--metadata-output",
                "yaml",
                "--metadata-level",
                "project",
                "--metadata-level",
                "collection",
                "--allow-destination-collisions",
            ],
        )

        # Assert: Command should succeed with metadata options
        assert_cli_success(result, context="Package command with metadata options")

        # Assert: Dataset is created with metadata options applied
        dataset_dir = project_dir / "datasets" / "test_dataset_meta"
        assert dataset_dir.exists(), "Dataset directory should be created with metadata options"
        assert dataset_dir.is_dir(), "Dataset path should be a directory, not a file"

        # Assert: Expected dataset files exist with correct structure
        manifest_file = dataset_dir / "manifest.txt"
        summary_file = dataset_dir / "summary.md"
        assert manifest_file.exists(), "Dataset manifest should be created with metadata options"
        assert manifest_file.is_file(), "Manifest should be a file, not a directory"
        assert summary_file.exists(), "Dataset summary should be created with metadata options"

        # Assert: Manifest contains expected content structure
        manifest_content = manifest_file.read_text()
        assert len(manifest_content.strip()) > 0, "Manifest file should not be empty"
        assert "summary.md" in manifest_content, "Manifest should reference dataset files"
        assert ":" in manifest_content, "Manifest should contain hash entries with colons"

        # Assert: Summary file contains meaningful content
        summary_content = summary_file.read_text()
        assert len(summary_content.strip()) > 0, "Summary file should not be empty"

    def test_package_dry_run_preserves_filesystem(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test dataset packaging dry run does not modify filesystem.

        Verifies that the package command with --dry-run flag processes all options
        correctly without creating actual dataset files or modifying the filesystem.
        This ensures the dry-run functionality provides safe preview capabilities.
        """
        # Arrange: Set up project and data directories
        project_dir = tmp_path / "test_project"
        data_dir = tmp_path / "sample_data"
        data_dir.mkdir()

        # Create sample test data
        TestDataFactory.create_test_images(data_dir, image_count=2, image_size="minimal")
        (data_dir / "metadata.txt").write_text("sample metadata")

        # Arrange: Create project infrastructure
        result = cli_runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(
            result,
            expected_message="Created new Marimba project",
            context="Project creation for dry run test",
        )

        # Arrange: Import data to create collection for packaging
        result = cli_runner.invoke(
            app,
            ["import", "test_collection", str(data_dir), "--project-dir", str(project_dir)],
        )
        assert_cli_success(result, context="Collection import for dry run test")

        # Arrange: Capture initial filesystem state for comprehensive comparison
        datasets_dir = project_dir / "datasets"
        initial_datasets_exist = datasets_dir.exists()
        initial_dataset_dirs = list(datasets_dir.iterdir()) if initial_datasets_exist else []
        test_dataset_dir = datasets_dir / "test_dataset_dry"

        # Capture complete filesystem state before dry run
        initial_dataset_count = len(initial_dataset_dirs)
        initial_manifest_exists = (test_dataset_dir / "manifest.txt").exists()
        initial_summary_exists = (test_dataset_dir / "summary.md").exists()

        # Act: Execute package command with dry run option
        result = cli_runner.invoke(
            app,
            [
                "package",
                "test_dataset_dry",
                "--collection-name",
                "test_collection",
                "--project-dir",
                str(project_dir),
                "--version",
                "1.0",
                "--contact-name",
                "Test User",
                "--contact-email",
                "test@example.com",
                "--dry-run",
            ],
        )

        # Assert: Dry run command should succeed with proper exit code
        assert_cli_success(result, context="Dry run package command should succeed")

        # Assert: Dry run output should indicate simulation behavior
        assert (
            "dry" in result.output.lower() or "simulation" in result.output.lower()
        ), f"Dry run output should indicate simulation behavior. Output: {result.output}"

        # Assert: Filesystem should remain completely unchanged
        final_datasets_exist = datasets_dir.exists()
        final_dataset_dirs = list(datasets_dir.iterdir()) if final_datasets_exist else []
        final_dataset_count = len(final_dataset_dirs)

        assert (
            initial_datasets_exist == final_datasets_exist
        ), "Dry run should not change datasets directory existence state"
        assert initial_dataset_count == final_dataset_count, (
            f"Dry run should not create or remove dataset directories: "
            f"initial count={initial_dataset_count}, final count={final_dataset_count}"
        )
        assert not test_dataset_dir.exists(), f"Dry run should not create target dataset directory: {test_dataset_dir}"

        # Assert: No dataset files should be created during dry run
        manifest_file = test_dataset_dir / "manifest.txt"
        summary_file = test_dataset_dir / "summary.md"

        assert not initial_manifest_exists, "Manifest should not exist initially"
        assert not manifest_file.exists(), "Dry run should not create manifest.txt file"
        assert not initial_summary_exists, "Summary should not exist initially"
        assert not summary_file.exists(), "Dry run should not create summary.md file"


@pytest.mark.e2e
class TestDatasetDeletion:
    """Test dataset deletion workflows."""

    def test_delete_nonexistent_dataset_fails_gracefully(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test dataset deletion handles non-existent datasets with proper error message.

        Verifies that the delete dataset command fails gracefully when attempting to delete
        a dataset that does not exist, providing a clear and informative error message
        that includes the dataset name.
        """
        # Arrange: Set up project directory with basic structure
        project_dir = tmp_path / "test_project"

        # Arrange: Create project infrastructure
        result = cli_runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(
            result,
            expected_message="Created new Marimba project",
            context="Project creation for dataset deletion test",
        )

        # Arrange: Verify datasets directory exists but is empty
        datasets_dir = project_dir / "datasets"
        assert datasets_dir.exists(), "Datasets directory should exist after project creation"
        assert not list(datasets_dir.iterdir()), "Datasets directory should be empty initially"

        # Act: Attempt to delete non-existent dataset
        result = cli_runner.invoke(
            app,
            ["delete", "dataset", "nonexistent_dataset", "--project-dir", str(project_dir)],
        )

        # Assert: Command should fail with appropriate error
        assert_cli_failure(
            result,
            expected_error="nonexistent_dataset",
            context="Delete non-existent dataset should fail with informative error",
        )

        # Assert: Error output should contain both dataset name and failure indication
        error_output = result.output.lower()
        assert "nonexistent_dataset" in error_output, (
            f"Error message should mention the specific dataset name 'nonexistent_dataset'. "
            f"Actual output: {result.output}"
        )

        # Assert: Error message should clearly indicate the dataset was not found
        error_indicators = ["not found", "does not exist", "cannot find", "not exist"]
        has_error_indicator = any(indicator in error_output for indicator in error_indicators)
        assert has_error_indicator, (
            f"Error message should indicate dataset was not found using one of {error_indicators}. "
            f"Actual output: {result.output}"
        )

        # Assert: Filesystem should remain unchanged after failed delete attempt
        assert datasets_dir.exists(), "Datasets directory should still exist after failed delete"
        assert not list(datasets_dir.iterdir()), "Datasets directory should remain empty after failed delete"

    def test_delete_dataset_workflow(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Test complete dataset deletion workflow from creation to removal.

        Verifies that a dataset can be successfully created, verified to exist,
        then deleted through the CLI interface. Tests both the successful
        deletion path and proper cleanup of filesystem resources.
        """
        # Arrange: Set up project and data directories
        project_dir = tmp_path / "test_project"
        data_dir = tmp_path / "sample_data"
        data_dir.mkdir()

        # Create sample test data
        TestDataFactory.create_test_images(data_dir, image_count=2, image_size="minimal")
        (data_dir / "metadata.txt").write_text("sample metadata")

        # Arrange: Create project infrastructure
        result = cli_runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(
            result,
            expected_message="Created new Marimba project",
            context="Project creation for dataset deletion workflow",
        )

        # Arrange: Import data to create collection for packaging
        result = cli_runner.invoke(
            app,
            ["import", "test_collection", str(data_dir), "--project-dir", str(project_dir)],
        )
        assert_cli_success(result, context="Collection import for dataset deletion workflow")

        # Arrange: Create dataset to be deleted
        result = cli_runner.invoke(
            app,
            [
                "package",
                "test_dataset_to_delete",
                "--collection-name",
                "test_collection",
                "--project-dir",
                str(project_dir),
                "--version",
                "1.0",
                "--contact-name",
                "Test User",
                "--contact-email",
                "test@example.com",
            ],
        )
        assert_cli_success(result, context="Dataset creation for deletion workflow")

        # Arrange: Verify dataset exists before deletion
        dataset_dir = project_dir / "datasets" / "test_dataset_to_delete"
        assert dataset_dir.exists(), "Dataset directory should exist before deletion"
        manifest_file = dataset_dir / "manifest.txt"
        assert manifest_file.exists(), "Dataset manifest should exist before deletion"

        # Act: Delete the created dataset
        result = cli_runner.invoke(
            app,
            ["delete", "dataset", "test_dataset_to_delete", "--project-dir", str(project_dir)],
        )

        # Assert: Delete command should succeed
        assert_cli_success(result, context="Dataset deletion")

        # Assert: Dataset directory should be removed
        assert not dataset_dir.exists(), "Dataset directory should be removed after deletion"

        # Assert: Dataset files should be cleaned up completely
        assert not manifest_file.exists(), "Dataset manifest should be removed after deletion"

        # Assert: Parent datasets directory should still exist (not over-deleted)
        datasets_parent_dir = project_dir / "datasets"
        assert datasets_parent_dir.exists(), "Datasets parent directory should remain after individual dataset deletion"
