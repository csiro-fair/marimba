"""
End-to-End tests for collection operations.

These tests validate data import, collection management, and batch operations.
"""

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import (
    TestDataFactory,
    assert_cli_success,
)


@pytest.mark.e2e
class TestCollectionImport:
    """Test data import and collection creation workflows."""

    def test_collection_import_basic_workflow(
        self,
        runner: CliRunner,
        temp_project_dir: Path,
        temp_data_dir: Path,
    ) -> None:
        """Test basic data import workflow creates collection structure.

        Verifies that importing data into a new collection successfully creates
        the collection directory and configuration file.
        """
        # Arrange: Create project first
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Act: Import data into new collection
        result = runner.invoke(
            app,
            ["import", "test_collection", str(temp_data_dir), "--project-dir", str(temp_project_dir)],
        )

        # Assert: Import should succeed
        assert_cli_success(result, context="Collection import")

        # Assert: Collection directory structure is created
        collection_dir = temp_project_dir / "collections" / "test_collection"
        assert collection_dir.exists(), "Collection directory should be created"

        collection_config = collection_dir / "collection.yml"
        assert collection_config.exists(), "Collection config should be created"

    def test_import_with_config_options(
        self,
        runner: CliRunner,
        temp_project_dir: Path,
        temp_data_dir: Path,
        test_data_factory: TestDataFactory,
    ) -> None:
        """Test import with JSON configuration options.

        Verifies that JSON configuration data passed via --config flag is properly
        parsed, applied during import, and persisted to the collection.yml file
        with correct values and structure.
        """
        # Arrange: Create project first
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Arrange: Generate test configuration data using factory
        collection_config_data = test_data_factory.create_collection_config(
            site_id="FACTORY_SITE_01",
            field_of_view="1500",
        )
        config_json = json.dumps(
            {
                "site_id": collection_config_data["site_id"],
                "field_of_view": collection_config_data["field_of_view"],
            },
        )

        # Act: Import collection with JSON configuration
        result = runner.invoke(
            app,
            [
                "import",
                "test_collection",
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--config",
                config_json,
            ],
        )

        # Assert: Import command should succeed
        assert_cli_success(result, context="Import with config")

        # Assert: Collection directory structure is properly created
        collection_dir = temp_project_dir / "collections" / "test_collection"
        assert collection_dir.exists(), "Collection directory should be created after import"

        collection_config = collection_dir / "collection.yml"
        assert collection_config.exists(), "Collection config file should be created after import"

        # Assert: Configuration values are correctly persisted
        parsed_config = yaml.safe_load(collection_config.read_text())
        assert isinstance(parsed_config, dict), "Config should be parseable as YAML dictionary"
        assert parsed_config.get("site_id") == "FACTORY_SITE_01", "Config should contain correct site_id value"
        assert parsed_config.get("field_of_view") == "1500", "Config should contain correct field_of_view value"

    def test_import_with_overwrite_and_operations(
        self,
        runner: CliRunner,
        temp_project_dir: Path,
        temp_data_dir: Path,
    ) -> None:
        """Test import with overwrite flag and different file operations.

        Validates three critical import scenarios:
        1. Overwriting existing collections with --overwrite flag succeeds
        2. Import with --operation copy creates proper collection structure
        3. Import with --operation link creates proper collection structure

        Each operation should create a valid collection.yml config file and
        maintain proper directory structure without interfering with other collections.
        """
        # Arrange: Create project and initial collection
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        result = runner.invoke(
            app,
            ["import", "test_collection", str(temp_data_dir), "--project-dir", str(temp_project_dir)],
        )
        assert_cli_success(result, context="Initial collection import")

        # Arrange: Verify initial collection exists before overwrite test
        initial_collection_dir = temp_project_dir / "collections" / "test_collection"
        assert initial_collection_dir.exists(), "Initial collection should exist before overwrite test"

        # Act: Import with overwrite flag to replace existing collection
        result = runner.invoke(
            app,
            ["import", "test_collection", str(temp_data_dir), "--project-dir", str(temp_project_dir), "--overwrite"],
        )

        # Assert: Overwrite import should succeed and collection should still exist
        assert_cli_success(result, context="Import with overwrite flag")
        assert (
            initial_collection_dir.exists()
        ), f"Collection directory {initial_collection_dir} should exist after overwrite"
        assert (
            initial_collection_dir / "collection.yml"
        ).exists(), f"Collection config {initial_collection_dir / 'collection.yml'} should exist after overwrite"

        # Act: Import with copy operation
        result = runner.invoke(
            app,
            [
                "import",
                "test_collection_copy",
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--operation",
                "copy",
            ],
        )

        # Assert: Copy operation import should succeed with proper structure
        assert_cli_success(result, context="Import with copy operation")

        copy_collection_dir = temp_project_dir / "collections" / "test_collection_copy"
        assert copy_collection_dir.exists(), f"Copy collection directory {copy_collection_dir} should be created"
        assert (
            copy_collection_dir / "collection.yml"
        ).exists(), f"Copy collection config {copy_collection_dir / 'collection.yml'} should be created"

        # Act: Import with link operation
        result = runner.invoke(
            app,
            [
                "import",
                "test_collection_link",
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--operation",
                "link",
            ],
        )

        # Assert: Link operation import should succeed with proper structure
        assert_cli_success(result, context="Import with link operation")

        link_collection_dir = temp_project_dir / "collections" / "test_collection_link"
        assert link_collection_dir.exists(), f"Link collection directory {link_collection_dir} should be created"
        assert (
            link_collection_dir / "collection.yml"
        ).exists(), f"Link collection config {link_collection_dir / 'collection.yml'} should be created"

    def test_import_with_complex_config(
        self,
        runner: CliRunner,
        temp_project_dir: Path,
        temp_data_dir: Path,
        test_data_factory: TestDataFactory,
    ) -> None:
        """Test import with complex nested configuration parsing.

        Verifies that complex nested JSON configurations with multiple levels
        of nesting (metadata objects, depth ranges) are properly parsed by the CLI,
        applied during import, and correctly persisted to collection.yml files.
        """
        # Arrange: Create project
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Arrange: Generate complex nested configuration using factory
        base_config = test_data_factory.create_collection_config(
            site_id="COMPLEX_SITE_01",
            field_of_view="2000",
            instrument_type="flowcam",
        )

        # Arrange: Add complex nested structures for this specific test
        complex_config = {
            **base_config,
            "depth_range": {"min": 5.0, "max": 25.0},
            "metadata": {"operator": "test_user", "mission": "test_mission_2024"},
        }
        config_json = json.dumps(complex_config)

        # Act: Import collection with complex nested configuration
        result = runner.invoke(
            app,
            [
                "import",
                "batch_complex",
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--operation",
                "link",
                "--config",
                config_json,
            ],
        )

        # Assert: Import should succeed with complex config parsing
        assert_cli_success(result, context="Import with complex config")

        # Assert: Collection directory structure is created
        collection_dir = temp_project_dir / "collections" / "batch_complex"
        assert collection_dir.exists(), "Collection directory should be created after complex config import"

        collection_config_path = collection_dir / "collection.yml"
        assert collection_config_path.exists(), "Collection config should be created after complex config import"

        # Assert: Configuration content should contain all expected values with proper structure
        parsed_config = yaml.safe_load(collection_config_path.read_text())
        assert isinstance(parsed_config, dict), "Config should be parseable as YAML dictionary"

        # Assert: Verify top-level values are correctly persisted
        assert parsed_config.get("site_id") == "COMPLEX_SITE_01", "Config should contain correct site_id"
        assert parsed_config.get("field_of_view") == "2000", "Config should contain correct field_of_view"
        assert parsed_config.get("instrument_type") == "flowcam", "Config should contain correct instrument_type"

        # Assert: Verify nested depth_range structure is correctly persisted
        assert "depth_range" in parsed_config, "Parsed config should contain depth_range nested object"
        depth_range = parsed_config["depth_range"]
        assert isinstance(depth_range, dict), "Depth range should be a dictionary"
        assert depth_range.get("min") == 5.0, "Depth range should contain correct min value"
        assert depth_range.get("max") == 25.0, "Depth range should contain correct max value"

        # Assert: Verify nested metadata structure is correctly persisted
        assert "metadata" in parsed_config, "Parsed config should contain metadata nested object"
        metadata = parsed_config["metadata"]
        assert isinstance(metadata, dict), "Metadata should be a dictionary"
        assert metadata.get("operator") == "test_user", "Metadata should contain correct operator"
        assert metadata.get("mission") == "test_mission_2024", "Metadata should contain correct mission"


@pytest.mark.e2e
class TestCollectionDeletion:
    """Test collection deletion and batch operations."""

    def test_delete_nonexistent_collection_fails(self, runner: CliRunner, temp_project_dir: Path) -> None:
        """Test deletion of non-existent collection fails gracefully.

        Verifies that attempting to delete a collection that doesn't exist
        returns appropriate error code and message without affecting project structure.
        """
        # Arrange: Create project first
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Act: Attempt to delete non-existent collection
        result = runner.invoke(
            app,
            ["delete", "collection", "nonexistent_collection", "--project-dir", str(temp_project_dir)],
        )

        # Assert: Command should fail with appropriate error
        assert result.exit_code != 0, "Deleting non-existent collection should return non-zero exit code"
        assert result.output, "Error output should contain error message, not be empty"

        # Assert: Error message should be meaningful and specific
        error_output = result.output.lower()
        expected_error_indicators = ["not found", "does not exist", "error"]
        assert any(
            phrase in error_output for phrase in expected_error_indicators
        ), f"Error message should contain one of {expected_error_indicators}, but got: {result.output}"

        # Assert: Project structure should remain intact after failed deletion
        assert temp_project_dir.exists(), "Project directory should remain after failed deletion"
        assert (temp_project_dir / ".marimba").exists(), "Marimba config should remain after failed deletion"
        assert (temp_project_dir / "collections").exists(), "Collections directory should remain after failed deletion"

    def test_batch_collection_operations(self, runner: CliRunner, temp_project_dir: Path, temp_data_dir: Path) -> None:
        """Test batch deletion of multiple collections.

        Verifies that multiple collections can be created and then
        deleted in a single batch operation with proper error handling.
        """
        # Arrange: Create project
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Arrange: Create multiple collections using marimba new collection
        collection_names = ["batch_test_1", "batch_test_2", "batch_test_3", "batch_test_4"]

        for collection_name in collection_names:
            result = runner.invoke(
                app,
                [
                    "new",
                    "collection",
                    collection_name,
                    "--project-dir",
                    str(temp_project_dir),
                    "--config",
                    '{"test": "data"}',
                ],
            )
            assert_cli_success(result, context=f"Create collection {collection_name}")

        # Assert: Verify all collections were created successfully
        for collection_name in collection_names:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert collection_dir.exists(), f"Collection {collection_name} should exist after creation"

        # Act: Test batch delete with first 3 collections
        collections_to_delete = collection_names[:3]
        result = runner.invoke(
            app,
            ["delete", "collection", *collections_to_delete, "--project-dir", str(temp_project_dir)],
        )

        # Assert: Batch delete should succeed
        assert_cli_success(result, context="Batch delete collections")

        # Assert: Verify deleted collections no longer exist
        for collection_name in collections_to_delete:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert not collection_dir.exists(), f"Collection {collection_name} should be deleted after batch delete"

        # Assert: Verify remaining collection still exists
        remaining_collection = collection_names[3]
        remaining_dir = temp_project_dir / "collections" / remaining_collection
        assert remaining_dir.exists(), f"Collection {remaining_collection} should still exist after batch delete"

    def test_flowcam_style_workflow(self, runner: CliRunner, temp_project_dir: Path, temp_data_dir: Path) -> None:
        """Test workflow mimicking FlowCam processing patterns.

        Tests a realistic workflow of creating multiple collections,
        verifying their existence, then performing batch deletion.
        """
        # Arrange: Step 1 - Create project
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Act: Step 2 - Create sample collections for batch deletion test
        collection_names = ["CS17August2022", "CS20August2022", "OW43August2022"]
        for collection_name in collection_names:
            result = runner.invoke(
                app,
                [
                    "new",
                    "collection",
                    collection_name,
                    "--project-dir",
                    str(temp_project_dir),
                    "--config",
                    '{"test": "data"}',
                ],
            )
            assert_cli_success(result, context=f"Create collection {collection_name}")

        # Assert: Step 3 - Verify collections were created with proper structure
        for collection_name in collection_names:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert collection_dir.exists(), f"Collection {collection_name} directory should exist after creation"
            collection_config = collection_dir / "collection.yml"
            assert collection_config.exists(), f"Collection {collection_name} config file should exist after creation"

        # Act: Step 4 - Test batch delete multiple collections
        result = runner.invoke(
            app,
            ["delete", "collection", *collection_names, "--project-dir", str(temp_project_dir)],
        )
        assert_cli_success(result, context="Batch collection deletion")

        # Assert: Verify deleted collections no longer exist
        for collection_name in collection_names:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert not collection_dir.exists(), f"Collection {collection_name} should be deleted"

        # Act: Step 5 - Test import with config options (should succeed)
        result = runner.invoke(
            app,
            [
                "import",
                "test_collection",
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--config",
                '{"site_id": "TEST01", "field_of_view": "1000"}',
            ],
        )
        # Assert: Import should succeed - config parsing works without pipelines
        assert_cli_success(result, context="Import with config")

        # Assert: Verify collection was created
        test_collection_dir = temp_project_dir / "collections" / "test_collection"
        assert test_collection_dir.exists(), "Test collection should be created"


@pytest.mark.e2e
class TestCollectionWorkflows:
    """Test complex collection workflows and simulations."""

    def test_collection_import_and_batch_delete_workflow(
        self,
        runner: CliRunner,
        temp_project_dir: Path,
        temp_data_dir: Path,
    ) -> None:
        """Test complete collection import and batch deletion workflow.

        Validates the end-to-end workflow of importing multiple collections
        and then performing batch deletion operations to ensure CLI integration
        works correctly for typical user workflows.
        """
        # Arrange: Create project and define collection names
        collection_names = ["test_025", "test_026", "test_045"]
        result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
        assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

        # Act: Import multiple collections
        imported_collections = []
        for collection_name in collection_names:
            result = runner.invoke(
                app,
                ["import", collection_name, str(temp_data_dir), "--project-dir", str(temp_project_dir)],
            )
            assert_cli_success(result, context=f"Import collection {collection_name}")
            imported_collections.append(collection_name)

        # Assert: Verify all collections were created with proper structure
        for collection_name in imported_collections:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert collection_dir.exists(), f"Collection directory {collection_name} should exist after import"
            assert (
                collection_dir / "collection.yml"
            ).exists(), f"Collection config {collection_name} should exist after import"

        # Act: Perform batch deletion of all imported collections
        result = runner.invoke(
            app,
            ["delete", "collection", *collection_names, "--project-dir", str(temp_project_dir)],
        )

        # Assert: Batch deletion should succeed
        assert_cli_success(result, context="Batch deletion of imported collections")

        # Assert: Verify all collections were completely deleted
        for collection_name in collection_names:
            collection_dir = temp_project_dir / "collections" / collection_name
            assert (
                not collection_dir.exists()
            ), f"Collection directory {collection_name} should not exist after deletion"


@pytest.mark.e2e
def test_delete_operations_workflow(
    runner: CliRunner,
    temp_project_dir: Path,
    temp_data_dir: Path,
) -> None:
    """Test comprehensive delete operations workflow with multiple scenarios.

    This e2e workflow test validates the complete lifecycle of collection deletion
    operations including creation, verification, batch deletion, error handling,
    and cleanup verification across multiple collection types and operations.
    """
    # Arrange: Create project and define collection configuration
    collections_config = [
        ("workflow_copy", "copy"),
        ("workflow_link", "link"),
        ("workflow_copy2", "copy"),
        ("workflow_link2", "link"),
    ]
    config_json = '{"site_id": "WORKFLOW_TEST", "field_of_view": "2000"}'

    result = runner.invoke(app, ["new", "project", str(temp_project_dir)])
    assert_cli_success(result, expected_message="Created new Marimba project", context="Project creation")

    # Act: Create multiple collections with different operations
    for collection_name, operation in collections_config:
        result = runner.invoke(
            app,
            [
                "import",
                collection_name,
                str(temp_data_dir),
                "--project-dir",
                str(temp_project_dir),
                "--operation",
                operation,
                "--config",
                config_json,
            ],
        )
        assert_cli_success(result, context=f"Import collection {collection_name} with {operation} operation")

    # Assert: Verify all collections were created with proper structure
    for collection_name, _ in collections_config:
        collection_dir = temp_project_dir / "collections" / collection_name
        assert collection_dir.exists(), f"Collection directory {collection_name} should exist after import"
        assert (
            collection_dir / "collection.yml"
        ).exists(), f"Collection config file for {collection_name} should exist"

    # Act: Test single collection deletion
    single_delete_target = "workflow_copy"
    result = runner.invoke(
        app,
        ["delete", "collection", single_delete_target, "--project-dir", str(temp_project_dir)],
    )
    assert_cli_success(result, context="Single collection deletion")

    # Assert: Verify single collection was completely deleted
    deleted_dir = temp_project_dir / "collections" / single_delete_target
    assert not deleted_dir.exists(), f"Single collection {single_delete_target} should not exist after deletion"

    # Act: Test batch deletion of remaining collections
    remaining_collections = ["workflow_link", "workflow_copy2", "workflow_link2"]
    result = runner.invoke(
        app,
        ["delete", "collection", *remaining_collections, "--project-dir", str(temp_project_dir)],
    )
    assert_cli_success(result, context="Batch deletion of remaining collections")

    # Assert: Verify all remaining collections were completely deleted
    for collection_name in remaining_collections:
        collection_dir = temp_project_dir / "collections" / collection_name
        assert not collection_dir.exists(), f"Collection {collection_name} should not exist after batch deletion"

    # Act: Test error handling for non-existent collection deletion
    result = runner.invoke(
        app,
        ["delete", "collection", "nonexistent_collection", "--project-dir", str(temp_project_dir)],
    )

    # Assert: Should fail gracefully for non-existent collections
    assert result.exit_code != 0, "Deleting non-existent collection should return non-zero exit code"
    assert result.output, "Error output should contain error message, not be empty"
    error_output = result.output.lower()
    expected_error_indicators = ["not found", "does not exist", "error"]
    assert any(
        phrase in error_output for phrase in expected_error_indicators
    ), f"Error message should contain one of {expected_error_indicators}, but got: {result.output}"

    # Assert: Verify project structure remains intact after all deletions
    assert temp_project_dir.exists(), "Project root directory should remain after collection deletions"
    assert (temp_project_dir / ".marimba").exists(), "Marimba config directory should remain intact"
    assert (temp_project_dir / "collections").exists(), "Collections directory should still exist (empty)"
    assert (temp_project_dir / "pipelines").exists(), "Pipelines directory should remain intact"
    assert (temp_project_dir / "datasets").exists(), "Datasets directory should remain intact"

    # Assert: Collections directory should be empty after all deletions
    collections_dir = temp_project_dir / "collections"
    remaining_items = list(collections_dir.iterdir())
    assert (
        len(remaining_items) == 0
    ), f"Collections directory should be empty after all deletions, but contains: {remaining_items}"
