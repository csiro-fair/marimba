"""Tests for marimba.core.cli.delete module."""

from pathlib import Path

import pytest
import typer
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from marimba.core.cli.delete import (
    batch_delete_operation,
    print_results,
)
from marimba.core.wrappers.project import ProjectWrapper
from marimba.main import marimba_cli
from tests.conftest import assert_cli_failure, assert_cli_success

runner = CliRunner()


@pytest.fixture
def setup_project_dir(tmp_path: Path) -> Path:
    """Set up a test project directory."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    (project_dir / ".marimba").mkdir()
    return project_dir


class TestBatchDeleteOperationSuccess:
    """Test batch_delete_operation function with successful operations."""

    @pytest.mark.unit
    def test_batch_delete_operation_all_successful(self, mocker: MockerFixture) -> None:
        """Test batch_delete_operation with all successful operations.

        This test verifies that when all delete operations succeed,
        batch_delete_operation correctly returns all items in the success list
        and no errors, maintaining the expected tuple structure and order.
        """
        # Arrange
        items = ["item1", "item2"]

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify dry_run parameter is passed correctly
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            return Path(f"/path/to/{name}")

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "test_entity",
            "Testing...",
            False,
        )

        # Assert
        assert len(success_items) == 2, "Should have exactly 2 successful operations"
        assert len(errors) == 0, "Should have no errors when all operations succeed"
        assert success_items[0] == ("item1", Path("/path/to/item1")), "First item should have correct name and path"
        assert success_items[1] == ("item2", Path("/path/to/item2")), "Second item should have correct name and path"


class TestBatchDeleteOperationMixedResults:
    """Test batch_delete_operation function with mixed success/error scenarios."""

    @pytest.mark.unit
    def test_batch_delete_operation_handles_partial_failures(self, mocker: MockerFixture) -> None:
        """Test that batch_delete_operation correctly handles mixed success and failure outcomes.

        This test verifies that when some items succeed and others fail during batch deletion,
        the function properly separates successful operations from failed ones and preserves
        the exact error messages from exceptions. It also verifies that processing continues
        for remaining items even when failures occur.
        """
        # Arrange
        items = ["successful_item1", "failing_item", "successful_item2"]
        expected_error_message = "Collection 'failing_item' not found in project"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            assert isinstance(dry_run, bool)
            if name == "failing_item":
                raise ProjectWrapper.NoSuchCollectionError(expected_error_message)
            return Path(f"/mock/path/to/{name}")

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "collection",
            "Deleting collections...",
            False,
        )

        # Assert
        assert len(success_items) == 2, "Should have exactly 2 successful operations when 2 items succeed"
        assert len(errors) == 1, "Should have exactly 1 error when 1 item fails"

        # Verify error details
        assert errors[0] == (
            "failing_item",
            expected_error_message,
        ), "Should capture exact item name and error message for failed item"

        # Verify successful operations maintain processing order
        expected_success_items = [
            ("successful_item1", Path("/mock/path/to/successful_item1")),
            ("successful_item2", Path("/mock/path/to/successful_item2")),
        ]
        assert (
            success_items == expected_success_items
        ), "Should return successful items in processing order with correct names and paths"

    @pytest.mark.unit
    def test_batch_delete_operation_preserves_processing_order(self, mocker: MockerFixture) -> None:
        """Test that batch_delete_operation preserves the order of successful operations.

        This unit test verifies that when processing multiple items with mixed
        success/failure outcomes, the batch_delete_operation maintains the original
        order for both successful and failed items in their respective result lists.

        This is a unit test because it tests the batch_delete_operation function in isolation
        with a mock delete function.
        """
        # Arrange
        items = ["first", "second_fails", "third", "fourth_fails", "fifth"]

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify dry_run parameter for test consistency
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            if "fails" in name:
                error_msg = f"Failed to delete {name}"
                raise ProjectWrapper.NoSuchCollectionError(error_msg)
            return Path(f"/order/test/{name}")

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "collection",
            "Testing order...",
            False,
        )

        # Assert
        assert len(success_items) == 3, "Should have exactly 3 successful operations"
        assert len(errors) == 2, "Should have exactly 2 failed operations"

        # Verify successful items maintain their original order
        actual_successful_names = [name for name, _ in success_items]
        expected_successful_names = ["first", "third", "fifth"]
        assert (
            actual_successful_names == expected_successful_names
        ), f"Expected successful items {expected_successful_names}, but got {actual_successful_names}"

        # Verify failed items maintain their original order
        actual_failed_names = [name for name, _ in errors]
        expected_failed_names = ["second_fails", "fourth_fails"]
        assert (
            actual_failed_names == expected_failed_names
        ), f"Expected failed items {expected_failed_names}, but got {actual_failed_names}"

        # Verify successful items have correct paths
        expected_success_paths = [
            ("first", Path("/order/test/first")),
            ("third", Path("/order/test/third")),
            ("fifth", Path("/order/test/fifth")),
        ]
        assert success_items == expected_success_paths, "Successful items should have correct names and paths"

        # Verify error messages are properly captured
        for name, error_msg in errors:
            assert (
                f"Failed to delete {name}" in error_msg
            ), f"Error message should contain deletion failure info for {name}"


class TestBatchDeleteOperationExceptionHandling:
    """Test exception handling in batch_delete_operation function."""

    @pytest.mark.unit
    def test_handles_no_such_collection_error(self, mocker: MockerFixture) -> None:
        """Test that NoSuchCollectionError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.NoSuchCollectionError is raised during
        batch deletion (e.g., when a collection doesn't exist), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["missing_collection"]
        expected_error_message = "Collection 'missing_collection' not found"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.NoSuchCollectionError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "collection",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "missing_collection",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_no_such_pipeline_error(self, mocker: MockerFixture) -> None:
        """Test that NoSuchPipelineError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.NoSuchPipelineError is raised during
        batch deletion (e.g., when a pipeline doesn't exist), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["missing_pipeline"]
        expected_error_message = "Pipeline 'missing_pipeline' not found"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.NoSuchPipelineError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "pipeline",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "missing_pipeline",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_no_such_dataset_error(self, mocker: MockerFixture) -> None:
        """Test that NoSuchDatasetError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.NoSuchDatasetError is raised during
        batch deletion (e.g., when a dataset doesn't exist), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["missing_dataset"]
        expected_error_message = "Dataset 'missing_dataset' not found"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.NoSuchDatasetError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "dataset",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "missing_dataset",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_no_such_target_error(self, mocker: MockerFixture) -> None:
        """Test that NoSuchTargetError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.NoSuchTargetError is raised during
        batch deletion (e.g., when a target doesn't exist), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["missing_target"]
        expected_error_message = "Target 'missing_target' not found"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.NoSuchTargetError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "target",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "missing_target",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_delete_pipeline_error_with_dependency_conflict(self, mocker: MockerFixture) -> None:
        """Test that DeletePipelineError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.DeletePipelineError is raised during
        pipeline deletion (e.g., due to dependency conflicts), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["problematic_pipeline"]
        expected_error_message = "Cannot delete pipeline due to dependency"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.DeletePipelineError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "pipeline",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "problematic_pipeline",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_invalid_name_error(self, mocker: MockerFixture) -> None:
        """Test that InvalidNameError is properly handled and error message captured.

        This test verifies that when ProjectWrapper.InvalidNameError is raised during
        batch deletion (e.g., due to invalid characters in name), batch_delete_operation
        correctly captures the error message and returns it in the errors list rather
        than allowing the exception to propagate uncaught.
        """
        # Arrange
        items = ["invalid@name"]
        expected_error_message = "Invalid characters in name"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise ProjectWrapper.InvalidNameError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "entity",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "invalid@name",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_file_exists_error(self, mocker: MockerFixture) -> None:
        """Test that FileExistsError is properly handled and error message captured.

        This test verifies that when ProjectWrapper raises FileExistsError during dataset
        deletion (used when dataset doesn't exist), batch_delete_operation correctly captures
        the error message and returns it in the errors list rather than allowing the exception
        to propagate uncaught.
        """
        # Arrange
        items = ["nonexistent_dataset"]
        expected_error_message = "Dataset file does not exist"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise FileExistsError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "dataset",
            "Testing...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify error details
        assert errors[0] == (
            "nonexistent_dataset",
            expected_error_message,
        ), "Should capture exact item name and error message"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"

    @pytest.mark.unit
    def test_handles_unexpected_exception(self, mocker: MockerFixture) -> None:
        """Test that unexpected exceptions are properly handled and error message captured.

        This test verifies that when an unexpected RuntimeError is raised during batch deletion
        (not one of the expected exception types), batch_delete_operation correctly captures
        the error message and returns it in the errors list rather than allowing the exception
        to propagate uncaught. It ensures robust exception handling for all error scenarios.
        """
        # Arrange
        items = ["problematic_item"]
        expected_error_message = "Unexpected system error occurred"

        def mock_delete_func(name: str, dry_run: bool) -> Path:
            # Verify parameters are passed correctly
            assert isinstance(name, str), "name should be a string"
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            raise RuntimeError(expected_error_message)

        # Act
        success_items, errors = batch_delete_operation(
            items,
            mock_delete_func,
            "entity",
            "Testing unexpected exception handling...",
            False,
        )

        # Assert
        # Verify return types
        assert isinstance(success_items, list), "Should return success_items as list"
        assert isinstance(errors, list), "Should return errors as list"

        # Verify counts
        assert len(success_items) == 0, "Should have no successful deletions"
        assert len(errors) == 1, "Should have exactly one error"

        # Verify total items processed
        total_processed = len(success_items) + len(errors)
        assert total_processed == len(items), "Should process all input items"

        # Verify error details with exact message matching
        assert errors[0] == (
            "problematic_item",
            expected_error_message,
        ), "Should capture exact item name and error message from RuntimeError"

        # Verify error tuple structure
        error_name, error_msg = errors[0]
        assert isinstance(error_name, str), "Error name should be string"
        assert isinstance(error_msg, str), "Error message should be string"
        assert error_msg == expected_error_message, "Error message should match expected RuntimeError message"


class TestPrintResults:
    """Test print_results function behavior and output formatting."""

    @pytest.mark.unit
    def test_print_results_success_only_displays_correct_messages(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test that print_results displays correct success messages and does not exit when no errors occur.

        This test verifies that when print_results is called with only successful operations
        and no errors, it correctly formats and displays success messages for each item
        without raising typer.Exit. The test ensures proper message formatting including
        entity type, item name, and path information in the expected format.
        """
        # Arrange
        success_items = [
            ("marine_collection", Path("/project/collections/marine_collection")),
            ("coastal_data", Path("/project/collections/coastal_data")),
        ]
        errors: list[tuple[str, str]] = []
        entity_type = "collection"

        mock_success_panel = mocker.patch("marimba.core.cli.delete.success_panel")
        mock_rprint = mocker.patch("marimba.core.cli.delete.rprint")

        # Act
        print_results(success_items, errors, entity_type)

        # Assert
        assert mock_success_panel.call_count == 2, "Should call success_panel once for each successful item"
        assert mock_rprint.call_count == 2, "Should call rprint once for each successful item"

        # Verify success_panel calls contain correctly formatted messages
        success_panel_calls = mock_success_panel.call_args_list

        # Verify first success message contains all required components
        first_message = success_panel_calls[0][0][0]
        assert '"marine_collection"' in first_message, "First message should contain collection name in quotes"
        assert "/project/collections/marine_collection" in first_message, "First message should contain full path"
        assert "Deleted" in first_message, "First message should indicate successful deletion"
        assert "collection" in first_message, "First message should specify entity type"

        # Verify second success message contains all required components
        second_message = success_panel_calls[1][0][0]
        assert '"coastal_data"' in second_message, "Second message should contain collection name in quotes"
        assert "/project/collections/coastal_data" in second_message, "Second message should contain full path"
        assert "Deleted" in second_message, "Second message should indicate successful deletion"
        assert "collection" in second_message, "Second message should specify entity type"

        # Verify rprint was called with success_panel return values
        for i, call in enumerate(mock_rprint.call_args_list):
            assert call[0][0] == mock_success_panel.return_value, f"rprint call {i+1} should use success_panel output"

        # Verify function completes without raising typer.Exit (implicit - test would fail if Exit was raised)

    @pytest.mark.unit
    def test_print_results_with_errors_displays_messages_and_exits(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test that print_results displays both success and error messages, then exits with code 1.

        This test verifies that when print_results is called with both successful operations and errors:
        - It displays formatted success messages for successful items
        - It displays formatted error messages for failed items
        - It raises typer.Exit with exit code 1 to indicate failures occurred
        - It calls rprint with the expected content for both success and error panels
        """
        # Arrange
        success_items = [("successful_item", Path("/project/entities/successful_item"))]
        errors = [("failed_item", "Item not found in project")]
        entity_type = "entity"

        # Mock success_panel and error_panel to capture the messages passed to them
        mock_success_panel = mocker.patch("marimba.core.cli.delete.success_panel")
        mock_error_panel = mocker.patch("marimba.core.cli.delete.error_panel")
        mock_rprint = mocker.patch("marimba.core.cli.delete.rprint")

        # Act & Assert
        with pytest.raises(typer.Exit) as exc_info:
            print_results(success_items, errors, entity_type)

        # Verify typer.Exit was raised with correct exit code
        assert exc_info.value.exit_code == 1, "Should exit with code 1 when errors are present"

        # Verify success_panel was called once for the successful item
        assert mock_success_panel.call_count == 1, "Should call success_panel once for successful item"

        # Verify error_panel was called once for the failed item
        assert mock_error_panel.call_count == 1, "Should call error_panel once for error"

        # Verify rprint was called exactly twice (once for success, once for error)
        assert mock_rprint.call_count == 2, "Should call rprint for both success and error messages"

        # Verify the content of the success_panel call
        success_message = mock_success_panel.call_args[0][0]
        assert '"successful_item"' in success_message, "Should mention successful entity name"
        assert "/project/entities/successful_item" in success_message, "Should show successful entity path"
        assert "Deleted" in success_message, "Should display success message"
        assert "entity" in success_message, "Should mention entity type"

        # Verify the content of the error_panel call
        error_message = mock_error_panel.call_args[0][0]
        assert 'Failed to delete entity "failed_item"' in error_message, "Should mention failed entity name"
        assert "Item not found in project" in error_message, "Should show specific error message"


class TestDeleteProjectCommand:
    """Test CLI delete project command integration."""

    @pytest.mark.unit
    def test_delete_project_command_successful_deletion(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test successful deletion of a Marimba project via CLI command.

        This unit test verifies that the delete project CLI command correctly:
        - Finds the project directory using the provided path
        - Creates a ProjectWrapper instance with correct parameters (dry_run=False by default)
        - Calls the delete_project method exactly once on the wrapper
        - Displays success message with the deleted project path
        - Exits with code 0 on successful completion
        - Shows no error messages for successful operations

        This test mocks external dependencies (find_project_dir_or_exit and ProjectWrapper.delete_project)
        while testing the real CLI command flow, making it appropriate for unit-level testing.
        """
        # Arrange
        expected_deleted_path = setup_project_dir

        mock_find_project = mocker.patch(
            "marimba.core.cli.delete.find_project_dir_or_exit",
            return_value=setup_project_dir,
        )
        mock_delete = mocker.patch.object(
            ProjectWrapper,
            "delete_project",
            return_value=expected_deleted_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "project", "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert_cli_success(result, context="Project deletion command")

        # Verify external dependencies were called with correct parameters
        mock_find_project.assert_called_once_with(setup_project_dir)
        mock_delete.assert_called_once_with()

        # Verify CLI output contains required success elements
        assert "Deleted" in result.output, "Should display success message with 'Deleted' text"
        assert "project" in result.output, "Should mention 'project' in success message"
        assert "test_project" in result.output, "Should show the deleted project directory name"

        # Verify no error messages appear for successful operation
        assert "Failed" not in result.output, "Should not display failure messages for successful operation"
        assert "Error" not in result.output, "Should not display error messages for successful operation"
        assert "not valid project" not in result.output, "Should not display invalid project messages"

    @pytest.mark.unit
    def test_delete_project_invalid_structure(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete project command properly handles InvalidStructureError.

        This unit test verifies that when attempting to delete a project
        with an invalid directory structure, the CLI command:
        - Catches the InvalidStructureError from ProjectWrapper initialization
        - Displays appropriate error message indicating the project is not valid
        - Exits with error code 1
        - Shows the specific error context about invalid structure
        - Does not display any success messages for the failed operation

        This is a unit test because it mocks the ProjectWrapper._check_file_structure method
        to isolate the CLI error handling behavior.
        """
        # Arrange
        expected_error_message = "Invalid project structure detected"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)

        # Mock only the specific method that would detect invalid structure
        # This allows testing the error handling path without over-mocking
        mocker.patch.object(
            ProjectWrapper,
            "_check_file_structure",
            side_effect=ProjectWrapper.InvalidStructureError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "project", "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Use the established helper function for CLI failure assertions
        assert_cli_failure(
            result,
            expected_error="not valid project",
            expected_exit_code=1,
            context="Project deletion with invalid structure",
        )

        # Verify CLI output contains specific error messages
        assert "Marimba" in result.output, "Should mention Marimba in the error message"
        assert setup_project_dir.name in result.output, "Should mention the project directory name"

        # The asserted exit code of 1 already guarantees the success branch never ran. Match on the
        # "Success" panel title rather than "Deleted": the rendered error traceback includes the
        # success_panel source line ("Project Deleted ..."), which is source code, not user-facing output.
        assert "Success" not in result.output, "Should not display the success panel for a failed operation"

    @pytest.mark.unit
    def test_delete_project_dry_run(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete project command with dry-run flag passes correct parameter.

        This unit test verifies that the delete project CLI command correctly:
        - Parses the --dry-run flag from command line arguments
        - Passes dry_run=True to the ProjectWrapper constructor
        - Still calls the delete_project method (dry-run behavior is handled within ProjectWrapper)
        - Displays success message as if the operation completed
        - Exits with success code 0
        - Shows no error messages for successful dry-run operations
        - Displays properly formatted success message with project path

        This is a unit test because it mocks the ProjectWrapper class to isolate
        the CLI dry-run flag handling behavior.
        """
        # Arrange
        expected_deleted_path = setup_project_dir

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)

        # Mock ProjectWrapper class to capture initialization arguments
        mock_project_wrapper_class = mocker.patch("marimba.core.cli.delete.ProjectWrapper")
        mock_project_wrapper_instance = mocker.MagicMock()
        mock_project_wrapper_instance.root_dir = setup_project_dir
        mock_project_wrapper_instance.delete_project.return_value = expected_deleted_path
        mock_project_wrapper_class.return_value = mock_project_wrapper_instance

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "project", "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        # Use the established helper function for CLI success assertions
        assert_cli_success(result, context="Project deletion with dry-run flag")

        # Verify ProjectWrapper was initialized with dry_run=True (most important assertion for dry-run test)
        mock_project_wrapper_class.assert_called_once_with(setup_project_dir, dry_run=True)

        # Verify delete_project was called exactly once (dry-run logic is handled within the ProjectWrapper)
        mock_project_wrapper_instance.delete_project.assert_called_once()

        # Verify CLI output contains success message and project details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert str(expected_deleted_path) in result.output, "Should show the project path"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed" not in result.output, "Should not display error messages for successful dry-run"
        assert "Error" not in result.output, "Should not display error messages for successful dry-run"

        # Verify specific success message format
        assert "Marimba project" in result.output, "Should display formatted success message with project identifier"


class TestDeleteCollectionCommand:
    """Test CLI delete collection command integration."""

    @pytest.mark.unit
    def test_delete_multiple_collections_successful(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test successful deletion of multiple collections via CLI command.

        This unit test verifies that the delete collection CLI command correctly:
        - Processes multiple collection names from command line arguments
        - Calls the ProjectWrapper.delete_collection method for each collection
        - Displays success messages for each deleted collection
        - Exits with code 0 on successful completion

        This is a unit test because it mocks the core delete_collection business logic.
        """
        # Arrange
        collection_names = ["marine_data", "coastal_survey"]
        expected_paths = [
            Path("/project/collections/marine_data"),
            Path("/project/collections/coastal_survey"),
        ]

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_collection = mocker.patch.object(
            ProjectWrapper,
            "delete_collection",
            side_effect=expected_paths,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "collection", *collection_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert_cli_success(result, context="Delete multiple collections command")

        # Verify ProjectWrapper.delete_collection was called correctly for each collection
        assert mock_delete_collection.call_count == 2, "Should call delete_collection twice"
        mock_delete_collection.assert_any_call("marine_data", False)
        mock_delete_collection.assert_any_call("coastal_survey", False)

        # Verify calls were made with correct dry_run parameter (False by default)
        for call in mock_delete_collection.call_args_list:
            assert call[0][1] is False, "Should call delete_collection with dry_run=False by default"

        # Verify CLI output contains success messages and paths for both collections
        assert "Deleted" in result.output, "Should display success messages"
        assert "marine_data" in result.output, "Should mention first collection name"
        assert "coastal_survey" in result.output, "Should mention second collection name"
        assert str(expected_paths[0]) in result.output, "Should show first collection path"
        assert str(expected_paths[1]) in result.output, "Should show second collection path"

        # Verify success message format for each collection
        for collection_name in collection_names:
            assert (
                f'collection "{collection_name}"' in result.output
            ), f"Should display formatted success message with collection name {collection_name}"

        # Verify no error messages appear
        assert "Failed to delete" not in result.output, "Should not display error messages for successful operations"

    @pytest.mark.unit
    def test_delete_collection_with_dry_run_flag(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete collection command with dry-run flag passes correct parameter to ProjectWrapper.

        This unit test verifies that the delete collection CLI command correctly:
        - Parses the --dry-run flag from command line arguments
        - Passes dry_run=True to the ProjectWrapper.delete_collection method
        - Still displays success messages as if the operation completed
        - Exits with success code 0
        - Shows the specific collection path in output

        This is a unit test because it mocks the core delete_collection business logic
        to isolate the CLI dry-run flag handling behavior.
        """
        # Arrange
        collection_names = ["test_collection"]
        expected_path = Path("/project/collections/test_collection")

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)

        # Mock only the external dependency - ProjectWrapper.delete_collection method
        # This allows testing real batch_delete_operation logic while controlling the deletion outcome
        mock_delete_collection = mocker.patch.object(
            ProjectWrapper,
            "delete_collection",
            return_value=expected_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "collection", *collection_names, "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        # Verify CLI execution succeeded
        assert_cli_success(result, context="Delete collection with dry-run flag")

        # Verify ProjectWrapper.delete_collection was called with dry_run=True (most critical assertion)
        mock_delete_collection.assert_called_once_with("test_collection", True)

        # Verify that exactly one call was made
        assert mock_delete_collection.call_count == 1, "Should call delete_collection exactly once"

        # Verify CLI output contains success message and collection details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert "test_collection" in result.output, "Should mention collection name"
        assert str(expected_path) in result.output, "Should show the collection path"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed to delete" not in result.output, "Should not display error messages for successful dry-run"

    @pytest.mark.unit
    def test_delete_collection_handles_nonexistent_collection_error(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete collection command properly handles NoSuchCollectionError.

        This unit test verifies that when attempting to delete a collection
        that doesn't exist, the CLI command:
        - Catches the NoSuchCollectionError from ProjectWrapper
        - Displays appropriate error message with collection name
        - Exits with error code 1
        - Shows the specific error message from the exception

        This is a unit test because it mocks the core delete_collection business logic.
        """
        # Arrange
        nonexistent_collection = "missing_marine_collection"
        expected_error_message = f"Collection '{nonexistent_collection}' not found in project"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_collection = mocker.patch.object(
            ProjectWrapper,
            "delete_collection",
            side_effect=ProjectWrapper.NoSuchCollectionError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "collection", nonexistent_collection, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Verify CLI execution failed with correct exit code
        assert result.exit_code == 1, f"CLI should exit with code 1 for missing collection, got: {result.output}"

        # Verify ProjectWrapper.delete_collection was called with correct parameters
        mock_delete_collection.assert_called_once_with(nonexistent_collection, False)

        # Verify that exactly one call was made (no retries or duplicates)
        assert mock_delete_collection.call_count == 1, "Should call delete_collection exactly once"

        # Verify CLI output contains error messages
        assert "Failed to delete" in result.output, "Should display failure message"
        assert nonexistent_collection in result.output, "Should mention the collection name that failed"
        assert "not found in project" in result.output, "Should display the specific error message from exception"

        # Verify that no success messages are shown for failed operation
        assert "Deleted" not in result.output, "Should not display success message for failed operation"

        # Verify error message format follows expected pattern
        assert (
            f'Failed to delete collection "{nonexistent_collection}"' in result.output
        ), "Should display formatted error message with collection name"

    @pytest.mark.unit
    def test_delete_multiple_collections_with_partial_failures(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete collection command with mixed success and failure results.

        This unit test verifies that when deleting multiple collections where some exist
        and others don't, the CLI properly processes all collections, shows appropriate success
        and error messages, and exits with error code 1 due to failures. It tests the resilience
        of batch processing where failures don't stop processing of remaining items.

        This is a unit test because it mocks the core delete_collection business logic.
        """
        # Arrange
        collection_names = ["existing_collection", "missing_collection", "another_existing"]
        existing_path = Path("/project/collections/existing_collection")
        another_existing_path = Path("/project/collections/another_existing")
        expected_error_message = "Collection 'missing_collection' not found in project"

        def mock_delete_side_effect(name: str, dry_run: bool) -> Path:
            # Verify dry_run parameter for test consistency
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"
            if name == "missing_collection":
                raise ProjectWrapper.NoSuchCollectionError(expected_error_message)
            if name == "existing_collection":
                return existing_path
            if name == "another_existing":
                return another_existing_path
            unexpected_msg = f"Unexpected collection name: {name}"
            raise ValueError(unexpected_msg)

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_collection = mocker.patch.object(
            ProjectWrapper,
            "delete_collection",
            side_effect=mock_delete_side_effect,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "collection", *collection_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert result.exit_code == 1, f"Should exit with code 1 for partial failures, got exit code: {result.exit_code}"

        # Verify all collections were attempted
        assert mock_delete_collection.call_count == 3, "Should attempt to delete all 3 collections"

        # Verify specific method calls
        expected_calls = [
            ("existing_collection", False),
            ("missing_collection", False),
            ("another_existing", False),
        ]
        for expected_name, expected_dry_run in expected_calls:
            mock_delete_collection.assert_any_call(expected_name, expected_dry_run)

        # Verify success messages for existing collections
        assert "Deleted" in result.output, "Should display success messages for existing collections"
        assert 'collection "existing_collection"' in result.output, "Should show formatted success for first collection"
        assert 'collection "another_existing"' in result.output, "Should show formatted success for second collection"
        assert str(existing_path) in result.output, "Should show path for first successful collection"
        assert str(another_existing_path) in result.output, "Should show path for second successful collection"

        # Verify error message for missing collection
        assert "Failed to delete" in result.output, "Should display failure message for missing collection"
        assert (
            'Failed to delete collection "missing_collection"' in result.output
        ), "Should show formatted error message"
        assert "not found in project" in result.output, "Should display error message about collection not found"

        # Verify batch processing resilience - all collections mentioned in output
        for collection_name in collection_names:
            assert collection_name in result.output, f"Should mention {collection_name} in output"


class TestDeleteTargetCommand:
    """Test CLI delete target command integration."""

    @pytest.mark.unit
    def test_delete_multiple_targets_successful(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test successful deletion of multiple targets via CLI command.

        This unit test verifies that the delete target CLI command correctly:
        - Processes multiple target names from command line arguments
        - Calls the ProjectWrapper.delete_target method for each target
        - Displays success messages for each deleted target
        - Exits with code 0 on successful completion

        This is a unit test because it mocks the core delete_target business logic.
        """
        # Arrange
        target_names = ["s3_target", "dap_server_target"]
        expected_paths = [
            Path("/project/targets/s3_target"),
            Path("/project/targets/dap_server_target"),
        ]

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_target = mocker.patch.object(
            ProjectWrapper,
            "delete_target",
            side_effect=expected_paths,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "target", *target_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert_cli_success(result, context="Delete multiple targets command")

        # Verify ProjectWrapper.delete_target was called correctly for each target
        assert mock_delete_target.call_count == 2, "Should call delete_target exactly twice"
        mock_delete_target.assert_any_call("s3_target", False)
        mock_delete_target.assert_any_call("dap_server_target", False)

        # Verify calls were made with correct dry_run parameter (False by default)
        for call in mock_delete_target.call_args_list:
            assert call[0][1] is False, "Should call delete_target with dry_run=False by default"

        # Verify CLI output contains success messages for each target
        assert "Deleted" in result.output, "Should display success message"
        assert "s3_target" in result.output, "Should mention first target name"
        assert "dap_server_target" in result.output, "Should mention second target name"
        assert str(expected_paths[0]) in result.output, "Should show first target path"
        assert str(expected_paths[1]) in result.output, "Should show second target path"

        # Verify no error messages appear for successful operations
        assert "Failed to delete" not in result.output, "Should not display error messages for successful operations"

        # Verify success message format for each target
        for target_name in target_names:
            assert "Deleted" in result.output, "Should display success message for each target"
            assert (
                f'target "{target_name}"' in result.output
            ), f"Should display formatted success message with target name {target_name}"

    @pytest.mark.unit
    def test_delete_target_handles_nonexistent_target_error(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete target command properly handles NoSuchTargetError.

        This unit test verifies that when attempting to delete a target
        that doesn't exist, the CLI command:
        - Catches the NoSuchTargetError from ProjectWrapper
        - Displays appropriate error message with target name
        - Exits with error code 1
        - Shows the specific error message from the exception

        This is a unit test because it mocks the core delete_target business logic.
        """
        # Arrange
        nonexistent_target = "missing_s3_target"
        expected_error_message = f"Target '{nonexistent_target}' not found in project"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_target = mocker.patch.object(
            ProjectWrapper,
            "delete_target",
            side_effect=ProjectWrapper.NoSuchTargetError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "target", nonexistent_target, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Verify CLI execution failed with correct exit code
        assert result.exit_code == 1, f"CLI should exit with code 1 for missing target, got: {result.output}"

        # Verify ProjectWrapper.delete_target was called with correct parameters
        mock_delete_target.assert_called_once_with(nonexistent_target, False)

        # Verify that exactly one call was made (no retries or duplicates)
        assert mock_delete_target.call_count == 1, "Should call delete_target exactly once"

        # Verify CLI output contains error messages
        assert "Failed to delete" in result.output, "Should display failure message"
        assert nonexistent_target in result.output, "Should mention the target name that failed"
        assert "not found" in result.output, "Should display the specific error message from exception"

        # Verify that no success messages are shown for failed operation
        assert "Deleted" not in result.output, "Should not display success message for failed operation"

        # Verify error message format follows expected pattern
        assert (
            f'Failed to delete target "{nonexistent_target}"' in result.output
        ), "Should display formatted error message with target name"

    @pytest.mark.unit
    def test_delete_target_with_dry_run_flag(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete target command with dry-run flag passes correct parameter to ProjectWrapper.

        This unit test verifies that the delete target CLI command correctly:
        - Parses the --dry-run flag from command line arguments
        - Passes dry_run=True to the ProjectWrapper.delete_target method
        - Still displays success messages as if the operation completed
        - Exits with success code 0
        - Shows the specific target path in output

        This is a unit test because it mocks the core delete_target business logic
        to isolate the CLI dry-run flag handling behavior.
        """
        # Arrange
        target_names = ["test_target"]
        expected_path = Path("/project/targets/test_target")

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)

        # Mock only the external dependency - ProjectWrapper.delete_target method
        # This allows testing real batch_delete_operation logic while controlling the deletion outcome
        mock_delete_target = mocker.patch.object(
            ProjectWrapper,
            "delete_target",
            return_value=expected_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "target", *target_names, "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        assert_cli_success(result, context="Delete target with dry-run flag")

        # Verify ProjectWrapper.delete_target was called with dry_run=True (most critical assertion)
        mock_delete_target.assert_called_once_with("test_target", True)

        # Verify that exactly one call was made
        assert mock_delete_target.call_count == 1, "Should call delete_target exactly once"

        # Verify CLI output contains success message and target details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert "test_target" in result.output, "Should mention target name"
        assert str(expected_path) in result.output, "Should show the target path"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed to delete" not in result.output, "Should not display error messages for successful dry-run"

    @pytest.mark.unit
    def test_delete_multiple_targets_with_partial_failures(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete target command with mixed success and failure results.

        This unit test verifies that when deleting multiple targets where some exist
        and others don't, the CLI properly:
        - Processes all targets in the batch
        - Shows success messages for existing targets
        - Shows error messages for missing targets
        - Exits with error code 1 due to failures

        This is a unit test because it mocks the core delete_target business logic
        to isolate the CLI batch processing and error handling behavior.
        """
        # Arrange
        target_names = ["existing_target", "missing_target", "another_existing"]
        existing_path = Path("/project/targets/existing_target")
        another_existing_path = Path("/project/targets/another_existing")
        missing_error = "Target 'missing_target' not found"

        def mock_delete_side_effect(name: str, dry_run: bool) -> Path:
            # Verify dry_run parameter is passed correctly
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"

            if name == "missing_target":
                raise ProjectWrapper.NoSuchTargetError(missing_error)
            if name == "existing_target":
                return existing_path
            if name == "another_existing":
                return another_existing_path
            unexpected_name_msg = f"Unexpected target name: {name}"
            raise ValueError(unexpected_name_msg)

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_target = mocker.patch.object(
            ProjectWrapper,
            "delete_target",
            side_effect=mock_delete_side_effect,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "target", *target_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Verify CLI execution failed due to partial failures
        assert result.exit_code == 1, f"CLI should exit with code 1 for partial failures, got: {result.output}"

        # Verify all targets were attempted
        assert mock_delete_target.call_count == 3, "Should attempt to delete all 3 targets"
        mock_delete_target.assert_any_call("existing_target", False)
        mock_delete_target.assert_any_call("missing_target", False)
        mock_delete_target.assert_any_call("another_existing", False)

        # Verify all calls were made with correct dry_run parameter
        for call in mock_delete_target.call_args_list:
            assert call[0][1] is False, "All calls should use dry_run=False by default"

        # Verify CLI output contains both success and error messages
        assert "Deleted" in result.output, "Should show success messages for existing targets"
        assert "Failed to delete" in result.output, "Should show failure message for missing target"
        assert "existing_target" in result.output, "Should mention successful target"
        assert "another_existing" in result.output, "Should mention other successful target"
        assert "missing_target" in result.output, "Should mention failed target"
        assert "not found" in result.output, "Should display specific error message"

        # Verify successful target paths appear in output
        assert str(existing_path) in result.output, "Should show path for first successful target"
        assert str(another_existing_path) in result.output, "Should show path for second successful target"

        # Verify specific success message format for successful targets
        assert 'target "existing_target"' in result.output, "Should show formatted success message for first target"
        assert 'target "another_existing"' in result.output, "Should show formatted success message for second target"

        # Verify specific error message format for failed target
        assert 'Failed to delete target "missing_target"' in result.output, "Should show formatted error message"

        # Verify that batch processing continued after failure (resilience test)
        successful_targets = ["existing_target", "another_existing"]
        for target in successful_targets:
            assert "Deleted" in result.output, f"Should show success for {target} despite other failures"


class TestDeleteDatasetCommand:
    """Test CLI delete dataset command integration."""

    @pytest.mark.unit
    def test_delete_multiple_datasets_successful(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test successful deletion of multiple datasets via CLI command.

        This unit test verifies that the delete dataset CLI command correctly:
        - Processes multiple dataset names from command line arguments
        - Calls the ProjectWrapper.delete_dataset method for each dataset
        - Displays success messages for each deleted dataset
        - Exits with code 0 on successful completion

        This is a unit test because it mocks the core delete_dataset business logic.
        """
        # Arrange
        dataset_names = ["marine_analysis_2023", "coastal_survey_results"]
        expected_paths = [
            Path("/project/datasets/marine_analysis_2023"),
            Path("/project/datasets/coastal_survey_results"),
        ]

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_dataset = mocker.patch.object(
            ProjectWrapper,
            "delete_dataset",
            side_effect=expected_paths,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "dataset", *dataset_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert_cli_success(result, context="Delete multiple datasets command")

        # Verify ProjectWrapper.delete_dataset was called correctly for each dataset
        assert mock_delete_dataset.call_count == 2, "Should call delete_dataset exactly twice"
        mock_delete_dataset.assert_any_call("marine_analysis_2023", False)
        mock_delete_dataset.assert_any_call("coastal_survey_results", False)

        # Verify calls were made with correct dry_run parameter (False by default)
        for call in mock_delete_dataset.call_args_list:
            assert call[0][1] is False, "Should call delete_dataset with dry_run=False by default"

        # Verify CLI output contains success messages for each dataset
        assert "Deleted" in result.output, "Should display success message"
        assert "marine_analysis_2023" in result.output, "Should mention first dataset name"
        assert "coastal_survey_results" in result.output, "Should mention second dataset name"
        assert str(expected_paths[0]) in result.output, "Should show first dataset path"
        assert str(expected_paths[1]) in result.output, "Should show second dataset path"

        # Verify no error messages appear for successful operations
        assert "Failed to delete" not in result.output, "Should not display error messages for successful operations"

        # Verify success message format for each dataset
        for dataset_name in dataset_names:
            assert "Deleted" in result.output, "Should display success message for each dataset"
            assert (
                f'dataset "{dataset_name}"' in result.output
            ), f"Should display formatted success message with dataset name {dataset_name}"

    @pytest.mark.unit
    def test_delete_dataset_with_dry_run_flag(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete dataset command with dry-run flag passes correct parameter to ProjectWrapper.

        This unit test verifies that the delete dataset CLI command correctly:
        - Parses the --dry-run flag from command line arguments
        - Passes dry_run=True to the ProjectWrapper.delete_dataset method
        - Still displays success messages as if the operation completed
        - Exits with success code 0
        - Shows the specific dataset path in output

        This is a unit test because it mocks the core delete_dataset business logic
        to isolate the CLI dry-run flag handling behavior.
        """
        # Arrange
        dataset_names = ["test_dataset"]
        expected_path = Path("/project/datasets/test_dataset")

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)

        # Mock only the external dependency - ProjectWrapper.delete_dataset method
        # This allows testing real batch_delete_operation logic while controlling the deletion outcome
        mock_delete_dataset = mocker.patch.object(
            ProjectWrapper,
            "delete_dataset",
            return_value=expected_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "dataset", *dataset_names, "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        assert_cli_success(result, context="Delete dataset with dry-run flag")

        # Verify ProjectWrapper.delete_dataset was called with dry_run=True (most critical assertion)
        mock_delete_dataset.assert_called_once_with("test_dataset", True)

        # Verify that exactly one call was made
        assert mock_delete_dataset.call_count == 1, "Should call delete_dataset exactly once"

        # Verify CLI output contains success message and dataset details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert "test_dataset" in result.output, "Should mention dataset name"
        assert str(expected_path) in result.output, "Should show the dataset path"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed to delete" not in result.output, "Should not display error messages for successful dry-run"

    @pytest.mark.unit
    def test_delete_dataset_handles_file_exists_error(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete dataset command properly handles FileExistsError for missing datasets.

        This unit test verifies that when attempting to delete a dataset
        that doesn't exist, the CLI command:
        - Catches the FileExistsError from ProjectWrapper (used when dataset doesn't exist)
        - Displays appropriate error message with dataset name
        - Exits with error code 1
        - Shows the specific error message from the exception

        Tests the batch_delete_operation function in marimba/core/cli/delete.py:65
        and the dataset command in marimba/core/cli/delete.py:262.

        This is a unit test because it mocks the core delete_dataset business logic.
        """
        # Arrange
        nonexistent_dataset = "missing_climate_data_2023"
        expected_error_message = f"Dataset '{nonexistent_dataset}' does not exist in project"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_dataset = mocker.patch.object(
            ProjectWrapper,
            "delete_dataset",
            side_effect=FileExistsError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "dataset", nonexistent_dataset, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Verify CLI execution failed with correct exit code
        assert result.exit_code == 1, f"CLI should exit with code 1 for missing dataset, got: {result.output}"

        # Verify ProjectWrapper.delete_dataset was called with correct parameters
        mock_delete_dataset.assert_called_once_with(nonexistent_dataset, False)

        # Verify that exactly one call was made (no retries or duplicates)
        assert mock_delete_dataset.call_count == 1, "Should call delete_dataset exactly once"

        # Verify CLI output contains error messages
        assert "Failed to delete" in result.output, "Should display failure message"
        assert nonexistent_dataset in result.output, "Should mention the dataset name that failed"
        assert "does not exist" in result.output, "Should display specific error message about missing dataset"

        # Verify that no success messages are shown for failed operation
        assert "Deleted" not in result.output, "Should not display success message for failed operation"

        # Verify error message format follows expected pattern
        assert (
            f'Failed to delete dataset "{nonexistent_dataset}"' in result.output
        ), "Should display formatted error message with dataset name"

    @pytest.mark.unit
    def test_delete_dataset_handles_no_such_dataset_error(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete dataset command properly handles NoSuchDatasetError.

        This unit test verifies that when attempting to delete a dataset
        that doesn't exist, the CLI command:
        - Catches the NoSuchDatasetError from ProjectWrapper
        - Displays appropriate error message with dataset name
        - Exits with error code 1
        - Shows the specific error message from the exception

        This is a unit test because it mocks the core delete_dataset business logic.
        """
        # Arrange
        nonexistent_dataset = "missing_research_dataset"
        expected_error_message = f"Dataset '{nonexistent_dataset}' not found in project"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_dataset = mocker.patch.object(
            ProjectWrapper,
            "delete_dataset",
            side_effect=ProjectWrapper.NoSuchDatasetError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "dataset", nonexistent_dataset, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Use the established helper function for CLI failure assertions
        assert_cli_failure(
            result,
            expected_error="Failed to delete",
            expected_exit_code=1,
            context="Dataset deletion with NoSuchDatasetError",
        )

        # Verify ProjectWrapper.delete_dataset was called with correct parameters
        mock_delete_dataset.assert_called_once_with(nonexistent_dataset, False)

        # Verify CLI output contains specific error details
        assert nonexistent_dataset in result.output, "Should mention the dataset name that failed"
        assert "not found in project" in result.output, "Should display specific error message from exception"

        # Verify that no success messages are shown for failed operation
        assert "Deleted" not in result.output, "Should not display success message for failed operation"

        # Verify error message format follows expected pattern
        assert (
            f'Failed to delete dataset "{nonexistent_dataset}"' in result.output
        ), "Should display formatted error message with dataset name"


class TestDeleteCommandDryRun:
    """Test dry-run functionality across different delete commands."""

    @pytest.mark.unit
    def test_delete_pipeline_with_dry_run_flag_propagation(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test that --dry-run flag is properly propagated from CLI to ProjectWrapper.delete_pipeline.

        This unit test verifies the complete dry-run flag propagation chain:
        - CLI parses --dry-run flag from command line arguments
        - Flag is passed through batch_delete_operation to the delete function
        - ProjectWrapper.delete_pipeline receives dry_run=True parameter
        - Success message is displayed even in dry-run mode
        - No error messages appear for successful dry-run operations

        This test is critical for ensuring dry-run functionality works correctly,
        allowing users to preview deletions without making actual changes.
        """
        # Arrange
        pipeline_names = ["test_processing_pipeline"]
        expected_path = Path("/project/pipelines/test_processing_pipeline")

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_pipeline = mocker.patch.object(
            ProjectWrapper,
            "delete_pipeline",
            return_value=expected_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "pipeline", *pipeline_names, "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        # Verify CLI execution succeeded
        assert_cli_success(result, context="Delete pipeline with dry-run flag")

        # Verify ProjectWrapper.delete_pipeline was called with dry_run=True (most critical assertion)
        mock_delete_pipeline.assert_called_once_with("test_processing_pipeline", True)

        # Verify that exactly one call was made
        assert mock_delete_pipeline.call_count == 1, "Should call delete_pipeline exactly once"

        # Verify CLI output contains success message and pipeline details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert "test_processing_pipeline" in result.output, "Should mention the pipeline name"
        assert str(expected_path) in result.output, "Should show the pipeline path"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed to delete" not in result.output, "Should not display error messages for successful dry-run"

        # Verify specific success message format
        assert (
            'pipeline "test_processing_pipeline"' in result.output
        ), "Should display formatted success message with pipeline name"


class TestDeletePipelineCommand:
    """Test CLI delete pipeline command integration."""

    @pytest.mark.unit
    def test_delete_multiple_pipelines_with_partial_failures(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete pipeline command with mixed success and failure results.

        This unit test verifies that when deleting multiple pipelines where some exist
        and others don't, the CLI properly:
        - Processes all pipelines in the batch operation
        - Shows success messages for existing pipelines
        - Shows error messages for missing pipelines
        - Exits with error code 1 due to failures
        - Maintains processing order and handles each item appropriately
        - Continues processing after failures (resilience testing)

        This is a unit test because it mocks the core delete_pipeline business logic.
        """
        # Arrange
        pipeline_names = ["data_processing_pipeline", "missing_pipeline", "analysis_pipeline"]
        existing_path_1 = Path("/project/pipelines/data_processing_pipeline")
        existing_path_2 = Path("/project/pipelines/analysis_pipeline")
        missing_error = "Pipeline 'missing_pipeline' not found in project"

        def mock_delete_side_effect(name: str, dry_run: bool) -> Path:
            # Verify dry_run parameter is passed correctly
            assert isinstance(dry_run, bool), "dry_run should be a boolean"
            assert dry_run is False, "Should pass dry_run=False by default"

            if name == "missing_pipeline":
                raise ProjectWrapper.NoSuchPipelineError(missing_error)
            if name == "data_processing_pipeline":
                return existing_path_1
            if name == "analysis_pipeline":
                return existing_path_2
            unexpected_name_msg = f"Unexpected pipeline name: {name}"
            raise ValueError(unexpected_name_msg)

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_pipeline = mocker.patch.object(
            ProjectWrapper,
            "delete_pipeline",
            side_effect=mock_delete_side_effect,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "pipeline", *pipeline_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Verify CLI execution failed due to partial failures
        assert (
            result.exit_code == 1
        ), f"CLI should exit with code 1 for partial failures, got exit code: {result.exit_code}"

        # Verify all pipelines were attempted
        assert mock_delete_pipeline.call_count == 3, "Should attempt to delete all 3 pipelines"
        mock_delete_pipeline.assert_any_call("data_processing_pipeline", False)
        mock_delete_pipeline.assert_any_call("missing_pipeline", False)
        mock_delete_pipeline.assert_any_call("analysis_pipeline", False)

        # Verify all calls were made with correct dry_run parameter
        for call in mock_delete_pipeline.call_args_list:
            assert call[0][1] is False, "All calls should use dry_run=False by default"

        # Verify CLI output contains both success and error messages
        assert "Deleted" in result.output, "Should show success messages for existing pipelines"
        assert "Failed to delete" in result.output, "Should show failure message for missing pipeline"
        assert "data_processing_pipeline" in result.output, "Should mention successful pipeline"
        assert "analysis_pipeline" in result.output, "Should mention other successful pipeline"
        assert "missing_pipeline" in result.output, "Should mention failed pipeline"
        assert "not found" in result.output, "Should display specific error message"

        # Verify successful pipeline paths appear in output
        assert str(existing_path_1) in result.output, "Should show path for first successful pipeline"
        assert str(existing_path_2) in result.output, "Should show path for second successful pipeline"

        # Verify specific success message format for successful pipelines
        assert (
            'pipeline "data_processing_pipeline"' in result.output
        ), "Should show formatted success message for first pipeline"
        assert (
            'pipeline "analysis_pipeline"' in result.output
        ), "Should show formatted success message for second pipeline"

        # Verify specific error message format for failed pipeline
        assert 'Failed to delete pipeline "missing_pipeline"' in result.output, "Should show formatted error message"

        # Verify that batch processing continued after failure (resilience test)
        successful_pipelines = ["data_processing_pipeline", "analysis_pipeline"]
        for pipeline in successful_pipelines:
            assert "Deleted" in result.output, f"Should show success for {pipeline} despite other failures"

    @pytest.mark.unit
    def test_delete_multiple_pipelines_successful(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test successful deletion of multiple pipelines via CLI command.

        This unit test verifies that the delete pipeline CLI command correctly:
        - Processes multiple pipeline names from command line arguments
        - Calls the ProjectWrapper.delete_pipeline method for each pipeline
        - Displays success messages for each deleted pipeline
        - Exits with code 0 on successful completion
        - Uses batch_delete_operation to coordinate the deletion process

        This is a unit test because it mocks the core delete_pipeline business logic.
        """
        # Arrange
        pipeline_names = ["image_processing_pipeline", "data_analysis_pipeline"]
        expected_paths = [
            Path("/project/pipelines/image_processing_pipeline"),
            Path("/project/pipelines/data_analysis_pipeline"),
        ]

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_pipeline = mocker.patch.object(
            ProjectWrapper,
            "delete_pipeline",
            side_effect=expected_paths,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "pipeline", *pipeline_names, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        assert_cli_success(result, context="Delete multiple pipelines command")

        # Verify ProjectWrapper.delete_pipeline was called correctly for each pipeline
        assert mock_delete_pipeline.call_count == 2, "Should call delete_pipeline exactly twice"
        mock_delete_pipeline.assert_any_call("image_processing_pipeline", False)
        mock_delete_pipeline.assert_any_call("data_analysis_pipeline", False)

        # Verify calls were made with correct dry_run parameter (False by default)
        for call in mock_delete_pipeline.call_args_list:
            assert call[0][1] is False, "Should call delete_pipeline with dry_run=False by default"

        # Verify CLI output contains success messages for each pipeline
        assert "Deleted" in result.output, "Should display success message"
        assert "image_processing_pipeline" in result.output, "Should mention first pipeline name"
        assert "data_analysis_pipeline" in result.output, "Should mention second pipeline name"
        assert str(expected_paths[0]) in result.output, "Should show first pipeline path"
        assert str(expected_paths[1]) in result.output, "Should show second pipeline path"

        # Verify no error messages appear for successful operations
        assert "Failed to delete" not in result.output, "Should not display error messages for successful operations"

        # Verify success message format for each pipeline
        for pipeline_name in pipeline_names:
            assert (
                f'pipeline "{pipeline_name}"' in result.output
            ), f"Should display formatted success message with pipeline name {pipeline_name}"

    @pytest.mark.unit
    def test_delete_pipeline_with_dry_run_flag(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete pipeline command with dry-run flag passes correct parameter and displays proper output.

        This unit test ensures that the --dry-run flag is properly propagated through
        the CLI to the underlying delete_pipeline method and verifies comprehensive output formatting.
        It specifically tests:
        - Dry-run flag propagation to ProjectWrapper.delete_pipeline
        - Success message formatting in dry-run mode
        - Path display in output
        - No error messages for successful dry-run operations

        This is a unit test because it mocks the core delete_pipeline business logic.
        """
        # Arrange
        pipeline_names = ["test_pipeline"]
        expected_path = Path("/project/pipelines/test_pipeline")

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_pipeline = mocker.patch.object(
            ProjectWrapper,
            "delete_pipeline",
            return_value=expected_path,
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "pipeline", *pipeline_names, "--project-dir", str(setup_project_dir), "--dry-run"],
        )

        # Assert
        assert_cli_success(result, context="Delete pipeline with dry-run flag")

        # Verify delete_pipeline was called with dry_run=True (most critical assertion for dry-run test)
        mock_delete_pipeline.assert_called_once_with("test_pipeline", True)

        # Verify that exactly one call was made
        assert mock_delete_pipeline.call_count == 1, "Should call delete_pipeline exactly once"

        # Verify CLI output contains success message and details
        assert "Deleted" in result.output, "Should display success message even in dry-run mode"
        assert "test_pipeline" in result.output, "Should mention pipeline name"
        assert str(expected_path) in result.output, "Should show the pipeline path in output"

        # Verify no error messages appear for successful dry-run operation
        assert "Failed to delete" not in result.output, "Should not display error messages for successful dry-run"

        # Verify specific success message format
        assert (
            'pipeline "test_pipeline"' in result.output
        ), "Should display formatted success message with pipeline name"

        # Verify the complete success message format includes all expected elements
        expected_elements = ["Deleted", "pipeline", "test_pipeline", str(expected_path)]
        for element in expected_elements:
            assert element in result.output, f"Output should contain '{element}' for complete success message"

    @pytest.mark.unit
    def test_delete_pipeline_handles_nonexistent_pipeline_error(
        self,
        mocker: MockerFixture,
        setup_project_dir: Path,
    ) -> None:
        """Test delete pipeline command properly handles NoSuchPipelineError.

        This unit test verifies that when attempting to delete a pipeline
        that doesn't exist, the CLI command:
        - Catches the NoSuchPipelineError from ProjectWrapper
        - Displays appropriate error message with pipeline name
        - Exits with error code 1
        - Shows the specific error message from the exception
        - Uses batch_delete_operation to coordinate the error handling

        This is a unit test because it mocks the core delete_pipeline business logic.
        """
        # Arrange
        nonexistent_pipeline = "missing_analysis_pipeline"
        expected_error_message = f"Pipeline '{nonexistent_pipeline}' not found in project"

        mocker.patch("marimba.core.cli.delete.find_project_dir_or_exit", return_value=setup_project_dir)
        mock_delete_pipeline = mocker.patch.object(
            ProjectWrapper,
            "delete_pipeline",
            side_effect=ProjectWrapper.NoSuchPipelineError(expected_error_message),
        )

        # Act
        result = runner.invoke(
            marimba_cli,
            ["delete", "pipeline", nonexistent_pipeline, "--project-dir", str(setup_project_dir)],
        )

        # Assert
        # Use the established helper function for CLI failure assertions
        assert_cli_failure(
            result,
            expected_error="Failed to delete",
            expected_exit_code=1,
            context="Pipeline deletion with NoSuchPipelineError",
        )

        # Verify ProjectWrapper.delete_pipeline was called with correct parameters
        mock_delete_pipeline.assert_called_once_with(nonexistent_pipeline, False)

        # Verify that exactly one call was made (no retries or duplicates)
        assert mock_delete_pipeline.call_count == 1, "Should call delete_pipeline exactly once"

        # Verify CLI output contains specific error details
        assert nonexistent_pipeline in result.output, "Should mention the pipeline name that failed"
        assert "not found in project" in result.output, "Should display the specific error message from exception"

        # Verify that no success messages are shown for failed operation
        assert "Deleted" not in result.output, "Should not display success message for failed operation"

        # Verify error message format follows expected pattern
        assert (
            f'Failed to delete pipeline "{nonexistent_pipeline}"' in result.output
        ), "Should display formatted error message with pipeline name"
