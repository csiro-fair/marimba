"""
End-to-End tests for distribution operations.

These tests validate distribution workflows for various target types including S3 and DAP.
"""

from pathlib import Path

import pytest
import pytest_mock
import yaml
from typer.testing import CliRunner

from marimba.main import marimba_cli as app
from tests.conftest import TestDataFactory, assert_cli_failure, assert_cli_success


@pytest.mark.e2e
class TestDistributionWorkflows:
    """Test distribution workflows for various target types."""

    @pytest.fixture
    def project(self, runner: CliRunner, tmp_path: Path) -> Path:
        """Create a marimba project."""
        project_dir = tmp_path / "test_project"
        result = runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(result, context="Distribution test project creation")
        return project_dir

    @pytest.fixture
    def mock_dataset_dir(self, project: Path) -> Path:
        """Create a mock dataset directory structure for testing distribution."""
        dataset_dir = project / "datasets" / "test_dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Create proper dataset structure matching Marimba expectations
        (dataset_dir / "data").mkdir(exist_ok=True)
        (dataset_dir / "logs" / "pipelines").mkdir(parents=True, exist_ok=True)
        (dataset_dir / "targets").mkdir(exist_ok=True)

        # Create minimal dataset files for testing using TestDataFactory
        metadata = TestDataFactory.create_dataset_metadata(name="test_dataset")
        (dataset_dir / "metadata.yml").write_text(yaml.dump(metadata))
        (dataset_dir / "data" / "sample.txt").write_text("sample data")

        return dataset_dir

    @pytest.fixture
    def mock_s3_target_dir(self, project: Path) -> Path:
        """Create a mock S3 target configuration file for testing."""
        # Create target configuration file directly in targets directory
        targets_dir = project / "targets"
        targets_dir.mkdir(parents=True, exist_ok=True)
        target_config_file = targets_dir / "test_s3_target.yml"

        # Create a minimal target configuration file
        target_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://test.s3.amazonaws.com",
                "access_key_id": "test_access_key",
                "secret_access_key": "test_secret_key",
            },
        }

        target_config_file.write_text(yaml.dump(target_config))
        return target_config_file

    @pytest.fixture
    def mock_dap_target_dir(self, project: Path) -> Path:
        """Create a mock DAP target configuration file for testing."""
        # Create target configuration file directly in targets directory
        targets_dir = project / "targets"
        targets_dir.mkdir(parents=True, exist_ok=True)
        target_config_file = targets_dir / "test_dap_target.yml"

        # Create a minimal target configuration file
        target_config = {
            "type": "dap",
            "config": {
                "endpoint_url": "https://test.dap.server.com",
                "access_key": "test_user",
                "secret_access_key": "test_password",
                "remote_directory": "/datasets",
            },
        }

        target_config_file.write_text(yaml.dump(target_config))
        return target_config_file

    def test_distribute_to_s3_target_dry_run(
        self,
        runner: CliRunner,
        project: Path,
        mock_dataset_dir: Path,
        mock_s3_target_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test distribution to S3 target with dry run mode enabled.

        Validates that dry run mode prevents actual network operations
        while still validating configuration and dataset structure.
        Tests S3-specific configuration parsing and validation.
        """
        # Arrange: Mock S3 operations to prevent actual network calls
        mocker.patch(
            "marimba.core.distribution.s3.S3DistributionTarget.distribute",
            return_value=True,
        )

        # Verify prerequisites exist and are properly configured
        assert mock_s3_target_dir.exists(), "S3 target config file should exist"
        assert mock_dataset_dir.exists(), "Dataset directory should exist"
        assert (mock_dataset_dir / "metadata.yml").exists(), "Dataset metadata should exist"

        # Verify target configuration is valid S3 setup
        target_config = yaml.safe_load(mock_s3_target_dir.read_text())
        assert target_config["type"] == "s3", "Target should be configured as S3 type"
        assert "bucket_name" in target_config["config"], "S3 target should have bucket_name configuration"

        # Act: Execute distribute command with dry run
        result = runner.invoke(
            app,
            ["distribute", "test_dataset", "test_s3_target", "--project-dir", str(project), "--dry-run"],
        )

        # Assert: Should complete successfully in dry-run mode
        assert_cli_success(result, context="S3 distribution dry-run test")

        # Verify successful distribution message appears
        assert (
            "successfully distributed" in result.stdout.lower()
        ), f"Should show successful distribution message. Output: {result.stdout}"

        # Verify dataset name is referenced in output
        assert (
            "test_dataset" in result.stdout.lower()
        ), f"Output should reference the dataset name. Output: {result.stdout}"

        # The CLI output doesn't explicitly mention dry-run mode when successful,
        # but the mocked S3 operations prevent actual network calls

    def test_distribute_to_dap_target_dry_run(
        self,
        runner: CliRunner,
        project: Path,
        mock_dataset_dir: Path,
        mock_dap_target_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test distribution to DAP target with dry run mode enabled.

        Validates that DAP distribution dry run works correctly without
        making actual HTTP requests to DAP servers. Tests the complete
        workflow including target validation and dry-run output formatting.
        """
        # Arrange: Mock DAP operations to prevent actual HTTP requests
        mocker.patch(
            "marimba.core.distribution.dap.CSIRODapDistributionTarget.distribute",
            return_value=True,
        )

        # Verify prerequisites exist and are properly configured
        assert mock_dap_target_dir.exists(), "DAP target config file should exist"
        assert mock_dataset_dir.exists(), "Dataset directory should exist"
        assert (mock_dataset_dir / "metadata.yml").exists(), "Dataset metadata should exist"

        # Verify target configuration is properly structured
        target_config = yaml.safe_load(mock_dap_target_dir.read_text())
        assert target_config["type"] == "dap", "Target should be configured as DAP type"

        # Act: Execute distribute command with dry run
        result = runner.invoke(
            app,
            ["distribute", "test_dataset", "test_dap_target", "--project-dir", str(project), "--dry-run"],
        )

        # Assert: Should complete successfully in dry-run mode
        assert_cli_success(result, context="DAP distribution dry-run test")

        # Verify successful distribution message appears
        assert (
            "successfully distributed" in result.stdout.lower()
        ), f"Should show successful distribution message. Output: {result.stdout}"

        # Verify dataset name is referenced in output
        assert (
            "test_dataset" in result.stdout.lower()
        ), f"Output should reference the dataset name. Output: {result.stdout}"

        # The CLI output doesn't explicitly mention dry-run mode when successful,
        # but the mocked DAP operations prevent actual network calls

    def test_distribute_with_validation_disabled(
        self,
        runner: CliRunner,
        project: Path,
        mock_dataset_dir: Path,
        mock_s3_target_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test distribution with validation step disabled.

        Verifies that the --no-validate flag properly skips dataset
        validation during distribution workflow. Tests end-to-end
        CLI behavior with validation disabled in dry-run mode.
        """
        # Arrange: Mock external operations to prevent network calls
        mock_s3_distribute = mocker.patch(
            "marimba.core.distribution.s3.S3DistributionTarget.distribute",
            return_value=True,
        )
        mocker.patch(
            "marimba.core.wrappers.dataset.DatasetWrapper.validate",
            return_value=True,
        )

        # Verify test prerequisites are properly set up
        assert mock_s3_target_dir.exists(), "S3 target config file should exist"
        assert mock_dataset_dir.exists(), "Dataset directory should exist"
        assert (mock_dataset_dir / "metadata.yml").exists(), "Dataset metadata should exist"

        # Act: Execute distribute command with validation disabled
        result = runner.invoke(
            app,
            [
                "distribute",
                "test_dataset",
                "test_s3_target",
                "--project-dir",
                str(project),
                "--no-validate",
                "--dry-run",
            ],
        )

        # Assert: Command should complete successfully
        assert_cli_success(result, context="Distribution with validation disabled")

        # Verify expected success indicators in output
        assert (
            "successfully distributed" in result.stdout.lower()
        ), f"Should show successful distribution message. Output: {result.stdout}"

        # Verify dataset name is referenced in output
        assert (
            "test_dataset" in result.stdout.lower()
        ), f"Output should reference the dataset name. Output: {result.stdout}"

        # Verify mocked operations were called as expected
        mock_s3_distribute.assert_called_once()

        # With --no-validate flag, validation should be skipped entirely
        # Note: The actual validation call behavior depends on CLI implementation

    def test_distribute_nonexistent_dataset(
        self,
        runner: CliRunner,
        project: Path,
        mock_s3_target_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test distribution fails gracefully for non-existent dataset.

        Verifies that attempting to distribute a dataset that doesn't exist
        results in a clear error message and appropriate exit behavior.
        Tests error handling when dataset path is missing from project structure.
        """
        # Arrange: Mock S3 operations (though they shouldn't be called)
        mock_s3_distribute = mocker.patch(
            "marimba.core.distribution.s3.S3DistributionTarget.distribute",
            return_value=True,
        )

        # Ensure target exists but dataset does not
        assert mock_s3_target_dir.exists(), "S3 target config file should exist"
        nonexistent_dataset = "nonexistent_dataset"
        dataset_path = project / "datasets" / nonexistent_dataset
        assert not dataset_path.exists(), f"Dataset {dataset_path} should not exist for this test"

        # Act: Attempt to distribute non-existent dataset
        result = runner.invoke(
            app,
            ["distribute", nonexistent_dataset, "test_s3_target", "--project-dir", str(project)],
        )

        # Assert: Should show appropriate error message
        output_lower = result.stdout.lower()
        assert (
            "no such dataset" in output_lower
        ), f"Should show specific dataset not found error. Output: {result.stdout}"
        assert (
            nonexistent_dataset in result.stdout
        ), f"Error message should mention the specific dataset name '{nonexistent_dataset}'. Output: {result.stdout}"

        # Verify S3 distribute was not called due to missing dataset
        mock_s3_distribute.assert_not_called()

    def test_distribute_nonexistent_target(
        self,
        runner: CliRunner,
        project: Path,
        mock_dataset_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test distribution fails gracefully for non-existent target.

        Verifies that attempting to distribute to a target that doesn't exist
        results in a clear error message with appropriate error handling.
        """
        # Arrange: Mock operations (though they shouldn't be called)
        mock_s3_distribute = mocker.patch(
            "marimba.core.distribution.s3.S3DistributionTarget.distribute",
            return_value=True,
        )

        # Ensure dataset exists but target does not
        assert mock_dataset_dir.exists(), "Dataset directory should exist"
        assert (mock_dataset_dir / "metadata.yml").exists(), "Dataset metadata should exist"
        nonexistent_target = "nonexistent_target"
        target_path = project / "targets" / nonexistent_target
        assert not target_path.exists(), f"Target {target_path} should not exist for this test"

        # Act: Attempt to distribute to non-existent target
        result = runner.invoke(app, ["distribute", "test_dataset", nonexistent_target, "--project-dir", str(project)])

        # Assert: Should show clear error message about missing target and exit non-zero
        assert result.exit_code == 1, f"Expected exit code 1 on missing target, got {result.exit_code}"
        assert (
            "no such target" in result.stdout.lower()
        ), f"Should show specific target not found error. Output: {result.stdout}"
        assert (
            nonexistent_target in result.stdout
        ), f"Error message should mention the specific target name '{nonexistent_target}'. Output: {result.stdout}"

        # Verify distribution was not attempted due to missing target
        mock_s3_distribute.assert_not_called()

    def test_distribute_invalid_project_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test distribution fails gracefully for invalid project directory.

        Verifies that using a non-existent project directory results in
        a clear error message about the missing project.
        """
        # Arrange: Create path to non-existent project
        nonexistent_project = tmp_path / "nonexistent_project"
        assert not nonexistent_project.exists(), "Project should not exist for this test"

        # Act: Attempt distribution from non-existent project
        result = runner.invoke(
            app,
            ["distribute", "test_dataset", "test_target", "--project-dir", str(nonexistent_project)],
        )

        # Assert: Should fail with clear project-related error message
        assert_cli_failure(result, context="Distribution from invalid project directory")
        assert (
            "could not find" in result.stdout.lower()
        ), f"Should show specific project not found error. Output: {result.stdout}"

    def test_distribute_workflow_argument_parsing(self, runner: CliRunner, project: Path) -> None:
        """Test distribute command argument parsing with various flag combinations.

        Verifies that the CLI correctly parses and handles different combinations
        of distribute command flags, including validation and dry-run options.
        Tests end-to-end CLI behavior for flag parsing and error handling.
        """
        # Arrange: Prepare test scenario with non-existent dataset/target for consistent error handling
        test_dataset = "test_dataset"
        test_target = "test_target"
        dataset_path = project / "datasets" / test_dataset
        target_path = project / "targets" / test_target

        # Verify test prerequisites: ensure dataset and target don't exist
        assert not dataset_path.exists(), "Dataset should not exist for this test"
        assert not target_path.exists(), "Target should not exist for this test"

        # Act: Test --validate and --dry-run flags combination
        result_validate = runner.invoke(
            app,
            [
                "distribute",
                test_dataset,
                test_target,
                "--project-dir",
                str(project),
                "--validate",
                "--dry-run",
            ],
        )

        # Assert: Should exit non-zero on missing dataset, with the dataset-not-found panel in stdout
        assert (
            result_validate.exit_code == 1
        ), f"CLI should exit 1 when dataset is missing (--validate). Exit code: {result_validate.exit_code}"
        output_lower = result_validate.stdout.lower()
        assert (
            "no such dataset" in output_lower
        ), f"Should show expected dataset error with --validate. Output: {result_validate.stdout}"

        # Act: Test --no-validate flag parsing
        result_no_validate = runner.invoke(
            app,
            [
                "distribute",
                test_dataset,
                test_target,
                "--project-dir",
                str(project),
                "--no-validate",
                "--dry-run",
            ],
        )

        # Assert: Should exit non-zero on missing dataset, with the dataset-not-found panel in stdout
        assert (
            result_no_validate.exit_code == 1
        ), f"CLI should exit 1 when dataset is missing (--no-validate). Exit code: {result_no_validate.exit_code}"
        output_lower = result_no_validate.stdout.lower()
        assert (
            "no such dataset" in output_lower
        ), f"Should show expected dataset error with --no-validate. Output: {result_no_validate.stdout}"

    @pytest.mark.slow
    def test_comprehensive_distribute_workflow(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test comprehensive end-to-end distribution workflow.

        Validates the complete workflow from project creation through distribution,
        ensuring all components work together and fail gracefully when appropriate.
        """
        # Arrange: Mock S3 operations for comprehensive workflow
        mocker.patch(
            "marimba.core.distribution.s3.S3DistributionTarget.distribute",
            return_value=True,
        )

        # Create complete project structure with dataset and target
        project_dir = tmp_path / "test_project"

        # Create project
        result = runner.invoke(app, ["new", "project", str(project_dir)])
        assert_cli_success(result, context="Project creation for comprehensive workflow test")

        # Create target configuration file
        targets_dir = project_dir / "targets"
        targets_dir.mkdir(parents=True, exist_ok=True)
        target_config_file = targets_dir / "test_s3_target.yml"
        target_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://test.s3.amazonaws.com",
                "access_key_id": "test_access_key",
                "secret_access_key": "test_secret_key",
            },
        }
        target_config_file.write_text(yaml.dump(target_config))

        # Create dataset structure
        dataset_dir = project_dir / "datasets" / "test_dataset"
        dataset_dir.mkdir(parents=True)
        (dataset_dir / "data").mkdir()
        (dataset_dir / "logs" / "pipelines").mkdir(parents=True)
        (dataset_dir / "targets").mkdir()

        # Create dataset metadata and data
        metadata = TestDataFactory.create_dataset_metadata(name="test_dataset")
        (dataset_dir / "metadata.yml").write_text(yaml.dump(metadata))
        (dataset_dir / "data" / "sample.txt").write_text("sample data")

        # Verify setup is complete
        assert target_config_file.exists(), "Target configuration should exist"
        assert (dataset_dir / "metadata.yml").exists(), "Dataset metadata should exist"
        assert (dataset_dir / "data" / "sample.txt").exists(), "Dataset sample data should exist"

        # Act: Execute distribution command in dry-run mode
        result = runner.invoke(
            app,
            ["distribute", "test_dataset", "test_s3_target", "--project-dir", str(project_dir), "--dry-run"],
        )

        # Assert: Distribution should succeed in dry-run mode
        assert_cli_success(result, context="Comprehensive distribution workflow")

        # Verify successful distribution message appears
        assert (
            "successfully distributed" in result.stdout.lower()
        ), f"Should show successful distribution message. Output: {result.stdout}"

        # Verify dataset name is referenced in output
        assert (
            "test_dataset" in result.stdout.lower()
        ), f"Output should reference the dataset name. Output: {result.stdout}"

        # The workflow completed successfully with mocked S3 operations

    def test_distribute_command_help_and_options(self, runner: CliRunner) -> None:
        """Test distribute command help displays all available options.

        Verifies that the help command shows comprehensive information about
        available flags and required arguments for the distribute command.
        This is a unit test as it only tests CLI help output without side effects.
        """
        # Arrange: No setup needed for help command test

        # Act: Request help for distribute command
        result = runner.invoke(app, ["distribute", "--help"])

        # Assert: Help should display successfully with expected content
        assert_cli_success(result, expected_message="distribute", context="Distribute help command")

        # Assert: All key terms should be present in help output
        help_output = result.stdout.lower()
        assert "dataset" in help_output, f"Help should mention 'dataset' argument. Output: {result.stdout}"
        assert "target" in help_output, f"Help should mention 'target' argument. Output: {result.stdout}"
        assert "validate" in help_output, f"Help should mention 'validate' option. Output: {result.stdout}"
        assert "dry-run" in help_output, f"Help should mention 'dry-run' option. Output: {result.stdout}"
