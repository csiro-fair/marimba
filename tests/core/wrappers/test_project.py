"""Tests for marimba.core.wrappers.project module."""

import os
import re
from pathlib import Path
from typing import Any

import pytest
import pytest_mock
import yaml

from marimba.core.installer.pipeline_installer import PipelineInstaller
from marimba.core.wrappers.collection import CollectionWrapper
from marimba.core.wrappers.dataset import DatasetWrapper
from marimba.core.wrappers.pipeline import PipelineWrapper
from marimba.core.wrappers.project import ProjectWrapper, get_merged_keyword_args
from marimba.core.wrappers.target import DistributionTargetWrapper


class TestProjectWrapper:
    """Test ProjectWrapper functionality."""

    @pytest.fixture
    def mock_project_dir(self, tmp_path: Path) -> Path:
        """Create a mock project directory structure."""
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()

        # Create basic project structure
        (project_dir / "pipelines").mkdir()
        (project_dir / "collections").mkdir()
        (project_dir / "datasets").mkdir()
        (project_dir / "targets").mkdir()
        (project_dir / ".marimba").mkdir()

        return project_dir

    @pytest.fixture
    def project_wrapper(self, mock_project_dir: Path) -> ProjectWrapper:
        """Create a ProjectWrapper instance."""
        return ProjectWrapper(mock_project_dir)

    @pytest.fixture
    def mock_pipeline_wrapper(self, mock_project_dir: Path, mocker: pytest_mock.MockerFixture) -> Any:
        """Create a real PipelineWrapper instance for testing integration."""
        # Create a minimal pipeline directory structure
        pipeline_dir = mock_project_dir / "pipelines" / "test_pipeline"
        pipeline_dir.mkdir(parents=True)
        repo_dir = pipeline_dir / "repo"
        repo_dir.mkdir()

        # Create minimal config file
        config_file = pipeline_dir / "pipeline.yml"
        config_file.write_text("test_param: test_value")

        # Import here to avoid circular imports during module loading
        from marimba.core.wrappers.pipeline import PipelineWrapper

        # Create wrapper with mocked dependencies
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")
        return PipelineWrapper(pipeline_dir, dry_run=True)

    @pytest.fixture
    def mock_collection_wrapper(self, mock_project_dir):
        """Create a real CollectionWrapper instance for testing integration."""
        # Create a minimal collection directory structure
        collection_dir = mock_project_dir / "collections" / "test_collection"
        collection_dir.mkdir(parents=True)

        # Create minimal config file
        config_file = collection_dir / "collection.yml"
        config_file.write_text("name: test_collection")

        # Import here to avoid circular imports
        from marimba.core.wrappers.collection import CollectionWrapper

        return CollectionWrapper(collection_dir)

    @pytest.mark.integration
    def test_project_wrapper_init_successful_initialization(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that ProjectWrapper initialization properly configures the project instance.

        This integration test verifies that ProjectWrapper.__init__ correctly:
        - Initializes all project attributes with expected values
        - Sets up directory paths correctly and creates them as needed
        - Creates empty wrapper dictionaries for pipelines, collections, datasets, and targets
        - Executes file structure validation successfully
        - Configures logging properly (verified by log file creation)

        Tests real component interactions with minimal mocking to ensure proper integration behavior.
        """
        # Arrange
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()

        # Create required .marimba directory (ProjectWrapper expects this for valid structure)
        (project_dir / ".marimba").mkdir()

        expected_dry_run = False
        expected_name = project_dir.name

        # Act
        wrapper = ProjectWrapper(project_dir, dry_run=expected_dry_run)

        # Assert - Core attributes are set correctly
        assert wrapper.root_dir == project_dir, f"Expected root_dir {project_dir}, got {wrapper.root_dir}"
        assert wrapper.dry_run == expected_dry_run, f"Expected dry_run {expected_dry_run}, got {wrapper.dry_run}"
        assert wrapper.name == expected_name, f"Expected name {expected_name}, got {wrapper.name}"

        # Assert - Directory properties return correct paths and directories are created
        assert wrapper.pipelines_dir == project_dir / "pipelines", "Pipelines directory path should be correct"
        assert wrapper.pipelines_dir.exists(), "Pipelines directory should be created"
        assert wrapper.collections_dir == project_dir / "collections", "Collections directory path should be correct"
        assert wrapper.collections_dir.exists(), "Collections directory should be created"
        assert wrapper.datasets_dir == project_dir / "datasets", "Datasets directory path should be correct"
        assert wrapper.datasets_dir.exists(), "Datasets directory should be created"
        assert wrapper.targets_dir == project_dir / "targets", "Targets directory path should be correct"
        assert wrapper.targets_dir.exists(), "Targets directory should be created"
        assert wrapper.marimba_dir == project_dir / ".marimba", "Marimba directory path should be correct"
        assert wrapper.marimba_dir.exists(), "Marimba directory should exist"
        assert wrapper.log_path == project_dir / "project.log", "Log path should be correct"

        # Assert - Wrapper collections are properly initialized as empty dictionaries
        assert isinstance(wrapper.pipeline_wrappers, dict), "Pipeline wrappers should be a dictionary"
        assert len(wrapper.pipeline_wrappers) == 0, "Pipeline wrappers should be empty for project without pipelines"
        assert isinstance(wrapper.collection_wrappers, dict), "Collection wrappers should be a dictionary"
        assert (
            len(wrapper.collection_wrappers) == 0
        ), "Collection wrappers should be empty for project without collections"
        assert isinstance(wrapper.dataset_wrappers, dict), "Dataset wrappers should be a dictionary"
        assert len(wrapper.dataset_wrappers) == 0, "Dataset wrappers should be empty for project without datasets"
        assert isinstance(wrapper.target_wrappers, dict), "Target wrappers should be a dictionary"
        assert len(wrapper.target_wrappers) == 0, "Target wrappers should be empty for project without targets"

        # Assert - Logging setup occurred (log file should be created)
        assert wrapper.log_path.exists(), f"Log file should be created at {wrapper.log_path}"

    @pytest.mark.integration
    def test_create_project_with_new_directory_creates_project_structure(
        self,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that creating a project with new directory creates essential structure and initializes properly.

        This integration test verifies that ProjectWrapper.create properly creates the root directory
        and .marimba directory, then successfully initializes a ProjectWrapper instance. It tests the
        complete project creation workflow with minimal mocking to ensure real component interactions.
        """
        # Arrange
        project_path = tmp_path / "new_test_project"

        # Only mock logging setup to avoid file creation during tests
        mock_setup_logging = mocker.patch.object(ProjectWrapper, "_setup_logging")

        assert not project_path.exists(), "Project directory should not exist before creation"

        # Act
        project = ProjectWrapper.create(project_path)

        # Assert - Verify return value and basic project structure
        assert isinstance(project, ProjectWrapper), "create should return a ProjectWrapper instance"
        assert project.root_dir == project_path, f"Project root_dir should be {project_path}"
        assert project_path.exists(), "Project directory should be created by create method"
        assert project_path.is_dir(), "Project path should be a directory"

        # Verify .marimba directory is created by create method
        marimba_dir = project_path / ".marimba"
        assert marimba_dir.exists(), "Marimba directory should be created by create method"
        assert marimba_dir.is_dir(), "Marimba directory should be a directory"

        # Verify project properties are properly configured
        assert project.name == project_path.name, f"Project name should be '{project_path.name}'"
        assert project.dry_run is False, "Default dry_run should be False"

        # Verify that lazy directory creation works when accessing directory properties
        assert project.pipelines_dir.exists(), "Pipelines directory should be created when accessed"
        assert project.pipelines_dir.is_dir(), "Pipelines directory should be a directory"
        assert project.collections_dir.exists(), "Collections directory should be created when accessed"
        assert project.collections_dir.is_dir(), "Collections directory should be a directory"
        assert project.datasets_dir.exists(), "Datasets directory should be created when accessed"
        assert project.datasets_dir.is_dir(), "Datasets directory should be a directory"
        assert project.targets_dir.exists(), "Targets directory should be created when accessed"
        assert project.targets_dir.is_dir(), "Targets directory should be a directory"

        # Verify wrapper collections are initialized as empty dictionaries
        assert isinstance(project.pipeline_wrappers, dict), "pipeline_wrappers should be a dictionary"
        assert len(project.pipeline_wrappers) == 0, "pipeline_wrappers should be empty for new project"
        assert isinstance(project.collection_wrappers, dict), "collection_wrappers should be a dictionary"
        assert len(project.collection_wrappers) == 0, "collection_wrappers should be empty for new project"
        assert isinstance(project.dataset_wrappers, dict), "dataset_wrappers should be a dictionary"
        assert len(project.dataset_wrappers) == 0, "dataset_wrappers should be empty for new project"
        assert isinstance(project.target_wrappers, dict), "target_wrappers should be a dictionary"
        assert len(project.target_wrappers) == 0, "target_wrappers should be empty for new project"

        # Verify logging setup was called during initialization
        mock_setup_logging.assert_called_once()  # Logging setup should be called during initialization

        # Verify directory properties work correctly (these create directories lazily)
        pipelines_dir = project.pipelines_dir
        collections_dir = project.collections_dir
        datasets_dir = project.datasets_dir
        targets_dir = project.targets_dir

        assert pipelines_dir == project_path / "pipelines", "pipelines_dir should return correct path"
        assert collections_dir == project_path / "collections", "collections_dir should return correct path"
        assert datasets_dir == project_path / "datasets", "datasets_dir should return correct path"
        assert targets_dir == project_path / "targets", "targets_dir should return correct path"

        # Verify accessing properties creates the directories
        assert pipelines_dir.exists(), "Accessing pipelines_dir should create the directory"
        assert collections_dir.exists(), "Accessing collections_dir should create the directory"
        assert datasets_dir.exists(), "Accessing datasets_dir should create the directory"
        assert targets_dir.exists(), "Accessing targets_dir should create the directory"

    @pytest.mark.integration
    def test_pipeline_wrappers_property_loads_valid_pipeline_from_filesystem(
        self,
        mock_project_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that pipeline_wrappers property loads valid pipeline from filesystem after project creation.

        This integration test verifies that when a properly structured pipeline directory
        with a valid configuration file exists, the pipeline_wrappers property correctly
        loads and returns a PipelineWrapper instance for that pipeline. It tests the
        integration between the project's pipeline loading mechanism and the filesystem.
        """
        # Arrange
        pipeline_name = "test_pipeline"
        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        pipeline_dir.mkdir(parents=True)
        repo_dir = pipeline_dir / "repo"
        repo_dir.mkdir()

        # Create valid pipeline configuration file that PipelineWrapper expects
        config_content = f"""name: {pipeline_name}
test_param: test_value
version: 1.0
"""
        config_file = pipeline_dir / "pipeline.yml"
        config_file.write_text(config_content)

        # Mock external dependencies: logging setup (filesystem) and installer (external tools)
        # These mocks prevent side effects while testing the core loading logic
        mock_logging = mocker.patch.object(PipelineWrapper, "_setup_logging")
        mock_installer = mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create ProjectWrapper after setting up pipeline structure to ensure it's loaded during initialization
        project_wrapper = ProjectWrapper(mock_project_dir)

        # Act
        wrappers = project_wrapper.pipeline_wrappers

        # Assert
        assert isinstance(wrappers, dict), "pipeline_wrappers should return a dictionary"
        assert pipeline_name in wrappers, f"Pipeline '{pipeline_name}' should be loaded into wrappers"

        # Verify the pipeline wrapper is properly configured
        pipeline_wrapper = wrappers[pipeline_name]
        assert isinstance(pipeline_wrapper, PipelineWrapper), "Should be a PipelineWrapper instance"
        assert (
            pipeline_wrapper.root_dir == pipeline_dir
        ), f"Pipeline wrapper root_dir should be {pipeline_dir}, got {pipeline_wrapper.root_dir}"
        assert (
            pipeline_wrapper.config_path == config_file
        ), f"Pipeline wrapper config_path should be {config_file}, got {pipeline_wrapper.config_path}"
        assert (
            pipeline_wrapper.repo_dir == repo_dir
        ), f"Pipeline wrapper repo_dir should be {repo_dir}, got {pipeline_wrapper.repo_dir}"

        # Verify pipeline wrapper can be used for real operations (integration aspect)
        assert pipeline_wrapper.dry_run == project_wrapper.dry_run, "Pipeline should inherit project's dry_run setting"
        assert pipeline_wrapper.name == pipeline_name, f"Pipeline name should be {pipeline_name}"

        # Verify the pipeline is accessible through the project's pipeline collection
        assert len(wrappers) >= 1, "Should have at least one pipeline loaded"

        # Verify that external dependencies were called appropriately during initialization
        mock_logging.assert_called_once()
        # PipelineInstaller.create is called with (pipeline_path, logger)
        mock_installer.assert_called_once()

    @pytest.mark.integration
    def test_collection_wrappers_property_with_valid_collection(self, mock_project_dir: Path) -> None:
        """Test that collection_wrappers property successfully loads valid collection from filesystem.

        This integration test verifies that when a properly structured collection directory
        with a valid configuration file exists, the collection_wrappers property correctly
        loads and returns a CollectionWrapper instance for that collection.
        """
        # Arrange
        collection_name = "test_collection"
        collection_dir = mock_project_dir / "collections" / collection_name
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create a valid collection configuration file with required fields
        config_content = """name: test_collection
site_id: TEST_SITE_01
field_of_view: 1000
instrument_type: camera
operation: copy
created: 2024-01-01T00:00:00Z
"""
        config_file = collection_dir / "collection.yml"
        config_file.write_text(config_content)

        # Act
        project_wrapper = ProjectWrapper(mock_project_dir)
        wrappers = project_wrapper.collection_wrappers

        # Assert
        assert isinstance(wrappers, dict), "collection_wrappers should return a dictionary"
        assert len(wrappers) == 1, f"Expected exactly 1 collection, got {len(wrappers)}"
        assert collection_name in wrappers, f"Collection '{collection_name}' should be loaded into wrappers"

        # Verify the collection wrapper is properly configured
        collection_wrapper = wrappers[collection_name]
        assert isinstance(collection_wrapper, CollectionWrapper), "Should be a CollectionWrapper instance"
        assert (
            collection_wrapper.root_dir == collection_dir
        ), f"Collection wrapper root_dir should be {collection_dir}, got {collection_wrapper.root_dir}"
        assert (
            collection_wrapper.config_path == config_file
        ), f"Collection wrapper config_path should be {config_file}, got {collection_wrapper.config_path}"

        # Verify the configuration can be loaded and contains expected data
        loaded_config = collection_wrapper.load_config()
        assert (
            loaded_config["name"] == collection_name
        ), f"Config name should be '{collection_name}', got '{loaded_config.get('name')}'"
        assert (
            loaded_config["site_id"] == "TEST_SITE_01"
        ), f"Config site_id should be 'TEST_SITE_01', got '{loaded_config.get('site_id')}'"

    @pytest.mark.integration
    def test_dataset_wrappers_property_loads_valid_datasets_from_filesystem(self, mock_project_dir: Path) -> None:
        """Test that dataset_wrappers property correctly loads valid dataset directories from filesystem.

        This integration test verifies that the dataset_wrappers property properly discovers
        and loads DatasetWrapper instances for valid dataset directories found in the datasets
        directory. Tests the real interaction between ProjectWrapper and DatasetWrapper classes.
        """
        # Arrange: Create a valid dataset using DatasetWrapper.create() to ensure proper structure
        valid_dataset_name = "valid_dataset"
        valid_dataset_dir = mock_project_dir / "datasets" / valid_dataset_name

        # Create and immediately close the dataset wrapper to avoid resource leaks
        created_dataset_wrapper = DatasetWrapper.create(
            valid_dataset_dir,
            version="1.0",
            contact_name="Test User",
            contact_email="test@example.com",
            dry_run=False,
        )
        created_dataset_wrapper.close()

        # Verify the dataset was created correctly before proceeding
        assert valid_dataset_dir.exists(), f"Dataset directory should exist at {valid_dataset_dir}"
        assert valid_dataset_dir.is_dir(), "Dataset path should be a directory"

        # Create ProjectWrapper after setting up valid dataset structure
        project_wrapper = ProjectWrapper(mock_project_dir)

        # Act: Access the dataset_wrappers property which triggers _load_datasets()
        wrappers = project_wrapper.dataset_wrappers

        # Assert: Verify basic structure and content
        assert isinstance(wrappers, dict), "dataset_wrappers property must return a dictionary"
        assert len(wrappers) == 1, f"Expected exactly 1 dataset wrapper, found {len(wrappers)}"

        # Verify the specific dataset was loaded correctly
        assert (
            valid_dataset_name in wrappers
        ), f"Dataset '{valid_dataset_name}' should be present in wrappers. Available keys: {list(wrappers.keys())}"

        valid_wrapper = wrappers[valid_dataset_name]
        assert isinstance(
            valid_wrapper,
            DatasetWrapper,
        ), f"Wrapper for '{valid_dataset_name}' should be DatasetWrapper instance, got {type(valid_wrapper)}"
        assert (
            valid_wrapper.root_dir == valid_dataset_dir
        ), f"DatasetWrapper root_dir should be {valid_dataset_dir}, got {valid_wrapper.root_dir}"
        assert (
            valid_wrapper.name == valid_dataset_name
        ), f"DatasetWrapper name should be '{valid_dataset_name}', got '{valid_wrapper.name}'"

        # Verify all loaded wrappers have required attributes and proper types
        for dataset_name_key, dataset_wrapper in wrappers.items():
            assert isinstance(
                dataset_name_key,
                str,
            ), f"Dataset name key '{dataset_name_key}' should be string, got {type(dataset_name_key)}"
            assert isinstance(
                dataset_wrapper,
                DatasetWrapper,
            ), f"Wrapper for '{dataset_name_key}' should be DatasetWrapper instance, got {type(dataset_wrapper)}"
            assert hasattr(
                dataset_wrapper,
                "root_dir",
            ), f"DatasetWrapper '{dataset_name_key}' missing required 'root_dir' attribute"
            assert hasattr(
                dataset_wrapper,
                "name",
            ), f"DatasetWrapper '{dataset_name_key}' missing required 'name' attribute"

            # Verify the wrapper is functional by accessing core properties
            assert (
                dataset_wrapper.root_dir.exists()
            ), f"Dataset directory for '{dataset_name_key}' should exist at {dataset_wrapper.root_dir}"
            assert (
                dataset_wrapper.name == dataset_name_key
            ), f"Dataset wrapper name '{dataset_wrapper.name}' should match key '{dataset_name_key}'"

        # Cleanup: Close all dataset wrappers to avoid resource warnings
        for dataset_wrapper in wrappers.values():
            dataset_wrapper.close()

    @pytest.mark.integration
    def test_pipeline_wrappers_property_loads_existing_pipeline_with_valid_structure(
        self,
        mock_project_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that pipeline_wrappers property loads existing pipeline with valid directory structure.

        This integration test verifies that when a properly structured pipeline directory
        with valid configuration exists, the pipeline_wrappers property correctly loads
        and returns a functional PipelineWrapper instance. It tests the integration
        between the project's pipeline loading mechanism and pipeline wrapper creation.
        """
        # Arrange: Set up pipeline directory structure with valid configuration
        pipeline_name = "test_pipeline"
        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        pipeline_dir.mkdir(parents=True)
        repo_dir = pipeline_dir / "repo"
        repo_dir.mkdir()

        config_content = """name: test_pipeline
test_param: test_value
version: 1.0
"""
        config_file = pipeline_dir / "pipeline.yml"
        config_file.write_text(config_content)

        # Mock external dependencies only
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        project_wrapper = ProjectWrapper(mock_project_dir)

        # Act: Access the pipeline_wrappers property
        wrappers = project_wrapper.pipeline_wrappers

        # Assert: Verify dictionary structure and pipeline loading
        assert isinstance(wrappers, dict), "Pipeline wrappers should return a dictionary"
        assert len(wrappers) == 1, f"Expected exactly 1 pipeline wrapper, got {len(wrappers)}"
        assert pipeline_name in wrappers, f"Pipeline '{pipeline_name}' should be loaded into wrappers"

        # Assert: Verify pipeline wrapper instance and properties
        pipeline_wrapper = wrappers[pipeline_name]
        assert isinstance(pipeline_wrapper, PipelineWrapper), "Should return a PipelineWrapper instance"
        assert (
            pipeline_wrapper.root_dir == pipeline_dir
        ), f"Pipeline wrapper root_dir should be {pipeline_dir}, got {pipeline_wrapper.root_dir}"
        assert (
            pipeline_wrapper.config_path == config_file
        ), f"Pipeline wrapper config_path should be {config_file}, got {pipeline_wrapper.config_path}"
        assert (
            pipeline_wrapper.repo_dir == repo_dir
        ), f"Pipeline wrapper repo_dir should be {repo_dir}, got {pipeline_wrapper.repo_dir}"
        assert (
            pipeline_wrapper.name == pipeline_name
        ), f"Pipeline name should be '{pipeline_name}', got '{pipeline_wrapper.name}'"

        # Assert: Verify integration aspects - pipeline inherits project settings
        assert pipeline_wrapper.dry_run == project_wrapper.dry_run, (
            f"Pipeline dry_run should match project dry_run ({project_wrapper.dry_run}), "
            f"got {pipeline_wrapper.dry_run}"
        )

    @pytest.mark.unit
    def test_get_pipeline_nonexistent_raises_key_error_with_pipeline_name(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that _get_pipeline raises KeyError when accessing non-existent pipeline.

        This unit test verifies that the _get_pipeline private method raises a KeyError
        containing the pipeline name when attempting to access a pipeline that does not
        exist in the project's _pipeline_wrappers dictionary. This tests the expected
        dictionary key access failure behavior.
        """
        # Arrange
        nonexistent_pipeline_name = "nonexistent_pipeline"

        # Verify initial state - pipeline should not exist in pipeline_wrappers
        assert (
            nonexistent_pipeline_name not in project_wrapper.pipeline_wrappers
        ), "Pipeline should not exist in pipeline_wrappers for this test to be valid"

        # Act & Assert
        with pytest.raises(KeyError, match=rf"^['\"]?{nonexistent_pipeline_name}['\"]?$"):
            project_wrapper._get_pipeline(nonexistent_pipeline_name)

    @pytest.mark.integration
    def test_get_collection_existing(self, mock_project_dir: Path) -> None:
        """Test that collection_wrappers property loads an existing collection from the filesystem.

        This integration test verifies that when a valid collection directory with a proper
        configuration file exists, the collection_wrappers property correctly loads and returns
        a CollectionWrapper instance for that collection.
        """
        # Arrange
        collection_name = "test_collection"
        collection_dir = mock_project_dir / "collections" / collection_name
        collection_dir.mkdir(parents=True)

        # Create a valid collection configuration file (CollectionWrapper expects collection.yml)
        config_content = """name: test_collection
site_id: TEST_SITE_01
field_of_view: 1000
instrument_type: camera
operation: copy
created: 2024-01-01T00:00:00Z
"""
        config_file = collection_dir / "collection.yml"
        config_file.write_text(config_content)

        # Create ProjectWrapper to test integration loading
        project_wrapper = ProjectWrapper(mock_project_dir)

        # Act
        wrappers = project_wrapper.collection_wrappers

        # Assert
        assert isinstance(wrappers, dict), "collection_wrappers should return a dictionary"
        assert len(wrappers) == 1, f"Expected exactly 1 collection, got {len(wrappers)}"
        assert collection_name in wrappers, f"Collection '{collection_name}' should be loaded into wrappers"

        # Verify the collection wrapper is properly configured
        collection_wrapper = wrappers[collection_name]
        assert isinstance(collection_wrapper, CollectionWrapper), "Should be a CollectionWrapper instance"
        assert collection_wrapper.root_dir == collection_dir, f"Collection root_dir should be {collection_dir}"
        assert collection_wrapper.config_path == config_file, f"Collection config_path should be {config_file}"

        # Verify the configuration can be loaded and contains expected data
        loaded_config = collection_wrapper.load_config()
        assert loaded_config["name"] == collection_name, f"Config name should be {collection_name}"
        assert loaded_config["site_id"] == "TEST_SITE_01", "Config should contain expected site_id"

    @pytest.mark.unit
    def test_collection_wrappers_property_returns_empty_dict_when_no_collections_exist(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that collection_wrappers property returns empty dict when no collections exist.

        This unit test verifies that when no collection directories exist in the collections
        directory, the collection_wrappers property returns an empty dictionary and properly
        handles membership checks for non-existent collections.
        """
        # Arrange
        nonexistent_collection_name = "nonexistent_collection"

        # Verify initial state - collections directory should be empty (no collection subdirectories)
        collections_in_dir = [p for p in project_wrapper.collections_dir.iterdir() if p.is_dir()]
        assert len(collections_in_dir) == 0, "Collections directory should be empty for this test"

        # Act
        wrappers = project_wrapper.collection_wrappers

        # Assert
        assert isinstance(wrappers, dict), "collection_wrappers should return a dictionary"
        assert wrappers == {}, "collection_wrappers should return empty dict when no collections exist"
        assert len(wrappers) == 0, "collection_wrappers should have length 0 when no collections exist"
        assert (
            nonexistent_collection_name not in wrappers
        ), f"Non-existent collection '{nonexistent_collection_name}' should not be in collection_wrappers"

        # Verify that property returns the same instance as internal _collection_wrappers
        assert (
            wrappers is project_wrapper._collection_wrappers
        ), "collection_wrappers property should return the same instance as _collection_wrappers"

    @pytest.mark.integration
    def test_get_dataset_existing(self, mock_project_dir: Path) -> None:
        """Test that dataset_wrappers property successfully loads existing dataset with proper directory structure.

        This integration test verifies that when a valid dataset directory with the required structure
        (data/, logs/, logs/pipelines/) exists, the dataset_wrappers property correctly loads and returns
        a DatasetWrapper instance for that dataset, testing the real interaction between ProjectWrapper
        and DatasetWrapper components.
        """
        # Arrange
        dataset_name = "test_dataset"
        dataset_dir = mock_project_dir / "datasets" / dataset_name
        dataset_dir.mkdir(parents=True)

        # Create the required dataset directory structure that DatasetWrapper expects
        (dataset_dir / "data").mkdir()
        (dataset_dir / "logs").mkdir()
        (dataset_dir / "logs" / "pipelines").mkdir()

        # Create optional metadata file (not required for structure validation)
        (dataset_dir / "metadata.yml").touch()

        # Create ProjectWrapper after setting up dataset structure to ensure it's loaded during initialization
        project_wrapper = ProjectWrapper(mock_project_dir)

        # Act
        wrappers = project_wrapper.dataset_wrappers

        # Assert
        assert isinstance(wrappers, dict), "dataset_wrappers property should return a dictionary"
        assert len(wrappers) == 1, "Should contain exactly one loaded dataset"
        assert dataset_name in wrappers, f"Dataset '{dataset_name}' should be present in loaded wrappers"

        # Verify the dataset wrapper is properly configured
        dataset_wrapper = wrappers[dataset_name]
        assert isinstance(dataset_wrapper, DatasetWrapper), "Loaded instance should be a DatasetWrapper"
        assert (
            dataset_wrapper.root_dir == dataset_dir
        ), f"DatasetWrapper root_dir should match expected path: {dataset_dir}"
        assert dataset_wrapper.name == dataset_name, f"DatasetWrapper name should match expected: {dataset_name}"

        # Verify the required directory structure exists and is accessible through the wrapper
        assert dataset_wrapper.data_dir.exists(), "Dataset data directory should exist and be accessible"
        assert dataset_wrapper.logs_dir.exists(), "Dataset logs directory should exist and be accessible"
        assert (
            dataset_wrapper.pipeline_logs_dir.exists()
        ), "Dataset pipeline logs directory should exist and be accessible"

        # Verify integration: the wrapper should be properly managed by ProjectWrapper
        assert (
            wrappers is project_wrapper._dataset_wrappers
        ), "dataset_wrappers property should return the same instance as _dataset_wrappers"

        # Clean up resources
        dataset_wrapper.close()

    @pytest.mark.unit
    def test_dataset_wrappers_empty_when_no_datasets_exist(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test dataset_wrappers property returns empty dict when no datasets exist.

        This test verifies that ProjectWrapper.dataset_wrappers property returns an empty
        dictionary when no dataset directories exist in the datasets folder.
        """
        # Arrange - Verify initial state has no dataset directories
        dataset_dirs = [path for path in project_wrapper.datasets_dir.iterdir() if path.is_dir()]
        assert len(dataset_dirs) == 0, "Pre-condition: datasets directory should be empty for this test"

        # Act
        result = project_wrapper.dataset_wrappers

        # Assert - Verify empty state and behavior
        assert isinstance(result, dict), "dataset_wrappers should return a dictionary"
        assert result == {}, "dataset_wrappers should return empty dict when no datasets exist"
        assert len(result) == 0, "dataset_wrappers should have length 0 when no datasets exist"
        assert "nonexistent_dataset" not in result, "containment check should work on empty dict"

    @pytest.mark.integration
    def test_create_pipeline_with_valid_name_creates_pipeline_wrapper(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test creating a new pipeline with valid name successfully creates PipelineWrapper.

        This integration test verifies ProjectWrapper.create_pipeline orchestrates the complete
        pipeline creation workflow: validates the name using real business logic, checks for
        existing pipelines, delegates to PipelineWrapper.create with correct parameters,
        handles pipeline configuration prompting and saving, and updates project state.

        External dependencies (git operations) are mocked to avoid network calls in tests.
        """
        # Arrange
        pipeline_name = "new_pipeline"
        repo_url = "https://example.com/repo.git"
        input_config = {"key": "value"}
        expected_pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        processed_config = {"processed_key": "processed_value"}

        # Mock external dependencies (git operations) but not core business logic
        mock_pipeline_wrapper = mocker.Mock()
        mock_create = mocker.patch("marimba.core.wrappers.pipeline.PipelineWrapper.create")
        mock_create.return_value = mock_pipeline_wrapper

        # Mock pipeline configuration workflow to avoid interactive prompts
        mock_pipeline_wrapper.prompt_pipeline_config.return_value = processed_config
        mock_pipeline_wrapper.save_config.return_value = None

        # Verify initial state
        assert not expected_pipeline_dir.exists(), "Pipeline directory should not exist before creation"
        assert pipeline_name not in project_wrapper.pipeline_wrappers, "Pipeline should not exist initially"
        initial_pipeline_count = len(project_wrapper.pipeline_wrappers)

        # Act
        result = project_wrapper.create_pipeline(pipeline_name, repo_url, input_config)

        # Assert - Verify return value
        assert result is mock_pipeline_wrapper, "create_pipeline should return the created PipelineWrapper instance"

        # Assert - Verify PipelineWrapper.create called with correct parameters
        mock_create.assert_called_once_with(
            expected_pipeline_dir,
            repo_url,
            dry_run=project_wrapper.dry_run,
        ), "PipelineWrapper.create should be called with correct directory, URL and dry_run flag"

        # Assert - Verify pipeline configuration workflow executed correctly
        mock_pipeline_wrapper.prompt_pipeline_config.assert_called_once_with(
            input_config,
            project_logger=project_wrapper.logger,
            allow_empty=True,
            accept_defaults=False,
        ), "prompt_pipeline_config should be called with input config and project logger"

        mock_pipeline_wrapper.save_config.assert_called_once_with(
            processed_config,
        ), "save_config should be called with the processed configuration"

        # Assert - Verify project state updated correctly
        assert (
            pipeline_name in project_wrapper.pipeline_wrappers
        ), "Pipeline should be added to project's pipeline wrappers dictionary"
        assert (
            project_wrapper.pipeline_wrappers[pipeline_name] is mock_pipeline_wrapper
        ), "Correct pipeline wrapper instance should be stored in project"
        assert (
            len(project_wrapper.pipeline_wrappers) == initial_pipeline_count + 1
        ), "Pipeline count should increase by exactly 1"

    @pytest.mark.integration
    def test_create_pipeline_with_invalid_name_raises_invalid_name_error(self, project_wrapper: ProjectWrapper) -> None:
        """Test creating pipeline with invalid name raises InvalidNameError.

        This integration test verifies that create_pipeline properly validates names
        and raises InvalidNameError for names containing invalid characters.
        """
        # Arrange
        invalid_name = "invalid name"  # Contains space which is invalid
        repo_url = "https://example.com/repo.git"
        config = {"key": "value"}

        # Act & Assert
        with pytest.raises(ProjectWrapper.InvalidNameError, match=f"^{re.escape(invalid_name)}$"):
            project_wrapper.create_pipeline(invalid_name, repo_url, config)

    @pytest.mark.integration
    def test_create_pipeline_with_existing_name_raises_create_pipeline_error(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test creating pipeline with existing name raises CreatePipelineError.

        This integration test verifies that create_pipeline properly checks for existing
        pipeline directories and raises CreatePipelineError when attempting to create
        a pipeline with a name that already exists.
        """
        # Arrange
        pipeline_name = "existing_pipeline"
        repo_url = "https://example.com/repo.git"
        config = {"key": "value"}
        existing_pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        existing_pipeline_dir.mkdir(parents=True)

        # Verify setup - pipeline directory should exist before attempting creation
        assert existing_pipeline_dir.exists(), "Pipeline directory should exist for test setup"
        assert existing_pipeline_dir.is_dir(), "Pipeline path should be a directory"

        # Act & Assert
        with pytest.raises(
            ProjectWrapper.CreatePipelineError,
            match=f'A pipeline with the name "{pipeline_name}" already exists',
        ):
            project_wrapper.create_pipeline(pipeline_name, repo_url, config)

    @pytest.mark.integration
    def test_create_collection_with_valid_parameters_creates_collection_successfully(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that creating a collection with valid parameters successfully creates a CollectionWrapper.

        This integration test verifies the complete create_collection workflow: validates collection name
        using real business logic, creates the collection directory structure, creates pipeline-specific
        directories for any existing pipelines, adds the collection to project state, and returns
        a properly configured CollectionWrapper instance.
        """
        # Arrange
        collection_name = "test_collection"
        collection_config = {"site_id": "TEST_SITE", "operation": "copy", "name": collection_name}
        expected_collection_dir = mock_project_dir / "collections" / collection_name
        initial_collection_count = len(project_wrapper.collection_wrappers)

        # Create an existing pipeline to test pipeline data directory creation
        test_pipeline_dir = mock_project_dir / "pipelines" / "test_pipeline"
        test_pipeline_dir.mkdir()
        (test_pipeline_dir / "repo").mkdir()  # PipelineWrapper requires a repo directory
        (test_pipeline_dir / "pipeline.yml").write_text("name: test_pipeline\n")
        # Force reload of pipeline wrappers to include the new pipeline
        project_wrapper._load_pipelines()

        # Verify initial state - collection should not exist
        assert collection_name not in project_wrapper.collection_wrappers, "Collection should not exist before creation"
        assert not expected_collection_dir.exists(), "Collection directory should not exist before creation"
        assert len(project_wrapper.pipeline_wrappers) > 0, "Should have at least one pipeline for this test"

        # Act
        result = project_wrapper.create_collection(collection_name, collection_config)

        # Assert - verify return value is a proper CollectionWrapper instance
        assert isinstance(result, CollectionWrapper), "create_collection should return a CollectionWrapper instance"
        assert result.root_dir == expected_collection_dir, f"Collection root_dir should be {expected_collection_dir}"

        # Verify collection directory structure was created
        assert expected_collection_dir.exists(), "Collection directory should be created"
        assert expected_collection_dir.is_dir(), "Collection path should be a directory"
        assert (expected_collection_dir / "collection.yml").exists(), "Collection config file should be created"

        # Verify pipeline data directories were created for existing pipelines
        pipeline_count = 0
        for pipeline_name in project_wrapper.pipeline_wrappers:
            pipeline_data_dir = expected_collection_dir / pipeline_name
            assert pipeline_data_dir.exists(), f"Pipeline data directory should be created for {pipeline_name}"
            assert pipeline_data_dir.is_dir(), f"Pipeline data path should be a directory for {pipeline_name}"
            pipeline_count += 1
        assert pipeline_count > 0, "Should have tested at least one pipeline data directory creation"

        # Verify collection is added to project's collection wrappers
        assert (
            collection_name in project_wrapper.collection_wrappers
        ), "Collection should be added to project's collection wrappers"
        assert (
            project_wrapper.collection_wrappers[collection_name] == result
        ), "Collection wrapper should be stored correctly in project state"
        assert (
            len(project_wrapper.collection_wrappers) == initial_collection_count + 1
        ), "Collection count should increase by exactly 1"

        # Verify collection can load its configuration correctly
        loaded_config = result.load_config()
        assert loaded_config["name"] == collection_name, f"Loaded config name should be '{collection_name}'"
        assert loaded_config["site_id"] == "TEST_SITE", "Loaded config should contain expected site_id value"
        assert loaded_config["operation"] == "copy", "Loaded config should contain expected operation value"

    @pytest.mark.integration
    def test_create_collection_with_invalid_name_raises_invalid_name_error(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that creating a collection with invalid name raises InvalidNameError.

        Verifies that create_collection validates collection names and raises InvalidNameError
        for names containing invalid characters like spaces.
        """
        # Arrange
        invalid_collection_name = "invalid collection"  # Contains space which is invalid
        collection_config = {"site_id": "TEST_SITE", "operation": "copy"}

        # Act & Assert
        with pytest.raises(ProjectWrapper.InvalidNameError, match=f"^{re.escape(invalid_collection_name)}$"):
            project_wrapper.create_collection(invalid_collection_name, collection_config)

    @pytest.mark.integration
    def test_create_collection_with_existing_name_raises_create_collection_error(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that creating a collection with existing name raises CreateCollectionError.

        This integration test verifies that create_collection properly checks for existing
        collection directories and raises CreateCollectionError when attempting to create
        a collection with a name that already exists.
        """
        # Arrange
        collection_name = "existing_collection"
        collection_config = {"site_id": "TEST_SITE", "operation": "copy"}
        existing_collection_dir = mock_project_dir / "collections" / collection_name
        existing_collection_dir.mkdir(parents=True)

        # Verify setup
        assert existing_collection_dir.exists(), "Collection directory should exist for test setup"

        # Act & Assert
        with pytest.raises(
            ProjectWrapper.CreateCollectionError,
            match=f'^{re.escape(f'A collection with the name "{collection_name}" already exists')}$',
        ):
            project_wrapper.create_collection(collection_name, collection_config)

        # Verify side effects: existing directory should remain unchanged
        assert existing_collection_dir.exists(), "Existing collection directory should remain after failed creation"

    @pytest.mark.integration
    def test_create_dataset_with_minimal_parameters_creates_dataset_wrapper(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test creating dataset with minimal parameters creates valid DatasetWrapper.

        This integration test verifies that create_dataset properly validates the dataset name,
        creates the dataset directory structure, and returns a DatasetWrapper instance when
        called with minimal required parameters.
        """
        # Arrange
        dataset_name = "new_dataset"
        expected_dataset_dir = mock_project_dir / "datasets" / dataset_name
        empty_dataset_mapping: dict[
            str,
            dict[str, dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]]],
        ] = {}
        empty_metadata_processors: list[Any] = []
        empty_post_processors: list[Any] = []

        # Verify initial state - dataset should not exist before creation
        assert not expected_dataset_dir.exists(), "Dataset directory should not exist before creation"

        # Act
        result = project_wrapper.create_dataset(
            dataset_name,
            empty_dataset_mapping,
            empty_metadata_processors,
            empty_post_processors,
        )

        # Assert
        assert isinstance(result, DatasetWrapper), "create_dataset should return a DatasetWrapper instance"
        assert expected_dataset_dir.exists(), f"Dataset directory should be created at {expected_dataset_dir}"
        assert expected_dataset_dir.is_dir(), "Dataset path should be a directory"

        # Verify DatasetWrapper is correctly configured
        assert result.root_dir == expected_dataset_dir, f"DatasetWrapper root_dir should be {expected_dataset_dir}"

        # Verify dataset is added to project's dataset wrappers
        assert dataset_name in project_wrapper.dataset_wrappers, "Dataset should be added to project's dataset wrappers"
        assert project_wrapper.dataset_wrappers[dataset_name] == result, "Dataset wrapper should be stored correctly"

        # Cleanup
        result.close()

    @pytest.mark.integration
    def test_delete_project_successfully_deletes_directory_and_returns_path(
        self,
        mocker: pytest_mock.MockerFixture,
        mock_project_dir: Path,
    ) -> None:
        """Test that delete_project delegates directory removal and returns the project path.

        This integration test verifies that when dry_run=False, delete_project properly delegates
        to the remove_directory_tree utility with correct parameters and returns the project
        directory path. Tests the integration between ProjectWrapper and the file system utility.
        """
        # Arrange
        project_wrapper = ProjectWrapper(mock_project_dir, dry_run=False)
        mock_remove_tree = mocker.patch("marimba.core.wrappers.project.remove_directory_tree")

        assert mock_project_dir.exists(), "Project directory must exist before deletion"

        # Act
        result = project_wrapper.delete_project()

        # Assert
        assert result == mock_project_dir, "delete_project must return the project directory path"
        mock_remove_tree.assert_called_once_with(
            mock_project_dir.resolve(),
            "project",
            False,
        ), "remove_directory_tree must be called with resolved path, project type, and dry_run=False"

    @pytest.mark.integration
    def test_delete_project_dry_run_preserves_directory_and_returns_path(
        self,
        mocker: pytest_mock.MockerFixture,
        mock_project_dir: Path,
    ) -> None:
        """Test that delete_project in dry-run mode preserves the directory but returns the path.

        This integration test verifies that when ProjectWrapper is initialized with dry_run=True,
        the delete_project method does not call the directory removal utility but still returns
        the expected project directory path for consistency.
        """
        # Arrange
        project_wrapper = ProjectWrapper(mock_project_dir, dry_run=True)
        mock_remove_tree = mocker.patch("marimba.core.wrappers.project.remove_directory_tree")

        # Act
        result = project_wrapper.delete_project()

        # Assert
        mock_remove_tree.assert_not_called(), "remove_directory_tree should not be called in dry-run mode"
        assert result == mock_project_dir, "delete_project should return the project directory path"
        assert mock_project_dir.exists(), "project directory should still exist after dry-run delete"

    @pytest.mark.unit
    def test_delete_pipeline_existing_pipeline_deletes_directory_and_returns_path(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that deleting an existing pipeline successfully removes the directory and returns the correct path.

        This unit test verifies that the delete_pipeline method properly validates the pipeline name,
        calls the remove_directory_tree utility function, logs the deletion operation, and returns the
        deleted directory path. It mocks external dependencies while testing the core business logic.
        """
        # Arrange
        pipeline_name = "test_pipeline"
        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        pipeline_dir.mkdir(parents=True)

        # Mock external dependencies
        mock_remove_tree = mocker.patch("marimba.core.wrappers.project.remove_directory_tree")
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")
        mock_format_path = mocker.patch(
            "marimba.core.wrappers.project.format_path_for_logging",
            return_value=f"pipelines/{pipeline_name}",
        )

        # Verify initial state - pipeline directory should exist before deletion
        assert pipeline_dir.exists(), "Pipeline directory should exist before deletion"
        assert pipeline_dir.is_dir(), "Pipeline path should be a directory"

        # Act
        result = project_wrapper.delete_pipeline(pipeline_name, dry_run=False)

        # Assert
        # Verify return value
        assert result == pipeline_dir, f"delete_pipeline should return the deleted directory path: {pipeline_dir}"

        # Verify external dependencies were called correctly
        mock_remove_tree.assert_called_once_with(pipeline_dir, "pipeline", False)
        mock_format_path.assert_called_once_with(pipeline_dir, mock_project_dir)
        mock_logger_info.assert_called_once_with(
            f'Deleted pipeline "{pipeline_name}" at pipelines/{pipeline_name}',
        )

    @pytest.mark.unit
    def test_delete_collection_existing_collection_deletes_directory_and_returns_path(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that deleting an existing collection successfully removes the directory and returns the correct path.

        This unit test verifies that the delete_collection method properly validates the collection name,
        removes the collection directory from the filesystem, logs the deletion operation, and returns the
        deleted directory path.
        """
        # Arrange
        collection_name = "test_collection"
        collection_dir = mock_project_dir / "collections" / collection_name
        collection_dir.mkdir(parents=True)

        # Mock the logger to verify logging behavior
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")

        # Verify initial state - collection directory should exist before deletion
        assert collection_dir.exists(), "Collection directory should exist before deletion"

        # Act
        result = project_wrapper.delete_collection(collection_name, dry_run=False)

        # Assert
        assert not collection_dir.exists(), "Collection directory should be deleted after delete_collection call"
        assert result == collection_dir, f"delete_collection should return the deleted directory path: {collection_dir}"

        # Verify logging occurred
        mock_logger_info.assert_called_once()
        assert f'Deleted collection "{collection_name}"' in mock_logger_info.call_args[0][0]

    @pytest.mark.integration
    def test_delete_dataset_existing_dataset_deletes_directory_and_returns_path(
        self,
        mock_project_dir: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that deleting an existing dataset successfully removes the directory and returns the correct path.

        This integration test verifies that the delete_dataset method properly validates the dataset name,
        removes the dataset directory from the filesystem, logs the deletion operation, and returns the
        deleted directory path. It tests the real directory deletion functionality with minimal mocking.
        """
        # Arrange
        dataset_name = "test_dataset"
        dataset_dir = mock_project_dir / "datasets" / dataset_name
        dataset_dir.mkdir(parents=True)

        # Create required dataset directory structure that DatasetWrapper expects
        (dataset_dir / "data").mkdir()
        (dataset_dir / "logs").mkdir()
        (dataset_dir / "logs" / "pipelines").mkdir()
        (dataset_dir / "metadata.yml").touch()
        (dataset_dir / "manifest.json").touch()

        project_wrapper = ProjectWrapper(mock_project_dir, dry_run=False)
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")

        # Verify initial state - dataset directory should exist before deletion
        assert dataset_dir.exists(), "Dataset directory should exist before deletion"

        # Act
        result = project_wrapper.delete_dataset(dataset_name, dry_run=False)

        # Assert
        assert not dataset_dir.exists(), "Dataset directory should be deleted after delete_dataset call"
        assert result == dataset_dir, f"delete_dataset should return the deleted directory path: {dataset_dir}"

        # Verify logging occurred with expected message
        mock_logger_info.assert_called_once()
        log_message = mock_logger_info.call_args[0][0]
        assert f'Deleted dataset "{dataset_name}"' in log_message

    @pytest.mark.integration
    def test_delete_dataset_dry_run_preserves_directory_and_returns_path(
        self,
        mock_project_dir: Path,
    ) -> None:
        """Test that delete_dataset in dry-run mode preserves the dataset directory but returns the path.

        This integration test verifies that when ProjectWrapper is initialized with dry_run=True,
        the delete_dataset method preserves the directory structure while still validating the
        dataset name and returning the expected path. The implementation uses the instance-level
        dry_run setting rather than the method parameter.
        """
        # Arrange
        dataset_name = "test_dataset"
        dataset_dir = mock_project_dir / "datasets" / dataset_name

        # Create dataset directory with required structure for DatasetWrapper validation
        dataset_dir.mkdir(parents=True)
        (dataset_dir / "data").mkdir()
        (dataset_dir / "logs").mkdir()
        (dataset_dir / "logs" / "pipelines").mkdir()
        (dataset_dir / "metadata.yml").touch()
        (dataset_dir / "manifest.json").touch()

        # Create ProjectWrapper with dry_run=True
        dry_run_project_wrapper = ProjectWrapper(mock_project_dir, dry_run=True)

        try:
            # Verify initial state - dataset directory should exist before deletion attempt
            assert dataset_dir.exists(), "Dataset directory should exist before dry-run deletion"
            assert dataset_dir.is_dir(), "Dataset path should be a directory"

            # Act - the dry_run parameter is ignored; instance dry_run setting is used
            result = dry_run_project_wrapper.delete_dataset(dataset_name, dry_run=True)

            # Assert - directory should still exist in dry-run mode
            assert dataset_dir.exists(), "Dataset directory should still exist after dry-run deletion"
            assert dataset_dir.is_dir(), "Dataset directory should remain a directory after dry-run"
            assert result == dataset_dir, f"delete_dataset should return the dataset directory path: {dataset_dir}"

        finally:
            # Cleanup - close any dataset wrappers that were created during ProjectWrapper initialization
            for dataset_wrapper in dry_run_project_wrapper.dataset_wrappers.values():
                # Close the wrapper first
                dataset_wrapper.close()
                # Also ensure all logger handlers are closed (handles dry-run case where _file_handler might not exist)
                if hasattr(dataset_wrapper, "_logger"):
                    for handler in dataset_wrapper._logger.handlers[:]:
                        if hasattr(handler, "close"):
                            handler.close()
                        dataset_wrapper._logger.removeHandler(handler)
            # Clear the wrappers dict and force garbage collection to ensure resources are cleaned up
            dry_run_project_wrapper.dataset_wrappers.clear()
            import gc

            gc.collect()

    @pytest.mark.integration
    def test_install_pipelines_with_valid_pipeline_calls_install_and_logs_success(
        self,
        mocker: pytest_mock.MockerFixture,
        mock_project_dir: Path,
    ) -> None:
        """Test that install_pipelines calls install() on each valid pipeline wrapper and logs success.

        This integration test verifies that the install_pipelines method properly iterates through
        all pipeline wrappers, calls the install() method on each pipeline, and logs successful
        installation messages. It tests the integration between ProjectWrapper and PipelineWrapper
        while mocking only the external pipeline installation dependency.
        """
        # Arrange
        pipeline_name = "test_pipeline"
        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        pipeline_dir.mkdir(parents=True)
        repo_dir = pipeline_dir / "repo"
        repo_dir.mkdir()

        # Create valid pipeline configuration that will pass PipelineWrapper validation
        config_content = f"""name: {pipeline_name}
test: config
version: 1.0
"""
        config_file = pipeline_dir / "pipeline.yml"
        config_file.write_text(config_content)

        # Mock only external dependencies to ensure pipeline wrapper loads successfully
        mocker.patch.object(PipelineWrapper, "_setup_logging")
        mocker.patch("marimba.core.installer.pipeline_installer.PipelineInstaller.create")

        # Create ProjectWrapper which will load the pipeline during initialization
        project_wrapper = ProjectWrapper(mock_project_dir)

        # Verify initial state - pipeline should be loaded into pipeline_wrappers
        assert pipeline_name in project_wrapper.pipeline_wrappers, "Pipeline must be loaded for test setup"

        # Get the actual pipeline wrapper instance for mocking
        pipeline_wrapper = project_wrapper.pipeline_wrappers[pipeline_name]

        # Mock the install method on the specific pipeline wrapper instance
        mock_install = mocker.patch.object(pipeline_wrapper, "install")
        mock_install.return_value = None

        # Mock the logger to verify logging behavior
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")

        # Act
        project_wrapper.install_pipelines()

        # Assert
        mock_install.assert_called_once_with()
        expected_message = f'Installed dependencies for pipeline "{pipeline_name}"'
        mock_logger_info.assert_called_once_with(expected_message)

    @pytest.mark.integration
    def test_install_pipelines_with_installation_error_logs_and_raises(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """install_pipelines processes every pipeline, then raises InstallPipelinesError naming the failures."""
        # Arrange
        failing_pipeline_name = "failing_pipeline"
        successful_pipeline_name = "successful_pipeline"

        # Mock the pipeline wrappers to exist in the project
        mock_failing_wrapper = mocker.MagicMock()
        mock_successful_wrapper = mocker.MagicMock()

        # Configure the failing wrapper to raise InstallError
        error_message = "Installation failed"
        mock_failing_wrapper.install.side_effect = PipelineInstaller.InstallError(error_message)
        mock_successful_wrapper.install.return_value = None

        # Add both wrappers to the project's pipeline_wrappers dict
        project_wrapper._pipeline_wrappers = {
            failing_pipeline_name: mock_failing_wrapper,
            successful_pipeline_name: mock_successful_wrapper,
        }

        # Mock logger methods to verify logging behavior
        mock_logger_exception = mocker.patch.object(project_wrapper.logger, "exception")
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")

        # Act / Assert
        with pytest.raises(ProjectWrapper.InstallPipelinesError, match=failing_pipeline_name):
            project_wrapper.install_pipelines()

        # Verify install() was called for both pipelines (all-or-nothing aggregation)
        mock_failing_wrapper.install.assert_called_once_with()
        mock_successful_wrapper.install.assert_called_once_with()

        # Verify exception was logged for the failing pipeline
        mock_logger_exception.assert_called_once_with(
            f'Failed to install dependencies for pipeline "{failing_pipeline_name}"',
        ), "Exception should be logged when pipeline installation fails"

        # Verify success was logged for the successful pipeline
        mock_logger_info.assert_called_with(
            f'Installed dependencies for pipeline "{successful_pipeline_name}"',
        ), "Success should be logged when pipeline installation succeeds"

    @pytest.mark.unit
    def test_install_pipelines_with_no_pipelines_completes_without_errors(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that install_pipelines completes successfully when no pipelines exist.

        Verifies that the install_pipelines method handles an empty pipeline collection
        gracefully without raising exceptions, testing the basic iteration behavior
        over an empty dictionary.
        """
        # Arrange
        assert len(project_wrapper.pipeline_wrappers) == 0, "Test requires empty pipeline collection"

        # Act
        project_wrapper.install_pipelines()

        # Assert
        assert len(project_wrapper.pipeline_wrappers) == 0, "Pipeline collection should remain empty"

    @pytest.mark.unit
    def test_update_pipelines_calls_update_on_all_pipeline_wrappers(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that update_pipelines calls update() on all pipeline wrappers.

        This unit test verifies that when update_pipelines() is called,
        it correctly iterates through the pipeline_wrappers property and calls
        the update() method on each wrapper instance.
        """
        # Arrange
        mock_pipeline_wrapper_1 = mocker.Mock()
        mock_pipeline_wrapper_2 = mocker.Mock()
        project_wrapper._pipeline_wrappers = {
            "pipeline_1": mock_pipeline_wrapper_1,
            "pipeline_2": mock_pipeline_wrapper_2,
        }

        # Act
        project_wrapper.update_pipelines()

        # Assert
        mock_pipeline_wrapper_1.update.assert_called_once_with()
        mock_pipeline_wrapper_2.update.assert_called_once_with()

    @pytest.mark.integration
    def test_update_pipelines_continues_on_pipeline_errors(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that update_pipelines continues updating other pipelines when one fails.

        This integration test verifies that when a pipeline update fails with OSError or ValueError,
        the update process continues for remaining pipelines and logs the error appropriately.
        Tests the error recovery and continuation behavior of the pipeline update process.
        """
        # Arrange
        mock_pipeline_wrapper_1 = mocker.Mock()
        mock_pipeline_wrapper_2 = mocker.Mock()
        mock_pipeline_wrapper_3 = mocker.Mock()

        # Configure first pipeline to raise OSError
        mock_pipeline_wrapper_1.update.side_effect = OSError("Git repository not found")

        project_wrapper._pipeline_wrappers = {
            "failing_pipeline": mock_pipeline_wrapper_1,
            "working_pipeline_1": mock_pipeline_wrapper_2,
            "working_pipeline_2": mock_pipeline_wrapper_3,
        }

        mock_logger = mocker.Mock()
        mocker.patch.object(
            project_wrapper.__class__,
            "logger",
            new_callable=mocker.PropertyMock,
            return_value=mock_logger,
        )

        # Act
        project_wrapper.update_pipelines()

        # Assert
        # Verify all pipelines had update() called despite failure
        mock_pipeline_wrapper_1.update.assert_called_once_with()
        mock_pipeline_wrapper_2.update.assert_called_once_with()
        mock_pipeline_wrapper_3.update.assert_called_once_with()

        # Verify error logging behavior
        assert mock_logger.exception.call_count == 1, "Should log exactly one exception for the failing pipeline"

        # Verify logged error message contains expected content
        logged_message = mock_logger.exception.call_args[0][0]
        assert "Failed to update pipeline" in logged_message, "Should log pipeline update failure message"
        assert "failing_pipeline" in logged_message, "Should include specific failing pipeline name in error"
        assert "I/O or value error" in logged_message, "Should specify error type in logged message"

        # Verify info logging for successful pipelines
        expected_info_calls = [
            mocker.call('Updating pipeline "failing_pipeline"'),
            mocker.call('Updating pipeline "working_pipeline_1"'),
            mocker.call('Updated pipeline "working_pipeline_1"'),
            mocker.call('Updating pipeline "working_pipeline_2"'),
            mocker.call('Updated pipeline "working_pipeline_2"'),
        ]
        assert mock_logger.info.call_count == 5, "Should log info messages for all pipeline operations"
        mock_logger.info.assert_has_calls(expected_info_calls, any_order=True)

    @pytest.mark.unit
    def test_update_pipelines_continues_on_value_errors(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that update_pipelines continues when a pipeline raises ValueError.

        This unit test verifies ValueError handling specifically, ensuring that
        ValueError exceptions are caught and logged appropriately while other pipelines
        continue to be processed.
        """
        # Arrange
        mock_pipeline_wrapper_1 = mocker.Mock()
        mock_pipeline_wrapper_2 = mocker.Mock()

        # Configure first pipeline to raise ValueError
        mock_pipeline_wrapper_1.update.side_effect = ValueError("Invalid pipeline configuration")

        project_wrapper._pipeline_wrappers = {
            "invalid_config_pipeline": mock_pipeline_wrapper_1,
            "working_pipeline": mock_pipeline_wrapper_2,
        }

        mock_logger = mocker.Mock()
        mocker.patch.object(
            project_wrapper.__class__,
            "logger",
            new_callable=mocker.PropertyMock,
            return_value=mock_logger,
        )

        # Act
        project_wrapper.update_pipelines()

        # Assert
        # Verify both pipelines had update() called
        mock_pipeline_wrapper_1.update.assert_called_once_with()
        mock_pipeline_wrapper_2.update.assert_called_once_with()

        # Verify error logging for ValueError
        mock_logger.exception.assert_called_once()

        # Verify logged error message contains expected ValueError-specific content
        logged_message = mock_logger.exception.call_args[0][0]
        assert "Failed to update pipeline" in logged_message, "Should log pipeline update failure message"
        assert "invalid_config_pipeline" in logged_message, "Should include failing pipeline name"
        assert "I/O or value error" in logged_message, "Should specify error type for ValueError"

        # Verify info logs are called for all pipeline operations (2 for start, 1 for success)
        expected_info_calls = [
            mocker.call('Updating pipeline "invalid_config_pipeline"'),
            mocker.call('Updating pipeline "working_pipeline"'),
            mocker.call('Updated pipeline "working_pipeline"'),
        ]
        mock_logger.info.assert_has_calls(expected_info_calls, any_order=True)
        assert (
            mock_logger.info.call_count == 3
        ), "Should log start messages for both pipelines and success for working pipeline"

    @pytest.mark.unit
    def test_update_pipelines_with_empty_pipeline_wrappers_completes_successfully(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that update_pipelines handles empty pipeline_wrappers gracefully.

        This unit test verifies that when no pipelines exist in a project,
        update_pipelines() completes successfully without attempting to update any pipelines
        or generating any log messages.
        """
        # Arrange - Start with a fresh project that has no pipelines
        # The project fixture already provides an empty project with no pipelines
        mock_logger_info = mocker.patch.object(project_wrapper.logger, "info")
        mock_logger_exception = mocker.patch.object(project_wrapper.logger, "exception")

        # Verify precondition: project has no pipelines
        assert len(project_wrapper.pipeline_wrappers) == 0, "Project should start with no pipelines"

        # Act
        project_wrapper.update_pipelines()

        # Assert
        # Verify the method completed without errors and no state changes
        assert len(project_wrapper.pipeline_wrappers) == 0, "Pipeline wrappers count should remain unchanged"
        mock_logger_info.assert_not_called()
        mock_logger_exception.assert_not_called()

    @pytest.mark.unit
    def test_error_handling_invalid_project_dir_raises_invalid_structure_error(self, tmp_path: Path) -> None:
        """Test that ProjectWrapper raises InvalidStructureError when initialized with non-existent directory.

        This unit test verifies that when ProjectWrapper is initialized with a path
        that does not exist on the filesystem, it properly validates the directory structure
        during initialization and raises InvalidStructureError with a descriptive message.
        """
        # Arrange
        nonexistent_directory = tmp_path / "nonexistent_project_dir"

        # Verify setup - directory should not exist
        assert not nonexistent_directory.exists(), "Test directory should not exist for this test"

        # Act & Assert
        expected_error_pattern = re.escape(f'"{nonexistent_directory}" does not exist or is not a directory')
        with pytest.raises(ProjectWrapper.InvalidStructureError, match=f"^{expected_error_pattern}$"):
            ProjectWrapper(nonexistent_directory)

    @pytest.mark.unit
    def test_project_wrapper_dry_run_mode_prevents_destructive_operations(
        self,
        mocker: pytest_mock.MockerFixture,
        mock_project_dir: Path,
    ) -> None:
        """Test that ProjectWrapper in dry-run mode prevents destructive file system operations.

        This unit test verifies that when ProjectWrapper is initialized with dry_run=True,
        destructive operations like project deletion are properly handled by the dry-run mode,
        preserving the file system while still returning expected paths for consistency.
        The test uses mocking to isolate the dry-run logic from actual file system operations.
        """
        # Arrange
        project_wrapper = ProjectWrapper(mock_project_dir, dry_run=True)
        mock_remove_tree = mocker.patch("marimba.core.wrappers.project.remove_directory_tree")

        # Verify initial state - project directory should exist before dry-run operation
        assert mock_project_dir.exists(), "Project directory should exist before dry-run operation"
        assert project_wrapper.dry_run is True, "ProjectWrapper should be in dry-run mode"

        # Act
        result = project_wrapper.delete_project()

        # Assert - verify return value
        assert result == mock_project_dir, "delete_project should return the project directory path"

        # Assert - verify dry-run behavior preserves file system
        assert mock_project_dir.exists(), "Project directory should still exist after dry-run deletion"
        assert mock_project_dir.is_dir(), "Project path should remain a directory after dry-run deletion"

        # Assert - verify external operations are not called
        mock_remove_tree.assert_not_called()

        # Assert - verify dry_run property consistency
        assert project_wrapper.dry_run is True, "dry_run property should remain True throughout operation"

        # Assert - verify project structure remains intact
        assert project_wrapper.pipelines_dir.exists(), "Pipelines directory should remain intact"
        assert project_wrapper.collections_dir.exists(), "Collections directory should remain intact"
        assert project_wrapper.datasets_dir.exists(), "Datasets directory should remain intact"
        assert project_wrapper.targets_dir.exists(), "Targets directory should remain intact"

    @pytest.mark.unit
    def test_project_wrapper_properties_return_correct_directory_paths(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that ProjectWrapper properties return the correct directory paths and project metadata.

        This unit test verifies that all property accessors on the ProjectWrapper class return
        the expected Path objects and values based on the root directory structure. It tests
        individual property access in isolation, verifying return values and Path object types.
        """
        # Arrange
        expected_pipelines_dir = mock_project_dir / "pipelines"
        expected_collections_dir = mock_project_dir / "collections"
        expected_datasets_dir = mock_project_dir / "datasets"
        expected_targets_dir = mock_project_dir / "targets"
        expected_marimba_dir = mock_project_dir / ".marimba"
        expected_log_path = mock_project_dir / "project.log"
        expected_name = mock_project_dir.name
        expected_dry_run = False

        # Act - Access all relevant properties
        actual_root_dir = project_wrapper.root_dir
        actual_pipelines_dir = project_wrapper.pipelines_dir
        actual_collections_dir = project_wrapper.collections_dir
        actual_datasets_dir = project_wrapper.datasets_dir
        actual_targets_dir = project_wrapper.targets_dir
        actual_marimba_dir = project_wrapper.marimba_dir
        actual_log_path = project_wrapper.log_path
        actual_name = project_wrapper.name
        actual_dry_run = project_wrapper.dry_run

        # Assert - Verify directory path values
        assert actual_root_dir == mock_project_dir, f"root_dir should be {mock_project_dir}, got {actual_root_dir}"
        assert (
            actual_pipelines_dir == expected_pipelines_dir
        ), f"pipelines_dir should be {expected_pipelines_dir}, got {actual_pipelines_dir}"
        assert (
            actual_collections_dir == expected_collections_dir
        ), f"collections_dir should be {expected_collections_dir}, got {actual_collections_dir}"
        assert (
            actual_datasets_dir == expected_datasets_dir
        ), f"datasets_dir should be {expected_datasets_dir}, got {actual_datasets_dir}"
        assert (
            actual_targets_dir == expected_targets_dir
        ), f"targets_dir should be {expected_targets_dir}, got {actual_targets_dir}"
        assert (
            actual_marimba_dir == expected_marimba_dir
        ), f"marimba_dir should be {expected_marimba_dir}, got {actual_marimba_dir}"
        assert actual_log_path == expected_log_path, f"log_path should be {expected_log_path}, got {actual_log_path}"

        # Assert - Verify project metadata values
        assert actual_name == expected_name, f"name should be {expected_name}, got {actual_name}"
        assert (
            actual_dry_run is expected_dry_run
        ), f"dry_run should be {expected_dry_run} by default, got {actual_dry_run}"

        # Assert - Verify all path properties return Path objects
        assert isinstance(actual_root_dir, Path), f"root_dir should be Path instance, got {type(actual_root_dir)}"
        assert isinstance(
            actual_pipelines_dir,
            Path,
        ), f"pipelines_dir should be Path instance, got {type(actual_pipelines_dir)}"
        assert isinstance(
            actual_collections_dir,
            Path,
        ), f"collections_dir should be Path instance, got {type(actual_collections_dir)}"
        assert isinstance(
            actual_datasets_dir,
            Path,
        ), f"datasets_dir should be Path instance, got {type(actual_datasets_dir)}"
        assert isinstance(
            actual_targets_dir,
            Path,
        ), f"targets_dir should be Path instance, got {type(actual_targets_dir)}"
        assert isinstance(
            actual_marimba_dir,
            Path,
        ), f"marimba_dir should be Path instance, got {type(actual_marimba_dir)}"
        assert isinstance(actual_log_path, Path), f"log_path should be Path instance, got {type(actual_log_path)}"

    @pytest.mark.unit
    def test_project_wrapper_directory_properties_create_directories_on_access(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that ProjectWrapper directory properties create directories when accessed.

        This unit test verifies that directory property accessors create their respective
        directories when accessed and return the correct Path objects. It tests the idempotent
        behavior where directory properties call mkdir(exist_ok=True) to ensure directories exist.
        """
        # Arrange - Create minimal project structure with only required .marimba directory
        project_dir = tmp_path / "test_project"
        project_dir.mkdir()
        (project_dir / ".marimba").mkdir()

        # Define expected directory paths
        expected_pipelines_dir = project_dir / "pipelines"
        expected_collections_dir = project_dir / "collections"
        expected_datasets_dir = project_dir / "datasets"
        expected_targets_dir = project_dir / "targets"
        expected_marimba_dir = project_dir / ".marimba"

        # Create project wrapper (this will create directories via _load_* methods)
        project_wrapper = ProjectWrapper(project_dir)

        # Act - Access directory properties multiple times to test idempotent behavior
        actual_pipelines_dir_1 = project_wrapper.pipelines_dir
        actual_pipelines_dir_2 = project_wrapper.pipelines_dir
        actual_collections_dir = project_wrapper.collections_dir
        actual_datasets_dir = project_wrapper.datasets_dir
        actual_targets_dir = project_wrapper.targets_dir
        actual_marimba_dir = project_wrapper.marimba_dir

        # Assert - Verify directories exist after property access
        assert expected_pipelines_dir.exists(), "pipelines_dir property should ensure directory exists"
        assert expected_collections_dir.exists(), "collections_dir property should ensure directory exists"
        assert expected_datasets_dir.exists(), "datasets_dir property should ensure directory exists"
        assert expected_targets_dir.exists(), "targets_dir property should ensure directory exists"
        assert expected_marimba_dir.exists(), "marimba_dir should exist"

        # Assert - Verify returned paths are correct
        assert actual_pipelines_dir_1 == expected_pipelines_dir, "pipelines_dir path should be correct"
        assert actual_pipelines_dir_2 == expected_pipelines_dir, "pipelines_dir should return consistent path"
        assert actual_collections_dir == expected_collections_dir, "collections_dir path should be correct"
        assert actual_datasets_dir == expected_datasets_dir, "datasets_dir path should be correct"
        assert actual_targets_dir == expected_targets_dir, "targets_dir path should be correct"
        assert actual_marimba_dir == expected_marimba_dir, "marimba_dir path should be correct"

        # Assert - Verify directories are actual directories, not files
        assert expected_pipelines_dir.is_dir(), "pipelines_dir should be a directory"
        assert expected_collections_dir.is_dir(), "collections_dir should be a directory"
        assert expected_datasets_dir.is_dir(), "datasets_dir should be a directory"
        assert expected_targets_dir.is_dir(), "targets_dir should be a directory"
        assert expected_marimba_dir.is_dir(), "marimba_dir should be a directory"

        # Assert - Verify properties are idempotent (multiple calls return same result)
        assert actual_pipelines_dir_1 is not actual_pipelines_dir_2, "Each call should return a new Path object"
        assert actual_pipelines_dir_1 == actual_pipelines_dir_2, "But Path objects should be equal"

    @pytest.mark.integration
    def test_create_target_with_valid_parameters_creates_target_successfully(
        self,
        mocker: pytest_mock.MockerFixture,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that creating a target with valid parameters successfully creates a DistributionTargetWrapper.

        This integration test verifies that create_target validates the target name,
        creates the target configuration file, properly delegates to DistributionTargetWrapper.create,
        stores the wrapper in the project's target collection, and logs the creation.
        """
        # Arrange
        target_name = "test_target"
        target_type = "s3"
        target_config = {"bucket": "test-bucket", "region": "us-west-2"}
        expected_target_path = mock_project_dir / "targets" / f"{target_name}.yml"

        # Mock only the external file creation and DistributionTargetWrapper.create to avoid filesystem dependencies
        mock_target_wrapper = mocker.Mock(spec=DistributionTargetWrapper)
        mock_target_wrapper.__class__ = DistributionTargetWrapper  # type: ignore[assignment]
        mock_create = mocker.patch("marimba.core.wrappers.target.DistributionTargetWrapper.create")
        mock_create.return_value = mock_target_wrapper

        # Ensure targets directory exists (integration aspect)
        assert project_wrapper.targets_dir.exists(), "Targets directory should be created by project wrapper"

        # Verify initial state
        assert target_name not in project_wrapper._target_wrappers, "Target should not exist initially"

        # Act
        result = project_wrapper.create_target(target_name, target_type, target_config)

        # Assert
        assert (
            result is mock_target_wrapper
        ), "create_target should return the created DistributionTargetWrapper instance"

        # Verify DistributionTargetWrapper.create was called with correct parameters
        mock_create.assert_called_once_with(
            expected_target_path,
            target_type,
            target_config,
        ), "DistributionTargetWrapper.create should be called with correct target path, type, and config"

        # Verify target is stored in project's internal target collection
        assert (
            target_name in project_wrapper._target_wrappers
        ), "Target should be added to project's internal target collection"
        assert (
            project_wrapper._target_wrappers[target_name] is mock_target_wrapper
        ), "Target wrapper should be stored correctly with exact reference"

        # Verify target is accessible through public property
        assert (
            target_name in project_wrapper.target_wrappers
        ), "Target should be accessible through public target_wrappers property"
        assert (
            project_wrapper.target_wrappers[target_name] is mock_target_wrapper
        ), "Target wrapper should be accessible through public property"

    @pytest.mark.integration
    def test_create_target_with_invalid_name_raises_invalid_name_error(self, project_wrapper: ProjectWrapper) -> None:
        """Test that creating a target with invalid name raises InvalidNameError.

        Verifies that create_target properly validates target names and raises
        InvalidNameError for names containing invalid characters.
        """
        # Arrange
        invalid_target_name = "invalid target"  # Contains space which is invalid
        target_type = "s3"
        target_config = {"bucket": "test-bucket"}

        # Act & Assert
        with pytest.raises(ProjectWrapper.InvalidNameError, match=invalid_target_name):
            project_wrapper.create_target(invalid_target_name, target_type, target_config)

    @pytest.mark.unit
    def test_check_name_valid(self):
        """Test that check_name accepts valid names without raising exceptions.

        Valid names should contain only alphanumeric characters, underscores, and dashes.
        This test verifies that the static method correctly validates compliant names.
        """
        # Arrange
        valid_names = [
            "valid_name",  # underscore
            "valid-name",  # dash
            "valid123",  # alphanumeric mix
            "ValidName",  # mixed case
            "a",  # single character
            "test_123-abc",  # combination of valid characters
        ]

        # Act & Assert
        for name in valid_names:
            # Should complete without raising any exception
            ProjectWrapper.check_name(name)  # This should not raise any exception

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "invalid_name",
        [
            "invalid name",  # spaces
            "invalid/name",  # slash
            "invalid@name",  # special chars
            "invalid\\name",  # backslash
            "invalid.name",  # dot
            "invalid#name",  # hash
            "invalid$name",  # dollar sign
        ],
    )
    def test_check_name_invalid(self, invalid_name):
        """Test that invalid names raise InvalidNameError with the specific invalid name in the exception.

        The check_name static method should validate that names contain only alphanumeric characters,
        underscores, and dashes. Any name containing other characters should raise InvalidNameError
        with the invalid name included in the exception message.
        """
        # Arrange
        expected_exception_type = ProjectWrapper.InvalidNameError
        escaped_name = re.escape(invalid_name)
        expected_pattern = f"^{escaped_name}$"

        # Act & Assert
        with pytest.raises(expected_exception_type, match=expected_pattern) as exc_info:
            ProjectWrapper.check_name(invalid_name)

        # Additional assertion to verify the exception contains the invalid name
        assert (
            str(exc_info.value) == invalid_name
        ), f"Exception message should be '{invalid_name}', got '{exc_info.value!s}'"

    @pytest.mark.unit
    def test_check_name_empty_string(self):
        """Test that check_name accepts empty string without raising InvalidNameError.

        Empty strings should pass validation since the character validation loop
        doesn't execute when there are no characters to validate.
        """
        # Arrange
        empty_name = ""

        # Act & Assert
        # check_name should complete without raising any exception for empty string
        # If an exception is raised, the test will fail
        ProjectWrapper.check_name(empty_name)

        # Explicit assertion to verify the function completed successfully
        assert True, "check_name completed successfully without raising an exception"

    @pytest.mark.integration
    def test_delete_pipeline_nonexistent_pipeline_raises_delete_pipeline_error(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that deleting a non-existent pipeline raises DeletePipelineError with specific message.

        This integration test verifies that delete_pipeline properly validates that the pipeline exists
        before attempting deletion and raises the appropriate exception with a descriptive error message
        when the pipeline directory is not found.
        """
        # Arrange
        nonexistent_pipeline_name = "nonexistent_pipeline"
        expected_pipeline_dir = project_wrapper.pipelines_dir / nonexistent_pipeline_name
        expected_error_message = f'A pipeline with the name "{nonexistent_pipeline_name}" does not exist'

        # Verify setup - pipeline should not exist
        assert not expected_pipeline_dir.exists(), "Pipeline directory should not exist for this test"

        # Act & Assert
        with pytest.raises(ProjectWrapper.DeletePipelineError, match=f"^{re.escape(expected_error_message)}$"):
            project_wrapper.delete_pipeline(nonexistent_pipeline_name, dry_run=False)

    @pytest.mark.integration
    def test_delete_collection_nonexistent_collection_raises_no_such_collection_error(self, project_wrapper):
        """Test that deleting a non-existent collection raises NoSuchCollectionError with specific message.

        This integration test verifies that delete_collection properly validates that the collection exists
        before attempting deletion and raises the appropriate exception with a descriptive error message
        when the collection directory is not found.
        """
        # Arrange
        nonexistent_collection_name = "nonexistent_collection"
        expected_error_message = f'A collection with the name "{nonexistent_collection_name}" does not exist'

        # Act & Assert
        with pytest.raises(ProjectWrapper.NoSuchCollectionError, match=f"^{re.escape(expected_error_message)}$"):
            project_wrapper.delete_collection(nonexistent_collection_name, dry_run=False)

    @pytest.mark.integration
    def test_delete_dataset_nonexistent(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that deleting a non-existent dataset raises NoSuchDatasetError with specific message.

        This integration test verifies that delete_dataset properly validates that the dataset exists
        before attempting deletion and raises the appropriate exception with a descriptive error message
        when the dataset directory is not found.
        """
        # Arrange
        nonexistent_dataset_name = "nonexistent_dataset"
        expected_dataset_dir = project_wrapper.datasets_dir / nonexistent_dataset_name
        expected_error_message = f'A dataset with the name "{nonexistent_dataset_name}" does not exist'

        # Verify setup - dataset should not exist
        assert not expected_dataset_dir.exists(), "Dataset directory should not exist for this test"

        # Act & Assert
        with pytest.raises(
            ProjectWrapper.NoSuchDatasetError,
            match=f"^{re.escape(expected_error_message)}$",
        ):
            project_wrapper.delete_dataset(nonexistent_dataset_name, dry_run=False)

    @pytest.mark.integration
    def test_delete_target_nonexistent_target_raises_no_such_target_error(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that deleting a non-existent target raises NoSuchTargetError with specific message.

        This integration test verifies that delete_target properly validates that the target file exists
        before attempting deletion and raises the appropriate NoSuchTargetError with a descriptive error message
        when the target configuration file is not found.
        """
        # Arrange
        nonexistent_target_name = "nonexistent_target"
        expected_target_path = project_wrapper.targets_dir / f"{nonexistent_target_name}.yml"
        expected_error_message = f'A distribution target with the name "{nonexistent_target_name}" does not exist'

        # Verify setup - target should not exist
        assert not expected_target_path.exists(), "Target file should not exist for this test"

        # Act & Assert
        with pytest.raises(
            ProjectWrapper.NoSuchTargetError,
            match=f"^{re.escape(expected_error_message)}$",
        ):
            project_wrapper.delete_target(nonexistent_target_name, dry_run=False)

    @pytest.mark.integration
    def test_delete_target_existing_target_deletes_file_and_returns_path(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that deleting an existing target successfully removes the file and returns the correct path.

        This integration test verifies that the delete_target method properly validates the target name,
        removes the target configuration file from the filesystem, logs the deletion operation, and returns
        the deleted file path. It tests the real file deletion functionality with minimal mocking.
        """
        # Arrange
        target_name = "test_target"
        target_file = mock_project_dir / "targets" / f"{target_name}.yml"
        target_file.parent.mkdir(parents=True, exist_ok=True)
        target_file.touch()

        # Verify initial state - target file should exist before deletion
        assert target_file.exists(), f"Target file should exist at {target_file} before deletion"
        assert target_file.is_file(), f"Target path {target_file} should be a file, not a directory"

        # Act
        result = project_wrapper.delete_target(target_name, dry_run=False)

        # Assert
        assert not target_file.exists(), f"Target file {target_file} should be deleted after delete_target call"
        assert result == target_file, f"delete_target should return the deleted file path {target_file}, got {result}"

    @pytest.mark.integration
    def test_dry_run_delete_operations_preserve_directories_and_return_paths(
        self,
        mocker: pytest_mock.MockerFixture,
        mock_project_dir: Path,
    ) -> None:
        """Test that dry-run mode for delete operations preserves directories and returns correct paths.

        This integration test verifies that when delete operations are called with dry_run=True,
        the directories remain untouched on the filesystem while still returning the expected
        directory paths. It tests the integration between the ProjectWrapper's dry-run logic
        and the underlying remove_directory_tree utility function.
        """
        # Arrange
        project_wrapper = ProjectWrapper(mock_project_dir, dry_run=False)

        # Create test directories with proper structure
        pipeline_name = "test_pipeline"
        collection_name = "test_collection"
        dataset_name = "test_dataset"

        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        collection_dir = mock_project_dir / "collections" / collection_name
        dataset_dir = mock_project_dir / "datasets" / dataset_name

        pipeline_dir.mkdir(parents=True)
        collection_dir.mkdir(parents=True)
        dataset_dir.mkdir(parents=True)

        # Create required dataset structure for DatasetWrapper validation
        (dataset_dir / "data").mkdir()
        (dataset_dir / "logs").mkdir()
        (dataset_dir / "logs" / "pipelines").mkdir()
        (dataset_dir / "metadata.yml").touch()
        (dataset_dir / "manifest.json").touch()

        # Mock the remove_directory_tree function to verify it's called with correct parameters
        mock_remove_tree = mocker.patch("marimba.core.wrappers.project.remove_directory_tree")

        # Verify initial state - all directories should exist before dry-run deletion
        assert pipeline_dir.exists(), "Pipeline directory should exist before dry-run deletion"
        assert collection_dir.exists(), "Collection directory should exist before dry-run deletion"
        assert dataset_dir.exists(), "Dataset directory should exist before dry-run deletion"

        # Act - Test dry run doesn't actually delete but calls remove_directory_tree appropriately
        pipeline_result = project_wrapper.delete_pipeline(pipeline_name, dry_run=True)
        collection_result = project_wrapper.delete_collection(collection_name, dry_run=True)
        dataset_result = project_wrapper.delete_dataset(dataset_name, dry_run=True)

        # Assert - Verify directories still exist after dry-run operations
        assert pipeline_dir.exists(), "Pipeline directory should still exist after dry-run deletion"
        assert collection_dir.exists(), "Collection directory should still exist after dry-run deletion"
        assert dataset_dir.exists(), "Dataset directory should still exist after dry-run deletion"

        # Verify correct paths are returned
        assert pipeline_result == pipeline_dir, f"delete_pipeline should return pipeline directory path: {pipeline_dir}"
        assert (
            collection_result == collection_dir
        ), f"delete_collection should return collection directory path: {collection_dir}"
        assert dataset_result == dataset_dir, f"delete_dataset should return dataset directory path: {dataset_dir}"

        # Verify remove_directory_tree behavior - each method handles dry_run differently:
        # - delete_pipeline: doesn't call remove_directory_tree when dry_run=True
        # - delete_collection: always calls remove_directory_tree with dry_run parameter
        # - delete_dataset: checks self.dry_run (False), not the parameter, so it calls remove_directory_tree
        expected_calls = [
            mocker.call(collection_dir, "collection", True),
            mocker.call(dataset_dir, "dataset", True),
        ]
        mock_remove_tree.assert_has_calls(expected_calls, any_order=True)
        assert (
            mock_remove_tree.call_count == 2
        ), f"Expected 2 calls to remove_directory_tree, got {mock_remove_tree.call_count}"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("method_name", "args"),
        [
            ("create_pipeline", ("invalid name", "https://example.com", {})),
            ("create_collection", ("invalid name", {})),
            ("create_target", ("invalid name", "s3", {})),
            ("create_dataset", ("invalid name", {}, [], [])),
        ],
    )
    def test_create_methods_invalid_name(self, project_wrapper, method_name, args):
        """Test that create methods reject invalid names by raising InvalidNameError.

        This test verifies that all component creation methods (create_pipeline, create_collection,
        create_target, create_dataset) properly validate names using ProjectWrapper.check_name()
        and raise InvalidNameError when provided with names containing invalid characters like spaces.
        The exception message should contain the invalid name that was provided.
        """
        # Arrange
        invalid_name = args[0]  # First argument is always the name
        method = getattr(project_wrapper, method_name)

        # Act & Assert
        with pytest.raises(ProjectWrapper.InvalidNameError) as exc_info:
            method(*args)

        # Verify the exception message contains the invalid name
        assert (
            str(exc_info.value) == invalid_name
        ), f"Exception message should be the invalid name '{invalid_name}', but got '{exc_info.value!s}'"

    @pytest.mark.integration
    def test_create_project_already_exists(self, tmp_path: Path) -> None:
        """Test that creating a project when directory already exists raises FileExistsError.

        This integration test verifies that ProjectWrapper.create properly detects existing
        directories and raises FileExistsError with the expected message when attempting
        to create a project in a directory that already exists.
        """
        # Arrange
        existing_project_path = tmp_path / "existing_project"
        existing_project_path.mkdir()  # Create the directory to simulate it already exists

        # Verify setup - directory should exist before the test
        assert existing_project_path.exists(), "Setup failed: project directory should exist for this test"
        assert existing_project_path.is_dir(), "Setup failed: project path should be a directory"

        # Act & Assert
        expected_message = f'"{existing_project_path}" already exists'
        with pytest.raises(FileExistsError, match=f"^{re.escape(expected_message)}$"):
            ProjectWrapper.create(existing_project_path)

    @pytest.mark.unit
    def test_target_wrappers_property_returns_empty_dict_when_no_targets_exist(
        self,
        project_wrapper: ProjectWrapper,
    ) -> None:
        """Test that target_wrappers property returns empty dict when no target config files exist.

        This test verifies that the target_wrappers property correctly returns an empty dictionary
        when the targets directory exists but contains no configuration files.
        """
        # Arrange
        # The project_wrapper fixture creates a mock project with empty targets directory
        expected_result: dict[str, DistributionTargetWrapper] = {}

        # Verify precondition: targets directory should exist but be empty
        assert project_wrapper.targets_dir.exists(), "Targets directory should exist"
        assert len(list(project_wrapper.targets_dir.iterdir())) == 0, "Targets directory should be empty"

        # Act
        result = project_wrapper.target_wrappers

        # Assert
        assert result == expected_result, f"Expected empty dict {expected_result}, but got {result}"
        assert isinstance(result, dict), "target_wrappers should return a dictionary type"
        assert len(result) == 0, "target_wrappers should return an empty dictionary"

    @pytest.mark.integration
    def test_target_wrappers_property_returns_loaded_targets_with_valid_configs(
        self,
        mock_project_dir: Path,
    ) -> None:
        """
        Test that target_wrappers property loads and returns DistributionTargetWrapper instances.

        This test verifies that the target_wrappers property correctly loads DistributionTargetWrapper
        instances from valid configuration files in the targets directory. This is an integration test
        that verifies the interaction between ProjectWrapper and DistributionTargetWrapper when loading
        target configurations from the filesystem.
        """
        # Arrange
        targets_dir = mock_project_dir / "targets"
        target_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://s3.amazonaws.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        }
        target_path = targets_dir / "test_target.yml"
        target_path.write_text(yaml.dump(target_config))

        # Act
        project_wrapper = ProjectWrapper(mock_project_dir)
        result = project_wrapper.target_wrappers

        # Assert
        assert isinstance(result, dict), "target_wrappers property should return a dictionary"
        assert len(result) == 1, f"Expected exactly 1 target, got {len(result)}"
        assert "test_target" in result, f"Expected 'test_target' key in {list(result.keys())}"

        target_wrapper = result["test_target"]
        assert isinstance(
            target_wrapper,
            DistributionTargetWrapper,
        ), f"Expected DistributionTargetWrapper, got {type(target_wrapper)}"

        # Verify the wrapper configuration matches what was written to file
        assert (
            target_wrapper.config == target_config
        ), f"Target wrapper config mismatch. Expected: {target_config}, Got: {target_wrapper.config}"
        assert target_wrapper.config["type"] == "s3", f"Expected type 's3', got {target_wrapper.config.get('type')}"
        assert (
            target_wrapper.config_path == target_path
        ), f"Config path mismatch. Expected: {target_path}, Got: {target_wrapper.config_path}"

        # Verify nested config structure
        config_section = target_wrapper.config["config"]
        assert (
            config_section["bucket_name"] == "test-bucket"
        ), f"Expected bucket_name 'test-bucket', got {config_section.get('bucket_name')}"
        assert (
            config_section["endpoint_url"] == "https://s3.amazonaws.com"
        ), f"Expected endpoint_url 'https://s3.amazonaws.com', got {config_section.get('endpoint_url')}"
        assert (
            config_section["access_key_id"] == "test-key"
        ), f"Expected access_key_id 'test-key', got {config_section.get('access_key_id')}"

    @pytest.mark.integration
    def test_delete_pipeline_with_readonly_files_successfully_removes_directory(
        self,
        project_wrapper: ProjectWrapper,
        mock_project_dir: Path,
    ) -> None:
        """Test that delete_pipeline successfully removes directory containing readonly files.

        This integration test verifies that the delete_pipeline method can handle directories
        containing readonly files by utilizing the underlying remove_directory_tree function
        which uses shutil.rmtree. The test creates actual readonly files and verifies they
        are successfully deleted, testing the real deletion behavior without mocking.
        """
        # Arrange
        pipeline_name = "readonly_pipeline"
        pipeline_dir = mock_project_dir / "pipelines" / pipeline_name
        pipeline_dir.mkdir(parents=True)

        # Create test files with different permission levels
        regular_file = pipeline_dir / "regular.txt"
        readonly_file = pipeline_dir / "readonly.txt"
        nested_dir = pipeline_dir / "nested"
        nested_dir.mkdir()
        nested_readonly_file = nested_dir / "nested_readonly.txt"

        # Write content to files
        regular_file.write_text("regular content")
        readonly_file.write_text("readonly content")
        nested_readonly_file.write_text("nested readonly content")

        # Set readonly permissions
        readonly_file.chmod(0o444)  # Read-only permissions
        nested_readonly_file.chmod(0o444)  # Read-only permissions

        # Verify setup conditions
        assert pipeline_dir.exists(), "Pipeline directory should exist before deletion"
        assert regular_file.exists(), "Regular file should exist before deletion"
        assert readonly_file.exists(), "Readonly file should exist before deletion"
        assert nested_readonly_file.exists(), "Nested readonly file should exist before deletion"
        assert not os.access(readonly_file, os.W_OK), "File should be readonly"
        assert not os.access(nested_readonly_file, os.W_OK), "Nested file should be readonly"

        # Act
        result = project_wrapper.delete_pipeline(pipeline_name, dry_run=False)

        # Assert
        assert result == pipeline_dir, f"Should return the deleted directory path: {pipeline_dir}"
        assert not pipeline_dir.exists(), "Pipeline directory should be completely removed"
        assert not pipeline_dir.is_dir(), "Pipeline directory should no longer exist as a directory"
        assert not regular_file.exists(), "Regular file should be deleted"
        assert not readonly_file.exists(), "Readonly file should be deleted despite being readonly"
        assert not nested_readonly_file.exists(), "Nested readonly file should be deleted"
        assert not nested_dir.exists(), "Nested directory should be deleted"


class TestProjectWrapperExceptions:
    """Test ProjectWrapper exception classes."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("exception_class", "message"),
        [
            (ProjectWrapper.InvalidNameError, "Invalid name"),
            (ProjectWrapper.InvalidStructureError, "Invalid structure"),
            (ProjectWrapper.NoSuchCollectionError, "No such collection"),
            (ProjectWrapper.NoSuchPipelineError, "No such pipeline"),
            (ProjectWrapper.NoSuchDatasetError, "No such dataset"),
            (ProjectWrapper.NoSuchTargetError, "No such target"),
            (ProjectWrapper.DeletePipelineError, "Delete pipeline error"),
            (ProjectWrapper.CreateCollectionError, "Create collection error"),
        ],
    )
    def test_project_wrapper_exception_instantiation_and_string_representation(
        self,
        exception_class: type[Exception],
        message: str,
    ) -> None:
        """Test that ProjectWrapper exception classes can be instantiated and properly represent their messages.

        This unit test verifies that each ProjectWrapper exception class can be successfully instantiated
        with a message string and that the exception's string representation matches the provided message.
        """
        # Arrange
        expected_message = message

        # Act
        exception_instance = exception_class(message)

        # Assert
        assert str(exception_instance) == expected_message, (
            f"Exception string representation mismatch for {exception_class.__name__}: "
            f"expected '{expected_message}', got '{exception_instance!s}'"
        )
        assert isinstance(
            exception_instance,
            exception_class,
        ), f"Exception type mismatch: expected {exception_class.__name__}, got {type(exception_instance).__name__}"
        assert exception_instance.args[0] == expected_message, (
            f"Exception args[0] mismatch for {exception_class.__name__}: "
            f"expected '{expected_message}', got '{exception_instance.args[0]}'"
        )
        assert issubclass(exception_class, Exception), f"{exception_class.__name__} should be a subclass of Exception"


class TestUtilityFunctions:
    """Test utility functions."""

    @pytest.mark.unit
    def test_get_merged_keyword_args_empty(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that get_merged_keyword_args returns empty dict when both inputs are empty.

        This test verifies that when both kwargs is empty and extra_args is None,
        the function returns an empty dictionary without any side effects or warnings.
        """
        # Arrange
        logger = mocker.Mock()
        empty_kwargs: dict[str, Any] = {}
        extra_args = None

        # Act
        result = get_merged_keyword_args(empty_kwargs, extra_args, logger)

        # Assert
        assert result == {}, "Should return empty dict when both inputs are empty"
        logger.warning.assert_not_called(), "Should not log warnings for empty inputs"

    @pytest.mark.unit
    def test_get_merged_keyword_args_basic(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that basic keyword argument merging returns original kwargs when no extra args provided.

        This test verifies that when extra_args is None, the function returns the original
        kwargs dictionary unchanged without any modifications or side effects.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1", "key2": "value2"}

        # Act
        result = get_merged_keyword_args(kwargs, None, logger)

        # Assert
        assert result == kwargs, "Should return original kwargs unchanged when extra_args is None"
        assert result is not kwargs, "Should return a new dictionary, not the same reference"
        logger.warning.assert_not_called(), "Should not log any warnings for valid input"

    @pytest.mark.unit
    def test_get_merged_keyword_args_with_extra(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that get_merged_keyword_args correctly merges extra arguments with existing kwargs.

        This test verifies that when extra_args contains valid key=value pairs, the function
        correctly merges them with existing kwargs while attempting to evaluate string values
        as Python literals. When evaluation fails for non-literal strings, the function keeps
        the original string values and logs appropriate warnings.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1"}
        extra_args = ["key2=value2", "key3=value3"]
        expected = {"key1": "value1", "key2": "value2", "key3": "value3"}

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected, f"Should correctly merge kwargs with extra args, expected {expected}, got {result}"
        assert result is not kwargs, "Should return a new dictionary, not the same reference"

        # Verify warnings were logged for evaluation failures (non-literal strings)
        expected_warning_calls = [
            mocker.call('Could not evaluate extra argument value: "value2"'),
            mocker.call('Could not evaluate extra argument value: "value3"'),
        ]
        logger.warning.assert_has_calls(expected_warning_calls, any_order=False)

    @pytest.mark.unit
    def test_get_merged_keyword_args_override(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that extra args can override existing kwargs and add new ones with evaluation warnings.

        This test verifies that when extra_args contains key-value pairs that both override
        existing kwargs keys and add new keys, the function correctly merges them with the
        extra_args taking precedence. Since the values are not valid Python literals, they
        will be kept as strings and warnings will be logged for the evaluation failures.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "original"}
        extra_args = ["key1=override", "key2=new"]
        expected = {"key1": "override", "key2": "new"}

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert result is not kwargs, "Should return a new dictionary, not the same reference"

        # Verify warnings were logged for evaluation failures
        expected_warning_calls = [
            mocker.call('Could not evaluate extra argument value: "override"'),
            mocker.call('Could not evaluate extra argument value: "new"'),
        ]
        logger.warning.assert_has_calls(expected_warning_calls, any_order=False)

    @pytest.mark.unit
    def test_get_merged_keyword_args_evaluation_error_fallback_to_string(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that evaluation errors result in fallback to string values with warning log.

        When ast.literal_eval fails to parse a value (ValueError or SyntaxError), the function
        should keep the original string value and log a warning. This test verifies the
        error handling behavior for unparseable Python literals in extra arguments.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs: dict[str, Any] = {"existing_key": "existing_value"}
        extra_args = ["key1=not_valid_literal", "key2=123", "key3=another_invalid"]
        expected_result = {
            "existing_key": "existing_value",  # Original kwargs preserved
            "key1": "not_valid_literal",  # Invalid literal kept as string
            "key2": 123,  # Valid literal evaluated correctly
            "key3": "another_invalid",  # Another invalid literal kept as string
        }
        expected_warning_calls = [
            mocker.call('Could not evaluate extra argument value: "not_valid_literal"'),
            mocker.call('Could not evaluate extra argument value: "another_invalid"'),
        ]

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected_result, f"Expected {expected_result}, but got {result}"
        assert len(result) == 4, "Should contain all keys from kwargs and extra_args"
        assert result["existing_key"] == "existing_value", "Should preserve original kwargs"
        assert isinstance(result["key1"], str), "Should fallback to string for evaluation error"
        assert isinstance(result["key2"], int), "Should correctly evaluate valid literals"
        assert isinstance(result["key3"], str), "Should fallback to string for evaluation error"
        logger.warning.assert_has_calls(expected_warning_calls, any_order=False)

    @pytest.mark.unit
    def test_get_merged_keyword_args_syntax_error_fallback_to_string(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that SyntaxError in evaluation results in fallback to string values with warning log.

        When ast.literal_eval encounters a syntax error (e.g., malformed Python syntax), the function
        should keep the original string value and log a warning. This test verifies the error handling
        behavior specifically for syntax errors in extra arguments.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs: dict[str, Any] = {}
        extra_args = ["key1={malformed_dict"]  # Missing closing brace - syntax error
        expected_result = {"key1": "{malformed_dict"}  # Should keep as string after syntax error
        expected_warning_message = 'Could not evaluate extra argument value: "{malformed_dict"'

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected_result, "Should return dict with original string value after syntax error"
        assert "key1" in result, "Should contain the key from extra_args"
        assert result["key1"] == "{malformed_dict", "Should keep malformed value as string"
        logger.warning.assert_called_once_with(expected_warning_message)

    @pytest.mark.unit
    def test_get_merged_keyword_args_numeric_values(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that get_merged_keyword_args correctly parses and converts numeric values from extra_args.

        This test verifies that the function can handle various Python literal types (integers, floats,
        booleans, lists) in the extra_args list by using ast.literal_eval to convert string representations
        to their proper Python types. It ensures that no warnings are logged for valid numeric literals.
        """
        # Arrange
        mock_logger = mocker.Mock()
        kwargs: dict[str, Any] = {}
        extra_args = ["int_val=42", "float_val=3.14", "bool_val=True", "list_val=[1,2,3]"]
        expected = {"int_val": 42, "float_val": 3.14, "bool_val": True, "list_val": [1, 2, 3]}

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, mock_logger)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        mock_logger.warning.assert_not_called(), "Should not log warnings when processing valid numeric types"

    @pytest.mark.unit
    def test_get_merged_keyword_args_none_extra_returns_copy_without_warnings(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test that get_merged_keyword_args returns a copy of kwargs when extra_args is None.

        This unit test verifies that when extra_args is None, the function returns the original
        kwargs dictionary unchanged without any modifications or side effects. It ensures that
        a new dictionary is returned (not the same reference) and that no warnings are logged
        to the provided logger instance.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1"}

        # Act
        result = get_merged_keyword_args(kwargs, None, logger)

        # Assert
        assert result == kwargs, "Should return original kwargs unchanged when extra_args is None"
        assert result is not kwargs, "Should return a new dictionary, not the same reference"
        logger.warning.assert_not_called(), "Should not log any warnings when extra_args is None"

    @pytest.mark.unit
    def test_get_merged_keyword_args_complex_types(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that get_merged_keyword_args correctly parses and merges complex data types from extra_args.

        This test verifies that the function can handle various Python literal types (integers, lists,
        dictionaries) in the extra_args list by using ast.literal_eval to convert string representations
        to their proper Python types, and that these are correctly merged with existing kwargs.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1"}
        extra_args = ["key2=123", "key3=[1,2,3]", "key4={'nested': 'dict'}"]

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        expected = {"key1": "value1", "key2": 123, "key3": [1, 2, 3], "key4": {"nested": "dict"}}
        assert result == expected, f"Expected {expected}, but got {result}"

        # Verify no warnings were logged for valid input
        logger.warning.assert_not_called()

    @pytest.mark.unit
    def test_get_merged_keyword_args_skip_invalid_format(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that get_merged_keyword_args skips invalid format arguments while processing valid ones.

        This test verifies that when extra_args contains arguments without the expected key=value format,
        the function skips the invalid arguments, logs appropriate warnings, and still processes valid
        arguments correctly. It ensures the merged result contains only valid key-value pairs.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1"}
        extra_args = ["invalid_format", "key2=value2"]  # First arg missing '=', second is valid
        expected = {"key1": "value1", "key2": "value2"}

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"

        # Verify invalid format warning is logged
        logger.warning.assert_any_call('Invalid extra argument provided: "invalid_format"')

        # Verify evaluation warning IS called for 'value2' since it's not a valid Python literal
        evaluation_warning_calls = [
            call for call in logger.warning.call_args_list if "Could not evaluate extra argument value:" in str(call)
        ]
        assert (
            len(evaluation_warning_calls) == 1
        ), "Should log evaluation warning for 'value2' string which is not a valid Python literal"

    @pytest.mark.unit
    def test_get_merged_keyword_args_invalid_value(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test that invalid Python literal values are treated as strings and warnings are logged.

        This test verifies that when ast.literal_eval fails to parse values in extra_args
        (due to invalid Python literal syntax), the function falls back to treating the values
        as strings and logs appropriate warning messages for each failed evaluation.
        """
        # Arrange
        logger = mocker.Mock()
        kwargs = {"key1": "value1"}
        extra_args = ["key2=invalid_python_literal", 'key3="valid_string"']
        expected_result = {"key1": "value1", "key2": "invalid_python_literal", "key3": "valid_string"}

        # Act
        result = get_merged_keyword_args(kwargs, extra_args, logger)

        # Assert
        assert result == expected_result, f"Expected {expected_result}, but got {result}"
        logger.warning.assert_called_once_with(
            'Could not evaluate extra argument value: "invalid_python_literal"',
        ), "Should log exactly one warning for the invalid Python literal 'invalid_python_literal'"
