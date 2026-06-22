"""
Test cases for the BasePipeline abstract base class.

This module contains comprehensive tests for the BasePipeline class functionality,
including initialization, abstract methods, command execution, and logging.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_mock

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata


class ConcretePipeline(BasePipeline):
    """Concrete implementation of BasePipeline for testing purposes."""

    def __init__(
        self,
        root_path: Path | str,
        config: dict[str, Any] | None = None,
        metadata_class: type[BaseMetadata] = BaseMetadata,
        *,
        dry_run: bool = False,
    ) -> None:
        super().__init__(root_path, config, metadata_class, dry_run=dry_run)
        # Track method calls for testing
        self.import_called = False
        self.process_called = False
        self.package_called = False
        self.post_package_called = False

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        self.import_called = True
        self.last_import_args = (data_dir, source_path, config, kwargs)

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        self.process_called = True
        self.last_process_args = (data_dir, config, kwargs)

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: Any,
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        self.package_called = True
        self.last_package_args = (data_dir, config, kwargs)
        return {
            Path("source1.txt"): (Path("dest1.txt"), [], {"meta": "data1"}),
            Path("source2.txt"): (Path("dest2.txt"), None, None),
        }

    def _post_package(self, dataset_dir: Path) -> set[Path]:
        self.post_package_called = True
        self.last_post_package_args = (dataset_dir,)
        return {Path("changed1.txt"), Path("changed2.txt")}


class AbstractOnlyPipeline(BasePipeline):
    """Pipeline that doesn't implement abstract methods for testing."""


class TestBasePipelineInitialization:
    """Test cases for BasePipeline initialization."""

    @pytest.mark.unit
    def test_init_with_string_path(self) -> None:
        """
        Test initialization with string root path verifies all properties are set correctly.

        This test ensures that when a BasePipeline subclass is initialized with a string
        path instead of a Path object, all instance properties are correctly assigned
        and accessible through the public interface, with the string path preserved as-is.
        """
        # Arrange
        root_path = "/test/path"
        config = {"key": "value"}

        # Act
        pipeline = ConcretePipeline(root_path, config, dry_run=True)

        # Assert
        assert pipeline._root_path == root_path, "Expected root path to be set correctly as string"
        assert isinstance(
            pipeline._root_path,
            str,
        ), "Expected root path to remain as string type when initialized with string"
        assert pipeline.config == config, "Expected config to be accessible through public property"
        assert pipeline.config is config, "Expected config property to return the same object reference"
        assert pipeline._metadata_class == BaseMetadata, "Expected metadata class to be BaseMetadata by default"
        assert pipeline.dry_run is True, "Expected dry_run to be True when explicitly enabled"

    @pytest.mark.unit
    def test_init_with_path_object(self) -> None:
        """
        Test initialization with Path object sets all properties correctly with default values.

        This test verifies that when a BasePipeline subclass is initialized with a Path
        object instead of a string, all instance properties are correctly assigned with
        their default values and the Path object reference is preserved exactly.
        """
        # Arrange
        root_path = Path("/test/path")

        # Act
        pipeline = ConcretePipeline(root_path)

        # Assert - Test property access through public interfaces where possible
        assert (
            pipeline._root_path == root_path
        ), "Expected root path to be set to the exact Path object provided during initialization"
        assert (
            pipeline._root_path is root_path
        ), "Expected root path to preserve the same Path object reference provided during initialization"
        assert (
            pipeline.config is None
        ), "Expected config property to return None when no configuration is provided during initialization"
        assert (
            pipeline._metadata_class == BaseMetadata
        ), "Expected metadata class to be set to BaseMetadata default when not explicitly provided"
        assert (
            pipeline.dry_run is False
        ), "Expected dry_run property to return False when not explicitly enabled during initialization"

        # Test that Path object behavior is preserved
        assert isinstance(
            pipeline._root_path,
            Path,
        ), "Expected root path to remain as Path object instance after initialization"
        assert str(pipeline._root_path) == "/test/path", "Expected Path object to contain the correct path string value"

    @pytest.mark.unit
    def test_init_with_custom_metadata_class(self) -> None:
        """Test initialization with custom metadata class verifies metadata class is properly set."""

        # Arrange
        class CustomMetadata(BaseMetadata):
            @property
            def datetime(self) -> datetime | None:
                return None

            @property
            def latitude(self) -> float | None:
                return None

            @property
            def longitude(self) -> float | None:
                return None

            @property
            def altitude(self) -> float | None:
                return None

            @property
            def context(self) -> str | None:
                return None

            @property
            def license(self) -> str | None:
                return None

            @property
            def creators(self) -> list[str]:
                return []

            @property
            def hash_sha256(self) -> str | None:
                return None

            @hash_sha256.setter
            def hash_sha256(self, value: str) -> None:
                pass

            @classmethod
            def create_dataset_metadata(
                cls,
                dataset_name: str,
                root_dir: Path,
                items: dict[str, list[BaseMetadata]],
                metadata_name: str | None = None,
                *,
                dry_run: bool = False,
                saver_overwrite: Any | None = None,
            ) -> None:
                pass

            @classmethod
            def process_files(
                cls,
                dataset_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]],
                max_workers: int | None = None,
                logger: Any | None = None,
                *,
                dry_run: bool = False,
                chunk_size: int | None = None,
            ) -> None:
                pass

        root_path = "/test"

        # Act
        pipeline = ConcretePipeline(root_path, metadata_class=CustomMetadata)

        # Assert
        assert pipeline._metadata_class == CustomMetadata, "Expected custom metadata class to be set correctly"
        assert pipeline._root_path == root_path, "Expected root path to be set correctly"
        assert pipeline._config is None, "Expected config to be None by default"
        assert not pipeline._dry_run, "Expected dry_run to be False by default"

    @pytest.mark.unit
    def test_default_values(self) -> None:
        """Test that default values are set correctly when no optional parameters are provided."""
        # Arrange
        root_path = "/test"

        # Act
        pipeline = ConcretePipeline(root_path)

        # Assert
        assert pipeline._root_path == root_path, "Expected root path to be set correctly"
        assert pipeline.config is None, "Expected config to be None by default"
        assert not pipeline.dry_run, "Expected dry_run to be False by default"
        assert pipeline._metadata_class == BaseMetadata, "Expected metadata_class to be BaseMetadata by default"


class TestBasePipelineProperties:
    """Test cases for BasePipeline properties."""

    @pytest.mark.unit
    def test_config_property(self) -> None:
        """Test that the config property returns the exact configuration passed during initialization."""
        # Arrange
        config = {"test": "value", "number": 42}

        # Act
        pipeline = ConcretePipeline("/test", config=config)
        result = pipeline.config

        # Assert
        assert (
            result == config
        ), "Expected config property to return the exact configuration passed during initialization"
        assert result is config, "Expected config property to return the same object reference"

    @pytest.mark.unit
    def test_config_property_none(self) -> None:
        """Test that the config property returns None when no configuration is provided during initialization."""
        # Arrange & Act
        pipeline = ConcretePipeline("/test")
        result = pipeline.config

        # Assert
        assert result is None, "Expected config property to return None when no configuration is provided"

    @pytest.mark.unit
    def test_dry_run_property_enabled_returns_true(self) -> None:
        """
        Test dry_run property returns True when explicitly enabled during initialization.

        This test verifies that the dry_run property correctly returns True when the
        dry_run parameter is explicitly set to True during pipeline initialization,
        ensuring the property accurately reflects the initialization state.
        """
        # Arrange
        root_path = "/test"
        dry_run_enabled = True

        # Act
        pipeline = ConcretePipeline(root_path, dry_run=dry_run_enabled)
        result = pipeline.dry_run

        # Assert
        assert (
            result is True
        ), "Expected dry_run property to return True when dry_run flag is enabled during initialization"
        assert isinstance(result, bool), "Expected dry_run property to return a boolean type"

    @pytest.mark.unit
    def test_dry_run_property_false(self) -> None:
        """
        Test dry_run property returns False when explicitly disabled during initialization.

        This test verifies that the dry_run property correctly returns False when the
        dry_run parameter is explicitly set to False during pipeline initialization,
        ensuring the property accurately reflects the initialization state and returns
        the exact boolean False value rather than a truthy/falsy equivalent.
        """
        # Arrange
        root_path = "/test"
        dry_run_disabled = False

        # Act
        pipeline = ConcretePipeline(root_path, dry_run=dry_run_disabled)
        result = pipeline.dry_run

        # Assert
        assert result is False, "Expected dry_run property to return False when dry_run is explicitly disabled"
        assert isinstance(result, bool), "Expected dry_run property to return a boolean type"
        assert result == dry_run_disabled, "Expected dry_run property to match the initialization parameter value"

    @pytest.mark.unit
    def test_dry_run_property_default(self) -> None:
        """
        Test dry_run property returns False by default when not explicitly set during initialization.

        This test verifies that the dry_run property correctly returns False when no
        dry_run parameter is provided during pipeline initialization, ensuring the
        property accurately reflects the default state and returns the exact boolean
        False value rather than a truthy/falsy equivalent.
        """
        # Arrange
        root_path = "/test"

        # Act
        pipeline = ConcretePipeline(root_path)
        result = pipeline.dry_run

        # Assert
        assert result is False, "Expected dry_run property to return False by default when not explicitly set"
        assert isinstance(result, bool), "Expected dry_run property to return a boolean type"

    @pytest.mark.unit
    def test_class_name_property(self) -> None:
        """
        Test class_name property returns the exact class name as a string.

        This test verifies that the class_name property correctly returns the __name__
        attribute of the pipeline class, ensuring proper introspection functionality
        for logging and debugging purposes. The property should return the actual
        class name without modification regardless of the initialization parameters.
        """
        # Arrange
        test_root_path = "/test/pipeline"

        # Act
        pipeline = ConcretePipeline(test_root_path)
        result = pipeline.class_name

        # Assert
        assert result == "ConcretePipeline", (
            "Expected class_name property to return exact class name 'ConcretePipeline' "
            "matching the class __name__ attribute"
        )
        assert isinstance(result, str), "Expected class_name property to return a string type"


class TestBasePipelineStaticMethods:
    """Test cases for BasePipeline static methods."""

    @pytest.mark.unit
    def test_get_pipeline_config_schema_default(self) -> None:
        """Test the default pipeline config schema returns an empty dictionary."""
        # Arrange - no setup needed for static method

        # Act
        schema = BasePipeline.get_pipeline_config_schema()

        # Assert
        assert schema == {}, "Expected pipeline config schema to be an empty dictionary by default"
        assert isinstance(schema, dict), "Expected pipeline config schema to be a dictionary instance"

        # Verify it returns a new instance each time (not a shared reference)
        schema2 = BasePipeline.get_pipeline_config_schema()
        assert schema is not schema2, "Expected each call to return a new dictionary instance"

    @pytest.mark.unit
    def test_get_collection_config_schema_default(self) -> None:
        """Test the default collection config schema returns an empty dictionary."""
        # Arrange - no setup needed for static method

        # Act
        schema = BasePipeline.get_collection_config_schema()

        # Assert
        assert schema == {}, "Expected collection config schema to be an empty dictionary by default"
        assert isinstance(schema, dict), "Expected collection config schema to be a dictionary instance"

        # Verify it returns a new instance each time (not a shared reference)
        schema2 = BasePipeline.get_collection_config_schema()
        assert schema is not schema2, "Expected each call to return a new dictionary instance"

    @pytest.mark.unit
    def test_static_methods_can_be_overridden(self) -> None:
        """
        Test that static config schema methods can be overridden in pipeline subclasses.

        This test verifies that when a pipeline subclass overrides the get_pipeline_config_schema
        and get_collection_config_schema static methods, the overridden implementations are correctly
        called and return the expected custom schema configurations, confirming proper polymorphic
        behavior for static methods in the pipeline inheritance hierarchy.
        """

        # Arrange
        class CustomPipeline(ConcretePipeline):
            @staticmethod
            def get_pipeline_config_schema() -> dict[str, Any]:
                return {"custom_key": "default_value"}

            @staticmethod
            def get_collection_config_schema() -> dict[str, Any]:
                return {"collection_key": 123}

        expected_pipeline_schema = {"custom_key": "default_value"}
        expected_collection_schema = {"collection_key": 123}

        # Act
        pipeline_schema = CustomPipeline.get_pipeline_config_schema()
        collection_schema = CustomPipeline.get_collection_config_schema()

        # Assert
        assert (
            pipeline_schema == expected_pipeline_schema
        ), "Expected pipeline config schema to match overridden implementation with custom configuration"
        assert isinstance(pipeline_schema, dict), "Expected pipeline schema to be a dictionary instance"
        assert (
            "custom_key" in pipeline_schema
        ), "Expected pipeline schema to contain the custom key from overridden method"
        assert (
            pipeline_schema["custom_key"] == "default_value"
        ), "Expected custom key to have the correct default value from overridden implementation"

        assert (
            collection_schema == expected_collection_schema
        ), "Expected collection config schema to match overridden implementation with custom configuration"
        assert isinstance(collection_schema, dict), "Expected collection schema to be a dictionary instance"
        assert (
            "collection_key" in collection_schema
        ), "Expected collection schema to contain the custom key from overridden method"
        assert (
            collection_schema["collection_key"] == 123
        ), "Expected custom collection key to have the correct integer value from overridden implementation"


class TestBasePipelineAbstractMethods:
    """Test cases for abstract method enforcement."""

    @pytest.mark.unit
    def test_abstract_pipeline_cannot_be_instantiated(self, tmp_path: Path) -> None:
        """
        Test that BasePipeline cannot be instantiated directly due to abstract methods.

        This test verifies that attempting to instantiate the BasePipeline abstract class
        directly raises a TypeError with a message indicating that abstract methods prevent
        instantiation, confirming the proper enforcement of the abstract base class pattern.
        """
        # Arrange
        test_root_path = tmp_path / "test_pipeline"

        # Act & Assert
        with pytest.raises(TypeError) as exc_info:
            BasePipeline(test_root_path)  # type: ignore[abstract]

        error_message = str(exc_info.value).lower()
        assert "abstract" in error_message, (
            f"Expected TypeError message to contain 'abstract' indicating abstract method enforcement, "
            f"but got: {exc_info.value}"
        )
        assert (
            "basepipeline" in error_message
        ), f"Expected error message to reference 'BasePipeline' class name, but got: {exc_info.value}"

    @pytest.mark.unit
    def test_incomplete_implementation_cannot_be_instantiated(self, tmp_path: Path) -> None:
        """
        Test that incomplete implementations cannot be instantiated due to missing abstract methods.

        This test verifies that attempting to instantiate a pipeline class that doesn't implement
        the required abstract methods (_package) raises a TypeError with a message indicating
        that abstract methods prevent instantiation, confirming proper enforcement of the
        abstract base class pattern.
        """
        # Arrange
        test_root_path = tmp_path / "test_pipeline"

        # Act & Assert
        with pytest.raises(TypeError) as exc_info:
            AbstractOnlyPipeline(test_root_path)  # type: ignore[abstract]

        # Verify the error message indicates the abstract method issue
        error_message = str(exc_info.value).lower()
        assert "abstract" in error_message, (
            f"Expected TypeError message to contain 'abstract' indicating missing abstract method implementation, "
            f"but got: {exc_info.value}"
        )
        assert (
            "abstractonlypipeline" in error_message
        ), f"Expected error message to reference 'AbstractOnlyPipeline' class name, but got: {exc_info.value}"

    @pytest.mark.unit
    def test_concrete_implementation_can_be_instantiated(self, tmp_path: Path) -> None:
        """
        Test that concrete implementations with all abstract methods can be successfully instantiated.

        This test verifies that when a pipeline class provides concrete implementations for all
        required abstract methods (specifically _package), the instantiation succeeds and all
        properties are correctly initialized, confirming proper abstract base class inheritance.
        """
        # Arrange
        test_root_path = tmp_path / "test_pipeline"
        test_config = {"key": "value"}

        # Act
        pipeline = ConcretePipeline(test_root_path, config=test_config, dry_run=True)

        # Assert - Verify successful instantiation and inheritance hierarchy
        assert isinstance(
            pipeline,
            BasePipeline,
        ), "Expected ConcretePipeline to be an instance of BasePipeline abstract base class"
        assert isinstance(
            pipeline,
            ConcretePipeline,
        ), "Expected instantiated object to be an instance of ConcretePipeline concrete class"

        # Assert - Verify all initialization parameters are correctly set
        assert (
            pipeline._root_path == test_root_path
        ), f"Expected root path to be {test_root_path}, but got {pipeline._root_path}"
        assert pipeline.config == test_config, f"Expected config to be {test_config}, but got {pipeline.config}"
        assert (
            pipeline.config is test_config
        ), "Expected config property to return the same object reference passed during initialization"
        assert pipeline.dry_run is True, "Expected dry_run flag to be True when explicitly set during initialization"

        # Assert - Verify derived properties and metadata class
        assert (
            pipeline.class_name == "ConcretePipeline"
        ), f"Expected class_name property to return 'ConcretePipeline', but got '{pipeline.class_name}'"
        assert (
            pipeline._metadata_class == BaseMetadata
        ), f"Expected metadata_class to be BaseMetadata by default, but got {pipeline._metadata_class}"

        # Assert - Verify that abstract method implementations are callable without errors
        assert hasattr(pipeline, "_package"), "Expected concrete implementation to have _package method"
        assert callable(pipeline._package), "Expected _package method to be callable"


class TestRunImportCommand:
    """Test cases for the run_import command."""

    @pytest.mark.integration
    def test_run_import_success(
        self,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """
        Test successful import command execution calls _import method with correct arguments and logs appropriately.

        This integration test verifies that when run_import is called with valid directory paths,
        the method successfully executes the underlying _import implementation, passes all arguments
        correctly, and logs both start and completion messages with properly formatted content
        using real logging integration rather than mocked components.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        data_dir = tmp_path / "data"
        source_dir = tmp_path / "source"

        root_path.mkdir(parents=True)
        data_dir.mkdir()
        source_dir.mkdir()

        pipeline = ConcretePipeline(str(root_path))
        config = {"test": "config"}
        kwargs: dict[str, Any] = {"extra": "args"}

        # Act
        with caplog.at_level("INFO"):
            pipeline.run_import(data_dir, source_dir, config, **kwargs)

        # Assert
        assert pipeline.import_called, "Expected _import method to be called when run_import succeeds"
        assert pipeline.last_import_args == (
            data_dir,
            source_dir,
            config,
            kwargs,
        ), (
            "Expected _import method to be called with correct arguments including "
            "data_dir, source_dir, config, and kwargs"
        )

        # Verify logging occurred - check for key content without mocking internal components
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert len(log_messages) == 2, "Expected exactly 2 info log messages for start and completion"

        start_message = log_messages[0]
        completion_message = log_messages[1]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "import" in start_message, "Expected start message to mention import command"
        assert "ConcretePipeline" in start_message, "Expected start message to include pipeline name"
        assert str(source_dir) in start_message, "Expected start message to include source path"
        assert "test': 'config'" in start_message, "Expected start message to include config contents"
        assert "extra': 'args'" in start_message, "Expected start message to include kwargs contents"

        assert "Completed" in completion_message, "Expected completion message to contain 'Completed'"
        assert "import" in completion_message, "Expected completion message to mention import command"
        assert "ConcretePipeline" in completion_message, "Expected completion message to include pipeline name"

    @pytest.mark.integration
    def test_run_import_invalid_source_path(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test import command with invalid source path logs error and returns early without calling _import.

        This test verifies that when run_import is called with a source path that doesn't exist,
        the method logs an appropriate exception message, returns early without executing the
        import logic, and doesn't call the underlying _import method implementation.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        data_dir = tmp_path / "data"
        nonexistent_source_path = tmp_path / "nonexistent"  # Path that deliberately doesn't exist

        root_path.mkdir(parents=True)
        data_dir.mkdir()
        # Explicitly ensure the source path does NOT exist to test the error condition
        assert not nonexistent_source_path.exists(), "Test setup error: source path should not exist"

        pipeline = ConcretePipeline(str(root_path))
        config: dict[str, Any] = {}

        # Act
        with caplog.at_level("INFO"):
            pipeline.run_import(data_dir, nonexistent_source_path, config)

        # Assert
        assert not pipeline.import_called, "Expected _import method not to be called with invalid source path"

        # Verify logging behavior using real log capture
        log_messages = [record.message for record in caplog.records]
        info_messages = [
            msg for record, msg in zip(caplog.records, log_messages, strict=False) if record.levelname == "INFO"
        ]
        error_messages = [
            msg for record, msg in zip(caplog.records, log_messages, strict=False) if record.levelname == "ERROR"
        ]

        assert len(info_messages) == 1, "Expected exactly one info log message (start message only)"
        assert len(error_messages) == 1, "Expected exactly one error log message for invalid source path"

        start_message = info_messages[0]
        error_message = error_messages[0]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "import" in start_message, "Expected start message to mention import command"
        assert str(nonexistent_source_path) in start_message, "Expected start message to include source path"

        assert (
            f"Source path {nonexistent_source_path} is not a directory" in error_message
        ), "Expected error message to indicate source path is not a directory"

    @pytest.mark.integration
    def test_run_import_source_path_is_file(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test import command with file source path logs exception and skips import without calling _import method.

        This test verifies that when run_import is called with a source path that is a file
        instead of a directory, the method logs an exception, returns early without executing
        the import logic, and doesn't call the underlying _import method implementation.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        data_dir = tmp_path / "data"
        source_file = tmp_path / "source.txt"

        root_path.mkdir(parents=True)
        data_dir.mkdir()
        source_file.touch()

        pipeline = ConcretePipeline(str(root_path))
        config: dict[str, Any] = {}

        # Act
        with caplog.at_level("INFO"):
            pipeline.run_import(data_dir, source_file, config)

        # Assert
        assert not pipeline.import_called, "Expected _import method not to be called when source path is a file"

        # Verify logging behavior using real log capture
        log_messages = [record.message for record in caplog.records]
        info_messages = [
            msg for record, msg in zip(caplog.records, log_messages, strict=False) if record.levelname == "INFO"
        ]
        error_messages = [
            msg for record, msg in zip(caplog.records, log_messages, strict=False) if record.levelname == "ERROR"
        ]

        assert len(info_messages) == 1, "Expected exactly one info log message (start message only)"
        assert len(error_messages) == 1, "Expected exactly one error log message for file source path"

        start_message = info_messages[0]
        error_message = error_messages[0]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "import" in start_message, "Expected start message to mention import command"
        assert str(source_file) in start_message, "Expected start message to include source file path"

        assert (
            f"Source path {source_file} is not a directory" in error_message
        ), "Expected error message to indicate source path is not a directory when source path is a file"


class TestRunProcessCommand:
    """Test cases for the run_process command."""

    @pytest.mark.integration
    def test_run_process_success(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test successful process command execution calls _process method with correct arguments and logs appropriately.

        This integration test verifies that when run_process is called with valid directory paths and configuration,
        the method successfully executes the underlying _process implementation (pipeline.py:176), passes all arguments
        correctly to the concrete implementation, and logs both start (pipeline.py:172-174) and completion messages
        (pipeline.py:179-180) with proper Rich formatting and path formatting integration.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        data_dir = tmp_path / "data"

        root_path.mkdir(parents=True)
        data_dir.mkdir()

        pipeline = ConcretePipeline(str(root_path))
        config = {"process": "config"}
        kwargs: dict[str, Any] = {"additional": "parameters"}

        # Act
        with caplog.at_level("INFO"):
            pipeline.run_process(data_dir, config, **kwargs)

        # Assert
        assert (
            pipeline.process_called
        ), "Expected _process method to be called when run_process succeeds (pipeline.py:176)"
        assert pipeline.last_process_args == (
            data_dir,
            config,
            kwargs,
        ), (
            "Expected _process method to be called with correct arguments including "
            "data_dir, config, and kwargs passed from run_process method"
        )

        # Verify logging occurred - check for key content without brittle string matching
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert len(log_messages) == 2, "Expected exactly 2 info log messages for start and completion"

        start_message = log_messages[0]
        completion_message = log_messages[1]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "process" in start_message, "Expected start message to mention process command"
        assert "ConcretePipeline" in start_message, "Expected start message to include pipeline name"
        assert "data_dir=" in start_message, "Expected start message to include data_dir parameter"
        assert "process': 'config'" in start_message, "Expected start message to include config contents"
        assert "additional': 'parameters'" in start_message, "Expected start message to include kwargs contents"

        assert "Completed" in completion_message, "Expected completion message to contain 'Completed'"
        assert "process" in completion_message, "Expected completion message to mention process command"
        assert "ConcretePipeline" in completion_message, "Expected completion message to include pipeline name"


class TestRunPackageCommand:
    """Test cases for the run_package command."""

    @pytest.mark.integration
    def test_run_package_success(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test successful package command execution calls _package method with correct arguments and logs appropriately.

        This integration test verifies that when run_package is called with valid directory paths and configuration,
        the method successfully executes the underlying _package implementation, passes all arguments correctly,
        returns the expected data mapping result, and logs both start and completion messages using real logging.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        data_dir = tmp_path / "data"
        root_path.mkdir(parents=True)
        data_dir.mkdir()

        pipeline = ConcretePipeline(str(root_path))
        config = {"package": "config"}
        kwargs: dict[str, Any] = {"extra": "options"}

        # Act
        with caplog.at_level("INFO"):
            result = pipeline.run_package(data_dir, config, **kwargs)

        # Assert
        assert pipeline.package_called, "Expected _package method to be called when run_package succeeds"
        assert pipeline.last_package_args == (
            data_dir,
            config,
            kwargs,
        ), "Expected _package method to be called with correct arguments including data_dir, config, and kwargs"

        expected_result: dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]] = {
            Path("source1.txt"): (Path("dest1.txt"), [], {"meta": "data1"}),
            Path("source2.txt"): (Path("dest2.txt"), None, None),
        }
        assert result == expected_result, "Expected correct package result mapping to be returned from _package method"
        assert isinstance(result, dict), "Expected result to be a dictionary instance"
        assert len(result) == 2, "Expected result to contain exactly 2 mapping entries"

        # Verify logging occurred with key content - check for essential information without brittle string matching
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert len(log_messages) == 2, "Expected exactly 2 info log messages for start and completion"

        start_message = log_messages[0]
        completion_message = log_messages[1]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "package" in start_message, "Expected start message to mention package command"
        assert "ConcretePipeline" in start_message, "Expected start message to include pipeline name"
        assert "data_dir=data" in start_message, "Expected start message to include formatted data directory path"
        assert "package': 'config'" in start_message, "Expected start message to include config contents"
        assert "extra': 'options'" in start_message, "Expected start message to include kwargs contents"

        assert "Completed" in completion_message, "Expected completion message to contain 'Completed'"
        assert "package" in completion_message, "Expected completion message to mention package command"
        assert "ConcretePipeline" in completion_message, "Expected completion message to include pipeline name"


class TestRunPostPackageCommand:
    """Test cases for the run_post_package command."""

    @pytest.mark.integration
    def test_run_post_package_success(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test successful post package command execution calls _post_package method with correct arguments.

        This integration test verifies that when run_post_package is called with a valid dataset directory,
        the method successfully executes the underlying _post_package implementation, passes arguments correctly,
        returns the expected set of changed files, and logs both start and completion messages using real logging.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test_pipeline"
        dataset_dir = tmp_path / "dataset"

        root_path.mkdir(parents=True)
        dataset_dir.mkdir()

        pipeline = ConcretePipeline(str(root_path))

        # Act
        with caplog.at_level("INFO"):
            result = pipeline.run_post_package(dataset_dir)

        # Assert
        assert pipeline.post_package_called, "Expected _post_package method to be called when run_post_package succeeds"
        assert pipeline.last_post_package_args == (
            dataset_dir,
        ), "Expected _post_package method to be called with correct dataset_dir argument"

        expected_result = {Path("changed1.txt"), Path("changed2.txt")}
        assert (
            result == expected_result
        ), "Expected correct set of changed files to be returned from _post_package method"
        assert isinstance(result, set), "Expected result to be a set instance"
        assert len(result) == 2, "Expected result to contain exactly 2 changed file paths"

        # Verify logging occurred - check for key content without mocking internal components
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert len(log_messages) == 2, "Expected exactly 2 info log messages for start and completion"

        start_message = log_messages[0]
        completion_message = log_messages[1]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "post package" in start_message, "Expected start message to mention post package command"
        assert "ConcretePipeline" in start_message, "Expected start message to include pipeline name"
        assert (
            "dataset_dir=dataset" in start_message
        ), "Expected start message to include formatted dataset directory path"

        assert "Completed" in completion_message, "Expected completion message to contain 'Completed'"
        assert "post package" in completion_message, "Expected completion message to mention post package command"
        assert "ConcretePipeline" in completion_message, "Expected completion message to include pipeline name"


class TestDefaultImplementations:
    """Test cases for default implementations of optional methods."""

    @pytest.mark.unit
    def test_import_default_warning(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test that the default _import implementation logs a warning message.

        This test verifies that when a pipeline inherits from BasePipeline but doesn't
        override the _import method, calling _import directly logs an appropriate warning
        about the missing implementation without performing any other operations.
        """

        # Arrange
        class DefaultImportPipeline(BasePipeline):
            def _package(
                self,
                data_dir: Path,
                config: dict[str, Any],
                **kwargs: Any,
            ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
                return {}

        pipeline = DefaultImportPipeline("/test")
        mock_get_logger = mocker.patch("marimba.core.utils.log.get_logger")
        mock_logger = mocker.Mock()
        mock_get_logger.return_value = mock_logger

        data_dir = Path("/data")
        source_path = Path("/source")
        config: dict[str, Any] = {}
        kwargs: dict[str, Any] = {"extra": "args"}

        # Act
        pipeline._import(data_dir, source_path, config, **kwargs)

        # Assert
        mock_logger.warning.assert_called_once_with(
            "There is no Marimba [steel_blue3]import[/steel_blue3] command implemented for pipeline "
            "[light_pink3]DefaultImportPipeline[/light_pink3]",
        ), "Expected warning message to be logged when using default _import implementation"

        # Verify no other logger methods were called
        mock_logger.info.assert_not_called(), "Expected no info logs from default _import implementation"
        mock_logger.error.assert_not_called(), "Expected no error logs from default _import implementation"
        mock_logger.exception.assert_not_called(), "Expected no exception logs from default _import implementation"

    @pytest.mark.unit
    def test_process_default_warning(self, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test that the default _process implementation logs a warning message.

        This test verifies that when a pipeline inherits from BasePipeline but doesn't
        override the _process method, calling _process directly logs an appropriate warning
        about the missing implementation without performing any other operations.
        """

        # Arrange
        class DefaultProcessPipeline(BasePipeline):
            def _package(
                self,
                data_dir: Path,
                config: dict[str, Any],
                **kwargs: Any,
            ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
                return {}

        pipeline = DefaultProcessPipeline("/test")
        mock_get_logger = mocker.patch("marimba.core.utils.log.get_logger")
        mock_logger = mocker.Mock()
        mock_get_logger.return_value = mock_logger

        data_dir = Path("/data")
        config: dict[str, Any] = {}
        kwargs: dict[str, Any] = {"additional": "parameters"}

        # Act
        pipeline._process(data_dir, config, **kwargs)

        # Assert
        mock_logger.warning.assert_called_once_with(
            "There is no Marimba [steel_blue3]process[/steel_blue3] command implemented for pipeline "
            "[light_pink3]DefaultProcessPipeline[/light_pink3]",
        ), "Expected warning message to be logged when using default _process implementation"

        # Verify no other logger methods were called
        mock_logger.info.assert_not_called(), "Expected no info logs from default _process implementation"
        mock_logger.error.assert_not_called(), "Expected no error logs from default _process implementation"
        mock_logger.exception.assert_not_called(), "Expected no exception logs from default _process implementation"

    @pytest.mark.unit
    def test_post_package_default_returns_empty_set(self, tmp_path: Path) -> None:
        """
        Test that default _post_package implementation returns empty set when called on pipeline instance.

        This test verifies that when a pipeline calls its own _post_package method and doesn't override
        the default implementation from BasePipeline, it returns an empty set as specified in the base
        class implementation (pipeline.py:289), confirming the expected default behavior.
        """

        # Arrange
        class DefaultPostPackagePipeline(BasePipeline):
            def _package(
                self,
                data_dir: Path,
                config: dict[str, Any],
                **kwargs: Any,
            ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
                return {}

        test_root_path = tmp_path / "test_pipeline"
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        pipeline = DefaultPostPackagePipeline(test_root_path)

        # Act
        result = pipeline._post_package(dataset_dir)

        # Assert
        assert result == set(), "Expected default _post_package implementation to return empty set"
        assert isinstance(result, set), "Expected result to be a set instance"
        assert len(result) == 0, "Expected empty set to contain no elements"


class TestErrorHandlingAndEdgeCases:
    """Test cases for error handling and edge cases."""

    @pytest.mark.unit
    def test_pipeline_with_empty_config(self, tmp_path: Path) -> None:
        """
        Test pipeline initialization with empty configuration preserves empty dict and sets other properties correctly.

        This test verifies that when a BasePipeline subclass is initialized with an empty
        configuration dictionary, all instance properties are correctly assigned including
        the empty config dict, default values for other properties, and proper object references.
        """
        # Arrange
        root_path = tmp_path / "test_pipeline"
        empty_config: dict[str, Any] = {}

        # Act
        pipeline = ConcretePipeline(root_path, config=empty_config)

        # Assert
        assert pipeline.config == {}, "Expected config property to return empty dictionary"
        assert pipeline.config is not None, "Expected config property to not be None"
        assert pipeline.config is empty_config, "Expected config property to return the same object reference"
        assert pipeline._root_path == root_path, "Expected root path to be set correctly"
        assert pipeline.dry_run is False, "Expected dry_run to be False by default"
        assert pipeline._metadata_class == BaseMetadata, "Expected metadata_class to be BaseMetadata by default"
        assert isinstance(pipeline.config, dict), "Expected config to be a dictionary instance"

    @pytest.mark.unit
    def test_pipeline_with_complex_config(self) -> None:
        """
        Test pipeline initialization with complex configuration preserves all data types and references.

        This test verifies that when a BasePipeline subclass is initialized with a complex
        configuration containing multiple data types (strings, integers, floats, booleans,
        lists, and nested dictionaries), the configuration is correctly stored and accessible
        through the config property with all data types preserved and the same object reference.
        """
        # Arrange
        config = {
            "string_val": "test",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "dict_val": {"nested": "value"},
        }
        root_path = "/test"

        # Act
        pipeline = ConcretePipeline(root_path, config=config)

        # Assert
        assert pipeline.config == config, "Expected config property to return the exact configuration data"
        assert pipeline.config is config, "Expected config property to return the same object reference"
        assert pipeline._root_path == root_path, "Expected root path to be set correctly"
        assert pipeline.dry_run is False, "Expected dry_run to be False by default"
        assert pipeline._metadata_class == BaseMetadata, "Expected metadata_class to be BaseMetadata by default"

        # Verify individual data types are preserved
        assert isinstance(pipeline.config["string_val"], str), "Expected string value to remain string type"
        assert isinstance(pipeline.config["int_val"], int), "Expected integer value to remain int type"
        assert isinstance(pipeline.config["float_val"], float), "Expected float value to remain float type"
        assert isinstance(pipeline.config["bool_val"], bool), "Expected boolean value to remain bool type"
        assert isinstance(pipeline.config["list_val"], list), "Expected list value to remain list type"
        assert isinstance(pipeline.config["dict_val"], dict), "Expected dict value to remain dict type"

    @pytest.mark.integration
    def test_logging_with_empty_kwargs(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test that run_process method handles empty configuration and kwargs correctly.

        This integration test verifies that when run_process is called with empty configuration
        and kwargs, the method successfully executes the underlying _process implementation,
        passes all arguments correctly, and logs both start and completion messages using
        real logging integration without exceptions or formatting errors.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test"
        root_path.mkdir(parents=True)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        pipeline = ConcretePipeline(str(root_path))
        empty_config: dict[str, Any] = {}
        empty_kwargs: dict[str, Any] = {}

        # Act
        with caplog.at_level("INFO"):
            pipeline.run_process(data_dir, empty_config, **empty_kwargs)

        # Assert
        # Verify the _process method was called with correct arguments
        assert pipeline.process_called, "Expected _process method to be called when run_process succeeds"
        assert pipeline.last_process_args == (
            data_dir,
            empty_config,
            empty_kwargs,
        ), "Expected _process method to be called with correct empty config and kwargs"

        # Verify logging occurred with real log capture
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert len(log_messages) == 2, "Expected exactly 2 info log messages for start and completion"

        start_message = log_messages[0]
        completion_message = log_messages[1]

        assert "Started" in start_message, "Expected start message to contain 'Started'"
        assert "process" in start_message, "Expected start message to mention process command"
        assert "ConcretePipeline" in start_message, "Expected start message to include pipeline name"
        assert "config={}" in start_message, "Expected start message to show empty config dictionary"
        assert "kwargs={}" in start_message, "Expected start message to show empty kwargs dictionary"

        assert "Completed" in completion_message, "Expected completion message to contain 'Completed'"
        assert "process" in completion_message, "Expected completion message to mention process command"
        assert "ConcretePipeline" in completion_message, "Expected completion message to include pipeline name"

    @pytest.mark.integration
    def test_path_formatting_in_logs(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> None:
        """
        Test that path formatting integration works correctly in process command logging.

        This integration test verifies that when run_process is called, the logging mechanism
        correctly integrates with the format_path_for_logging utility by passing the data
        directory path and the computed parent directory path (pipeline.py:173). This ensures
        proper path formatting in log messages for relative path display.
        """
        # Arrange
        root_path = tmp_path / "project" / "pipelines" / "test"
        root_path.mkdir(parents=True)
        data_path = tmp_path / "data"
        data_path.mkdir()

        pipeline = ConcretePipeline(str(root_path))

        mock_format = mocker.patch("marimba.core.pipeline.format_path_for_logging")
        mock_format.return_value = "mocked/path"
        mock_get_logger = mocker.patch("marimba.core.utils.log.get_logger")
        mock_logger = mocker.Mock()
        mock_get_logger.return_value = mock_logger

        # Act
        pipeline.run_process(data_path, {})

        # Assert
        expected_parent = Path(root_path).parents[2]  # Should match the source code implementation
        mock_format.assert_called_with(
            data_path,
            expected_parent,
        ), (
            f"Expected format_path_for_logging to be called with data_path={data_path} "
            f"and parent path={expected_parent} to enable proper relative path formatting in logs"
        )


if __name__ == "__main__":
    pytest.main([__file__])
