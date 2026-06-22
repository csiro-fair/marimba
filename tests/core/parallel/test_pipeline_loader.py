"""
Test cases for the pipeline loader module.

This module contains comprehensive tests for the pipeline loading functionality,
including module discovery, class instantiation, error handling, and logging configuration.
"""

import sys
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from marimba.core.parallel.pipeline_loader import (
    _configure_pipeline_logging,
    _find_pipeline_class,
    _find_pipeline_module_path,
    _is_valid_pipeline_class,
    _load_pipeline_module,
    _log_empty_repo_warning,
    load_pipeline_instance,
)
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata


class MockTestPipeline(BasePipeline):
    """Mock test pipeline class for testing purposes (renamed to avoid pytest collection)."""

    def __init__(self, repo_dir: Path | str, config: dict[str, Any] | None = None, *, dry_run: bool = False) -> None:
        super().__init__(repo_dir, config, dry_run=dry_run)

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {}

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: Any,
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {}


class TestFindPipelineModulePath:
    """Test cases for _find_pipeline_module_path function."""

    @pytest.mark.integration
    def test_find_single_pipeline_file(self, tmp_path: Path) -> None:
        """Test finding a single .pipeline.py file in repository root.

        Verifies that _find_pipeline_module_path correctly locates and returns
        a single pipeline file when it exists in the repository directory.
        This is an integration test as it tests the complete file discovery workflow
        including filesystem operations and glob pattern matching.
        """
        # Arrange
        repo_path = tmp_path
        pipeline_file = repo_path / "test.pipeline.py"
        pipeline_file.touch()

        # Act
        result = _find_pipeline_module_path(repo_path)

        # Assert
        assert result == pipeline_file, f"Expected to find pipeline at {pipeline_file}, but found {result}"
        assert result.exists(), f"Returned pipeline file {result} should exist on filesystem"

    @pytest.mark.unit
    def test_find_nested_pipeline_file(self, tmp_path: Path) -> None:
        """Test finding a .pipeline.py file in a nested subdirectory structure.

        Verifies that the glob pattern **/*.pipeline.py correctly discovers
        pipeline files in deeply nested directory structures. This is a unit test
        as it tests the isolated file discovery functionality.
        """
        # Arrange
        repo_path = tmp_path
        nested_dir = repo_path / "src" / "pipelines"
        nested_dir.mkdir(parents=True)
        pipeline_file = nested_dir / "my_pipeline.pipeline.py"
        pipeline_file.touch()

        # Act
        result = _find_pipeline_module_path(repo_path)

        # Assert
        assert result == pipeline_file, f"Expected to find pipeline at {pipeline_file}, but found {result}"
        assert result.exists(), f"Returned pipeline file {result} should exist on filesystem"

    @pytest.mark.unit
    def test_no_pipeline_file_raises_error(self, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised when no .pipeline.py file exists.

        Verifies that _find_pipeline_module_path raises appropriate error when no pipeline
        files are found in the repository directory. This is a unit test as it tests
        the isolated error handling behavior.
        """
        # Arrange
        repo_path = tmp_path
        # Create some non-pipeline files
        (repo_path / "README.md").touch()
        (repo_path / "config.py").touch()

        # Act & Assert
        with pytest.raises(FileNotFoundError) as context:
            _find_pipeline_module_path(repo_path)

        error_message = str(context.value)
        assert (
            "No pipeline implementation found" in error_message
        ), f"Error should mention no implementation found, got: {error_message}"
        assert ".pipeline.py" in error_message, f"Error should mention .pipeline.py requirement, got: {error_message}"
        assert str(repo_path) in error_message, f"Error should include the repository path, got: {error_message}"
        assert (
            "BasePipeline" in error_message
        ), f"Error should mention BasePipeline inheritance requirement, got: {error_message}"

    @pytest.mark.unit
    def test_no_pipeline_file_with_allow_empty(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Test that None is returned when no .pipeline.py file exists and allow_empty=True.

        Verifies that _find_pipeline_module_path returns None and calls the warning logger
        when no pipeline files are found but allow_empty=True. This is a unit test as it
        tests the isolated conditional logic and side effect behavior.
        """
        # Arrange
        repo_path = tmp_path
        mock_log = mocker.patch("marimba.core.parallel.pipeline_loader._log_empty_repo_warning")

        # Act
        result = _find_pipeline_module_path(repo_path, allow_empty=True)

        # Assert
        assert result is None, "Should return None when no pipeline file found and allow_empty=True"
        mock_log.assert_called_once_with(repo_path), "Should call warning logger exactly once with repo path"

    @pytest.mark.unit
    def test_multiple_pipeline_files_raises_error(self, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised when multiple .pipeline.py files exist.

        Verifies that _find_pipeline_module_path raises appropriate error when multiple
        pipeline files are found, ensuring only one implementation per repository.
        This is a unit test as it tests the isolated validation logic.
        """
        # Arrange
        repo_path = tmp_path
        pipeline_file1 = repo_path / "first.pipeline.py"
        pipeline_file2 = repo_path / "second.pipeline.py"
        pipeline_file1.touch()
        pipeline_file2.touch()

        # Act & Assert
        with pytest.raises(FileNotFoundError) as context:
            _find_pipeline_module_path(repo_path)

        error_message = str(context.value)
        assert (
            "Multiple pipeline implementations found" in error_message
        ), f"Error should mention multiple implementations, got: {error_message}"
        assert str(repo_path) in error_message, f"Error should include repository path, got: {error_message}"
        assert "first.pipeline.py" in error_message, f"Error should list first pipeline file, got: {error_message}"
        assert "second.pipeline.py" in error_message, f"Error should list second pipeline file, got: {error_message}"


class TestLogEmptyRepoWarning:
    """Test cases for _log_empty_repo_warning function."""

    @pytest.mark.unit
    def test_logs_warning_message(self, mocker: MockerFixture) -> None:
        """Test that warning message is logged with correct content and logger setup.

        Verifies that _log_empty_repo_warning correctly obtains the pipeline logger,
        calls the warning method exactly once, and includes all required guidance
        content in the warning message for users with empty pipeline repositories.
        This is a unit test as it tests isolated logging behavior with mocked dependencies.
        """
        # Arrange
        mock_logger = mocker.Mock()
        mock_get_logger = mocker.patch("marimba.core.parallel.pipeline_loader.get_logger")
        mock_get_logger.return_value = mock_logger
        repo_path = Path("/test/repo")

        # Act
        _log_empty_repo_warning(repo_path)

        # Assert
        # Verify logger setup and method calls
        mock_get_logger.assert_called_once_with("marimba.core.pipeline"), "Should get logger with correct name"
        mock_logger.warning.assert_called_once(), "Should call warning method exactly once"

        # Verify warning message contains all expected guidance content
        # The warning method should be called with exactly one argument (the formatted message)
        assert (
            len(mock_logger.warning.call_args[0]) == 1
        ), "Warning should be called with exactly one positional argument"
        warning_message = mock_logger.warning.call_args[0][0]

        # Verify specific content requirements
        assert (
            "no Marimba Pipeline implementation was found" in warning_message
        ), "Should mention missing implementation"
        assert ".pipeline.py" in warning_message, "Should mention required file extension"
        assert "Pipeline template" in warning_message, "Should mention template availability"
        assert (
            "https://raw.githubusercontent.com/csiro-fair/marimba/main/docs/templates/template.pipeline.py"
            in warning_message
        ), "Should include exact template URL"
        assert (
            f'Pipeline repository cloned to "{repo_path}"' in warning_message
        ), f"Should include repository path in expected format: {repo_path}"

        # Verify implementation guidance content
        assert "get_pipeline_config_schema()" in warning_message, "Should mention required schema method"
        assert "get_collection_config_schema()" in warning_message, "Should mention required collection schema method"
        assert "_import()" in warning_message, "Should mention required import method"
        assert "_process()" in warning_message, "Should mention required process method"
        assert "_package()" in warning_message, "Should mention required package method"
        assert (
            "https://github.com/csiro-fair/marimba/blob/main/docs/pipeline.md" in warning_message
        ), "Should include pipeline implementation guide URL"


class TestLoadPipelineModule:
    """Test cases for _load_pipeline_module function."""

    @pytest.mark.integration
    def test_load_valid_module(self, tmp_path: Path) -> None:
        """Test loading a valid Python module from filesystem with complete workflow verification.

        Verifies that _load_pipeline_module correctly loads a Python module file,
        registers it in sys.modules, and returns the expected module components.
        Also tests module execution and content accessibility to validate the complete
        module loading workflow. This is an integration test as it tests the complete
        filesystem-to-module loading process including all module components.
        """
        # Arrange
        module_path = tmp_path / "test.pipeline.py"
        module_content = """# Test module content
test_var = 'hello'
test_function = lambda: 'function_result'
TEST_CONSTANT = 42
"""
        module_path.write_text(module_content)

        # Act
        module_name, module, module_spec = _load_pipeline_module(module_path)

        # Execute the module to test the complete workflow
        if module_spec.loader is None:
            msg = "Module loader is None - cannot complete test"
            raise ImportError(msg)
        module_spec.loader.exec_module(module)

        # Assert
        assert module_name == "test.pipeline", f"Expected module name 'test.pipeline', got {module_name}"
        assert module is not None, "Module should not be None after successful loading"
        assert module_spec is not None, "Module spec should not be None after successful loading"
        assert "test.pipeline" in sys.modules, "Module should be registered in sys.modules after loading"

        # Verify module execution and content accessibility
        assert hasattr(module, "test_var"), "Module should have test_var attribute after execution"
        assert module.test_var == "hello", f"Expected test_var to be 'hello', got {module.test_var}"
        assert hasattr(module, "test_function"), "Module should have test_function attribute after execution"
        assert callable(module.test_function), "test_function should be callable after module execution"
        assert module.test_function() == "function_result", "test_function should return expected result"
        assert hasattr(module, "TEST_CONSTANT"), "Module should have TEST_CONSTANT attribute after execution"
        assert module.TEST_CONSTANT == 42, f"Expected TEST_CONSTANT to be 42, got {module.TEST_CONSTANT}"

    @pytest.mark.unit
    def test_load_module_spec_none(self, mocker: MockerFixture) -> None:
        """Test ImportError when module spec is None.

        Verifies that _load_pipeline_module raises ImportError with descriptive message
        when spec_from_file_location returns None. This is a unit test as it tests
        isolated error handling behavior with mocked external dependency.
        """
        # Arrange
        mock_spec_from_file = mocker.patch("marimba.core.parallel.pipeline_loader.spec_from_file_location")
        mock_spec_from_file.return_value = None
        module_path = Path("/fake/path/test.pipeline.py")

        # Act & Assert
        with pytest.raises(ImportError) as context:
            _load_pipeline_module(module_path)

        error_message = str(context.value)
        assert (
            "Could not load spec" in error_message
        ), f"Error should mention spec loading failure, got: {error_message}"
        assert "test.pipeline" in error_message, f"Error should include module name, got: {error_message}"
        assert str(module_path) in error_message, f"Error should include module path, got: {error_message}"

    @pytest.mark.unit
    def test_load_module_loader_none(self, mocker: MockerFixture) -> None:
        """Test ImportError when module spec loader is None.

        Verifies that _load_pipeline_module raises an ImportError with a descriptive
        message when the module spec has no loader available, which can occur when
        the file path is invalid or the module format is not recognized.
        """
        # Arrange
        mock_spec_from_file = mocker.patch("marimba.core.parallel.pipeline_loader.spec_from_file_location")
        mock_spec = mocker.Mock()
        mock_spec.loader = None
        mock_spec_from_file.return_value = mock_spec
        module_path = Path("/fake/path/test.pipeline.py")

        # Act & Assert
        with pytest.raises(ImportError) as exc_info:
            _load_pipeline_module(module_path)

        error_message = str(exc_info.value)
        assert (
            "Could not find loader" in error_message
        ), f"Expected 'Could not find loader' in error message but got: {error_message}"
        assert "test.pipeline" in error_message, "Error message should include the module name"
        assert str(module_path) in error_message, "Error message should include the module path"


class TestIsValidPipelineClass:
    """Test cases for _is_valid_pipeline_class function."""

    @pytest.mark.unit
    def test_valid_pipeline_class(self) -> None:
        """Test that a valid pipeline class returns True.

        Verifies that _is_valid_pipeline_class correctly identifies a concrete
        subclass of BasePipeline as valid for pipeline loading.
        """
        # Arrange
        pipeline_class = MockTestPipeline

        # Verify preconditions - ensure our test class is properly set up
        assert issubclass(
            pipeline_class,
            BasePipeline,
        ), "Test setup error: MockTestPipeline must inherit from BasePipeline"
        assert pipeline_class is not BasePipeline, "Test setup error: MockTestPipeline must not be the base class"

        # Act
        result = _is_valid_pipeline_class(pipeline_class)

        # Assert
        assert result is True, (
            f"Expected _is_valid_pipeline_class to return True for valid pipeline class {pipeline_class.__name__}, "
            f"but got {result}"
        )
        assert isinstance(
            result,
            bool,
        ), f"Expected _is_valid_pipeline_class to return boolean type, but got {type(result).__name__}"

    @pytest.mark.unit
    def test_base_pipeline_class_invalid(self) -> None:
        """Test that BasePipeline itself returns False.

        Verifies that _is_valid_pipeline_class correctly rejects the base
        BasePipeline class itself, as it's abstract and not a concrete implementation.
        """
        # Act
        result = _is_valid_pipeline_class(BasePipeline)

        # Assert
        assert result is False, "BasePipeline base class should not be valid for instantiation"
        assert isinstance(result, bool), f"Result should be boolean, got {type(result)}"

    @pytest.mark.unit
    def test_builtin_str_type_invalid(self) -> None:
        """Test that built-in str type returns False.

        Verifies that _is_valid_pipeline_class correctly rejects the built-in str type
        as it is not a BasePipeline subclass, ensuring only valid pipeline classes are accepted.
        """
        # Arrange
        test_class = str

        # Act
        result = _is_valid_pipeline_class(test_class)  # type: ignore[arg-type]

        # Assert
        assert result is False, "Built-in str type should not be identified as a valid pipeline class"
        assert isinstance(result, bool), f"Result should be boolean, got {type(result).__name__}"

    @pytest.mark.unit
    def test_builtin_int_type_invalid(self) -> None:
        """Test that built-in int type returns False.

        Verifies that _is_valid_pipeline_class correctly rejects the built-in int type
        as it is not a BasePipeline subclass, ensuring only valid pipeline classes are accepted.
        """
        # Arrange
        test_class = int

        # Act
        result = _is_valid_pipeline_class(test_class)  # type: ignore[arg-type]

        # Assert
        assert result is False, "Built-in int type should not be identified as a valid pipeline class"
        assert isinstance(result, bool), f"Result should be boolean, got {type(result).__name__}"

    @pytest.mark.unit
    def test_custom_non_pipeline_class_invalid(self) -> None:
        """Test that custom classes not inheriting from BasePipeline return False.

        Verifies that _is_valid_pipeline_class correctly rejects user-defined classes
        that don't inherit from BasePipeline, ensuring only valid pipeline implementations are accepted.
        This tests the core validation logic for pipeline class inheritance.
        """

        # Arrange
        class NotAPipeline:
            pass

        # Act
        result = _is_valid_pipeline_class(NotAPipeline)

        # Assert
        assert result is False, "Custom non-pipeline class should not be identified as a valid pipeline class"
        assert isinstance(result, bool), f"Result should be boolean, got {type(result)}"

    @pytest.mark.unit
    def test_non_class_object_invalid(self) -> None:
        """Test that non-class objects return False.

        Verifies that _is_valid_pipeline_class correctly rejects non-class objects
        (instances/values rather than types) by returning False when the isinstance(obj, type)
        check fails. This tests the first validation step in the function.
        """
        # Arrange
        test_objects = [
            42,  # int instance
            "hello",  # str instance
            [],  # list instance
            {},  # dict instance
            MockTestPipeline(".", {}),  # pipeline instance (not class)
        ]

        for test_obj in test_objects:
            # Act
            result = _is_valid_pipeline_class(test_obj)  # type: ignore[arg-type]

            # Assert
            assert result is False, f"Non-class object {test_obj} ({type(test_obj)}) should return False"
            assert isinstance(result, bool), f"Result should be boolean for {test_obj}, got {type(result)}"

    @pytest.mark.unit
    def test_type_error_handling(self) -> None:
        """Test that TypeError is handled gracefully.

        Verifies that _is_valid_pipeline_class properly handles edge cases where
        the isinstance check raises TypeError, returning False rather than propagating the exception.
        """

        # Arrange - Create an object that raises TypeError in isinstance check
        class ProblematicClass:
            @property  # type: ignore[misc]
            def __class__(self) -> Any:
                msg = "Can't determine class"
                raise TypeError(msg)

        # Act
        result = _is_valid_pipeline_class(ProblematicClass)  # type: ignore[arg-type]

        # Assert
        assert result is False, "Function should handle TypeError gracefully and return False"
        assert isinstance(result, bool), f"Result should be boolean, got {type(result)}"


class TestFindPipelineClass:
    """Test cases for _find_pipeline_class function."""

    @pytest.mark.unit
    def test_find_valid_pipeline_class(self, mocker: MockerFixture) -> None:
        """Test finding a valid pipeline class in a module with mixed content.

        Verifies that _find_pipeline_class correctly identifies and returns
        the valid pipeline class when the module contains various objects
        including functions, variables, and the target pipeline class.
        This test focuses on the class identification logic rather than module loading.
        """
        # Arrange
        mock_module = mocker.Mock()
        mock_module.__dict__ = {
            "MockTestPipeline": MockTestPipeline,
            "some_function": lambda: None,
            "some_variable": 42,
            "AnotherClass": str,  # Not a pipeline class
        }

        # Act
        result = _find_pipeline_class(mock_module)

        # Assert
        assert result is MockTestPipeline, (
            f"Expected _find_pipeline_class to return MockTestPipeline, but got {result}. "
            "The function should identify the valid BasePipeline subclass among mixed module content."
        )
        assert issubclass(
            result,
            BasePipeline,
        ), f"Returned class {result.__name__} must be a subclass of BasePipeline for valid pipeline identification"
        assert result is not BasePipeline, (
            "Result should not be the abstract base BasePipeline class itself, "
            f"but got {result.__name__} which should be a concrete implementation"
        )

    @pytest.mark.unit
    def test_find_pipeline_class_no_valid_class_raises_import_error(self, mocker: MockerFixture) -> None:
        """Test that ImportError is raised when no valid pipeline class is found.

        Verifies that _find_pipeline_class raises an ImportError with a descriptive
        message when the module contains various objects but no valid pipeline classes.
        This test ensures proper error handling when a pipeline module exists but lacks
        a concrete BasePipeline implementation.
        """
        # Arrange
        mock_module = mocker.Mock()
        mock_module.__dict__ = {
            "some_function": lambda: None,
            "some_variable": 42,
            "NotAPipeline": str,  # Not a pipeline class
            "AnotherNotAPipeline": int,  # Also not a pipeline class
        }

        # Act & Assert
        with pytest.raises(ImportError) as exc_info:
            _find_pipeline_class(mock_module)

        # Assert specific error message and content
        assert isinstance(exc_info.value, ImportError), f"Expected ImportError but got {type(exc_info.value).__name__}"
        error_message = str(exc_info.value)
        expected_message = "Pipeline class has not been set or could not be found"
        assert (
            error_message == expected_message
        ), f"Expected exact error message '{expected_message}' but got: '{error_message}'"

    @pytest.mark.unit
    def test_module_without_dict_raises_error(self) -> None:
        """Test that ImportError is raised when module lacks __dict__ attribute.

        Verifies that _find_pipeline_class handles modules without __dict__ gracefully
        by raising an ImportError with a clear diagnostic message for invalid modules.
        """

        # Arrange - Create an object that truly doesn't have __dict__
        class NoDict:
            __slots__: list[str] = []  # This prevents __dict__ from being created

        mock_module = NoDict()

        # Act & Assert
        with pytest.raises(ImportError) as exc_info:
            _find_pipeline_class(mock_module)  # type: ignore[arg-type]

        # Assert exact error message matches source code implementation
        error_message = str(exc_info.value)
        expected_message = "Invalid module: module has no __dict__ attribute"
        assert (
            error_message == expected_message
        ), f"Expected exact error message '{expected_message}' but got: '{error_message}'"

    @pytest.mark.unit
    def test_find_pipeline_class_multiple_classes_returns_first(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test that the first valid pipeline class is returned when multiple pipeline classes exist.

        Verifies that _find_pipeline_class returns the first valid pipeline class found
        during module.__dict__.values() iteration when multiple valid pipeline classes are present.
        This tests the implementation behavior of returning immediately upon finding the first
        valid class rather than validating uniqueness or raising errors for multiple classes.
        The test uses OrderedDict to ensure deterministic iteration order for reliable testing.
        """

        # Arrange - Create a second test pipeline class for testing multiple class scenario
        class SecondTestPipeline(BasePipeline):
            """Second test pipeline class to verify first-match behavior."""

            @staticmethod
            def get_pipeline_config_schema() -> dict[str, Any]:
                return {}

            @staticmethod
            def get_collection_config_schema() -> dict[str, Any]:
                return {}

            def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
                pass

            def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
                pass

            def _package(
                self,
                data_dir: Path,
                config: dict[str, Any],
                **kwargs: Any,
            ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
                return {}

        # Create mock module with predictable iteration order for deterministic testing
        mock_module = mocker.Mock()
        # Order matters: MockTestPipeline should be found first and returned immediately
        mock_module.__dict__ = OrderedDict(
            [
                ("some_variable", "not_a_class"),  # Non-class object - should be skipped
                ("NotAPipelineClass", str),  # Class but not a pipeline - should be skipped
                ("FirstValidPipeline", MockTestPipeline),  # First valid pipeline class - should be returned
                ("SecondValidPipeline", SecondTestPipeline),  # Second valid pipeline class - should be ignored
                ("third_variable", 42),  # Another non-class object - should be skipped
            ],
        )

        # Act
        result = _find_pipeline_class(mock_module)

        # Assert - Should return the first valid pipeline class encountered during iteration
        assert result is MockTestPipeline, (
            f"Expected first valid pipeline class MockTestPipeline, but got "
            f"{result.__name__ if hasattr(result, '__name__') else result}. "
            "The function should return the first valid pipeline class found during module.__dict__ iteration."
        )
        assert isinstance(result, type), f"Result should be a class type, got {type(result)}"
        assert issubclass(result, BasePipeline), f"Result {result.__name__} must be a subclass of BasePipeline"
        assert (
            result is not BasePipeline
        ), f"Result should not be the abstract base BasePipeline class itself, got {result.__name__}"

        # Verify the returned class is specifically the first one, not the second
        assert result is not SecondTestPipeline, (  # type: ignore[comparison-overlap]
            f"Should return MockTestPipeline (first), not SecondTestPipeline (second), "
            f"demonstrating first-match behavior during iteration. Got {result.__name__}"
        )


class TestConfigurePipelineLogging:
    """Test cases for _configure_pipeline_logging function."""

    @pytest.mark.unit
    def test_configure_logging_with_prefix(self, mocker: MockerFixture) -> None:
        """Test configuring pipeline logging with log prefix.

        Verifies that _configure_pipeline_logging correctly clears existing handlers,
        creates and applies the specified log prefix filter, and adds the new file handler.
        This tests the complete logging configuration workflow with prefix filtering.
        """
        # Arrange
        mock_get_file_handler = mocker.patch("marimba.core.parallel.pipeline_loader.get_file_handler")
        mock_prefix_filter = mocker.patch("marimba.core.parallel.pipeline_loader.LogPrefixFilter")

        # Set up existing handler to verify clearing behavior
        existing_handler = mocker.Mock()
        existing_handler.baseFilename = "/existing/log.log"

        mock_pipeline = mocker.Mock()
        mock_pipeline.logger = mocker.Mock()
        mock_pipeline.logger.handlers = [existing_handler]  # Start with existing handler

        mock_handler = mocker.Mock()
        mock_handler.baseFilename = "/test/log/file.log"
        mock_get_file_handler.return_value = mock_handler

        mock_filter_instance = mocker.Mock()
        mock_prefix_filter.return_value = mock_filter_instance

        root_dir = Path("/test/root")
        pipeline_name = "test_pipeline"
        log_prefix = "TEST_PREFIX"

        # Act
        _configure_pipeline_logging(mock_pipeline, root_dir, pipeline_name, False, log_prefix)

        # Assert
        # Verify handlers were cleared
        assert mock_pipeline.logger.handlers == [], "All existing handlers should be cleared before configuration"

        # Verify prefix filter was created and added
        mock_prefix_filter.assert_called_once_with(
            log_prefix,
        ), f"LogPrefixFilter should be created with the specified prefix '{log_prefix}'"
        mock_pipeline.logger.addFilter.assert_called_once_with(
            mock_filter_instance.apply_prefix,
        ), "Prefix filter should be added to logger"

        # Verify file handler was created with correct parameters and added
        mock_get_file_handler.assert_called_once_with(
            root_dir,
            pipeline_name,
            False,
        ), (
            f"File handler should be created with correct parameters: "
            f"root_dir={root_dir}, pipeline_name={pipeline_name}, dry_run=False"
        )
        mock_pipeline.logger.addHandler.assert_called_once_with(
            mock_handler,
        ), "File handler should be added to logger"

    @pytest.mark.unit
    def test_configure_logging_without_prefix(self, mocker: MockerFixture) -> None:
        """Test configuring pipeline logging without log prefix.

        Verifies that _configure_pipeline_logging correctly clears existing handlers,
        does not apply any prefix filter when log_string_prefix is None, creates a
        file handler with correct parameters, and adds it to the pipeline logger.
        Tests the complete logging configuration workflow without prefix filtering.
        """
        # Arrange
        mock_get_file_handler = mocker.patch("marimba.core.parallel.pipeline_loader.get_file_handler")

        # Set up existing handler to verify clearing behavior
        existing_handler = mocker.Mock()
        existing_handler.baseFilename = "/existing/log.log"

        mock_pipeline = mocker.Mock()
        mock_pipeline.logger = mocker.Mock()
        mock_pipeline.logger.handlers = [existing_handler]  # Start with existing handler

        mock_handler = mocker.Mock()
        mock_handler.baseFilename = "/test/log/file.log"
        mock_get_file_handler.return_value = mock_handler

        root_dir = Path("/test/root")
        pipeline_name = "test_pipeline"

        # Act
        _configure_pipeline_logging(mock_pipeline, root_dir, pipeline_name, True, None)

        # Assert
        # Verify handlers were cleared before configuration
        assert mock_pipeline.logger.handlers == [], "All existing handlers should be cleared before configuration"

        # Verify no filter was added when log_string_prefix is None
        mock_pipeline.logger.addFilter.assert_not_called(), "No filter should be added when log_string_prefix is None"

        # Verify file handler was created with correct parameters
        mock_get_file_handler.assert_called_once_with(
            root_dir,
            pipeline_name,
            True,
        ), "File handler should be created with correct parameters"

        # Verify file handler was added to the pipeline logger
        mock_pipeline.logger.addHandler.assert_called_once_with(mock_handler), "File handler should be added to logger"

    @pytest.mark.unit
    def test_clears_existing_handlers_before_adding_new_handler(self, mocker: MockerFixture) -> None:
        """Test that existing handlers are cleared before adding new file handler.

        Verifies that _configure_pipeline_logging clears all existing handlers from the
        pipeline logger before adding a new file handler. This ensures clean logging
        configuration without accumulating handlers across multiple calls.
        """
        # Arrange
        mock_get_file_handler = mocker.patch("marimba.core.parallel.pipeline_loader.get_file_handler")

        mock_pipeline = mocker.Mock()
        existing_handler1 = mocker.Mock()
        existing_handler1.baseFilename = "/existing/log1.log"
        existing_handler2 = mocker.Mock()
        existing_handler2.baseFilename = "/existing/log2.log"

        # Create a copy to verify initial state
        initial_handlers = [existing_handler1, existing_handler2]
        mock_pipeline.logger.handlers = initial_handlers.copy()

        new_handler = mocker.Mock()
        new_handler.baseFilename = "/test/log/file.log"
        mock_get_file_handler.return_value = new_handler

        root_dir = Path("/test/root")
        pipeline_name = "test_pipeline"

        # Verify preconditions - ensure test setup is correct
        assert len(mock_pipeline.logger.handlers) == 2, "Test setup error: should start with 2 existing handlers"
        assert (
            mock_pipeline.logger.handlers == initial_handlers
        ), "Test setup error: handlers should match initial setup"

        # Act
        _configure_pipeline_logging(mock_pipeline, root_dir, pipeline_name, False, None)

        # Assert
        # Verify handlers were cleared completely (implementation sets handlers = [])
        assert mock_pipeline.logger.handlers == [], "All existing handlers should be cleared before adding new handler"
        assert len(mock_pipeline.logger.handlers) == 0, "Handler list should be completely empty after clearing"

        # Verify file handler was created with correct parameters
        mock_get_file_handler.assert_called_once_with(
            root_dir,
            pipeline_name,
            False,
        ), (
            f"File handler should be created with parameters: "
            f"root_dir={root_dir}, pipeline_name={pipeline_name}, dry_run=False"
        )

        # Verify the new handler was added to the cleared logger
        mock_pipeline.logger.addHandler.assert_called_once_with(
            new_handler,
        ), "New file handler should be added exactly once after clearing existing handlers"

        # Verify no filter was added when log_string_prefix is None
        mock_pipeline.logger.addFilter.assert_not_called(), "No filter should be added when log_string_prefix is None"

    @pytest.mark.unit
    def test_prevent_duplicate_handlers(self, mocker: MockerFixture) -> None:
        """Test that duplicate handlers are prevented by clearing existing handlers.

        Verifies that _configure_pipeline_logging prevents handler duplication by
        clearing all existing handlers from the pipeline logger before adding new ones.
        This ensures clean logging configuration and prevents log message duplication.
        """
        # Arrange
        mock_get_file_handler = mocker.patch("marimba.core.parallel.pipeline_loader.get_file_handler")

        # Create mock existing handlers to verify clearing behavior
        existing_handler1 = mocker.Mock()
        existing_handler1.baseFilename = "/existing/log1.log"
        existing_handler2 = mocker.Mock()
        existing_handler2.baseFilename = "/existing/log2.log"

        mock_pipeline = mocker.Mock()
        mock_pipeline.logger = mocker.Mock()
        mock_pipeline.logger.handlers = [existing_handler1, existing_handler2]

        # Create new handler that will be added
        new_handler = mocker.Mock()
        new_handler.baseFilename = "/test/log/file.log"
        mock_get_file_handler.return_value = new_handler

        root_dir = Path("/test/root")
        pipeline_name = "test_pipeline"

        # Act
        _configure_pipeline_logging(mock_pipeline, root_dir, pipeline_name, False, None)

        # Assert
        # Verify that existing handlers were cleared to prevent duplication
        assert (
            mock_pipeline.logger.handlers == []
        ), "All existing handlers should be cleared to prevent duplication before adding new handler"

        # Verify the new file handler was created with correct parameters
        mock_get_file_handler.assert_called_once_with(root_dir, pipeline_name, False), (
            "File handler should be created with correct parameters"
        )

        # Verify the new handler was added to the cleared logger
        mock_pipeline.logger.addHandler.assert_called_once_with(new_handler), (
            "New file handler should be added after clearing existing handlers"
        )


class TestLoadPipelineInstance:
    """Test cases for load_pipeline_instance function."""

    @pytest.fixture
    def pipeline_test_dirs(self, tmp_path: Path) -> dict[str, Path]:
        """Set up test directories and files."""
        root_dir = tmp_path / "root"
        repo_dir = tmp_path / "repo"
        config_path = tmp_path / "config.yaml"

        root_dir.mkdir()
        repo_dir.mkdir()
        config_path.write_text("key: value")

        # Create a test pipeline file
        pipeline_file = repo_dir / "test.pipeline.py"
        pipeline_content = """
from pathlib import Path
from typing import Any
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata

class MockTestPipeline(BasePipeline):
    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {}

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _package(
        self, data_dir: Path, config: dict[str, Any], **kwargs: Any
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {}
"""
        pipeline_file.write_text(pipeline_content)

        return {
            "root_dir": root_dir,
            "repo_dir": repo_dir,
            "config_path": config_path,
            "pipeline_file": pipeline_file,
        }

    @pytest.mark.integration
    def test_load_pipeline_instance_success(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test successfully loading a pipeline instance with real module loading and class instantiation.

        Verifies that load_pipeline_instance correctly loads a pipeline module from the filesystem,
        instantiates the pipeline class, loads configuration, and sets up logging. This integration
        test uses real file system operations and module loading but mocks only external dependencies.
        """
        # Arrange
        mock_load_config = mocker.patch("marimba.core.parallel.pipeline_loader.load_config")
        test_config = {"test_key": "test_value", "nested": {"config": "data"}}
        mock_load_config.return_value = test_config

        # Mock only the logging configuration to avoid file system dependencies
        mock_configure_logging = mocker.patch("marimba.core.parallel.pipeline_loader._configure_pipeline_logging")

        # Act
        result = load_pipeline_instance(
            pipeline_test_dirs["root_dir"],
            pipeline_test_dirs["repo_dir"],
            "test_pipeline",
            pipeline_test_dirs["config_path"],
            dry_run=False,
            log_string_prefix="LOG_PREFIX",
        )

        # Assert
        # Verify pipeline instance was created successfully
        assert result is not None, "Pipeline instance should be successfully created"
        assert isinstance(result, BasePipeline), f"Result should be a BasePipeline instance, got {type(result)}"

        # Verify the pipeline was configured with the expected parameters
        assert (
            result._root_path == pipeline_test_dirs["repo_dir"]
        ), "Pipeline root_path should match provided repo_dir path"
        assert (
            result.config == test_config
        ), f"Pipeline config should match loaded config, expected {test_config}, got {result.config}"
        assert result.dry_run is False, "Pipeline dry_run should match provided value"

        # Verify external dependencies were called correctly
        mock_load_config.assert_called_once_with(pipeline_test_dirs["config_path"])
        mock_configure_logging.assert_called_once_with(
            result,
            pipeline_test_dirs["root_dir"],
            "test_pipeline",
            False,
            "LOG_PREFIX",
        )

        # Verify module was loaded and registered in sys.modules
        assert "test.pipeline" in sys.modules, "Pipeline module should be registered in sys.modules"

        # Verify the loaded module contains our expected pipeline class
        loaded_module = sys.modules["test.pipeline"]
        assert hasattr(loaded_module, "MockTestPipeline"), "Loaded module should contain MockTestPipeline class"
        assert issubclass(
            loaded_module.MockTestPipeline,
            BasePipeline,
        ), "MockTestPipeline should be a BasePipeline subclass"

    @pytest.mark.integration
    def test_load_pipeline_instance_empty_repo_allow_empty(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test loading from empty repository with allow_empty=True returns None and logs warning.

        Verifies that load_pipeline_instance correctly handles empty repositories when allow_empty=True
        by returning None instead of raising an error, and ensures the warning logging function is
        called with the correct repository path. This integration test validates the complete
        empty repository handling workflow including file system operations and side effects.
        """
        # Arrange
        # Remove the pipeline file to simulate empty repository
        pipeline_test_dirs["pipeline_file"].unlink()

        # Mock the warning logger to verify it gets called
        mock_log_warning = mocker.patch("marimba.core.parallel.pipeline_loader._log_empty_repo_warning")

        # Mock other functions that should NOT be called for empty repositories
        mock_load_config = mocker.patch("marimba.core.parallel.pipeline_loader.load_config")
        mock_configure_logging = mocker.patch("marimba.core.parallel.pipeline_loader._configure_pipeline_logging")

        # Act
        result = load_pipeline_instance(
            pipeline_test_dirs["root_dir"],
            pipeline_test_dirs["repo_dir"],
            "test_pipeline",
            pipeline_test_dirs["config_path"],
            dry_run=False,
            log_string_prefix=None,
            allow_empty=True,
        )

        # Assert
        assert result is None, "Should return None when no pipeline file found and allow_empty=True"
        mock_log_warning.assert_called_once_with(
            pipeline_test_dirs["repo_dir"],
        ), "Should call warning logger exactly once with the repository directory path"

        # Verify that config loading and logging setup are not called for empty repositories
        mock_load_config.assert_not_called(), "Config loading should not be called when repository is empty"
        mock_configure_logging.assert_not_called(), (
            "Logging configuration should not be called when repository is empty"
        )

    @pytest.mark.integration
    def test_load_pipeline_instance_empty_repo_no_allow_empty(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test loading from empty repository with allow_empty=False raises FileNotFoundError.

        Verifies that load_pipeline_instance raises a FileNotFoundError with appropriate
        error message when no pipeline implementation is found in the repository and
        allow_empty=False (default behavior). This integration test validates the complete
        error handling workflow including file system operations and error propagation.
        """
        # Arrange
        # Remove the pipeline file to simulate empty repository
        pipeline_test_dirs["pipeline_file"].unlink()

        # Act & Assert
        with pytest.raises(FileNotFoundError) as exc_info:
            load_pipeline_instance(
                pipeline_test_dirs["root_dir"],
                pipeline_test_dirs["repo_dir"],
                "test_pipeline",
                pipeline_test_dirs["config_path"],
                dry_run=False,
            )

        # Assert
        # Verify the error message contains expected content
        error_message = str(exc_info.value)
        assert (
            "No pipeline implementation found" in error_message
        ), f"Error message should mention missing implementation, got: {error_message}"
        assert (
            str(pipeline_test_dirs["repo_dir"]) in error_message
        ), f"Error message should include repository path {pipeline_test_dirs['repo_dir']}, got: {error_message}"
        assert (
            ".pipeline.py" in error_message
        ), f"Error message should mention .pipeline.py file requirement, got: {error_message}"
        assert (
            "BasePipeline" in error_message
        ), f"Error message should mention BasePipeline inheritance requirement, got: {error_message}"

    @pytest.mark.unit
    def test_load_pipeline_instance_import_error(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test that ImportError during module execution is properly propagated.

        Verifies that load_pipeline_instance correctly propagates ImportError exceptions
        when a pipeline module contains import statements that fail during execution.
        This unit test focuses on the error handling behavior of the load_pipeline_instance
        function, mocking external dependencies while testing real module loading failures.
        """
        # Arrange
        # Create a pipeline file with an import that will fail during module execution
        pipeline_test_dirs["pipeline_file"].unlink()  # Remove good file first
        failing_pipeline_file = pipeline_test_dirs["repo_dir"] / "failing.pipeline.py"
        failing_pipeline_content = """
from pathlib import Path
from typing import Any
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
import nonexistent_module  # This will cause ImportError during module execution

class FailingPipeline(BasePipeline):
    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {}

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _package(
        self, data_dir: Path, config: dict[str, Any], **kwargs: Any
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {}
"""
        failing_pipeline_file.write_text(failing_pipeline_content)

        # Mock external dependencies that we don't want to test
        mock_load_config = mocker.patch("marimba.core.parallel.pipeline_loader.load_config")
        mock_configure_logging = mocker.patch("marimba.core.parallel.pipeline_loader._configure_pipeline_logging")

        # Act & Assert
        with pytest.raises(ModuleNotFoundError) as exc_info:
            load_pipeline_instance(
                pipeline_test_dirs["root_dir"],
                pipeline_test_dirs["repo_dir"],
                "test_pipeline",
                pipeline_test_dirs["config_path"],
                dry_run=False,
            )

        # Assert
        error_message = str(exc_info.value)
        assert (
            "nonexistent_module" in error_message
        ), f"Error message should mention the missing module 'nonexistent_module', got: {error_message}"
        assert (
            "No module named" in error_message
        ), f"Error message should be a standard module not found error, got: {error_message}"

        # Verify external dependencies were not called due to early failure during module execution
        mock_load_config.assert_not_called(), "Config loading should not be called when module execution fails"
        mock_configure_logging.assert_not_called(), (
            "Logging configuration should not be called when module execution fails"
        )

    @pytest.mark.integration
    def test_sys_path_manipulation(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test that sys.path is properly manipulated during module loading.

        Verifies that load_pipeline_instance correctly adds the repository directory
        to sys.path during module loading to enable repo-relative imports, and then
        properly restores sys.path to its original state after loading completes.
        This is an integration test as it tests the real module loading workflow
        and side effects on the global sys.path state.
        """
        # Arrange
        mock_load_config = mocker.patch("marimba.core.parallel.pipeline_loader.load_config")
        mock_load_config.return_value = {}
        mock_configure_logging = mocker.patch("marimba.core.parallel.pipeline_loader._configure_pipeline_logging")
        original_path = sys.path.copy()

        # Create a mock module that imports from the repo directory to test path manipulation
        # Update the pipeline file to include a relative import that would only work if repo_dir is in sys.path
        pipeline_test_dirs["pipeline_file"].unlink()
        repo_module_dir = pipeline_test_dirs["repo_dir"] / "repo_module"
        repo_module_dir.mkdir()
        helper_file = repo_module_dir / "__init__.py"
        helper_file.write_text("HELPER_VALUE = 'from_repo'")

        # Create pipeline that imports from the repo module
        pipeline_file = pipeline_test_dirs["repo_dir"] / "test.pipeline.py"
        pipeline_content = """
from pathlib import Path
from typing import Any
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from repo_module import HELPER_VALUE  # This import requires repo_dir to be in sys.path

class MockTestPipeline(BasePipeline):
    helper_value = HELPER_VALUE  # Reference the imported value

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {}

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        pass

    def _package(
        self, data_dir: Path, config: dict[str, Any], **kwargs: Any
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        return {}
"""
        pipeline_file.write_text(pipeline_content)

        # Act
        result = load_pipeline_instance(
            pipeline_test_dirs["root_dir"],
            pipeline_test_dirs["repo_dir"],
            "test_pipeline",
            pipeline_test_dirs["config_path"],
            dry_run=False,
        )

        # Assert
        # Verify sys.path was restored to original state after module loading
        assert sys.path == original_path, (
            f"sys.path should be restored to original state after module loading. "
            f"Expected: {original_path}, Got: {sys.path}"
        )

        # Verify pipeline instance was created successfully and the relative import worked
        assert result is not None, "Pipeline instance should be created successfully"
        assert isinstance(result, BasePipeline), f"Result should be a BasePipeline instance, got {type(result)}"

        # Verify that the relative import from the repo directory worked
        # This proves that repo_dir was added to sys.path during module loading
        assert hasattr(result, "helper_value"), "Pipeline should have helper_value attribute from relative import"
        assert (
            result.helper_value == "from_repo"
        ), f"Expected helper_value to be 'from_repo' from relative import, got {result.helper_value}"

        # Verify external dependencies were called correctly
        mock_load_config.assert_called_once_with(pipeline_test_dirs["config_path"])
        mock_configure_logging.assert_called_once_with(
            result,
            pipeline_test_dirs["root_dir"],
            "test_pipeline",
            False,
            None,
        )

    @pytest.mark.integration
    def test_module_execution_failure(
        self,
        pipeline_test_dirs: dict[str, Path],
        mocker: MockerFixture,
    ) -> None:
        """Test handling of syntax errors during module execution in load_pipeline_instance.

        Verifies that load_pipeline_instance correctly propagates SyntaxError exceptions
        when a pipeline module contains invalid Python syntax that fails during the
        module_spec.loader.exec_module(module) call. This integration test validates
        the complete error handling workflow including file system operations, module
        loading, and exception propagation without reaching config or logging setup.
        """
        # Arrange
        mock_load_config = mocker.patch("marimba.core.parallel.pipeline_loader.load_config")
        mock_load_config.return_value = {}
        mock_configure_logging = mocker.patch("marimba.core.parallel.pipeline_loader._configure_pipeline_logging")

        # Create a pipeline file with syntax error that will fail during module execution
        pipeline_test_dirs["pipeline_file"].unlink()  # Remove good file first
        bad_pipeline_file = pipeline_test_dirs["repo_dir"] / "bad.pipeline.py"
        bad_pipeline_file.write_text("invalid python syntax <<<")

        # Act & Assert
        with pytest.raises(SyntaxError) as exc_info:
            load_pipeline_instance(
                pipeline_test_dirs["root_dir"],
                pipeline_test_dirs["repo_dir"],
                "test_pipeline",
                pipeline_test_dirs["config_path"],
                dry_run=False,
            )

        # Assert
        # Verify that a SyntaxError was raised with appropriate error details
        assert exc_info.value is not None, "SyntaxError should be raised for invalid Python syntax"
        assert isinstance(exc_info.value, SyntaxError), f"Expected SyntaxError, got {type(exc_info.value).__name__}"

        # Verify the error contains reference to the problematic syntax
        error_message = str(exc_info.value)
        assert (
            "invalid syntax" in error_message or "invalid character" in error_message or "unexpected" in error_message
        ), f"SyntaxError message should indicate syntax problem, got: {error_message}"

        # Verify external dependencies were not called due to early failure during module execution
        mock_load_config.assert_not_called()
        mock_configure_logging.assert_not_called()


class TestFindPipelineClassFailureIsolation:
    """Pin the failure-isolation contract: a bad object mid-iteration must not derail the search."""

    @pytest.mark.unit
    def test_problematic_class_does_not_abort_search(self) -> None:
        """A class whose isinstance check raises TypeError is skipped; the valid one is still found."""
        import types

        from marimba.core.parallel.pipeline_loader import _find_pipeline_class

        class ProblematicClass:
            @property  # type: ignore[misc]
            def __class__(self) -> Any:
                msg = "Can't determine class"
                raise TypeError(msg)

        # Construct a synthetic module with the problematic object BEFORE the valid pipeline class.
        module = types.ModuleType("fake_module")
        module.__dict__["bad"] = ProblematicClass
        module.__dict__["good"] = MockTestPipeline

        result = _find_pipeline_class(module)

        assert result is MockTestPipeline

    @pytest.mark.unit
    def test_module_without_dict_raises_import_error(self) -> None:
        """A module-like object without __dict__ raises ImportError with a clear message."""
        from marimba.core.parallel.pipeline_loader import _find_pipeline_class

        class FakeModule:
            __slots__ = ()

        with pytest.raises(ImportError, match="no __dict__ attribute"):
            _find_pipeline_class(FakeModule())  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_module_with_only_invalid_classes_raises_import_error(self) -> None:
        """If no valid pipeline class is found, raise a descriptive ImportError."""
        import types

        from marimba.core.parallel.pipeline_loader import _find_pipeline_class

        module = types.ModuleType("fake_module")
        module.__dict__["a"] = int
        module.__dict__["b"] = str

        with pytest.raises(ImportError, match="not been set or could not be found"):
            _find_pipeline_class(module)


class TestLoadPipelineInstanceThreadSafety:
    """Verify the sys.path mutation in load_pipeline_instance is serialised across threads."""

    @pytest.mark.integration
    def test_concurrent_loads_do_not_leak_syspath(self, tmp_path: Path) -> None:
        """Concurrent thread loads must leave sys.path unchanged.

        Drive load_pipeline_instance concurrently from multiple threads against distinct repos. Without the
        _PIPELINE_IMPORT_LOCK, two threads' sys.path.insert / sys.path.pop pairs can interleave and pop each
        other's entries, leaving stray repo paths in sys.path after all loads complete (or, worse, popping
        an unrelated caller's path entry).
        """
        from concurrent.futures import ThreadPoolExecutor
        from textwrap import dedent

        from marimba.core.parallel.pipeline_loader import load_pipeline_instance

        # Build N distinct pipeline-repo layouts in tmp_path/<i>/{root,repo,config}.
        num_pipelines = 8

        def build_pipeline_layout(i: int) -> tuple[Path, Path, Path]:
            root_dir = tmp_path / f"pipeline_{i}"
            repo_dir = root_dir / "repo"
            repo_dir.mkdir(parents=True)
            (repo_dir / f"pipeline_{i}.pipeline.py").write_text(
                dedent(
                    f"""
                    from pathlib import Path
                    from typing import Any
                    from marimba.core.pipeline import BasePipeline
                    from marimba.core.schemas.base import BaseMetadata


                    class Pipeline{i}(BasePipeline):
                        def _package(
                            self,
                            data_dir: Path,
                            config: dict[str, Any],
                            **kwargs: Any,
                        ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
                            return {{}}
                    """,
                ),
            )
            config_path = root_dir / "pipeline.yml"
            config_path.write_text("{}\n")
            return root_dir, repo_dir, config_path

        layouts = [build_pipeline_layout(i) for i in range(num_pipelines)]

        # Snapshot sys.path so we can assert no leakage at the end.
        sys_path_snapshot = list(sys.path)

        def load(idx: int) -> object | None:
            root_dir, repo_dir, config_path = layouts[idx]
            return load_pipeline_instance(
                root_dir=root_dir,
                repo_dir=repo_dir,
                pipeline_name=f"pipeline_{idx}",
                config_path=config_path,
                dry_run=False,
                log_string_prefix=None,
            )

        with ThreadPoolExecutor(max_workers=num_pipelines) as ex:
            results = list(ex.map(load, range(num_pipelines)))

        # Every load succeeded.
        assert all(r is not None for r in results), "Every concurrent load should produce a pipeline instance"
        assert len(results) == num_pipelines

        # sys.path is unchanged after all loads complete (no leaks, no popped-wrong-entry).
        assert sys.path == sys_path_snapshot, (
            "sys.path should be restored to its pre-load state after every concurrent thread completes; "
            f"snapshot={sys_path_snapshot[:5]}..., got={sys.path[:5]}..."
        )


if __name__ == "__main__":
    pytest.main([__file__])
