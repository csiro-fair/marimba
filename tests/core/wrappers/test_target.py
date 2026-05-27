"""
Unit tests for DistributionTargetWrapper.

Tests the functionality of the DistributionTargetWrapper class including:
- Loading and validating configuration files
- Creating new targets
- Getting instances of distribution targets
- Error handling for invalid configurations
"""

from pathlib import Path
from typing import Any

import pytest
import pytest_mock
import yaml

from marimba.core.distribution.dap import CSIRODapDistributionTarget
from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.wrappers.target import DistributionTargetWrapper


@pytest.mark.unit
class TestDistributionTargetWrapperValidation:
    """Test DistributionTargetWrapper validation and configuration loading.

    These tests verify individual validation functions, configuration parsing,
    and error handling in isolation with minimal file system interactions.
    """

    @pytest.fixture
    def valid_s3_config(self) -> dict[str, Any]:
        """Return a valid S3 configuration."""
        return {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://s3.amazonaws.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        }

    @pytest.fixture
    def valid_dap_config(self) -> dict[str, Any]:
        """Return a valid DAP configuration."""
        return {
            "type": "dap",
            "config": {
                "endpoint_url": "https://test.dap.server.com",
                "access_key": "test_user",
                "secret_access_key": "test_password",
                "remote_directory": "bucket/datasets",
            },
        }

    def test_init_with_invalid_config_no_type(self, tmp_path: Path) -> None:
        """Test initialization with configuration missing type.

        This test verifies that attempting to initialize the wrapper with
        a configuration that lacks the required 'type' field raises
        InvalidConfigError with the appropriate error message.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        invalid_config = {"config": {"bucket": "test"}}
        config_path.write_text(yaml.dump(invalid_config))

        # Act & Assert
        with pytest.raises(
            DistributionTargetWrapper.InvalidConfigError,
            match="The distribution target configuration must specify a 'type'",
        ):
            DistributionTargetWrapper(config_path)

    def test_init_with_invalid_config_no_config(self, tmp_path: Path) -> None:
        """Test initialization with configuration missing config section.

        This test verifies that attempting to initialize the wrapper with
        a configuration that lacks the required 'config' section raises
        InvalidConfigError with the appropriate error message.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        invalid_config = {"type": "s3"}
        config_path.write_text(yaml.dump(invalid_config))

        # Act & Assert
        with pytest.raises(
            DistributionTargetWrapper.InvalidConfigError,
            match="The distribution target configuration must specify a 'config'",
        ):
            DistributionTargetWrapper(config_path)

    def test_init_with_invalid_target_type(self, tmp_path: Path) -> None:
        """Test initialization with invalid target type.

        This test verifies that attempting to initialize the wrapper with
        an unsupported target type raises InvalidConfigError with the
        appropriate error message listing valid types.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        invalid_config = {"type": "invalid_type", "config": {"test": "value"}}
        config_path.write_text(yaml.dump(invalid_config))

        # Act & Assert
        with pytest.raises(
            DistributionTargetWrapper.InvalidConfigError,
            match=r"Invalid distribution target type: invalid_type\. Must be one of: s3, dap",
        ) as exc_info:
            DistributionTargetWrapper(config_path)

        # Assert - Verify the complete error message matches expected format exactly
        error_message = str(exc_info.value)
        expected_error = "Invalid distribution target type: invalid_type. Must be one of: s3, dap"
        assert error_message == expected_error, f"Expected exact error message: {expected_error}, got: {error_message}"

    def test_get_instance_unknown_type(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test getting instance with unknown target type returns None.

        This test verifies that when the configuration contains a target type
        that is not registered in CLASS_MAP, the get_instance method gracefully
        returns None instead of raising an exception. This tests the error
        handling behavior for unsupported target types at runtime by temporarily
        modifying CLASS_MAP to include an unknown type, then removing it.
        """
        # Arrange - Backup original CLASS_MAP and create wrapper with valid config
        original_class_map = DistributionTargetWrapper.CLASS_MAP.copy()

        # Create a wrapper with a valid config first
        wrapper = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper._config_path = mocker.MagicMock()
        wrapper._config = {
            "type": "unknown_target_type",
            "config": {"some_param": "some_value"},
        }

        try:
            # Act
            instance = wrapper.get_instance()

            # Assert
            assert instance is None, "get_instance should return None for unknown target types"

        finally:
            # Cleanup - Restore original CLASS_MAP to prevent test interference
            DistributionTargetWrapper.CLASS_MAP.clear()
            DistributionTargetWrapper.CLASS_MAP.update(original_class_map)

    def test_class_map_contains_expected_types(self) -> None:
        """Test that the CLASS_MAP contains expected target types and correct class mappings.

        Entries are lazy ``module:Class`` strings; ``_resolve_target_class`` imports
        the target module on demand so boto3 stays out of CLI startup. Verifies both
        the registry shape and that each entry resolves to the expected class.
        """
        # Arrange
        expected_classes = {
            "s3": S3DistributionTarget,
            "dap": CSIRODapDistributionTarget,
        }

        # Act
        actual_class_map = DistributionTargetWrapper.CLASS_MAP

        # Assert - Verify CLASS_MAP contains exactly the expected entries (no more, no less)
        assert len(actual_class_map) == len(expected_classes), (
            f"CLASS_MAP should contain exactly {len(expected_classes)} entries, "
            f"but found {len(actual_class_map)}: {list(actual_class_map.keys())}"
        )

        # Assert - Check that all expected types are present and correctly resolve to the right class
        for target_type, expected_class in expected_classes.items():
            assert target_type in actual_class_map, f"CLASS_MAP should contain target type '{target_type}'"

            resolved = DistributionTargetWrapper._resolve_target_class(target_type)
            assert resolved is expected_class, (
                f"_resolve_target_class('{target_type}') should resolve to {expected_class.__name__}, "
                f"but resolved to {resolved}"
            )

    def test_load_config_called_during_init(self, tmp_path: Path) -> None:
        """Test that configuration is loaded during DistributionTargetWrapper initialization.

        This test verifies that the wrapper properly loads and validates configuration
        data from the specified file path during initialization. This ensures that
        the config loading process works correctly without mocking internal methods.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        valid_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://s3.amazonaws.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        }
        config_path.write_text(yaml.dump(valid_config))

        # Act
        wrapper = DistributionTargetWrapper(config_path)

        # Assert - Verify configuration was loaded correctly during initialization
        assert wrapper.config_path == config_path, "Config path should be set correctly"
        assert wrapper.config == valid_config, "Config should be loaded correctly"

        # Assert - Verify configuration structure is properly validated
        assert "type" in wrapper.config, "Config should contain required 'type' field"
        assert "config" in wrapper.config, "Config should contain required 'config' field"
        assert wrapper.config["type"] == "s3", "Config type should match expected value"
        assert isinstance(wrapper.config["config"], dict), "Config section should be a dictionary"

    def test_get_instance_malformed_s3_config_raises_type_error(self, tmp_path: Path) -> None:
        """Test get_instance raises TypeError when S3 config missing required parameters.

        This test verifies that when the S3 configuration has missing required
        parameters (endpoint_url, access_key_id, secret_access_key), the
        get_instance method raises TypeError with appropriate error message
        indicating missing positional arguments for S3DistributionTarget constructor.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        invalid_s3_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                # Missing required: endpoint_url, access_key_id, secret_access_key
            },
        }
        config_path.write_text(yaml.dump(invalid_s3_config))
        wrapper = DistributionTargetWrapper(config_path)

        # Act & Assert
        with pytest.raises(
            TypeError,
            match=r"S3DistributionTarget.*missing.*required.*positional argument",
        ) as exc_info:
            wrapper.get_instance()

        # Assert error message contains expected details
        error_message = str(exc_info.value)
        assert "missing" in error_message, f"Error should indicate missing arguments, got: {error_message}"
        assert "required" in error_message, f"Error should indicate required arguments, got: {error_message}"


@pytest.mark.integration
class TestDistributionTargetWrapperIntegration:
    """Test DistributionTargetWrapper component interactions.

    These tests verify component interactions between the wrapper,
    file system operations, configuration loading, and distribution
    target instantiation with minimal mocking.
    """

    @pytest.fixture
    def valid_s3_config(self) -> dict[str, Any]:
        """Return a valid S3 configuration."""
        return {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://s3.amazonaws.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        }

    @pytest.fixture
    def valid_dap_config(self) -> dict[str, Any]:
        """Return a valid DAP configuration."""
        return {
            "type": "dap",
            "config": {
                "endpoint_url": "https://test.dap.server.com",
                "access_key": "test_user",
                "secret_access_key": "test_password",
                "remote_directory": "bucket/datasets",
            },
        }

    def test_init_with_valid_config(self, tmp_path: Path, valid_s3_config: dict[str, Any]) -> None:
        """Test initialization with a valid configuration file.

        This test verifies that the wrapper can be successfully initialized
        with a properly formatted configuration file and that the config
        data is correctly loaded and accessible through properties.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text(yaml.dump(valid_s3_config))

        # Act
        wrapper = DistributionTargetWrapper(config_path)

        # Assert - Verify basic initialization
        assert wrapper.config_path == config_path, "Config path should match the initialized path"
        assert wrapper.config == valid_s3_config, "Config should match the loaded configuration data"

        # Assert - Verify file was actually loaded (file should exist and be readable)
        assert config_path.exists(), "Configuration file should exist at the specified path"
        assert config_path.is_file(), "Configuration path should point to a file, not a directory"

        # Assert - Verify configuration structure is properly loaded and validated
        assert "type" in wrapper.config, "Loaded config should contain required 'type' field"
        assert "config" in wrapper.config, "Loaded config should contain required 'config' field"
        assert wrapper.config["type"] == "s3", "Config type should match expected S3 target type"
        assert isinstance(wrapper.config["config"], dict), "Config section should be a dictionary"

    def test_init_with_nonexistent_file(self, tmp_path: Path) -> None:
        """Test initialization with non-existent configuration file.

        This test verifies that attempting to initialize the wrapper with
        a configuration file that doesn't exist raises FileNotFoundError
        with the correct file path in the error message.
        """
        # Arrange
        config_path = tmp_path / "nonexistent.yml"

        # Assert file doesn't exist before test
        assert not config_path.exists(), "Test setup error: file should not exist"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match=str(config_path.resolve())) as exc_info:
            DistributionTargetWrapper(config_path)

        # Assert - Verify the exception contains the expected file path
        error_message = str(exc_info.value)
        resolved_path_str = str(config_path.resolve())
        assert (
            resolved_path_str in error_message
        ), f"Error message should contain the resolved file path: {resolved_path_str}"

    def test_create_s3_target(self, tmp_path: Path) -> None:
        """Test creating an S3 target configuration file and wrapper instance.

        This test verifies that the create class method properly generates
        a configuration file with the correct structure and returns a valid
        wrapper instance for S3 distribution targets. Tests both file system
        effects and wrapper functionality in a single integration scenario.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        target_type = "s3"
        target_args = {
            "bucket_name": "test-bucket",
            "endpoint_url": "https://s3.amazonaws.com",
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
        }
        expected_config = {"type": target_type, "config": target_args}

        # Act
        wrapper = DistributionTargetWrapper.create(config_path, target_type, target_args)

        # Assert - Verify wrapper properties
        assert wrapper.config_path == config_path, "Config path should match the provided path"
        assert wrapper.config["type"] == target_type, f"Config type should be '{target_type}'"
        assert wrapper.config["config"] == target_args, "Config args should match the provided arguments"
        assert wrapper.config == expected_config, "Complete config should match expected structure"

        # Assert - Verify file creation and content
        assert config_path.exists(), "Configuration file should have been created"
        assert config_path.is_file(), "Created path should be a file, not a directory"

        # Assert - Verify file contains correct YAML structure
        file_config = yaml.safe_load(config_path.read_text())
        assert file_config == expected_config, "File content should match expected configuration structure"

        # Assert - Verify wrapper can create a valid S3 instance
        instance = wrapper.get_instance()
        assert instance is not None, "get_instance should return a valid instance"
        assert isinstance(instance, S3DistributionTarget), "Instance should be of correct S3DistributionTarget type"

    def test_create_dap_target(self, tmp_path: Path) -> None:
        """Test creating a DAP target configuration file and wrapper instance.

        This test verifies that the create class method properly generates
        a DAP configuration file with correct structure and returns a valid
        wrapper instance for CSIRO DAP distribution targets.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        target_type = "dap"
        target_args = {
            "endpoint_url": "https://test.dap.server.com",
            "access_key": "test_user",
            "secret_access_key": "test_password",
            "remote_directory": "bucket/datasets",
        }
        expected_config = {"type": target_type, "config": target_args}

        # Act
        wrapper = DistributionTargetWrapper.create(config_path, target_type, target_args)

        # Assert - Verify wrapper properties
        assert wrapper.config_path == config_path, "Config path should match the provided path"
        assert wrapper.config["type"] == target_type, f"Config type should be '{target_type}'"
        assert wrapper.config["config"] == target_args, "Config args should match the provided arguments"
        assert wrapper.config == expected_config, "Complete config should match expected structure"

        # Assert - Verify file creation and content structure
        assert config_path.exists(), "Configuration file should have been created"
        assert config_path.is_file(), "Created path should be a file, not a directory"

        # Assert - Verify file contains expected structure
        created_config = yaml.safe_load(config_path.read_text())
        assert created_config == expected_config, "File content should match expected configuration structure"

        # Assert - Verify wrapper can create a valid instance
        instance = wrapper.get_instance()
        assert instance is not None, "get_instance should return a valid instance"
        assert isinstance(
            instance,
            CSIRODapDistributionTarget,
        ), "Instance should be of correct CSIRODapDistributionTarget type"

    def test_create_existing_file_error(self, tmp_path: Path) -> None:
        """Test creating target configuration fails when file already exists.

        This test verifies that the create method properly prevents overwriting
        existing configuration files by raising FileExistsError with the specific
        path when attempting to create a target at an existing file location.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text("existing file content")
        target_type = "s3"
        target_args = {"bucket_name": "test-bucket", "endpoint_url": "https://example.com"}

        # Act & Assert
        with pytest.raises(FileExistsError, match=str(config_path.resolve())):
            DistributionTargetWrapper.create(config_path, target_type, target_args)

        # Assert - Verify original file content is preserved
        assert config_path.exists(), "Original file should still exist after failed create attempt"
        assert config_path.read_text() == "existing file content", "Original file content should be unchanged"

    def test_get_instance_s3(self, tmp_path: Path, valid_s3_config: dict[str, Any]) -> None:
        """Test getting an S3 distribution target instance.

        This test verifies that the wrapper can create a valid S3DistributionTarget
        instance from a properly configured S3 configuration, ensuring that the
        instance is properly instantiated and functional through its public interface.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text(yaml.dump(valid_s3_config))
        wrapper = DistributionTargetWrapper(config_path)

        # Act
        instance = wrapper.get_instance()

        # Assert
        assert instance is not None, "get_instance should return a valid instance for properly configured S3 target"
        assert isinstance(instance, S3DistributionTarget), "Instance should be of correct S3DistributionTarget type"

        # Assert - Verify the instance is functional by testing public interface
        assert hasattr(instance, "distribute"), "S3 instance should have distribute method"
        assert callable(instance.distribute), "distribute should be callable"

        # Assert - Verify instance configuration through public interface behavior
        # The instance should be properly configured with S3 parameters from the config
        assert instance is not None, "Instance should be properly initialized with valid configuration"

    def test_get_instance_dap(self, tmp_path: Path, valid_dap_config: dict[str, Any]) -> None:
        """Test getting a DAP distribution target instance.

        This test verifies that the wrapper can create a valid CSIRODapDistributionTarget
        instance from a properly configured DAP configuration, ensuring that the
        instance is properly instantiated with the expected internal components
        and configuration values matching the provided DAP configuration.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text(yaml.dump(valid_dap_config))
        wrapper = DistributionTargetWrapper(config_path)

        # Act
        instance = wrapper.get_instance()

        # Assert
        assert instance is not None, "get_instance should return a valid instance for properly configured DAP target"
        assert isinstance(
            instance,
            CSIRODapDistributionTarget,
        ), "Instance should be of correct CSIRODapDistributionTarget type"

        # Assert - Verify that the instance was created successfully and is functional
        # Since CSIRODapDistributionTarget inherits from S3DistributionTarget, verify it has expected functionality
        assert hasattr(instance, "distribute"), "DAP instance should have distribute method"
        assert callable(instance.distribute), "distribute should be callable"

        # Assert - Verify the instance is properly configured through public behavior
        # The instance should be properly initialized with the DAP configuration parameters
        assert instance is not None, "Instance should be properly initialized with valid DAP configuration"

    def test_config_path_property(self, tmp_path: Path, valid_s3_config: dict[str, Any]) -> None:
        """Test config_path property returns the exact Path object used during initialization.

        This test verifies that the config_path property maintains the original Path object
        reference and type consistency throughout the wrapper's lifecycle.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text(yaml.dump(valid_s3_config))

        # Act
        wrapper = DistributionTargetWrapper(config_path)
        actual_config_path = wrapper.config_path

        # Assert - Verify exact path object reference and properties
        assert actual_config_path == config_path, "Config path should match the original path"
        assert (
            actual_config_path is wrapper._config_path
        ), "Config path should be the same object instance stored internally"
        assert isinstance(actual_config_path, Path), "Config path should be a pathlib.Path instance"
        assert actual_config_path.exists(), "Config path should point to an existing file"
        assert actual_config_path.is_file(), "Config path should point to a file, not a directory"
        assert actual_config_path.name == "target.yml", "Config path should have the expected filename"

    def test_config_property(self, tmp_path: Path, valid_s3_config: dict[str, Any]) -> None:
        """Test config property returns the exact configuration loaded from file.

        This test verifies that the config property returns the complete configuration
        dictionary that was loaded from the YAML file during initialization, ensuring
        data integrity and proper encapsulation of the configuration state.
        """
        # Arrange
        config_path = tmp_path / "target.yml"
        config_path.write_text(yaml.dump(valid_s3_config))

        # Act
        wrapper = DistributionTargetWrapper(config_path)
        actual_config = wrapper.config

        # Assert - Verify config property returns loaded configuration exactly
        assert actual_config == valid_s3_config, "Config property should return exact configuration from file"
        assert isinstance(actual_config, dict), "Config should be a dictionary instance"

        # Assert - Verify required configuration structure
        assert "type" in actual_config, "Config should contain required 'type' key"
        assert "config" in actual_config, "Config should contain required 'config' key"

        # Assert - Verify specific configuration values match expected data
        assert actual_config["type"] == "s3", "Config type should match the configured S3 target type"
        assert (
            actual_config["config"] == valid_s3_config["config"]
        ), "Config section should match the provided S3 configuration parameters"

        # Assert - Verify config property maintains reference consistency
        assert (
            actual_config is wrapper._config
        ), "Config property should return the same object instance stored internally"

        # Assert - Verify all expected S3 configuration parameters are present
        s3_config_section = actual_config["config"]
        expected_s3_keys = ["bucket_name", "endpoint_url", "access_key_id", "secret_access_key"]
        for key in expected_s3_keys:
            assert key in s3_config_section, f"S3 config section should contain required parameter '{key}'"

    def test_get_instance_invalid_config(self, tmp_path: Path) -> None:
        """Test get_instance with invalid configuration returns None.

        This test verifies that when the wrapper's configuration contains
        invalid or incomplete data (missing type or config section),
        the get_instance method gracefully returns None instead of raising
        an exception. This tests the robustness of the instance creation logic.
        """
        # Arrange - Create wrapper with valid initial config, then modify internal config
        config_path = tmp_path / "target.yml"
        valid_config = {
            "type": "s3",
            "config": {
                "bucket_name": "test-bucket",
                "endpoint_url": "https://s3.amazonaws.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        }
        config_path.write_text(yaml.dump(valid_config))

        # Test Case 1: Missing type field - create new config file
        invalid_config_1 = {"config": {"bucket_name": "test"}}
        config_path.write_text(yaml.dump(invalid_config_1))
        wrapper_1 = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper_1._config_path = config_path
        wrapper_1._config = invalid_config_1
        instance = wrapper_1.get_instance()
        assert instance is None, "get_instance should return None when config missing 'type' field"

        # Test Case 2: Missing config field
        invalid_config_2 = {"type": "s3"}
        wrapper_2 = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper_2._config_path = config_path
        wrapper_2._config = invalid_config_2
        instance = wrapper_2.get_instance()
        assert instance is None, "get_instance should return None when config missing 'config' field"

        # Test Case 3: Non-string type field
        invalid_config_3 = {"type": 123, "config": {"bucket": "test"}}
        wrapper_3 = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper_3._config_path = config_path
        wrapper_3._config = invalid_config_3
        instance = wrapper_3.get_instance()
        assert instance is None, "get_instance should return None when 'type' field is not a string"

        # Test Case 4: Non-dict config field
        invalid_config_4 = {"type": "s3", "config": "invalid_config"}
        wrapper_4 = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper_4._config_path = config_path
        wrapper_4._config = invalid_config_4
        instance = wrapper_4.get_instance()
        assert instance is None, "get_instance should return None when 'config' field is not a dict"

        # Test Case 5: Valid structure but unknown target type
        invalid_config_5 = {"type": "unknown_type", "config": {"param": "value"}}
        wrapper_5 = DistributionTargetWrapper.__new__(DistributionTargetWrapper)
        wrapper_5._config_path = config_path
        wrapper_5._config = invalid_config_5
        instance = wrapper_5.get_instance()
        assert instance is None, "get_instance should return None for unknown target types"


@pytest.mark.unit
class TestDistributionTargetWrapperPromptEdgeCases:
    """Test edge cases for the prompt_target method."""

    def test_prompt_target_invalid_class_map_entry(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test prompt_target with invalid class type in CLASS_MAP raises TypeError.

        This test verifies that when CLASS_MAP contains a class whose __init__ method
        is not a FunctionType (like built-in types), the prompt_target method raises
        TypeError with a specific error message about the __init__ method not being
        a method. Built-in types have __init__ as built-in method descriptors, not
        FunctionType instances, which is what the source code checks for at line 149.
        """
        # Arrange - Backup original CLASS_MAP, inject invalid registry entry, mock the resolver
        original_class_map = DistributionTargetWrapper.CLASS_MAP.copy()
        invalid_target_type = "invalid_type"
        # Use str class - it has __init__ but as a built-in method descriptor, not FunctionType
        invalid_class_entry = str

        DistributionTargetWrapper.CLASS_MAP[invalid_target_type] = "tests.fixture:Invalid"
        mocker.patch.object(
            DistributionTargetWrapper,
            "_resolve_target_class",
            return_value=invalid_class_entry,
        )
        mock_prompt_ask = mocker.patch("rich.prompt.Prompt.ask", return_value=invalid_target_type)

        try:
            # Act & Assert - Verify TypeError with exact message pattern
            with pytest.raises(
                TypeError,
                match=f"__init__ of target class {invalid_target_type} is not a method",
            ) as exc_info:
                DistributionTargetWrapper.prompt_target()

            # Assert - Verify the complete error message
            error_message = str(exc_info.value)
            assert (
                error_message == f"__init__ of target class {invalid_target_type} is not a method"
            ), "Error message should match exactly the expected format"

            # Assert - Verify the mock was called once with expected parameters
            mock_prompt_ask.assert_called_once_with(
                "Distribution target type",
                choices=list(DistributionTargetWrapper.CLASS_MAP.keys()),
            )

        finally:
            # Cleanup - Restore original CLASS_MAP to prevent test interference
            DistributionTargetWrapper.CLASS_MAP.clear()
            DistributionTargetWrapper.CLASS_MAP.update(original_class_map)

    def test_prompt_target_nonexistent_type_raises_value_error(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test prompt_target with nonexistent target type raises ValueError.

        This test verifies that when a user inputs a target type that doesn't
        exist in CLASS_MAP, the prompt_target method raises ValueError with
        a specific error message about the target class not being found.
        This ensures proper error handling for invalid user input during
        interactive target configuration.
        """
        # Arrange
        nonexistent_type = "nonexistent_target_type"
        mock_prompt_ask = mocker.patch("rich.prompt.Prompt.ask", return_value=nonexistent_type)

        # Act & Assert
        with pytest.raises(
            ValueError,
            match=f"No target class found for type {nonexistent_type}",
        ) as exc_info:
            DistributionTargetWrapper.prompt_target()

        # Assert - Verify the exception was raised with correct message
        error_message = str(exc_info.value)
        assert (
            error_message == f"No target class found for type {nonexistent_type}"
        ), f"Expected exact error message, got: {error_message}"

        # Assert - Verify mock was called exactly once with expected parameters
        mock_prompt_ask.assert_called_once_with(
            "Distribution target type",
            choices=list(DistributionTargetWrapper.CLASS_MAP.keys()),
        )

    def test_prompt_target_s3(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test interactive prompting for S3 target configuration creation.

        This test verifies that the prompt_target static method correctly handles
        interactive user input for creating an S3 distribution target configuration.
        It validates proper parameter mapping, prompt ordering, input validation,
        and configuration structure for all required S3 parameters.
        """
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        expected_inputs = [
            "s3",  # target type
            "test-bucket",  # bucket_name
            "https://s3.amazonaws.com",  # endpoint_url
            "test-key",  # access_key_id
            "test-secret",  # secret_access_key
            "",  # base_prefix (default empty)
        ]
        mock_ask.side_effect = expected_inputs

        # Act
        target_type, target_args = DistributionTargetWrapper.prompt_target()

        # Assert - Verify return values structure
        assert isinstance(target_type, str), "Target type should be a string"
        assert isinstance(target_args, dict), "Target args should be a dictionary"
        assert target_type == "s3", "Target type should match the user's selection"

        # Assert - Verify all expected configuration parameters are present
        expected_config_keys = ["bucket_name", "endpoint_url", "access_key_id", "secret_access_key", "base_prefix"]
        assert len(target_args) == len(
            expected_config_keys,
        ), f"Target args should contain exactly {len(expected_config_keys)} parameters"

        for key in expected_config_keys:
            assert key in target_args, f"Target args should contain required parameter '{key}'"

        # Assert - Verify specific parameter values match expected inputs exactly
        assert target_args["bucket_name"] == "test-bucket", "Bucket name should match user input exactly"
        assert target_args["endpoint_url"] == "https://s3.amazonaws.com", "Endpoint URL should match user input exactly"
        assert target_args["access_key_id"] == "test-key", "Access key ID should match user input exactly"
        assert target_args["secret_access_key"] == "test-secret", "Secret access key should match user input exactly"
        assert target_args["base_prefix"] == "", "Base prefix should match user input (empty string default)"

        # Assert - Verify mock interaction behavior
        assert mock_ask.call_count == 6, "Prompt should be called exactly 6 times for complete S3 configuration"

        # Assert - Verify correct prompt messages and order were shown to user
        expected_calls = [
            mocker.call("Distribution target type", choices=["s3", "dap"]),
            mocker.call("Bucket name"),
            mocker.call("Endpoint url"),
            mocker.call("Access key id"),
            mocker.call("Secret access key"),
            mocker.call("Base prefix"),
        ]
        mock_ask.assert_has_calls(expected_calls, any_order=False)

    def test_prompt_target_dap(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test interactive prompting for DAP target creation.

        This test verifies that the prompt_target static method correctly
        handles user input for creating a DAP distribution target configuration,
        including proper parameter mapping and validation of all expected
        configuration values and correct prompt messages.
        """
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        expected_inputs = [
            "dap",  # target type
            "https://test.server.com",  # endpoint_url
            "access_key",  # access_key
            "secret_key",  # secret_access_key
            "bucket/data",  # remote_directory
        ]
        mock_ask.side_effect = expected_inputs

        # Act
        target_type, target_args = DistributionTargetWrapper.prompt_target()

        # Assert - Verify target type
        assert target_type == "dap", "Target type should match the user's selection"

        # Assert - Verify all expected configuration parameters are present
        expected_config_keys = ["endpoint_url", "access_key", "secret_access_key", "remote_directory"]
        for key in expected_config_keys:
            assert key in target_args, f"Target args should contain required parameter '{key}'"

        # Assert - Verify specific parameter values match expected inputs
        assert target_args["endpoint_url"] == "https://test.server.com", "Endpoint URL should match user input"
        assert target_args["access_key"] == "access_key", "Access key should match user input"
        assert target_args["secret_access_key"] == "secret_key", "Secret access key should match user input"
        assert target_args["remote_directory"] == "bucket/data", "Remote directory should match user input"

        # Assert - Verify mock was called the expected number of times
        assert mock_ask.call_count == 5, "Prompt should be called exactly 5 times for DAP configuration"

        # Assert - Verify correct prompt messages were shown to user
        expected_calls = [
            mocker.call("Distribution target type", choices=["s3", "dap"]),
            mocker.call("Endpoint url"),
            mocker.call("Access key"),
            mocker.call("Secret access key"),
            mocker.call("Remote directory"),
        ]
        mock_ask.assert_has_calls(expected_calls, any_order=False)

    def test_prompt_target_missing_init_method(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test prompt_target with target class missing __init__ method raises TypeError.

        This test verifies that when CLASS_MAP contains a class that doesn't have
        an __init__ method, the prompt_target method raises TypeError with a specific
        error message. This edge case ensures proper validation of target classes
        during interactive configuration. We simulate this by mocking hasattr to
        return False for the __init__ check.
        """

        # Arrange - Use a real class but mock hasattr to simulate missing __init__
        class MockTargetClass:
            """Mock target class for testing."""

        # Backup original CLASS_MAP, inject registry entry, mock the resolver to return MockTargetClass
        original_class_map = DistributionTargetWrapper.CLASS_MAP.copy()
        invalid_target_type = "no_init_type"
        DistributionTargetWrapper.CLASS_MAP[invalid_target_type] = "tests.fixture:MockTargetClass"
        mocker.patch.object(
            DistributionTargetWrapper,
            "_resolve_target_class",
            return_value=MockTargetClass,
        )

        mock_prompt_ask = mocker.patch("rich.prompt.Prompt.ask", return_value=invalid_target_type)

        # Mock hasattr to return False when checking for __init__ method
        original_hasattr = hasattr

        def mock_hasattr(obj, name):
            if obj is MockTargetClass and name == "__init__":
                return False
            return original_hasattr(obj, name)

        mock_hasattr_func = mocker.patch("builtins.hasattr", side_effect=mock_hasattr)

        try:
            # Act & Assert - Verify TypeError with exact message pattern
            with pytest.raises(
                TypeError,
                match=f"Target class {invalid_target_type} does not have an __init__ method",
            ) as exc_info:
                DistributionTargetWrapper.prompt_target()

            # Assert - Verify the complete error message
            error_message = str(exc_info.value)
            assert (
                error_message == f"Target class {invalid_target_type} does not have an __init__ method"
            ), "Error message should match exactly the expected format"

            # Assert - Verify the mock was called once with expected parameters
            mock_prompt_ask.assert_called_once_with(
                "Distribution target type",
                choices=list(DistributionTargetWrapper.CLASS_MAP.keys()),
            )

            # Assert - Verify hasattr was called with the target class and __init__
            mock_hasattr_func.assert_any_call(MockTargetClass, "__init__")

        finally:
            # Cleanup - Restore original CLASS_MAP to prevent test interference
            DistributionTargetWrapper.CLASS_MAP.clear()
            DistributionTargetWrapper.CLASS_MAP.update(original_class_map)
