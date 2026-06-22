from pathlib import Path

import pytest
import yaml

from marimba.core.utils.config import load_config, save_config


class TestSaveConfig:
    """
    Test suite for save_config function.

    Tests cover save_config functionality including YAML serialization and file creation.
    """

    @pytest.mark.unit
    def test_save_config_creates_valid_yaml_file(self, tmp_path: Path) -> None:
        """
        Test that save_config creates a valid YAML file with expected content.

        Verifies that save_config function properly serializes dictionary data to YAML format
        and creates a file that can be parsed back to the original data structure.
        """
        # Arrange
        config_path = tmp_path / "unit_test_config.yaml"
        config_data = {"test_key": "test_value", "number": 42}

        # Act
        save_config(config_path, config_data)

        # Assert
        assert config_path.exists(), "Config file was not created"
        assert config_path.is_file(), "Created path should be a file, not a directory"

        # Verify file content by parsing it back (round-trip test)
        with config_path.open("r", encoding="utf-8") as f:
            parsed_content = yaml.safe_load(f)

        assert (
            parsed_content == config_data
        ), f"Parsed YAML does not match original data: expected {config_data}, got {parsed_content}"

    @pytest.mark.unit
    def test_save_config_with_complex_nested_data(self, tmp_path: Path) -> None:
        """
        Test that save_config handles complex nested data structures correctly.

        Verifies that save_config properly serializes deeply nested dictionaries, lists,
        various data types, and special values to YAML format that can be round-tripped
        without data loss. Tests realistic complex configuration scenarios.
        """
        # Arrange
        config_path = tmp_path / "complex_config.yaml"
        complex_data = {
            "metadata": {
                "name": "test_config",
                "version": "1.0.0",
                "author": "test-user",
                "created_at": "2024-01-01T00:00:00Z",
            },
            "database": {
                "connections": [
                    {"host": "primary.db", "port": 5432, "ssl": True},
                    {"host": "replica.db", "port": 5433, "ssl": False},
                ],
                "credentials": {
                    "primary": {"username": "admin", "timeout": 30.5},
                    "backup": {"username": "readonly", "timeout": None},
                },
            },
            "features": {
                "enabled": ["feature_a", "feature_b"],
                "disabled": [],
                "experimental": {"beta_feature": True, "alpha_feature": False},
            },
            "environment": {
                "variables": {"DEBUG": "true", "LOG_LEVEL": "info"},
                "paths": ["/var/log", "/tmp", "/opt/app"],
            },
            "limits": {"max_connections": 100, "timeout": 60.0, "retry_count": 3},
            "special_values": {"null_value": None, "empty_string": "", "zero": 0},
        }

        # Act
        save_config(config_path, complex_data)

        # Assert - File creation
        assert config_path.exists(), f"Config file was not created at path: {config_path}"
        assert config_path.is_file(), f"Created path should be a file, not a directory: {config_path}"

        # Assert - File content structure validation
        with config_path.open("r", encoding="utf-8") as f:
            yaml_content = f.read()

        assert yaml_content.strip(), "YAML file should not be empty"
        assert "metadata:" in yaml_content, "Missing 'metadata:' section in YAML output"
        assert "database:" in yaml_content, "Missing 'database:' section in YAML output"
        assert "connections:" in yaml_content, "Missing 'connections:' list in YAML output"

        # Assert - Round-trip data integrity
        with config_path.open("r", encoding="utf-8") as f:
            parsed_content = yaml.safe_load(f)

        assert parsed_content == complex_data, (
            f"Complex data not preserved during YAML round-trip.\n"
            f"Expected: {complex_data}\n"
            f"Got: {parsed_content}"
        )

        # Assert - Specific data type preservation
        assert isinstance(
            parsed_content["database"]["connections"],
            list,
        ), f"Database connections should be preserved as list, got {type(parsed_content['database']['connections'])}"
        assert (
            len(parsed_content["database"]["connections"]) == 2
        ), f"Should have exactly 2 database connections, got {len(parsed_content['database']['connections'])}"
        assert (
            parsed_content["database"]["connections"][0]["port"] == 5432
        ), f"First connection port should be 5432, got {parsed_content['database']['connections'][0]['port']}"
        assert (
            parsed_content["special_values"]["null_value"] is None
        ), f"Null value should be preserved as None, got {parsed_content['special_values']['null_value']}"
        assert (
            parsed_content["special_values"]["empty_string"] == ""
        ), f"Empty string should be preserved as empty string, got '{parsed_content['special_values']['empty_string']}'"
        assert parsed_content["limits"]["timeout"] == 60.0, (
            f"Float values should be preserved with correct type and value, "
            f"expected 60.0, got {parsed_content['limits']['timeout']} ({type(parsed_content['limits']['timeout'])})"
        )


class TestLoadConfig:
    """
    Test suite for load_config function.

    Tests cover load_config functionality including YAML parsing, error handling, and validation.
    """

    @pytest.mark.unit
    def test_load_config_with_valid_yaml(self, tmp_path: Path) -> None:
        """
        Test that load_config successfully loads valid YAML and returns expected dictionary.

        This unit test verifies YAML parsing behavior with a dictionary structure.
        """
        # Arrange
        config_path = tmp_path / "valid_config.yaml"
        expected_data = {"key": "value", "number": 42, "boolean": True}
        with config_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(expected_data, f)

        # Act
        loaded_data = load_config(config_path)

        # Assert
        assert loaded_data == expected_data, f"Expected {expected_data}, got {loaded_data}"
        assert isinstance(loaded_data, dict), "Loaded data should be a dictionary"

    @pytest.mark.unit
    def test_load_config_with_invalid_yaml(self, tmp_path: Path) -> None:
        """
        Test that load_config raises ScannerError for malformed YAML content.

        Verifies error handling when YAML parsing fails due to syntax errors.
        """
        # Arrange
        config_path = tmp_path / "invalid_config.yaml"
        with config_path.open("w", encoding="utf-8") as f:
            f.write("key: value\ninvalid")

        # Act & Assert
        with pytest.raises(yaml.scanner.ScannerError, match=r"could not find expected"):
            load_config(config_path)

    @pytest.mark.unit
    def test_load_config_nonexistent_file_raises_file_not_found_error(self, tmp_path: Path) -> None:
        """
        Test that load_config raises FileNotFoundError for non-existent files.

        Verifies error handling when the specified config file doesn't exist.
        """
        # Arrange
        nonexistent_path = tmp_path / "nonexistent_config.yaml"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match=rf"No such file or directory: '{nonexistent_path}'"):
            load_config(nonexistent_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("yaml_content", "content_type", "filename"),
        [
            ("- item1\n- item2", "list", "list_config.yaml"),
            ("just a string", "string", "string_config.yaml"),
            ("null", "null", "null_config.yaml"),
            ("42", "number", "number_config.yaml"),
            ("true", "boolean", "boolean_config.yaml"),
        ],
    )
    def test_load_config_with_non_dict_content_raises_type_error(
        self,
        tmp_path: Path,
        yaml_content: str,
        content_type: str,
        filename: str,
    ) -> None:
        """
        Test that load_config raises TypeError when YAML content is not a dictionary.

        Verifies that load_config validates parsed YAML data is a dictionary structure
        and raises TypeError for non-dictionary content.
        """
        # Arrange
        config_path = tmp_path / filename
        with config_path.open("w", encoding="utf-8") as f:
            f.write(yaml_content)

        # Act & Assert
        with pytest.raises(TypeError, match=r"Configuration data must be a dictionary"):
            load_config(config_path)

    @pytest.mark.unit
    def test_load_config_empty_yaml_file_raises_type_error(self, tmp_path: Path) -> None:
        """
        Test that load_config raises TypeError for empty YAML files.

        Verifies that empty YAML files (which parse to None) are handled correctly.
        """
        # Arrange
        config_path = tmp_path / "empty_config.yaml"
        config_path.touch()  # Create empty file

        # Act & Assert
        with pytest.raises(TypeError, match=r"Configuration data must be a dictionary"):
            load_config(config_path)

    @pytest.mark.unit
    def test_load_config_complex_yaml_structure_preserved(self, tmp_path: Path) -> None:
        """
        Test that load_config preserves complex YAML structures including nested objects.

        Verifies proper handling of complex configuration structures with nested
        dictionaries, lists, and various data types.
        """
        # Arrange
        config_path = tmp_path / "complex_config.yaml"
        expected_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {"user": "admin", "password": "secret"},
            },
            "features": ["feature1", "feature2"],
            "debug": True,
        }
        with config_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(expected_data, f)

        # Act
        loaded_data = load_config(config_path)

        # Assert
        assert (
            loaded_data == expected_data
        ), f"Complex structure not preserved: expected {expected_data}, got {loaded_data}"
        assert isinstance(
            loaded_data["database"]["credentials"],
            dict,
        ), f"Nested credentials should be dict, got {type(loaded_data['database']['credentials'])}"
        assert isinstance(
            loaded_data["features"],
            list,
        ), f"Features should be list, got {type(loaded_data['features'])}"
        assert (
            len(loaded_data["features"]) == 2
        ), f"Features list length should be 2, got {len(loaded_data['features'])}"
        assert loaded_data["database"]["port"] == 5432, f"Port should be 5432, got {loaded_data['database']['port']}"
        assert loaded_data["debug"] is True, f"Debug should be True, got {loaded_data['debug']}"
        assert (
            loaded_data["database"]["host"] == "localhost"
        ), f"Host should be 'localhost', got {loaded_data['database']['host']}"
        assert (
            loaded_data["database"]["credentials"]["user"] == "admin"
        ), f"User should be 'admin', got {loaded_data['database']['credentials']['user']}"
