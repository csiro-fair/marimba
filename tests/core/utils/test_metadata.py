"""Tests for marimba.core.utils.metadata module."""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from marimba.core.utils.metadata import (
    MetadataSaverTypes,
    get_saver,
    json_saver,
    yaml_saver,
)


class TestMetadataSaverTypes:
    """Test MetadataSaverTypes enum."""

    @pytest.mark.unit
    def test_metadata_saver_types_values(self) -> None:
        """Test MetadataSaverTypes enum values match expected string values and demonstrate enum behavior."""
        # Arrange
        expected_json_value = "json"
        expected_yaml_value = "yaml"

        # Act & Assert - Test enum string comparison
        assert MetadataSaverTypes.json == expected_json_value, "JSON saver type should equal 'json'"
        assert MetadataSaverTypes.yaml == expected_yaml_value, "YAML saver type should equal 'yaml'"

        # Act & Assert - Test enum .value attribute
        assert MetadataSaverTypes.json.value == expected_json_value, "JSON enum value should be 'json'"
        assert MetadataSaverTypes.yaml.value == expected_yaml_value, "YAML enum value should be 'yaml'"


class TestJsonSaver:
    """Test json_saver function for JSON file creation and data serialization."""

    @pytest.mark.unit
    def test_json_saver_creates_json_file(self, tmp_path: Path) -> None:
        """Test json_saver creates JSON file with correct content and formatting."""
        # Arrange
        test_data = {"name": "test_dataset", "items": [{"file": "test.jpg", "metadata": {"lat": 37.7749}}]}

        # Act
        json_saver(tmp_path, "test_metadata", test_data)

        # Assert
        output_file = tmp_path / "test_metadata.json"
        assert output_file.exists(), "JSON file should be created"
        assert output_file.stat().st_size > 0, "JSON file should not be empty"

        # Verify file contents can be loaded and match expected data
        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert loaded_data == test_data, "Loaded data should match original test data"

        # Verify JSON formatting is indented
        with output_file.open("r", encoding="utf-8") as f:
            content = f.read()
        assert "  " in content, "JSON should be indented for readability"

    @pytest.mark.unit
    def test_json_saver_with_unicode_data(self, tmp_path: Path) -> None:
        """Test json_saver handles unicode data correctly and preserves characters during round-trip."""
        # Arrange
        test_data = {
            "chinese": "测试数据",
            "accented": "café",
            "emoji": "🌟🎉",
            "mixed": "Hello 世界! 🚀",
        }

        # Act
        json_saver(tmp_path, "unicode_test", test_data)

        # Assert
        output_file = tmp_path / "unicode_test.json"
        assert output_file.exists(), "JSON file should be created"

        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert loaded_data == test_data, "Unicode data should round-trip correctly"

    @pytest.mark.unit
    def test_json_saver_empty_data(self, tmp_path: Path) -> None:
        """Test json_saver handles empty dictionary correctly and creates valid JSON file."""
        # Arrange
        test_data: dict[str, Any] = {}

        # Act
        json_saver(tmp_path, "empty_test", test_data)

        # Assert
        output_file = tmp_path / "empty_test.json"
        assert output_file.exists(), "JSON file should be created for empty data"

        # Verify file contains valid JSON syntax for empty object
        with output_file.open("r", encoding="utf-8") as f:
            content = f.read().strip()
        assert content == "{}", "Empty JSON file should contain exactly '{}'"

        # Verify data round-trip correctness
        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data, "Empty data should round-trip correctly"


class TestYamlSaver:
    """Test yaml_saver function for YAML file creation and data serialization."""

    @pytest.mark.unit
    def test_yaml_saver_creates_yaml_file(self, tmp_path: Path) -> None:
        """Test yaml_saver creates YAML file with correct content and formatting."""
        # Arrange
        test_data = {"name": "test_dataset", "items": [{"file": "test.jpg", "metadata": {"lat": 37.7749}}]}

        # Act
        yaml_saver(tmp_path, "test_metadata", test_data)

        # Assert
        output_file = tmp_path / "test_metadata.yml"
        assert output_file.exists(), "YAML file should be created"
        assert output_file.stat().st_size > 0, "YAML file should not be empty"

        # Verify file contents can be loaded and match expected data
        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = yaml.safe_load(f)

        assert loaded_data == test_data, "Loaded YAML data should match original test data"

        # Verify YAML formatting (readable structure)
        with output_file.open("r", encoding="utf-8") as f:
            content = f.read()
        assert "name:" in content, "YAML should contain properly formatted keys"
        assert "items:" in content, "YAML should contain list structures"

    @pytest.mark.unit
    def test_yaml_saver_nested_data(self, tmp_path: Path) -> None:
        """Test yaml_saver handles complex nested data structures with proper formatting and data integrity."""
        # Arrange
        test_data = {
            "dataset": {
                "name": "complex_test",
                "nested": {
                    "deep": {
                        "values": [1, 2, 3],
                        "strings": ["a", "b", "c"],
                    },
                },
            },
        }

        # Act
        yaml_saver(tmp_path, "nested_test", test_data)

        # Assert
        output_file = tmp_path / "nested_test.yml"
        assert output_file.exists(), "YAML file should be created"
        assert output_file.stat().st_size > 0, "YAML file should not be empty"

        # Verify file contents can be loaded and match expected data
        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = yaml.safe_load(f)

        assert loaded_data == test_data, "Nested data should round-trip correctly"

        # Verify nested structure preservation
        assert "dataset" in loaded_data, "Top-level key should be preserved"
        assert "nested" in loaded_data["dataset"], "Second-level nesting should be preserved"
        assert "deep" in loaded_data["dataset"]["nested"], "Third-level nesting should be preserved"
        assert loaded_data["dataset"]["nested"]["deep"]["values"] == [1, 2, 3], "Nested list values should be preserved"
        assert loaded_data["dataset"]["nested"]["deep"]["strings"] == [
            "a",
            "b",
            "c",
        ], "Nested string list should be preserved"

        # Verify YAML formatting shows proper nested structure
        with output_file.open("r", encoding="utf-8") as f:
            content = f.read()
        assert "dataset:" in content, "YAML should contain top-level key with colon"
        assert "nested:" in content, "YAML should contain nested structure markers"
        assert "deep:" in content, "YAML should contain deeply nested structure markers"

    @pytest.mark.unit
    def test_yaml_saver_empty_data(self, tmp_path: Path) -> None:
        """Test yaml_saver handles empty dictionary correctly and produces valid YAML content."""
        # Arrange
        test_data: dict[str, Any] = {}

        # Act
        yaml_saver(tmp_path, "empty_test", test_data)

        # Assert
        output_file = tmp_path / "empty_test.yml"
        assert output_file.exists(), "YAML file should be created for empty data"

        # Verify file content format
        with output_file.open("r", encoding="utf-8") as f:
            content = f.read().strip()
        assert content == "{}", "Empty YAML should produce '{}' as output"

        # Verify data round-trip correctness
        with output_file.open("r", encoding="utf-8") as f:
            loaded_data = yaml.safe_load(f)

        assert loaded_data == test_data, "Empty data should round-trip correctly"
        assert loaded_data == {}, "Loaded data should be empty dictionary"


class TestGetSaver:
    """Test get_saver function for returning appropriate saver functions."""

    @pytest.mark.unit
    def test_get_saver_json(self) -> None:
        """Test get_saver returns json_saver function for json type."""
        # Arrange
        expected_saver = json_saver

        # Act
        actual_saver = get_saver(MetadataSaverTypes.json)

        # Assert
        assert actual_saver == expected_saver, "Should return json_saver function"
        assert callable(actual_saver), "Returned saver should be callable"

    @pytest.mark.unit
    def test_get_saver_yaml(self) -> None:
        """Test get_saver returns yaml_saver function for yaml type."""
        # Arrange
        expected_saver = yaml_saver

        # Act
        actual_saver = get_saver(MetadataSaverTypes.yaml)

        # Assert
        assert actual_saver == expected_saver, "Should return yaml_saver function"
        assert callable(actual_saver), "Returned saver should be callable"

    @pytest.mark.unit
    def test_get_saver_unknown_type(self) -> None:
        """Test get_saver raises ValueError with specific message for unknown saver type."""
        # Arrange
        invalid_saver_type = "invalid_type"

        # Act & Assert
        with pytest.raises(ValueError, match=r"^Unknown saver: invalid_type$"):
            get_saver(invalid_saver_type)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_get_saver_functional_usage(self, tmp_path: Path) -> None:
        """Test get_saver returns functional savers that execute correctly with real data serialization."""
        # Arrange
        test_data = {"test": "data", "complex": {"nested": [1, 2, 3]}}

        # Act & Assert - Test JSON saver
        json_saver_func = get_saver(MetadataSaverTypes.json)
        json_saver_func(tmp_path, "functional_test", test_data)

        json_file = tmp_path / "functional_test.json"
        assert json_file.exists(), "JSON file should be created"

        with json_file.open("r", encoding="utf-8") as f:
            loaded_json_data = json.load(f)
        assert loaded_json_data == test_data, "JSON data should round-trip correctly"

        # Act & Assert - Test YAML saver
        yaml_saver_func = get_saver(MetadataSaverTypes.yaml)
        yaml_saver_func(tmp_path, "functional_test_yaml", test_data)

        yaml_file = tmp_path / "functional_test_yaml.yml"
        assert yaml_file.exists(), "YAML file should be created"

        with yaml_file.open("r", encoding="utf-8") as f:
            loaded_yaml_data = yaml.safe_load(f)
        assert loaded_yaml_data == test_data, "YAML data should round-trip correctly"
