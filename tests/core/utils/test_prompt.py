"""
Unit tests for prompt utilities.

Tests the functionality of the prompt_schema function including:
- Prompting for different data types (str, int, float, bool)
- Using default values
- Handling KeyboardInterrupt
- Error handling for unsupported types
"""

from typing import Any

import pytest
from pytest_mock import MockerFixture

from marimba.core.utils.prompt import prompt_schema


@pytest.mark.unit
class TestPromptSchema:
    """Test prompt_schema function."""

    def test_prompt_string_values(self, mocker: MockerFixture) -> None:
        """Test prompting for string values with custom input."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"name": "default_name", "description": "default_desc"}
        mock_ask.side_effect = ["custom_name", "custom_desc"]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {"name": "custom_name", "description": "custom_desc"}, "Should return custom string values"
        assert mock_ask.call_count == 2, "Should call Prompt.ask twice for two string fields"
        # Verify the correct parameters were passed to each call
        mock_ask.assert_any_call("name", default="default_name")
        mock_ask.assert_any_call("description", default="default_desc")

    def test_prompt_integer_values(self, mocker: MockerFixture) -> None:
        """Test prompting for integer values uses IntPrompt.ask and returns custom values."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.IntPrompt.ask")
        schema = {"count": 10, "max_items": 100}
        mock_ask.side_effect = [25, 200]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {"count": 25, "max_items": 200}, "Should return custom integer values provided by user"
        assert mock_ask.call_count == 2, "Should call IntPrompt.ask exactly twice for two integer fields"
        # Verify the correct parameters were passed to each call
        mock_ask.assert_any_call("count", default=10)
        mock_ask.assert_any_call("max_items", default=100)

    def test_prompt_float_values(self, mocker: MockerFixture) -> None:
        """Test prompting for float values uses FloatPrompt.ask and returns custom values."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.FloatPrompt.ask")
        schema = {"threshold": 0.5, "factor": 1.0}
        mock_ask.side_effect = [0.8, 2.5]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {"threshold": 0.8, "factor": 2.5}, "Should return custom float values"
        assert mock_ask.call_count == 2, "Should call FloatPrompt.ask twice for two float fields"
        # Verify the correct parameters were passed to each call
        mock_ask.assert_any_call("threshold", default=0.5)
        mock_ask.assert_any_call("factor", default=1.0)

    def test_prompt_boolean_values(self, mocker: MockerFixture) -> None:
        """Test prompting for boolean values uses Confirm.ask and returns custom values."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Confirm.ask")
        schema = {"enabled": True, "debug": False}
        mock_ask.side_effect = [False, True]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {"enabled": False, "debug": True}, "Should return custom boolean values"
        assert mock_ask.call_count == 2, "Should call Confirm.ask twice for two boolean fields"
        # Verify the correct parameters were passed to each call
        mock_ask.assert_any_call("enabled", default=True)
        mock_ask.assert_any_call("debug", default=False)

    def test_prompt_mixed_types(self, mocker: MockerFixture) -> None:
        """Test prompting for schema with mixed data types."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        mock_float = mocker.patch("rich.prompt.FloatPrompt.ask")
        mock_int = mocker.patch("rich.prompt.IntPrompt.ask")
        mock_confirm = mocker.patch("rich.prompt.Confirm.ask")
        schema = {"name": "test", "count": 5, "ratio": 0.5, "enabled": True}
        mock_prompt.return_value = "new_name"
        mock_int.return_value = 10
        mock_float.return_value = 0.75
        mock_confirm.return_value = False

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {
            "name": "new_name",
            "count": 10,
            "ratio": 0.75,
            "enabled": False,
        }, "Should return mixed type values"
        mock_prompt.assert_called_once_with("name", default="test")
        mock_int.assert_called_once_with("count", default=5)
        mock_float.assert_called_once_with("ratio", default=0.5)
        mock_confirm.assert_called_once_with("enabled", default=True)

    def test_prompt_with_default_values(self, mocker: MockerFixture) -> None:
        """Test prompting when user accepts default values by providing defaults to Rich prompt."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"name": "default_name", "description": "default_desc"}
        # Configure mock to return the default values (simulating user pressing Enter)
        mock_ask.side_effect = ["default_name", "default_desc"]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {
            "name": "default_name",
            "description": "default_desc",
        }, "Should return schema default values when user accepts defaults"
        assert mock_ask.call_count == 2, "Should call Prompt.ask twice for two string fields"
        mock_ask.assert_any_call("name", default="default_name")
        mock_ask.assert_any_call("description", default="default_desc")

    def test_prompt_keyboard_interrupt(self, mocker: MockerFixture) -> None:
        """Test handling KeyboardInterrupt during prompting."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"name": "default"}
        mock_ask.side_effect = KeyboardInterrupt()

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result is None, "Should return None when KeyboardInterrupt is raised"

    def test_prompt_unsupported_type(self) -> None:
        """Test error handling for unsupported data types."""
        # Arrange
        schema = {"data": [1, 2, 3]}  # List is not supported

        # Act & Assert
        with pytest.raises(NotImplementedError, match="Unsupported type: list"):
            prompt_schema(schema)

    def test_prompt_custom_object_type(self) -> None:
        """Test NotImplementedError raised for custom object types."""

        # Arrange
        class CustomType:
            pass

        schema = {"custom": CustomType()}

        # Act & Assert
        with pytest.raises(NotImplementedError, match="Unsupported type: CustomType"):
            prompt_schema(schema)

    def test_empty_schema(self, mocker: MockerFixture) -> None:
        """Test empty schema returns empty dict without calling any prompt functions."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        mock_int = mocker.patch("rich.prompt.IntPrompt.ask")
        mock_float = mocker.patch("rich.prompt.FloatPrompt.ask")
        mock_confirm = mocker.patch("rich.prompt.Confirm.ask")
        schema: dict[str, Any] = {}

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {}, "Should return empty dict for empty schema"
        assert result is not None, "Should not return None (indicating no KeyboardInterrupt)"
        # Verify no prompt functions were called
        mock_prompt.assert_not_called()
        mock_int.assert_not_called()
        mock_float.assert_not_called()
        mock_confirm.assert_not_called()

    def test_schema_original_not_modified(self, mocker: MockerFixture) -> None:
        """Test that original schema is not modified during prompting."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        mock_int = mocker.patch("rich.prompt.IntPrompt.ask")
        original_schema = {"name": "original", "count": 42}
        schema_copy = original_schema.copy()
        mock_prompt.return_value = "modified"
        mock_int.return_value = 100

        # Act
        result = prompt_schema(original_schema)

        # Assert
        assert original_schema == schema_copy, "Original schema should remain unchanged"
        assert result == {"name": "modified", "count": 100}, "Result should have new values"

    def test_keyboard_interrupt_after_partial_input(self, mocker: MockerFixture) -> None:
        """Test KeyboardInterrupt after partial input cancels entire operation and returns None."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Confirm.ask")
        schema = {"enabled": True, "debug": False}
        mock_ask.side_effect = [False, KeyboardInterrupt()]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result is None, "Should return None when KeyboardInterrupt is raised after partial input"
        assert mock_ask.call_count == 2, "Should have attempted to prompt for both fields before interruption"
        # Verify the exact calls that were made before interruption
        mock_ask.assert_any_call("enabled", default=True)
        mock_ask.assert_any_call("debug", default=False)

    def test_prompt_with_none_response(self, mocker: MockerFixture) -> None:
        """Test that None values from prompts preserve the original default values."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"name": "default_name", "description": "default_desc"}
        # Simulate prompt returning None (which shouldn't normally happen with rich.prompt but testing the logic)
        mock_ask.side_effect = [None, "custom_desc"]

        # Act
        result = prompt_schema(schema)

        # Assert
        assert result == {
            "name": "default_name",  # Should preserve default when prompt returns None
            "description": "custom_desc",  # Should use custom value when not None
        }, "Should preserve default values when prompt returns None"
        assert mock_ask.call_count == 2, "Should call Prompt.ask twice"
        mock_ask.assert_any_call("name", default="default_name")
        mock_ask.assert_any_call("description", default="default_desc")

    def test_accept_defaults_returns_schema_copy(self, mocker: MockerFixture) -> None:
        """Test that accept_defaults=True returns schema defaults without prompting."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        mock_int = mocker.patch("rich.prompt.IntPrompt.ask")
        mock_float = mocker.patch("rich.prompt.FloatPrompt.ask")
        mock_confirm = mocker.patch("rich.prompt.Confirm.ask")
        schema = {"name": "default_name", "count": 42, "ratio": 0.5, "enabled": True}

        # Act
        result = prompt_schema(schema, accept_defaults=True)

        # Assert
        assert result == schema, "Should return schema unchanged when accept_defaults=True"
        assert result is not schema, "Should return a copy, not the original schema object"
        # Verify no prompts were called
        mock_prompt.assert_not_called()
        mock_int.assert_not_called()
        mock_float.assert_not_called()
        mock_confirm.assert_not_called()

    def test_accept_defaults_with_empty_schema(self, mocker: MockerFixture) -> None:
        """Test that accept_defaults=True with empty schema returns empty dict without prompting."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        schema: dict[str, Any] = {}

        # Act
        result = prompt_schema(schema, accept_defaults=True)

        # Assert
        assert result == {}, "Should return empty dict for empty schema with accept_defaults=True"
        assert result is not None, "Should not return None"
        mock_prompt.assert_not_called()

    def test_accept_defaults_string_values(self, mocker: MockerFixture) -> None:
        """Test accept_defaults=True with string values returns defaults without calling Prompt.ask."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"voyage_id": "IN2018_V06", "voyage_pi": "Alan Williams", "platform_id": "MRITC"}

        # Act
        result = prompt_schema(schema, accept_defaults=True)

        # Assert
        assert result == {
            "voyage_id": "IN2018_V06",
            "voyage_pi": "Alan Williams",
            "platform_id": "MRITC",
        }, "Should return all default string values"
        mock_ask.assert_not_called()

    def test_accept_defaults_mixed_types(self, mocker: MockerFixture) -> None:
        """Test accept_defaults=True with mixed types returns all defaults without prompting."""
        # Arrange
        mock_prompt = mocker.patch("rich.prompt.Prompt.ask")
        mock_int = mocker.patch("rich.prompt.IntPrompt.ask")
        mock_float = mocker.patch("rich.prompt.FloatPrompt.ask")
        mock_confirm = mocker.patch("rich.prompt.Confirm.ask")
        schema = {
            "name": "test_pipeline",
            "max_workers": 4,
            "threshold": 0.75,
            "enabled": True,
            "debug": False,
        }

        # Act
        result = prompt_schema(schema, accept_defaults=True)

        # Assert
        assert result == schema, "Should return all default values for mixed types"
        # Verify no prompt functions were called
        mock_prompt.assert_not_called()
        mock_int.assert_not_called()
        mock_float.assert_not_called()
        mock_confirm.assert_not_called()

    def test_accept_defaults_false_prompts_normally(self, mocker: MockerFixture) -> None:
        """Test that accept_defaults=False still prompts for values (normal behavior)."""
        # Arrange
        mock_ask = mocker.patch("rich.prompt.Prompt.ask")
        schema = {"name": "default_name"}
        mock_ask.return_value = "custom_name"

        # Act
        result = prompt_schema(schema, accept_defaults=False)

        # Assert
        assert result == {"name": "custom_name"}, "Should prompt and return custom value when accept_defaults=False"
        mock_ask.assert_called_once_with("name", default="default_name")
