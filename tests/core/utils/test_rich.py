"""Tests for marimba.core.utils.rich module."""

from collections.abc import Callable

import pytest
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    ProgressColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from marimba.core.utils.rich import (
    MARIMBA,
    error_panel,
    format_command,
    format_entity,
    get_default_columns,
    success_panel,
    warning_panel,
)


class TestRichUtils:
    """Test rich utility functions."""

    @pytest.mark.unit
    def test_success_panel_basic_functionality(self) -> None:
        """Test success_panel creates proper panel with correct properties for basic messages.

        This unit test verifies that the success_panel function correctly creates a Panel
        with the expected success-specific styling (green border) and proper content rendering.
        This ensures the panel displays success messages with appropriate visual distinction
        to communicate positive outcomes to users.
        """
        # Arrange
        message = "Test message"

        # Act
        result = success_panel(message)

        # Assert
        assert isinstance(result, Panel), "success_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == "Success", "Panel title should default to 'Success'"
        assert result.border_style == "green", "Panel border_style should be 'green' for success"
        assert result.title_align == "left", "Panel title_align should be 'left'"

    @pytest.mark.unit
    def test_success_panel_custom_title(self) -> None:
        """Test success_panel with custom title maintains all properties correctly.

        This unit test verifies that the success_panel function correctly accepts
        and uses a custom title parameter while preserving all other panel properties
        including green border styling and proper content rendering. This ensures
        the panel customization works as expected while maintaining visual consistency
        for success communication.
        """
        # Arrange
        message = "Test message"
        custom_title = "Custom Success"

        # Act
        result = success_panel(message, title=custom_title)

        # Assert
        assert isinstance(result, Panel), "success_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == custom_title, f"Panel title should use custom title '{custom_title}'"
        assert result.border_style == "green", "Panel border_style should remain 'green' with custom title"
        assert result.title_align == "left", "Panel title_align should remain 'left' with custom title"

    @pytest.mark.unit
    def test_warning_panel_basic_functionality(self) -> None:
        """Test warning_panel creates proper panel with correct properties for basic messages.

        This unit test verifies that the warning_panel function correctly creates a Panel
        with the expected warning-specific styling (yellow border) and proper content rendering.
        This ensures the panel displays warning messages with appropriate visual distinction.
        """
        # Arrange
        message = "Warning message"

        # Act
        result = warning_panel(message)

        # Assert
        assert isinstance(result, Panel), "warning_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == "Warning", "Panel title should default to 'Warning'"
        assert result.border_style == "yellow", "Panel border_style should be 'yellow' for warning"
        assert result.title_align == "left", "Panel title_align should be 'left'"

    @pytest.mark.unit
    def test_warning_panel_custom_title(self) -> None:
        """Test warning_panel with custom title maintains all properties correctly.

        This unit test verifies that the warning_panel function correctly accepts
        and uses a custom title parameter while preserving all other panel properties
        including yellow border styling and proper content rendering. This ensures
        the panel customization works as expected while maintaining visual consistency.
        """
        # Arrange
        message = "Warning message"
        custom_title = "Custom Warning"

        # Act
        result = warning_panel(message, title=custom_title)

        # Assert
        assert isinstance(result, Panel), "warning_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == custom_title, f"Panel title should use custom title '{custom_title}'"
        assert result.border_style == "yellow", "Panel border_style should remain 'yellow' with custom title"
        assert result.title_align == "left", "Panel title_align should remain 'left' with custom title"

    @pytest.mark.unit
    def test_error_panel_basic_functionality(self) -> None:
        """Test error_panel creates proper panel with correct properties for basic messages.

        This unit test verifies that the error_panel function correctly creates a Panel
        with the expected error-specific styling (red border) and proper content rendering.
        This ensures the panel displays error messages with appropriate visual distinction
        to alert users of critical issues.
        """
        # Arrange
        message = "Error message"

        # Act
        result = error_panel(message)

        # Assert
        assert isinstance(result, Panel), "error_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == "Error", "Panel title should default to 'Error'"
        assert result.border_style == "red", "Panel border_style should be 'red' for error"
        assert result.title_align == "left", "Panel title_align should be 'left'"

    @pytest.mark.unit
    def test_error_panel_custom_title(self) -> None:
        """Test error_panel with custom title maintains all properties correctly.

        This unit test verifies that the error_panel function correctly accepts
        and uses a custom title parameter while preserving all other panel properties
        including red border styling and proper content rendering. This ensures
        the panel customization works as expected while maintaining visual consistency
        for error communication.
        """
        # Arrange
        message = "Error message"
        custom_title = "Custom Error"

        # Act
        result = error_panel(message, title=custom_title)

        # Assert
        assert isinstance(result, Panel), "error_panel should return a Panel instance"
        assert str(result.renderable) == message, f"Panel renderable should contain exact message '{message}'"
        assert result.title == custom_title, f"Panel title should use custom title '{custom_title}'"
        assert result.border_style == "red", "Panel border_style should remain 'red' with custom title"
        assert result.title_align == "left", "Panel title_align should remain 'left' with custom title"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("panel_func", "expected_border", "expected_title"),
        [
            (success_panel, "green", "Success"),
            (warning_panel, "yellow", "Warning"),
            (error_panel, "red", "Error"),
        ],
    )
    def test_panel_functions_with_multiline_message(
        self,
        panel_func: Callable[..., Panel],
        expected_border: str,
        expected_title: str,
    ) -> None:
        """Test panel functions handle multiline messages correctly with proper panel properties."""
        # Arrange
        multiline_message = "Line 1\nLine 2\nLine 3"

        # Act
        result = panel_func(multiline_message)

        # Assert
        assert isinstance(result, Panel), f"{expected_title.lower()}_panel should return Panel instance"
        assert (
            str(result.renderable) == multiline_message
        ), f"{expected_title} panel should preserve multiline message exactly as provided"
        assert (
            result.border_style == expected_border
        ), f"{expected_title} panel should maintain '{expected_border}' border_style with multiline content"
        assert (
            result.title == expected_title
        ), f"{expected_title} panel should maintain '{expected_title}' title with multiline content"
        assert (
            result.title_align == "left"
        ), f"{expected_title} panel title_align should remain 'left' with multiline content"

    @pytest.mark.unit
    def test_get_default_columns_returns_correct_count_and_type(self) -> None:
        """Test get_default_columns returns tuple with exactly 5 ProgressColumn instances.

        This unit test verifies that the function returns the correct data structure
        (tuple) containing exactly 5 columns, all of which are ProgressColumn instances.
        This ensures the basic contract of the function is maintained for progress bar setup.
        """
        # Act
        columns = get_default_columns()

        # Assert
        assert isinstance(columns, tuple), "get_default_columns should return a tuple"
        assert len(columns) == 5, f"get_default_columns should return exactly 5 columns, got {len(columns)}"

        # Verify all columns are ProgressColumn instances
        for i, column in enumerate(columns):
            assert isinstance(
                column,
                ProgressColumn,
            ), f"Column at index {i} should be a ProgressColumn instance, got {type(column).__name__}"

    @pytest.mark.unit
    def test_get_default_columns_returns_expected_column_types(self) -> None:
        """Test get_default_columns returns columns in correct order with expected types.

        This unit test verifies that the function returns the specific column types
        in the expected order for progress bar functionality and ensures consistent
        results across multiple calls.
        """
        # Arrange
        expected_types = [TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, TimeElapsedColumn]

        # Act
        columns = get_default_columns()

        # Assert
        assert len(columns) == len(
            expected_types,
        ), f"Column count mismatch: expected {len(expected_types)}, got {len(columns)}"

        # Verify each column type in order
        for i, (column, expected_type) in enumerate(zip(columns, expected_types, strict=True)):
            assert isinstance(
                column,
                expected_type,
            ), f"Column {i} should be {expected_type.__name__}, got {type(column).__name__}"

        # Verify consistency across multiple calls
        columns_second_call = get_default_columns()
        assert len(columns_second_call) == len(columns), "Function should return consistent column count across calls"
        for i, (col1, col2) in enumerate(zip(columns, columns_second_call, strict=True)):
            assert isinstance(col1, type(col2)), f"Column {i} type should be consistent across calls"

    @pytest.mark.unit
    def test_get_default_columns_text_column_configuration(self) -> None:
        """Test get_default_columns TextColumn has correct configuration.

        This unit test verifies the specific configuration of the TextColumn
        including text format and justification settings.
        """
        # Act
        columns = get_default_columns()
        text_column = columns[0]

        # Assert
        assert isinstance(text_column, TextColumn), "First column from get_default_columns should be TextColumn"
        assert (
            text_column.text_format == "[bold]{task.description}"
        ), "TextColumn text_format should be '[bold]{task.description}'"
        assert text_column.justify == "left", "TextColumn justify should be 'left'"

    @pytest.mark.unit
    def test_get_default_columns_bar_column_configuration(self) -> None:
        """Test get_default_columns BarColumn has correct configuration.

        This unit test verifies the specific configuration of the BarColumn
        including flexible width settings.
        """
        # Act
        columns = get_default_columns()
        bar_column = columns[1]

        # Assert
        assert isinstance(bar_column, BarColumn), "Second column from get_default_columns should be BarColumn"
        assert bar_column.bar_width is None, "BarColumn bar_width should be None for flexible width"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("panel_func", "expected_border", "expected_title"),
        [
            (success_panel, "green", "Success"),
            (warning_panel, "yellow", "Warning"),
            (error_panel, "red", "Error"),
        ],
    )
    def test_panel_functions_with_empty_message(
        self,
        panel_func: Callable[..., Panel],
        expected_border: str,
        expected_title: str,
    ) -> None:
        """Test all panel functions handle empty messages correctly with proper panel properties."""
        # Arrange
        empty_message = ""

        # Act
        result = panel_func(empty_message)

        # Assert
        assert isinstance(result, Panel), f"{expected_title.lower()}_panel should return Panel instance"
        assert (
            str(result.renderable) == empty_message
        ), f"{expected_title} panel should handle empty message by setting renderable to empty string"
        assert (
            result.border_style == expected_border
        ), f"{expected_title} panel should maintain '{expected_border}' border_style with empty content"
        assert (
            result.title == expected_title
        ), f"{expected_title} panel should maintain '{expected_title}' title with empty content"
        assert (
            result.title_align == "left"
        ), f"{expected_title} panel title_align should remain 'left' with empty content"

    @pytest.mark.unit
    def test_marimba_constant_format(self) -> None:
        """Test MARIMBA constant is properly formatted with bold aquamarine styling.

        This unit test verifies that the MARIMBA constant string contains the expected
        Rich markup tags for bold aquamarine styling, ensuring consistent branding
        across CLI output throughout the application.
        """
        # Arrange
        expected = "[bold][aquamarine3]Marimba[/aquamarine3][/bold]"

        # Act
        result = MARIMBA

        # Assert
        assert result == expected, f"MARIMBA constant should match expected format exactly, got: '{result}'"
        assert isinstance(result, str), "MARIMBA constant should be a string type"
        assert "[bold]" in result, "MARIMBA constant should contain opening '[bold]' tag"
        assert "[/bold]" in result, "MARIMBA constant should contain closing '[/bold]' tag"
        assert "[aquamarine3]" in result, "MARIMBA constant should contain opening '[aquamarine3]' tag"
        assert "[/aquamarine3]" in result, "MARIMBA constant should contain closing '[/aquamarine3]' tag"
        assert "Marimba" in result, "MARIMBA constant should contain the literal text 'Marimba'"

    @pytest.mark.unit
    def test_format_command_basic_functionality(self) -> None:
        """Test format_command creates properly formatted string with steel_blue3 styling.

        This unit test verifies that the format_command function correctly wraps
        a simple command name with the expected steel_blue3 styling tags,
        ensuring consistent command formatting across CLI output.
        """
        # Arrange
        command_name = "test-command"
        expected_result = "[steel_blue3]test-command[/steel_blue3]"

        # Act
        result = format_command(command_name)

        # Assert
        assert (
            result == expected_result
        ), f"format_command should return exact expected format, expected '{expected_result}', got '{result}'"
        assert isinstance(result, str), "format_command should return a string type"
        assert result.startswith("[steel_blue3]"), "format_command result should start with opening '[steel_blue3]' tag"
        assert result.endswith("[/steel_blue3]"), "format_command result should end with closing '[/steel_blue3]' tag"
        assert command_name in result, f"format_command result should contain original command name '{command_name}'"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("input_text", "expected_output"),
        [
            ("simple", "[steel_blue3]simple[/steel_blue3]"),
            ("multi-word-command", "[steel_blue3]multi-word-command[/steel_blue3]"),
            ("", "[steel_blue3][/steel_blue3]"),
            ("special@chars#test", "[steel_blue3]special@chars#test[/steel_blue3]"),
        ],
    )
    def test_format_command_with_various_inputs(self, input_text: str, expected_output: str) -> None:
        """Test format_command with various input types including edge cases.

        This unit test verifies that format_command correctly wraps any string input
        with steel_blue3 styling tags, regardless of content including special characters,
        empty strings, and multi-word commands. This ensures consistent command formatting
        across all CLI output scenarios.
        """
        # Act
        result = format_command(input_text)

        # Assert
        assert result == expected_output, f"format_command should format '{input_text}' correctly, got: {result}"
        assert isinstance(result, str), "format_command should return a string type"
        assert result.startswith("[steel_blue3]"), "format_command result should start with opening '[steel_blue3]' tag"
        assert result.endswith("[/steel_blue3]"), "format_command result should end with closing '[/steel_blue3]' tag"
        if input_text:  # Only check content for non-empty strings
            assert input_text in result, f"format_command result should contain original input text '{input_text}'"

    @pytest.mark.unit
    def test_format_entity_basic_functionality(self) -> None:
        """Test format_entity creates properly formatted string with light_pink3 styling.

        This unit test verifies that the format_entity function correctly wraps
        a simple entity name with the expected light_pink3 styling tags,
        ensuring consistent entity formatting across CLI output.
        """
        # Arrange
        entity_name = "test-entity"

        # Act
        result = format_entity(entity_name)

        # Assert
        assert (
            result == "[light_pink3]test-entity[/light_pink3]"
        ), f"format_entity should return exact expected format, got: {result}"
        assert isinstance(result, str), "format_entity should return a string type"
        assert result.startswith("[light_pink3]"), "format_entity result should start with opening '[light_pink3]' tag"
        assert result.endswith("[/light_pink3]"), "format_entity result should end with closing '[/light_pink3]' tag"
        assert "test-entity" in result, "format_entity result should contain original entity name 'test-entity'"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("input_text", "expected_output"),
        [
            ("entity", "[light_pink3]entity[/light_pink3]"),
            ("project-name", "[light_pink3]project-name[/light_pink3]"),
            ("", "[light_pink3][/light_pink3]"),
            ("entity.with.dots", "[light_pink3]entity.with.dots[/light_pink3]"),
        ],
    )
    def test_format_entity_with_various_inputs(self, input_text: str, expected_output: str) -> None:
        """Test format_entity with various input types including edge cases.

        This unit test verifies that format_entity correctly wraps any string input
        with light_pink3 styling tags, regardless of content including special characters,
        empty strings, and entity names with dots or hyphens. This ensures consistent
        entity formatting across all CLI output scenarios.
        """
        # Arrange
        # input_text and expected_output provided by parametrize

        # Act
        result = format_entity(input_text)

        # Assert
        assert result == expected_output, f"format_entity should format '{input_text}' correctly, got: {result}"
        assert isinstance(result, str), "format_entity should return a string type"
        assert result.startswith("[light_pink3]"), "format_entity result should start with opening '[light_pink3]' tag"
        assert result.endswith("[/light_pink3]"), "format_entity result should end with closing '[/light_pink3]' tag"
        if input_text:  # Only check content for non-empty strings
            assert input_text in result, f"format_entity result should contain original input text '{input_text}'"
