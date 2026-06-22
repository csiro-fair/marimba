"""Tests for marimba.core.utils.dependencies module."""

import pytest
import typer
from pytest_mock import MockerFixture
from rich.panel import Panel

from marimba.core.utils.dependencies import (
    Platform,
    ToolDependency,
    ToolInfo,
    check_dependency_available,
    get_current_platform,
    get_tool_info,
    show_dependency_error,
    show_dependency_error_and_exit,
    validate_dependencies,
)


class TestPlatform:
    """Test Platform enum."""

    @pytest.mark.unit
    def test_platform_enum_values(self) -> None:
        """Test Platform enum has expected string values for all supported platforms.

        This ensures that the Platform enum correctly maps to the expected string values
        that are used throughout the dependency management system for platform detection
        and platform-specific installation instructions.
        """
        # Arrange - Platform enum is imported and available

        # Act & Assert - Verify each platform enum has the correct string value
        assert Platform.WINDOWS.value == "windows", "WINDOWS platform should have value 'windows'"
        assert Platform.MACOS.value == "macos", "MACOS platform should have value 'macos'"
        assert Platform.LINUX.value == "linux", "LINUX platform should have value 'linux'"

        # Assert - Verify enum has exactly the expected number of values
        platform_values = list(Platform)
        assert len(platform_values) == 3, "Platform enum should have exactly 3 values"

        # Assert - Verify all enum values are strings
        for platform in Platform:
            assert isinstance(platform.value, str), f"Platform {platform.name} value should be a string"


class TestToolDependency:
    """Test ToolDependency enum."""

    @pytest.mark.unit
    def test_tool_dependency_enum_values(self) -> None:
        """Test ToolDependency enum has expected string values for all supported external tools.

        This ensures that the ToolDependency enum correctly maps to the expected string values
        that are used throughout the dependency management system for tool identification,
        availability checking, and error message display.
        """
        # Arrange - Define expected enum values based on source code
        expected_values = {
            "EXIFTOOL": "exiftool",
            "FFMPEG": "ffmpeg",
            "FFPROBE": "ffprobe",
        }

        # Act & Assert - Verify each tool dependency enum has the correct string value
        assert (
            ToolDependency.EXIFTOOL.value == expected_values["EXIFTOOL"]
        ), "EXIFTOOL tool should have value 'exiftool'"
        assert ToolDependency.FFMPEG.value == expected_values["FFMPEG"], "FFMPEG tool should have value 'ffmpeg'"
        assert ToolDependency.FFPROBE.value == expected_values["FFPROBE"], "FFPROBE tool should have value 'ffprobe'"

        # Assert - Verify enum integrity and expected mapping
        actual_values = {tool.name: tool.value for tool in ToolDependency}
        assert actual_values == expected_values, f"ToolDependency enum should match expected mapping: {expected_values}"

        # Assert - Verify all enum values are strings and unique
        enum_values = [tool.value for tool in ToolDependency]
        assert all(isinstance(value, str) for value in enum_values), "All ToolDependency values should be strings"
        assert len(enum_values) == len(set(enum_values)), "ToolDependency enum should have no duplicate values"


class TestToolInfo:
    """Test ToolInfo dataclass."""

    @pytest.fixture
    def sample_tool_info(self) -> ToolInfo:
        """Create sample ToolInfo for testing."""
        return ToolInfo(
            name="testtool",
            description="Test description",
            homepage="https://example.com",
            windows_instructions=["Windows install step 1", "Windows install step 2"],
            macos_instructions=["macOS install step 1"],
            linux_instructions=["Linux install step 1", "Linux install step 2", "Linux install step 3"],
        )

    @pytest.mark.unit
    def test_tool_info_initialization(self, sample_tool_info: ToolInfo) -> None:
        """Test ToolInfo initialization sets all fields correctly and validates data integrity.

        This test verifies that the ToolInfo dataclass properly initializes all required fields
        with the expected values from the fixture data. It ensures that basic field assignment
        works correctly and that the instruction lists maintain their expected structure.
        The test focuses on data integrity rather than business logic functionality.
        """
        # Arrange - sample_tool_info fixture provides the test data

        # Act - No explicit action needed, testing initialization from fixture

        # Assert - Verify all core fields are properly initialized
        assert sample_tool_info.name == "testtool", "Tool name should be initialized correctly"
        assert sample_tool_info.description == "Test description", "Tool description should be initialized correctly"
        assert sample_tool_info.homepage == "https://example.com", "Tool homepage should be initialized correctly"

        # Assert - Verify instruction lists maintain expected structure and content
        assert len(sample_tool_info.windows_instructions) == 2, "Should have 2 Windows instructions"
        assert len(sample_tool_info.macos_instructions) == 1, "Should have 1 macOS instruction"
        assert len(sample_tool_info.linux_instructions) == 3, "Should have 3 Linux instructions"

        # Assert - Verify instruction lists contain expected values from fixture
        expected_windows = ["Windows install step 1", "Windows install step 2"]
        expected_macos = ["macOS install step 1"]
        expected_linux = ["Linux install step 1", "Linux install step 2", "Linux install step 3"]

        assert (
            sample_tool_info.windows_instructions == expected_windows
        ), "Windows instructions should match fixture data"
        assert sample_tool_info.macos_instructions == expected_macos, "macOS instructions should match fixture data"
        assert sample_tool_info.linux_instructions == expected_linux, "Linux instructions should match fixture data"

        # Assert - Verify all fields contain non-empty, meaningful data
        assert sample_tool_info.name.strip(), "Tool name should be non-empty"
        assert sample_tool_info.description.strip(), "Tool description should be non-empty"
        assert sample_tool_info.homepage.startswith("https://"), "Homepage should be a valid HTTPS URL"
        assert all(
            instr.strip() for instr in sample_tool_info.windows_instructions
        ), "All Windows instructions should be non-empty"
        assert all(
            instr.strip() for instr in sample_tool_info.macos_instructions
        ), "All macOS instructions should be non-empty"
        assert all(
            instr.strip() for instr in sample_tool_info.linux_instructions
        ), "All Linux instructions should be non-empty"

    @pytest.mark.unit
    def test_get_platform_instructions_windows(self, sample_tool_info: ToolInfo) -> None:
        """Test get_platform_instructions returns correct Windows instructions.

        This test verifies that the get_platform_instructions method correctly
        returns the Windows-specific installation instructions when called with
        Platform.WINDOWS enum value.
        """
        # Arrange - use fixture provided sample_tool_info

        # Act
        instructions = sample_tool_info.get_platform_instructions(Platform.WINDOWS)

        # Assert
        expected_instructions = [
            "Windows install step 1",
            "Windows install step 2",
        ]
        assert instructions == expected_instructions, "Should return exact Windows instructions from fixture"

    @pytest.mark.unit
    def test_get_platform_instructions_macos(self, sample_tool_info: ToolInfo) -> None:
        """Test get_platform_instructions returns correct macOS instructions.

        This test verifies that the get_platform_instructions method correctly
        returns the macOS-specific installation instructions when called with
        Platform.MACOS enum value.
        """
        # Arrange - use fixture provided sample_tool_info

        # Act
        instructions = sample_tool_info.get_platform_instructions(Platform.MACOS)

        # Assert
        expected_instructions = ["macOS install step 1"]
        assert instructions == expected_instructions, "Should return exact macOS instructions from fixture"

    @pytest.mark.unit
    def test_get_platform_instructions_linux(self, sample_tool_info: ToolInfo) -> None:
        """Test get_platform_instructions returns correct Linux instructions.

        This test verifies that the get_platform_instructions method correctly
        returns the Linux-specific installation instructions when called with
        Platform.LINUX enum value.
        """
        # Arrange - use fixture provided sample_tool_info

        # Act
        instructions = sample_tool_info.get_platform_instructions(Platform.LINUX)

        # Assert
        expected_instructions = [
            "Linux install step 1",
            "Linux install step 2",
            "Linux install step 3",
        ]
        assert instructions == expected_instructions, "Should return exact Linux instructions from fixture"


class TestGetToolInfo:
    """Test get_tool_info function."""

    @pytest.mark.unit
    def test_get_tool_info_exiftool(self) -> None:
        """Test get_tool_info for exiftool returns correct metadata and meaningful instructions.

        This test verifies that get_tool_info returns complete ToolInfo with proper metadata
        and non-empty installation instructions for all platforms, ensuring users receive
        actionable guidance when ExifTool is missing from their system.
        """
        # Arrange & Act
        tool_info = get_tool_info(ToolDependency.EXIFTOOL)

        # Assert - Verify core metadata
        assert tool_info.name == "exiftool", "Tool name should match the enum value"
        assert "ExifTool" in tool_info.description, "Description should mention ExifTool"
        assert "EXIF metadata" in tool_info.description, "Description should mention EXIF metadata purpose"
        assert tool_info.homepage == "https://exiftool.org/", "Homepage URL should be correct"

        # Assert - Verify all platforms have meaningful instructions
        assert len(tool_info.windows_instructions) > 0, "Should have Windows installation instructions"
        assert len(tool_info.macos_instructions) > 0, "Should have macOS installation instructions"
        assert len(tool_info.linux_instructions) > 0, "Should have Linux installation instructions"

        # Assert - Verify instruction content is meaningful and non-empty
        for instruction in tool_info.windows_instructions:
            assert instruction.strip(), "Windows instructions should be non-empty"
            assert len(instruction) > 10, "Windows instructions should be meaningful"

        for instruction in tool_info.macos_instructions:
            assert instruction.strip(), "macOS instructions should be non-empty"
            assert len(instruction) > 10, "macOS instructions should be meaningful"

        for instruction in tool_info.linux_instructions:
            assert instruction.strip(), "Linux instructions should be non-empty"
            assert len(instruction) > 10, "Linux instructions should be meaningful"

        # Assert - Verify platform-specific content is appropriate
        macos_has_homebrew = any("brew install" in instr for instr in tool_info.macos_instructions)
        assert macos_has_homebrew, "macOS should include Homebrew installation"

        linux_has_apt = any("apt-get install" in instr for instr in tool_info.linux_instructions)
        assert linux_has_apt, "Linux should include apt-get installation"

        windows_has_website = any("exiftool.org" in instr for instr in tool_info.windows_instructions)
        assert windows_has_website, "Windows should reference official website"

    @pytest.mark.unit
    def test_get_tool_info_ffmpeg(self) -> None:
        """Test get_tool_info for ffmpeg returns correct metadata and meaningful instructions.

        This test verifies that get_tool_info returns complete ToolInfo with proper metadata
        and non-empty installation instructions for all platforms, ensuring users receive
        actionable guidance when FFmpeg is missing from their system.
        """
        # Arrange & Act
        tool_info = get_tool_info(ToolDependency.FFMPEG)

        # Assert - Verify core metadata
        assert tool_info.name == "ffmpeg", "Tool name should match the enum value"
        assert "FFmpeg" in tool_info.description, "Description should mention FFmpeg"
        assert "video processing" in tool_info.description, "Description should mention video processing purpose"
        assert tool_info.homepage == "https://ffmpeg.org/", "Homepage URL should be correct"

        # Assert - Verify all platforms have meaningful instructions
        assert len(tool_info.windows_instructions) > 0, "Should have Windows installation instructions"
        assert len(tool_info.macos_instructions) > 0, "Should have macOS installation instructions"
        assert len(tool_info.linux_instructions) > 0, "Should have Linux installation instructions"

        # Assert - Verify instruction content is meaningful and non-empty
        for instruction in tool_info.windows_instructions:
            assert instruction.strip(), "Windows instructions should be non-empty"
            assert len(instruction) > 10, "Windows instructions should be meaningful"

        for instruction in tool_info.macos_instructions:
            assert instruction.strip(), "macOS instructions should be non-empty"
            assert len(instruction) > 10, "macOS instructions should be meaningful"

        for instruction in tool_info.linux_instructions:
            assert instruction.strip(), "Linux instructions should be non-empty"
            assert len(instruction) > 10, "Linux instructions should be meaningful"

        # Assert - Verify platform-specific content is appropriate
        macos_has_homebrew = any("brew install" in instr for instr in tool_info.macos_instructions)
        assert macos_has_homebrew, "macOS should include Homebrew installation"

        linux_has_apt = any("apt-get install" in instr for instr in tool_info.linux_instructions)
        assert linux_has_apt, "Linux should include apt-get installation"

        windows_has_website = any("ffmpeg.org" in instr for instr in tool_info.windows_instructions)
        assert windows_has_website, "Windows should reference official website"

    @pytest.mark.unit
    def test_get_tool_info_unsupported_tool_raises_keyerror(self) -> None:
        """Test get_tool_info raises KeyError when requesting info for tool not in mapping.

        This test verifies that get_tool_info properly raises a KeyError when attempting
        to retrieve information for a tool that exists in the ToolDependency enum but
        is not included in the internal tool_info mapping dictionary. This ensures
        proper error handling for tools that are defined but don't have installation
        instructions configured, preventing silent failures or unexpected behavior.
        """
        # Arrange - FFPROBE exists in ToolDependency enum but is not in the get_tool_info mapping
        unsupported_tool = ToolDependency.FFPROBE

        # Act & Assert - Verify KeyError is raised with the enum object as the key
        with pytest.raises(KeyError) as exc_info:
            get_tool_info(unsupported_tool)

        # Assert - Verify the specific enum object is in the exception
        assert (
            exc_info.value.args[0] == ToolDependency.FFPROBE
        ), "KeyError should contain the ToolDependency.FFPROBE enum object"


class TestGetCurrentPlatform:
    """Test get_current_platform function."""

    @pytest.mark.unit
    def test_get_current_platform_darwin(self, mocker: MockerFixture) -> None:
        """Test get_current_platform returns macOS platform when platform.system() returns 'Darwin'.

        This test verifies that the get_current_platform function correctly maps Darwin
        (the underlying system name for macOS) to the Platform.MACOS enum value, ensuring
        proper platform detection on Apple systems.
        """
        # Arrange
        mock_system = mocker.patch("platform.system")
        mock_system.return_value = "Darwin"

        # Act
        result = get_current_platform()

        # Assert
        assert result == Platform.MACOS, "Should return MACOS platform for Darwin system"
        mock_system.assert_called_once_with()

    @pytest.mark.unit
    def test_get_current_platform_windows(self, mocker: MockerFixture) -> None:
        """Test get_current_platform returns Windows platform when platform.system() returns 'Windows'.

        This test verifies that the get_current_platform function correctly maps the 'Windows'
        system name to Platform.WINDOWS enum value, ensuring proper platform detection on Windows
        systems. Platform detection is critical for dependency management as it determines
        which platform-specific installation instructions are displayed to users when tools
        are missing. The function also performs case-insensitive conversion of the system name.
        """
        # Arrange
        mock_system = mocker.patch("platform.system")
        mock_system.return_value = "Windows"

        # Act
        result = get_current_platform()

        # Assert
        assert result == Platform.WINDOWS, "Should return WINDOWS platform for Windows system"
        assert result.value == "windows", "Platform.WINDOWS should have lowercase 'windows' value"
        mock_system.assert_called_once_with()

    @pytest.mark.unit
    def test_get_current_platform_linux(self, mocker: MockerFixture) -> None:
        """Test get_current_platform returns Linux platform when platform.system() returns 'Linux'.

        This test verifies that the get_current_platform function correctly maps the 'Linux'
        system name to Platform.LINUX enum value, ensuring proper platform detection on Linux
        systems. Platform detection is critical for dependency management as it determines
        which platform-specific installation instructions are displayed to users when tools
        are missing. The function also performs case-insensitive conversion of the system name.
        """
        # Arrange
        mock_system = mocker.patch("platform.system")
        mock_system.return_value = "Linux"

        # Act
        result = get_current_platform()

        # Assert
        assert result == Platform.LINUX, "Should return LINUX platform for Linux system"
        assert result.value == "linux", "Platform.LINUX should have lowercase 'linux' value"
        mock_system.assert_called_once_with()

    @pytest.mark.unit
    def test_get_current_platform_unknown_defaults_to_linux(self, mocker: MockerFixture) -> None:
        """Test get_current_platform defaults to Linux for unknown systems."""
        # Arrange
        mock_system = mocker.patch("platform.system")
        mock_system.return_value = "UnknownOS"

        # Act
        result = get_current_platform()

        # Assert
        assert result == Platform.LINUX, "Should default to LINUX platform for unknown systems"
        mock_system.assert_called_once()


class TestShowDependencyError:
    """Test show_dependency_error function."""

    @pytest.mark.unit
    def test_show_dependency_error_without_error_message_displays_exiftool_panel(self, mocker: MockerFixture) -> None:
        """Test show_dependency_error without error message displays formatted panel with ExifTool info.

        This unit test verifies that show_dependency_error properly formats and displays an error panel
        containing tool description, platform-specific installation instructions, and homepage link
        when called without a custom error message. It mocks only external platform detection to control
        test conditions while testing real Rich panel creation and content formatting.
        """
        # Arrange - Mock only external platform detection to control which instructions are shown
        mock_console = mocker.Mock()
        mock_console_class = mocker.patch("marimba.core.utils.dependencies.Console")
        mock_console_class.return_value = mock_console

        mock_platform_system = mocker.patch("platform.system")
        mock_platform_system.return_value = "Linux"  # Maps to Platform.LINUX for predictable output

        # Act
        show_dependency_error(ToolDependency.EXIFTOOL)

        # Assert - Verify console output structure (empty line, panel, empty line)
        assert mock_console.print.call_count == 3, "Console should print exactly 3 times: empty line, panel, empty line"

        print_calls = mock_console.print.call_args_list

        # Verify first and last calls are empty lines
        assert len(print_calls[0][0]) == 0, "First print call should be empty line for spacing"
        assert len(print_calls[2][0]) == 0, "Third print call should be empty line for spacing"

        # Verify middle call contains the formatted Rich panel
        panel_call = print_calls[1]
        panel_arg = panel_call[0][0]  # First positional argument

        assert isinstance(panel_arg, Panel), "Should create a Rich Panel object for formatted error display"
        assert panel_arg.border_style == "red", "Panel should have red border to indicate error state"
        assert panel_arg.title == "Error", "Panel should have 'Error' title for clear identification"
        assert panel_arg.title_align == "left", "Panel title should be left-aligned for consistency"
        assert panel_arg.padding == (1, 2), "Panel should have appropriate padding for readability"

        # Assert - Verify panel content contains specific ExifTool information for Linux platform
        panel_content = str(panel_arg.renderable)
        assert (
            "ExifTool is required for reading and writing EXIF metadata in images" in panel_content
        ), "Panel should contain complete ExifTool description from tool info"
        assert "https://exiftool.org/" in panel_content, "Panel should contain ExifTool homepage URL for reference"
        assert (
            "Installation Instructions for Linux:" in panel_content
        ), "Panel should contain Linux-specific installation section header"
        assert (
            "sudo apt-get install libimage-exiftool-perl" in panel_content
        ), "Panel should contain Ubuntu/Debian package manager install command"

        # Assert - Verify platform detection was called exactly once
        mock_platform_system.assert_called_once_with()

    @pytest.mark.unit
    def test_show_dependency_error_with_error_message(self, mocker: MockerFixture) -> None:
        """Test show_dependency_error displays custom error message with tool info and platform-specific instructions.

        This unit test verifies that show_dependency_error correctly creates a formatted Rich panel
        containing the custom error message, tool description, platform-specific installation instructions,
        and homepage URL when called with a custom error message.
        """
        # Arrange
        mock_console = mocker.Mock()
        mock_console_class = mocker.patch("marimba.core.utils.dependencies.Console")
        mock_console_class.return_value = mock_console

        # Mock platform detection to control which instructions are shown
        mock_platform_system = mocker.patch("platform.system")
        mock_platform_system.return_value = "Darwin"  # Will map to Platform.MACOS

        custom_error = "FFmpeg binary not found in PATH"

        # Act
        show_dependency_error(ToolDependency.FFMPEG, custom_error)

        # Assert - Verify console.print was called exactly 3 times (empty line, panel, empty line)
        assert mock_console.print.call_count == 3, "Console should print exactly 3 times for error display"

        # Assert - Verify print call structure
        print_calls = mock_console.print.call_args_list
        first_call = print_calls[0]
        panel_call = print_calls[1]
        last_call = print_calls[2]

        # Assert - Verify empty line calls (first and last)
        assert len(first_call[0]) == 0, "First print call should be empty line"
        assert len(last_call[0]) == 0, "Third print call should be empty line"

        # Assert - Verify the panel structure and properties
        panel_arg = panel_call[0][0]  # First positional argument

        assert isinstance(panel_arg, Panel), "Should create a rich Panel object"
        assert panel_arg.border_style == "red", "Panel should have red border for error"
        assert panel_arg.title == "Error", "Panel should have 'Error' title"
        assert panel_arg.title_align == "left", "Panel title should be left-aligned"
        assert panel_arg.padding == (1, 2), "Panel should have correct padding"

        # Assert - Verify panel content contains expected information
        panel_content = str(panel_arg.renderable)
        assert "FFmpeg is required for video processing" in panel_content, "Panel should contain FFmpeg description"
        assert custom_error in panel_content, "Panel should contain the custom error message"
        assert "Error Details:" in panel_content, "Panel should contain error details section"
        assert (
            "Installation Instructions for macOS:" in panel_content
        ), "Panel should contain macOS installation section"
        assert "brew install ffmpeg" in panel_content, "Panel should contain macOS-specific install command"
        assert "https://ffmpeg.org/" in panel_content, "Panel should contain FFmpeg homepage URL"

        # Assert - Verify platform detection was called
        mock_platform_system.assert_called_once_with()


class TestCheckDependencyAvailable:
    """Test check_dependency_available function."""

    @pytest.mark.unit
    def test_check_dependency_available_found(self, mocker: MockerFixture) -> None:
        """Test check_dependency_available returns True when tool is found in system PATH.

        This test verifies that check_dependency_available correctly identifies when a tool
        is available by mocking shutil.which to return a valid path, ensuring the function
        properly delegates to the system's which command and interprets the results correctly.
        """
        # Arrange
        expected_tool_path = "/usr/bin/exiftool"
        mock_which = mocker.patch("shutil.which")
        mock_which.return_value = expected_tool_path

        # Act
        result = check_dependency_available(ToolDependency.EXIFTOOL)

        # Assert
        assert result is True, "Should return True when tool is found in PATH"
        mock_which.assert_called_once_with("exiftool")

    @pytest.mark.unit
    def test_check_dependency_available_not_found(self, mocker: MockerFixture) -> None:
        """Test check_dependency_available returns False when tool is not found in system PATH.

        This test verifies that check_dependency_available correctly identifies when a tool
        is not available by mocking shutil.which to return None, ensuring the function
        properly delegates to the system's which command and interprets missing tools correctly.
        """
        # Arrange
        mock_which = mocker.patch("shutil.which")
        mock_which.return_value = None

        # Act
        result = check_dependency_available(ToolDependency.EXIFTOOL)

        # Assert
        assert result is False, "Should return False when tool is not found in PATH"
        mock_which.assert_called_once_with("exiftool")


class TestValidateDependencies:
    """Test validate_dependencies function."""

    @pytest.mark.unit
    def test_validate_dependencies_empty_list(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies with empty list returns early without checking dependencies.

        This test verifies that validate_dependencies properly handles the edge case of an empty
        dependency list by returning immediately without performing any dependency checks or
        external system calls. This ensures efficient handling of no-dependency scenarios.
        """
        # Arrange - Mock external dependency checking functions to verify they're not called
        mock_which = mocker.patch("shutil.which")
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        # Act - Call validate_dependencies with empty list (should complete without exception)
        validate_dependencies([])

        # Assert - Verify no dependency checking was performed
        mock_which.assert_not_called()
        mock_show_error_exit.assert_not_called()

    @pytest.mark.unit
    def test_validate_dependencies_all_available(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies when all tools are available does not raise exceptions.

        This test verifies that validate_dependencies properly handles the case where all
        required tools are available in the system PATH. It should complete without raising
        any exceptions and should properly check each tool's availability using the real
        check_dependency_available function, only mocking the external shutil.which call.
        """
        # Arrange - Mock only the external system dependency (shutil.which), not internal logic
        mock_which = mocker.patch("shutil.which")
        mock_which.return_value = "/usr/bin/exiftool"  # Tool found in PATH

        # Mock error handler to verify it's not called when tools are available
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        # Act - Should not raise any exception when all tools are available
        validate_dependencies([ToolDependency.EXIFTOOL])

        # Assert - Verify shutil.which was called with correct tool name
        mock_which.assert_called_once_with("exiftool")

        # Assert - Verify no error handler was called since tool is available
        mock_show_error_exit.assert_not_called()

        # Assert - Verify no exception was raised (implicit - test passes if no exception)

    @pytest.mark.unit
    def test_validate_dependencies_multiple_tools_all_available(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies with multiple tools when all are available completes successfully.

        This test verifies that validate_dependencies properly handles multiple tools by checking
        each one individually using shutil.which, and completes successfully without raising
        exceptions when all required tools (including secondary dependencies like ffprobe for ffmpeg)
        are available in the system PATH. This ensures the validation logic works correctly for
        multi-tool dependency scenarios, specifically testing the FFmpeg validation that requires
        both ffmpeg and ffprobe binaries to be present.
        """
        # Arrange - Mock shutil.which to return different paths for different tools
        mock_which = mocker.patch("shutil.which")
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        def which_side_effect(tool_name: str) -> str | None:
            """Return different paths for different tools to simulate all being available."""
            tool_paths = {
                "exiftool": "/usr/bin/exiftool",
                "ffmpeg": "/usr/bin/ffmpeg",
                "ffprobe": "/usr/bin/ffprobe",
            }
            return tool_paths.get(tool_name)

        mock_which.side_effect = which_side_effect

        # Act - Should complete successfully without any exception when all tools are available
        # Testing that no exception is raised when all dependencies are available
        validate_dependencies([ToolDependency.EXIFTOOL, ToolDependency.FFMPEG])

        # Assert - If we reach here without exception, the function completed successfully

        # Assert - Verify shutil.which was called for each tool (ffmpeg requires both ffmpeg and ffprobe checks)
        expected_calls = [
            mocker.call("exiftool"),
            mocker.call("ffmpeg"),
            mocker.call("ffprobe"),  # ffmpeg validation automatically checks ffprobe too
        ]
        mock_which.assert_has_calls(expected_calls, any_order=False)
        assert mock_which.call_count == 3, "Should check exiftool, ffmpeg, and ffprobe availability"

        # Assert - Verify no error handler was called since all tools are available
        mock_show_error_exit.assert_not_called()

    @pytest.mark.unit
    def test_validate_dependencies_missing_tool(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies when a tool is missing calls error handler with correct parameters.

        This test verifies that validate_dependencies properly identifies missing tools and
        delegates to the error handler with the expected dependency and error message. It tests
        the core validation logic by mocking only external dependencies to prevent actual
        program exit during testing while exercising real business logic.
        """
        # Arrange - Mock external dependency (shutil.which returns None = tool not found)
        mock_which = mocker.patch("shutil.which")
        mock_which.return_value = None  # Tool not found in PATH

        # Mock the error handler to prevent actual program exit during testing
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        # Act - Call validate_dependencies with a single missing tool
        validate_dependencies([ToolDependency.EXIFTOOL])

        # Assert - Verify shutil.which was called to check tool availability
        mock_which.assert_called_once_with("exiftool")

        # Assert - Verify error handler was called with correct dependency and error message
        mock_show_error_exit.assert_called_once_with(
            ToolDependency.EXIFTOOL,
            "Required dependency 'exiftool' is not available",
        )

    @pytest.mark.unit
    def test_validate_dependencies_ffmpeg_without_ffprobe(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies when ffmpeg is available but ffprobe is missing.

        This test verifies the special FFmpeg validation logic that requires both
        ffmpeg and ffprobe binaries to be available. When ffmpeg is found but ffprobe
        is missing, it should call the error handler with a specific FFprobe error message.
        """
        # Arrange - Mock shutil.which to simulate ffmpeg available but ffprobe missing
        mock_which = mocker.patch("shutil.which")

        def which_side_effect(tool_name: str) -> str | None:
            """Return path for ffmpeg but None for ffprobe to simulate ffprobe missing."""
            return "/usr/bin/ffmpeg" if tool_name == "ffmpeg" else None

        mock_which.side_effect = which_side_effect

        # Mock the error handler to prevent actual program exit during testing
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        # Act - Validate FFmpeg dependency which should trigger ffprobe check
        validate_dependencies([ToolDependency.FFMPEG])

        # Assert - Verify both ffmpeg and ffprobe availability were checked
        expected_calls = [
            mocker.call("ffmpeg"),
            mocker.call("ffprobe"),
        ]
        mock_which.assert_has_calls(expected_calls, any_order=False)
        assert mock_which.call_count == 2, "Should check both ffmpeg and ffprobe availability"

        # Assert - Verify error handler was called with FFprobe-specific error message
        mock_show_error_exit.assert_called_once_with(
            ToolDependency.FFMPEG,
            "FFprobe (part of FFmpeg) is not available",
        )

    @pytest.mark.unit
    def test_validate_dependencies_ffmpeg_with_ffprobe(self, mocker: MockerFixture) -> None:
        """Test validate_dependencies succeeds when both ffmpeg and ffprobe are available.

        This test verifies that when both ffmpeg and ffprobe tools are available in the system PATH,
        the validate_dependencies function completes successfully without raising any exceptions or
        calling error handlers. It tests the specific FFmpeg validation logic that requires both
        the main ffmpeg binary and ffprobe utility to be present for complete functionality.
        """
        # Arrange
        mock_which = mocker.patch("shutil.which")

        def which_side_effect(tool_name: str) -> str | None:
            """Return paths for both ffmpeg and ffprobe to simulate both being available."""
            tool_paths = {
                "ffmpeg": "/usr/bin/ffmpeg",
                "ffprobe": "/usr/bin/ffprobe",
            }
            return tool_paths.get(tool_name)

        mock_which.side_effect = which_side_effect
        mock_show_error_exit = mocker.patch("marimba.core.utils.dependencies.show_dependency_error_and_exit")

        # Act
        validate_dependencies([ToolDependency.FFMPEG])

        # Assert
        expected_calls = [
            mocker.call("ffmpeg"),
            mocker.call("ffprobe"),
        ]
        mock_which.assert_has_calls(expected_calls, any_order=False)
        assert mock_which.call_count == 2, "Should check both ffmpeg and ffprobe availability"
        mock_show_error_exit.assert_not_called()


class TestShowDependencyErrorAndExit:
    """Test show_dependency_error_and_exit function."""

    @pytest.mark.unit
    def test_show_dependency_error_and_exit_default_exit_code_displays_error_and_exits(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test show_dependency_error_and_exit displays error panel and exits with default code.

        This test verifies that show_dependency_error_and_exit properly displays a formatted error
        panel with tool information and installation instructions, shows an exit message, and
        raises typer.Exit with the default exit code of 1 when called with an error message.
        """
        # Arrange - Mock only external dependencies (platform detection and console), test real business logic
        mock_console_class = mocker.patch("marimba.core.utils.dependencies.Console")
        mock_console = mocker.Mock()
        mock_console_class.return_value = mock_console

        # Mock platform detection to control which instructions are shown
        mock_platform_system = mocker.patch("platform.system")
        mock_platform_system.return_value = "Linux"  # Will map to Platform.LINUX

        test_error_message = "Test error"

        # Act & Assert - Verify typer.Exit is raised with correct exit code
        with pytest.raises(typer.Exit) as exc_info:
            show_dependency_error_and_exit(ToolDependency.EXIFTOOL, test_error_message)

        # Assert - Verify exit code is default value
        assert (
            exc_info.value.exit_code == 1
        ), "Should exit with default exit code 1 when no exit_code parameter provided"

        # Assert - Verify console prints were made (error panel display + exit message + empty lines)
        expected_print_count = 5
        actual_print_count = mock_console.print.call_count
        assert actual_print_count == expected_print_count, (
            f"Console should print exactly {expected_print_count} times "
            f"(empty line, panel, empty line, exit message, empty line), but printed {actual_print_count} times"
        )

        # Assert - Verify the panel content includes expected ExifTool information
        print_calls = mock_console.print.call_args_list
        panel_call = print_calls[1]  # Second call contains the panel
        panel_arg = panel_call[0][0]  # First positional argument

        assert isinstance(panel_arg, Panel), "Should create a rich Panel object for error display"
        assert panel_arg.border_style == "red", "Panel should have red border style to indicate error"
        assert panel_arg.title == "Error", "Panel should have 'Error' title to clearly indicate error state"

        # Assert - Verify panel content contains expected information
        panel_content = str(panel_arg.renderable)
        assert "ExifTool" in panel_content, "Panel content should contain ExifTool description for user guidance"
        assert test_error_message in panel_content, "Panel content should display the provided error message"
        assert "Linux" in panel_content, "Panel content should contain Linux-specific installation instructions"
        assert "https://exiftool.org/" in panel_content, "Panel content should include ExifTool homepage for reference"

        # Assert - Verify exit message was displayed
        exit_message_call = print_calls[3]  # Fourth call contains exit message
        exit_message_content = str(exit_message_call)
        assert "missing dependency" in exit_message_content, "Exit message should clearly mention missing dependency"
        assert "install the required tool" in exit_message_content, "Exit message should guide user to install the tool"

    @pytest.mark.unit
    def test_show_dependency_error_and_exit_custom_exit_code(self, mocker: MockerFixture) -> None:
        """Test show_dependency_error_and_exit with custom exit code exits properly and displays error.

        This test verifies that show_dependency_error_and_exit properly handles custom exit codes
        when terminating the program due to missing dependencies. It should call the error display
        function, print appropriate exit messages to the console, and raise typer.Exit with the
        specified custom exit code. This ensures proper error handling and program termination
        behavior when dependencies are missing.
        """
        # Arrange
        mock_console_class = mocker.patch("marimba.core.utils.dependencies.Console")
        mock_show_error = mocker.patch("marimba.core.utils.dependencies.show_dependency_error")
        mock_console = mocker.Mock()
        mock_console_class.return_value = mock_console

        custom_exit_code = 5
        test_error_message = "Test error"

        # Act & Assert - Verify typer.Exit is raised with correct exit code
        with pytest.raises(typer.Exit) as exc_info:
            show_dependency_error_and_exit(ToolDependency.FFMPEG, test_error_message, exit_code=custom_exit_code)

        # Assert - Verify exit code is properly set
        assert exc_info.value.exit_code == custom_exit_code, f"Should exit with custom exit code {custom_exit_code}"

        # Assert - Verify error display function is called with correct parameters
        mock_show_error.assert_called_once_with(ToolDependency.FFMPEG, test_error_message)

        # Assert - Verify console prints are made (exit message and empty line)
        assert mock_console.print.call_count == 2, "Console should print exit message and empty line"

        # Assert - Verify the actual console print calls contain expected content
        print_calls = mock_console.print.call_args_list
        assert len(print_calls) == 2, "Should have exactly 2 console print calls"

        # Verify first call contains the exit message with dependency information
        first_call_content = str(print_calls[0])
        assert "missing dependency" in first_call_content, "First print should contain missing dependency message"
        assert "install the required tool" in first_call_content, "First print should contain installation guidance"

        # Verify second call is empty line
        second_call_args = print_calls[1][0]  # Get positional args from second call
        assert len(second_call_args) == 0 or second_call_args[0] == "", "Second print should be empty line"

    @pytest.mark.unit
    def test_show_dependency_error_and_exit_no_error_message(self, mocker: MockerFixture) -> None:
        """Test show_dependency_error_and_exit without error message exits with default code and displays error panel.

        This test verifies that when show_dependency_error_and_exit is called without an error message,
        it properly displays the error panel with tool information, shows exit messages, and raises
        typer.Exit with the default exit code of 1. This test only mocks external dependencies (platform
        detection and console) to test real business logic behavior.
        """
        # Arrange - Mock only external dependencies (platform detection and console), test real business logic
        mock_console_class = mocker.patch("marimba.core.utils.dependencies.Console")
        mock_console = mocker.Mock()
        mock_console_class.return_value = mock_console

        # Mock platform detection to control which instructions are shown
        mock_platform_system = mocker.patch("platform.system")
        mock_platform_system.return_value = "Linux"  # Will map to Platform.LINUX

        # Act & Assert - Verify typer.Exit is raised with correct exit code
        with pytest.raises(typer.Exit) as exc_info:
            show_dependency_error_and_exit(ToolDependency.EXIFTOOL)

        # Assert - Verify exit code is default value
        assert exc_info.value.exit_code == 1, "Should exit with default exit code 1"

        # Assert - Verify console prints were made (error panel display + exit message + empty lines)
        assert (
            mock_console.print.call_count == 5
        ), "Console should print 5 times: empty line, panel, empty line, exit message, empty line"

        # Assert - Verify the panel content includes expected ExifTool information
        print_calls = mock_console.print.call_args_list
        panel_call = print_calls[1]  # Second call contains the panel
        panel_arg = panel_call[0][0]  # First positional argument

        assert isinstance(panel_arg, Panel), "Should create a rich Panel object"
        assert panel_arg.border_style == "red", "Panel should have red border for error"
        assert panel_arg.title == "Error", "Panel should have 'Error' title"

        # Assert - Verify panel content contains expected information
        panel_content = str(panel_arg.renderable)
        assert "ExifTool" in panel_content, "Panel should contain ExifTool description"
        assert "Linux" in panel_content, "Panel should contain Linux installation instructions"
        assert "https://exiftool.org/" in panel_content, "Panel should contain ExifTool homepage"

        # Assert - Verify exit message was displayed
        exit_message_call = print_calls[3]  # Fourth call contains exit message
        exit_message_content = str(exit_message_call)
        assert "missing dependency" in exit_message_content, "Exit message should mention missing dependency"
        assert "install the required tool" in exit_message_content, "Exit message should mention installation"

        # Assert - Verify platform detection was used
        mock_platform_system.assert_called_once()
