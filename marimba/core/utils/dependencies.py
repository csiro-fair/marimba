"""
Marimba External Dependencies Management.

This module provides dependency checking and error handling for external tools
like exiftool and ffmpeg. It uses rich panels to provide clear, actionable
error messages when tools are missing.
"""

import platform
import shutil
from dataclasses import dataclass
from enum import Enum

import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text


class ToolDependency(Enum):
    """Enumeration of supported external tool dependencies."""

    EXIFTOOL = "exiftool"
    FFMPEG = "ffmpeg"
    FFPROBE = "ffprobe"


class Platform(Enum):
    """Enumeration of supported platforms."""

    WINDOWS = "windows"
    MACOS = "macos"
    LINUX = "linux"


@dataclass
class ToolInfo:
    """Information about a tool dependency including installation instructions."""

    name: str
    description: str
    homepage: str
    windows_instructions: list[str]
    macos_instructions: list[str]
    linux_instructions: list[str]

    def get_platform_instructions(self, platform: Platform) -> list[str]:
        """Get installation instructions for a specific platform."""
        return {
            Platform.WINDOWS: self.windows_instructions,
            Platform.MACOS: self.macos_instructions,
            Platform.LINUX: self.linux_instructions,
        }[platform]


def get_tool_info(dependency: ToolDependency) -> ToolInfo:
    """
    Get tool information for a dependency.

    Args:
        dependency: ToolDependency enum value

    Returns:
        ToolInfo object containing installation instructions and metadata
    """
    tool_info = {
        ToolDependency.EXIFTOOL: ToolInfo(
            name="exiftool",
            description="ExifTool is required for reading and writing EXIF metadata in images",
            homepage="https://exiftool.org/",
            windows_instructions=[
                "1. Download ExifTool from https://exiftool.org/",
                "2. Extract the .exe file to a folder in your PATH",
                "3. Alternatively, install via Chocolatey: choco install exiftool",
            ],
            macos_instructions=[
                "1. Install via Homebrew: brew install exiftool",
                "2. Alternatively, install via MacPorts: sudo port install p5.34-image-exiftool",
            ],
            linux_instructions=[
                "• Ubuntu/Debian: sudo apt-get install libimage-exiftool-perl",
                "• CentOS/RHEL/Fedora: sudo yum install perl-Image-ExifTool",
                "• Arch Linux: sudo pacman -S perl-image-exiftool",
                "• Or download from https://exiftool.org/",
            ],
        ),
        ToolDependency.FFMPEG: ToolInfo(
            name="ffmpeg",
            description="FFmpeg is required for video processing and analysis",
            homepage="https://ffmpeg.org/",
            windows_instructions=[
                "1. Download FFmpeg from https://ffmpeg.org/download.html",
                "2. Extract and add to your PATH environment variable",
                "3. Alternatively, install via Chocolatey: choco install ffmpeg",
            ],
            macos_instructions=[
                "1. Install via Homebrew: brew install ffmpeg",
                "2. Alternatively, install via MacPorts: sudo port install ffmpeg",
            ],
            linux_instructions=[
                "• Ubuntu/Debian: sudo apt-get install ffmpeg",
                "• CentOS/RHEL: sudo yum install ffmpeg",
                "• Fedora: sudo dnf install ffmpeg",
                "• Arch Linux: sudo pacman -S ffmpeg",
            ],
        ),
    }

    return tool_info[dependency]


def get_current_platform() -> Platform:
    """Get the current platform."""
    system = platform.system().lower()
    if system == "darwin":
        return Platform.MACOS
    if system == "windows":
        return Platform.WINDOWS
    return Platform.LINUX


def show_dependency_error(dependency: ToolDependency, error_message: str = "") -> None:
    """
    Display a rich error panel for a missing dependency with installation instructions.

    Args:
        dependency: ToolDependency enum value
        error_message: Optional specific error message to include
    """
    console = Console()

    tool_info = get_tool_info(dependency)
    current_platform = get_current_platform()

    platform_names = {
        Platform.MACOS: "macOS",
        Platform.WINDOWS: "Windows",
        Platform.LINUX: "Linux",
    }

    # Build the error message
    title = "Error"

    content = Text()
    content.append(f"{tool_info.description}\n\n", style="white")

    if error_message:
        content.append("Error Details:\n", style="bold red")
        content.append(f"{error_message}\n\n", style="red")

    content.append(f"Installation Instructions for {platform_names[current_platform]}:\n", style="bold cyan")

    platform_instructions = tool_info.get_platform_instructions(current_platform)
    for instruction in platform_instructions:
        content.append(f"{instruction}\n", style="yellow")

    content.append(f"\nFor more information, visit: {tool_info.homepage}", style="blue underline")

    panel = Panel(
        content,
        title=title,
        title_align="left",
        border_style="red",
        padding=(1, 2),
    )

    console.print()
    console.print(panel)
    console.print()


def check_dependency_available(dependency: ToolDependency) -> bool:
    """
    Check if a dependency is available on the system.

    Args:
        dependency: ToolDependency enum value to check

    Returns:
        True if the dependency is available, False otherwise
    """
    return shutil.which(dependency.value) is not None


def validate_dependencies(required_tools: list[ToolDependency]) -> None:
    """
    Validate that specified external dependencies are available.

    Args:
        required_tools: List of ToolDependency enum values to validate

    Raises:
        typer.Exit: If any required dependency is missing
    """
    if not required_tools:
        return

    for tool in required_tools:
        if not check_dependency_available(tool):
            show_dependency_error_and_exit(
                tool,
                f"Required dependency '{tool.value}' is not available",
            )

        if tool == ToolDependency.FFMPEG and not check_dependency_available(ToolDependency.FFPROBE):
            show_dependency_error_and_exit(
                ToolDependency.FFMPEG,
                "FFprobe (part of FFmpeg) is not available",
            )


def show_dependency_error_and_exit(dependency: ToolDependency, error_message: str = "", exit_code: int = 1) -> None:
    """
    Display a rich error panel for a missing dependency and exit the program.

    Args:
        dependency: ToolDependency enum value
        error_message: Optional specific error message to include
        exit_code: Exit code to use when exiting (default: 1)
    """
    console = Console()

    # Show the error panel
    show_dependency_error(dependency, error_message)

    # Add a final message about exiting
    console.print(
        "[bold red]Exiting due to missing dependency. Please install the required tool and try again.[/bold red]",
    )
    console.print()

    raise typer.Exit(exit_code)
