"""
Marimba External Dependencies Error Handling.

This module provides standardized error handling and user messaging for external dependencies
like exiftool and ffmpeg. It uses rich panels to provide clear, actionable error messages
when external tools are not found or not properly installed.

Functions:
    show_dependency_error: Display a rich error panel for missing dependencies
    check_dependency_available: Check if a dependency is available on the system
"""

import platform
import shutil
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text


def get_installation_instructions(dependency: str) -> dict[str, Any]:
    """
    Get installation instructions for a dependency based on the current platform.

    Args:
        dependency: Name of the dependency (e.g., 'exiftool', 'ffmpeg')

    Returns:
        Dictionary containing installation instructions for different platforms
    """
    instructions = {
        "exiftool": {
            "description": "ExifTool is required for reading and writing EXIF metadata in images",
            "homepage": "https://exiftool.org/",
            "windows": [
                "1. Download ExifTool from https://exiftool.org/",
                "2. Extract the .exe file to a folder in your PATH",
                "3. Alternatively, install via Chocolatey: choco install exiftool",
            ],
            "macos": [
                "1. Install via Homebrew: brew install exiftool",
                "2. Alternatively, install via MacPorts: sudo port install p5.34-image-exiftool",
            ],
            "linux": [
                "• Ubuntu/Debian: sudo apt-get install libimage-exiftool-perl",
                "• CentOS/RHEL/Fedora: sudo yum install perl-Image-ExifTool",
                "• Arch Linux: sudo pacman -S perl-image-exiftool",
                "• Or download from https://exiftool.org/",
            ],
        },
        "ffmpeg": {
            "description": "FFmpeg is required for video processing and analysis",
            "homepage": "https://ffmpeg.org/",
            "windows": [
                "1. Download FFmpeg from https://ffmpeg.org/download.html",
                "2. Extract and add to your PATH environment variable",
                "3. Alternatively, install via Chocolatey: choco install ffmpeg",
            ],
            "macos": [
                "1. Install via Homebrew: brew install ffmpeg",
                "2. Alternatively, install via MacPorts: sudo port install ffmpeg",
            ],
            "linux": [
                "• Ubuntu/Debian: sudo apt-get install ffmpeg",
                "• CentOS/RHEL: sudo yum install ffmpeg",
                "• Fedora: sudo dnf install ffmpeg",
                "• Arch Linux: sudo pacman -S ffmpeg",
            ],
        },
    }

    return instructions.get(dependency, {})


def show_dependency_error(dependency: str, error_message: str = "") -> None:
    """
    Display a rich error panel for a missing dependency with installation instructions.

    Args:
        dependency: Name of the missing dependency
        error_message: Optional specific error message to include
    """
    console = Console()

    instructions = get_installation_instructions(dependency)
    if not instructions:
        console.print(f"[red]Error: Unknown dependency '{dependency}'[/red]")
        return

    # Determine current platform
    system = platform.system().lower()
    if system == "darwin":
        platform_key = "macos"
        platform_name = "macOS"
    elif system == "windows":
        platform_key = "windows"
        platform_name = "Windows"
    else:
        platform_key = "linux"
        platform_name = "Linux"

    # Build the error message
    title = "Error"

    content = Text()
    content.append(f"{instructions['description']}\n\n", style="white")

    if error_message:
        content.append("Error Details:\n", style="bold red")
        content.append(f"{error_message}\n\n", style="red")

    content.append(f"Installation Instructions for {platform_name}:\n", style="bold cyan")

    platform_instructions = instructions.get(platform_key, [])
    for instruction in platform_instructions:
        content.append(f"{instruction}\n", style="yellow")

    content.append(f"\nFor more information, visit: {instructions['homepage']}", style="blue underline")

    # Create and display the panel
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


def check_dependency_available(dependency: str) -> bool:
    """
    Check if a dependency is available on the system.

    Args:
        dependency: Name of the dependency to check

    Returns:
        True if the dependency is available, False otherwise
    """
    return shutil.which(dependency) is not None


def validate_required_dependencies() -> None:
    """
    Validate that all required external dependencies are available.

    This should be called once at the start of pipeline processing to ensure
    all required tools are available before starting any work.

    Raises:
        typer.Exit: If any required dependency is missing
    """
    missing_dependencies = []

    # Check for ExifTool
    if not check_dependency_available("exiftool"):
        missing_dependencies.append("exiftool")

    # Check for FFmpeg tools
    if not check_dependency_available("ffmpeg"):
        missing_dependencies.append("ffmpeg")
    elif not check_dependency_available("ffprobe"):
        missing_dependencies.append("ffmpeg")  # ffprobe comes with ffmpeg

    # If any dependencies are missing, show error and exit
    if missing_dependencies:
        # Show error for the first missing dependency
        # (usually they're related, like ffmpeg/ffprobe)
        dependency = missing_dependencies[0]
        show_dependency_error_and_exit(
            dependency,
            f"Required dependency '{dependency}' is not available",
        )


def show_dependency_error_and_exit(dependency: str, error_message: str = "", exit_code: int = 1) -> None:
    """
    Display a rich error panel for a missing dependency and exit the program.

    Args:
        dependency: Name of the missing dependency
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

    # Exit using typer for proper CLI handling
    raise typer.Exit(exit_code)
