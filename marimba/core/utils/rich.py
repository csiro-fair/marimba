"""
Marimba Rich Utilities.

This module provides utility functions and constants for creating visually appealing console output using the Rich
library, specifically tailored for the Marimba project. It includes functions for creating styled panels, formatting
text, and configuring progress bars.

Imports:
    - typing: Provides type hinting support.
    - rich.panel: Used for creating stylized panels.
    - rich.progress: Provides components for creating progress bars.

Functions:
    - success_panel: Creates a green-bordered panel for success messages.
    - error_panel: Creates a red-bordered panel for error messages.
    - format_command: Formats a command name with steel blue color.
    - format_entity: Formats an entity name with light pink color.
    - get_default_columns: Returns a tuple of default progress columns.
"""

from rich.panel import Panel
from rich.progress import (
    BarColumn,
    ProgressColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

MARIMBA = "[bold][aquamarine3]Marimba[/aquamarine3][/bold]"


def success_panel(message: str, title: str = "Success") -> Panel:
    """
    Create a success panel.

    Args:
        message: The message to display.
        title: The title of the panel.

    Returns:
        A success panel.
    """
    return Panel(message, title=title, title_align="left", border_style="green")


def error_panel(message: str, title: str = "Error") -> Panel:
    """
    Create an error panel.

    Args:
        message: The message to display.
        title: The title of the panel.

    Returns:
        An error panel.
    """
    return Panel(message, title=title, title_align="left", border_style="red")


def format_command(command_name: str) -> str:
    """
    Format a command for Rich output.

    Args:
        command_name: The name of the command.

    Returns:
        The formatted command.
    """
    return f"[steel_blue3]{command_name}[/steel_blue3]"


def format_entity(entity_name: str) -> str:
    """
    Format an entity for Rich output.

    Args:
        entity_name: The name of the entity.

    Returns:
        The formatted entity.
    """
    return f"[light_pink3]{entity_name}[/light_pink3]"


def get_default_columns() -> tuple[ProgressColumn, ...]:
    """
    Get the default progress columns.

    Returns:
        The default progress columns.
    """
    return (
        TextColumn("[bold]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    )
