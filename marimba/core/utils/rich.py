"""
Rich console output utilities.
"""

from rich.panel import Panel

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
