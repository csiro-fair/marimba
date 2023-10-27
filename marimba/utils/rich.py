"""
Rich console output utilities.
"""

from rich.panel import Panel

MARIMBA = "[bold][aquamarine3]MarImBA[/aquamarine3][/bold]"


def success_panel(message: str, title: str = "Success") -> Panel:
    """
    Create a success panel.

    Args:
        message: The message to display.
        title: The title of the panel.

    Returns:
        A success panel.
    """
    return Panel(message, title=title, title_align="left", style="green")


def error_panel(message: str, title: str = "Error") -> Panel:
    """
    Create an error panel.

    Args:
        message: The message to display.
        title: The title of the panel.

    Returns:
        An error panel.
    """
    return Panel(message, title=title, title_align="left", style="red")
