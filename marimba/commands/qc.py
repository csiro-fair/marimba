import os

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.log import get_logger

logger = get_logger(__name__)


def check_input_args(
    input_path: str,
):
    """
    Check the input arguments for the qc command.

    Args:
        input_path: The path to the directory where the files to be qc'd are located.
    """
    # Check if source_path is valid
    if not os.path.isdir(input_path):
        print(
            Panel(
                f"The output_path argument [bold]{input_path}[/bold] is not a valid directory path",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()


def qc_command(
    input_path: str,
    recursive: bool,
):
    """
    Run quality control on files in a directory.

    Args:
        input_path: The path to the directory where the files to be qc'd are located.
        recursive: Whether to run qc recursively.
    """
    check_input_args(input_path)

    logger.info(f"Running QC on files at: {input_path}")
