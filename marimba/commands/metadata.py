import os
import pathlib

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def check_input_args(source_path: str, config_path: str):
    """
    Check the input arguments for the copy command.

    Args:
        source_path: The path to the directory where the files will be copied from.
        config_path: The path to the configuration file.
    """
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(
            Panel(
                f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    # Check if config_path is valid
    if not os.path.isfile(config_path):
        print(
            Panel(f"The config_path argument [bold]{config_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red")
        )
        raise typer.Exit()

    # Check if config_path file has the correct extension
    if pathlib.Path(config_path).suffix.lower() != ".yaml":
        print(
            Panel(
                f'The config_path argument [bold]{config_path}[/bold] does not have the correct extension (".yaml")',
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def merge_metadata(
    source_path: str,
    config_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    """
    Merge metadata for files in a directory.

    Args:
        source_path: The path to the directory where the files to be merged are located.
        config_path: The path to the configuration file.
        recursive: Whether to merge metadata recursively.
        overwrite: Whether to overwrite existing metadata files.
        dry_run: Whether to run the command without actually merging the metadata.
    """
    check_input_args(source_path, config_path)

    logger.info(f"Merging metadata from source directory: {source_path}")
