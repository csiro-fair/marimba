import os
import shutil

import typer
from rich import print
from rich.panel import Panel

import marimba.utils.file_system as fs
from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def check_input_args(source_path: str, destination_path: str):
    """
    Check the input arguments for the copy command.

    Args:
        source_path: The path to the directory where the files will be copied from.
        destination_path: The path to the directory where the files will be copied to.
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


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def copy_files(
    source_path: str,
    destination_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    """
    Copy files from one directory to another.

    Args:
        source_path: The path to the directory where the files will be copied from.
        destination_path: The path to the directory where the files will be copied to.
        recursive: Whether to copy files recursively.
        overwrite: Whether to overwrite existing files.
        dry_run: Whether to run the command without actually copying the files.
    """
    check_input_args(source_path)

    logger.info(f"Copying files recursively from: {source_path}")

    for directory_path, _, files in os.walk(source_path):
        for file in files:
            input_file_path = os.path.join(directory_path, file)
            output_file_path = os.path.join(destination_path, file)

            fs.create_directory_if_necessary(destination_path)

            if input_file_path == output_file_path:
                logger.info(f'File already exists at "{output_file_path}"')
            else:
                # shutil.copyfile(input_file_path, output_file_path)
                logger.info(f'Copied file from "{input_file_path}" to "{output_file_path}"')
