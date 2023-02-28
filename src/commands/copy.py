import logging
import os
import shutil

import typer
from rich import print
from rich.panel import Panel

import utils.file_system as fs


def check_input_args(
    source_path: str,
    destination_path: str
):
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def copy_files(
    source_path: str,
    destination_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(source_path)

    logging.info(f"Copying files recursively from: {source_path}")

    for directory_path, _, files in os.walk(source_path):

        for file in files:
            input_file_path = os.path.join(directory_path, file)
            output_file_path = os.path.join(destination_path, file)

            fs.create_directory_if_necessary(destination_path)

            if input_file_path == output_file_path:
                logging.info(f'File already exists at "{output_file_path}"')
            else:
                # shutil.copyfile(input_file_path, output_file_path)
                logging.info(f'Copied file from "{input_file_path}" to "{output_file_path}"')

