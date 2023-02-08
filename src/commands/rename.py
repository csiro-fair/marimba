import glob
import logging
import os
import pathlib

import typer
from rich import print
from rich.panel import Panel


def check_input_args(
    source_path: str,
    ifdo_path: str
):

    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Try to finde default ifdo is path not specified
    if not ifdo_path:
        # TODO: This needs to be able to handle upper and lower case file extensions
        ifdo_files = glob.glob(f"{source_path}/*.ifdo")
        if len(ifdo_files) < 1:
            # TODO: Need to validate ifdo file here...
            print(Panel(f"The [bold]ifdo_path[/bold] argument was not specified and no default ifdo file could be found in the source directory", title="Error", title_align="left", border_style="red"))
            raise typer.Exit()

    # Check if ifdo_path is valid
    if ifdo_path and not os.path.isfile(ifdo_path):
        print(Panel(f"The ifdo_path argument [bold]{ifdo_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Check if ifdo_path file has the correct extension
    if ifdo_path and pathlib.Path(ifdo_path).suffix.lower() != ".ifdo":
        print(Panel(f'The ifdo_path argument [bold]{ifdo_path}[/bold] does not have the correct extension (".ifdo")', title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def rename_files(
    source_path: str,
    ifdo_path: str,
    recursive: bool,
    overwrite: bool,
):
    check_input_args(source_path, ifdo_path)

    logging.info(f"Renaming files recursively from: {source_path}")

    for directory_path, _, files in os.walk(source_path):

        for file in files:
            file_path = os.path.join(directory_path, file)
            file_name, file_extension = os.path.splitext(file)

            output_file_name = file_name.upper().replace(" ", "_")

            output_file_path = os.path.join(directory_path, output_file_name + file_extension.upper())

            if file_extension.lower() in [".mp4", ".mpg", ".avi"]:

                if file_path == output_file_path:
                    logging.info(f'File name has not changed "{file_path}"')
                else:
                    os.rename(file_path, output_file_path)
                    logging.info(f'Renamed file from "{file_path}" to "{output_file_path}"')
