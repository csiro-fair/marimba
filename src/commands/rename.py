import glob
import logging
import os
import pathlib
import shutil
import sys

import typer
import utils.file_system as fs
# TODO: Need to look into a better way to import all instrument classes from platforms.instruments
from platforms.instruments import *
from rich import print
from rich.panel import Panel
from utils.ifdo import load_ifdo


def check_input_args(source_path: str, ifdo_path: str) -> str:

    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Try to find default ifdo if path not specified
    if not ifdo_path:
        # TODO: This needs to be able to handle upper and lower case file extensions
        ifdo_files = glob.glob(f"{source_path}/*.ifdo")
        if len(ifdo_files) < 1:
            # TODO: Need to validate high-level ifdo file here...
            print(Panel(f"The [bold]ifdo_path[/bold] argument was not specified and no default ifdo file could be found in the source directory", title="Error", title_align="left", border_style="red"))
            raise typer.Exit()
        else:
            # Get first iFDO file in directory if multiple exist
            ifdo_path = ifdo_files[0]
            return ifdo_path

    # Check if ifdo_path is valid
    if ifdo_path and not os.path.isfile(ifdo_path):
        print(Panel(f"The ifdo_path argument [bold]{ifdo_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Check if ifdo_path file has the correct extension
    if ifdo_path and pathlib.Path(ifdo_path).suffix.lower() != ".ifdo":
        print(Panel(f'The ifdo_path argument [bold]{ifdo_path}[/bold] does not have the correct extension (".ifdo")', title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    return ifdo_path


def rename_files(
    source_path: str,
    ifdo_path: str,
    destination_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    # Check input arguments and update iFDO file path if found automatically
    ifdo_path = check_input_args(source_path, ifdo_path)

    # Load iFDO file
    ifdo_data = load_ifdo(ifdo_path)
    instrument_class = ifdo_data.get("image-set-header").get("image-platform")

    # Instantiate instrument specification
    # TODO: Need to look into the best pythonic way to do this...
    instrument = getattr(sys.modules[__name__], instrument_class)(ifdo_data)

    logging.info(f"Renaming files recursively from: {source_path}")

    # Traverse the source directory
    # TODO: This is currently a recursive method - need to implement an optional recursive/non-recursive method and use the Typer argument
    for directory_path, _, files in os.walk(source_path):

        # Check if the directory is targeted for file renaming and prompt the user for any relevant metadata identifiers
        if instrument.is_target_rename_directory(directory_path) and instrument.get_manual_metadata_fields():

            # Process each file in target directory
            for file in files:

                # Get file extension and check if it is a targeted file type
                _, file_extension = os.path.splitext(file)
                if file_extension.lower() == instrument.filetype:

                    # Construct input and output file paths
                    file_path = os.path.join(directory_path, file)
                    output_file_name = instrument.get_output_file_name(file_path)
                    output_file_directory = instrument.get_output_file_directory(directory_path, destination_path)
                    output_file_path = os.path.join(output_file_directory, output_file_name)

                    # Check if input and output file paths are the same
                    if file_path == output_file_path:
                        logging.info(f"Skipping file - input and output file names are identical: {file_path}")
                    # Check if output file path already exists and the overwrite argument is not set
                    elif os.path.isfile(output_file_path) and not overwrite:
                        logging.info(f'Output file already exists and overwrite argument is not set: "{output_file_path}"')
                    # Perform file renaming
                    else:
                        renaming_or_overwriting = "Overwriting" if os.path.isfile(output_file_path) and overwrite else "Renaming"
                        if dry_run:
                            logging.info(f'DRY-RUN: {renaming_or_overwriting} file from "{file_path}" to "{output_file_path}"')
                        else:
                            logging.info(f'{renaming_or_overwriting} file from "{file_path}" to "{output_file_path}"')
                            try:
                                # Rename file if no destination path provided, otherwise copy and rename file to new destination
                                fs.create_directory_if_necessary(output_file_directory)
                                if not destination_path:
                                    os.rename(file_path, output_file_path)
                                else:
                                    shutil.copy(file_path, output_file_path)
                            # TODO: Check this is the correct exception to catch
                            except FileExistsError:
                                logging.error(f"Error renaming file {file_path} to {output_file_path}")

        else:
            logging.info("Skipping directory from renaming operation")
