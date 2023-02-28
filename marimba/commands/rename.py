import glob
import logging
import os
import pathlib
import shutil
import sys

import typer
from rich import print
from rich.panel import Panel

import marimba.utils.file_system as fs
# TODO: Need to look into a better way to import all instrument classes from platforms.instruments
from marimba.platforms.instruments import *
from marimba.utils.config import load_config


def check_input_args(source_path: str, config_path: str) -> str:

    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Try to find default config if path not specified
    if not config_path:
        # TODO: This needs to be able to handle upper and lower case file extensions
        config_files = glob.glob(f"{source_path}/*.yaml")
        if len(config_files) < 1:
            # TODO: Need to validate high-level config file here...
            print(Panel(f"The [bold]config_path[/bold] argument was not specified and no default config file could be found in the source directory", title="Error", title_align="left", border_style="red"))
            raise typer.Exit()
        else:
            # Get first config file in directory if multiple exist
            config_path = config_files[0]
            return config_path

    # Check if config_path is valid
    if config_path and not os.path.isfile(config_path):
        print(Panel(f"The config_path argument [bold]{config_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Check if config_path file has the correct extension
    if config_path and pathlib.Path(config_path).suffix.lower() != ".yaml":
        print(Panel(f'The config_path argument [bold]{config_path}[/bold] does not have the correct extension (".yaml")', title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    return config_path


def rename_files(
    source_path: str,
    config_path: str,
    destination_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    # Check input arguments and update config file path if found automatically
    config_path = check_input_args(source_path, config_path)

    # Load config file
    config_data = load_config(config_path)
    instrument_class = config_data.get("image-set-header").get("image-platform")

    # Instantiate instrument specification
    # TODO: Need to look into the best pythonic way to do this...
    instrument = getattr(sys.modules[__name__], instrument_class)(config_data)

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
