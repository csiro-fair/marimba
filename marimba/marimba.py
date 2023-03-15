#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os

import typer

from marimba.commands.catalogue import catalogue_files
from marimba.commands.chunk import chunk_files
from marimba.commands.config import (ConfigLevel, create_config,
                                     get_instrument_config)
from marimba.commands.convert import convert_files
from marimba.commands.copy import copy_files
from marimba.commands.extract import extract_frames
from marimba.commands.metadata import merge_metadata
from marimba.commands.qc import run_qc
from marimba.commands.rename import rename_files
from marimba.commands.template import create_tamplate
from marimba.platforms.instruments.noop_instrument import NoopInstrument
from marimba.utils.context import get_instrument_dir, set_collection_dir
from marimba.utils.log import (LogLevel, get_collection_logger,
                               get_rich_handler, init_collection_file_handler)

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"

marimba = typer.Typer(
    name="MarImBA - Marine Imagery Batch Actions",
    no_args_is_help=True,
    help="""MarImBA - Marine Imagery Batch Actions\n
        A Python CLI for batch processing, transforming and FAIR-ising large volumes of marine imagery.""",
    short_help="MarImBA - Marine Imagery Batch Actions",
)

logger = get_collection_logger()


@marimba.callback()
def global_options(
    level: LogLevel = typer.Option(LogLevel.WARNING, help="Logging level."),
    collection_dir: str = typer.Option(os.getcwd(), help="Path to collection directory."),
):
    """
    Global options for MarImBA CLI.
    """
    get_rich_handler().setLevel(logging.getLevelName(level.value))

    # Set the collection directory
    set_collection_dir(collection_dir)

    # Initialize the collection-level file handler
    init_collection_file_handler()

    logger.info(f"Initialized MarImBA CLI v{__version__}")


@marimba.command()
def qc(
    source_path: str = typer.Argument(..., help="Source path of files."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
):
    """
    Run quality control code on files to check for anomalies and generate datasets statistics.
    """

    run_qc(source_path, recursive)


@marimba.command()
def template(
    output_path: str = typer.Argument(..., help="Source path of files."),
    templatename: str = typer.Argument(..., help="Recursively process entire directory structure."),
):
    """
    Run quality control code on files to check for anomalies and generate datasets statistics.
    """

    create_tamplate(output_path, templatename)


@marimba.command()
def catalogue(
    source_path: str = typer.Argument(..., help="Source path for catalogue."),
    exiftool_path: str = typer.Option("exiftool", help="Path to exiftool"),
    file_extension: str = typer.Option("JPG", help="extension to catalogue"),
    glob_path: str = typer.Option("**", help="masked used in glob"),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
):
    """
    Create an exif catalogue of files stored in .exif_{extension}.
    """
    catalogue_files(source_path, file_extension, exiftool_path, glob_path, overwrite)


@marimba.command()
def config(
    level: ConfigLevel = typer.Argument(..., help="Level of config file to create."),
    output_path: str = typer.Argument(..., help="Output path for minimal config file."),
):
    """
    Create the initial minimal survey/deployment config file by answering a series of questions.
    """

    create_config(level, output_path)


@marimba.command()
def copy(
    source_path: str = typer.Argument(..., help="Source path to copy files."),
    destination_path: str = typer.Argument(..., help="Destination path to output files."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Copy image files from a source path to a destination path.
    """

    copy_files(source_path, destination_path, recursive, overwrite, dry_run)


@marimba.command()
def rename(
        collection_path: str = typer.Option(".", help="Path to MarImBA collection."),
        instrument_id: str = typer.Option(None, help="MarImBA instrument ID."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files and construct based on the instrument class specification.
    """

    rename_files(collection_path, instrument_id, dry_run)


@marimba.command()
def metadata(
    source_path: str = typer.Argument(..., help="Source path of files."),
    config_path: str = typer.Argument(..., help="Path to minimal survey/deployment config file."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process and write metadata including merging nav data files, writing metadata into image EXIF fields, and writing iFDO files into the dataset directory structure.
    """

    merge_metadata(source_path, config_path, recursive, overwrite, dry_run)


@marimba.command()
def convert(
    source_path: str = typer.Argument(..., help="Source path of files."),
    destination_path: str = typer.Argument(..., help="Destination path to output files."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Convert images and videos to standardised formats using Pillow and ffmpeg respectively.
    """

    convert_files(source_path, destination_path, recursive, overwrite, dry_run)


@marimba.command()
def chunk(
    source_path: str = typer.Argument(..., help="Source path of files."),
    destination_path: str = typer.Argument(..., help="Destination path to output files."),
    chunk_length: int = typer.Argument(10, help="Video chunk length in number of seconds."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Chunk video files into fixed-length videos (default 10 seconds).
    """

    chunk_files(source_path, destination_path, chunk_length)


@marimba.command()
def extract(
    source_path: str = typer.Argument(..., help="Source path of files."),
    destination_path: str = typer.Argument(..., help="Destination path to output files."),
    chunk_length: int = typer.Argument(None, help="Video chunk length in number of seconds."),
    recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Extract frames from videos using ffmpeg.
    """

    extract_frames(source_path, destination_path, chunk_length, recursive, overwrite, dry_run)


@marimba.command()
def test():
    """
    Test the marimba package.
    """

    noop_instrument = NoopInstrument(get_instrument_dir("noopinstrument"), {}, {})


if __name__ == "__main__":
    marimba()
