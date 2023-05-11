#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import typer

import marimba.commands.new as new
from marimba.commands.catalogue import catalogue_files
from marimba.commands.convert import convert_files
from marimba.commands.extract import extract_frames
from marimba.commands.metadata import merge_metadata
from marimba.commands.qc import run_qc
from marimba.commands.rename import rename_files
from marimba.commands.process import process_files
from marimba.utils.log import LogLevel, get_collection_logger, get_rich_handler

__author__ = "MarImBA Development Team"
__copyright__ = "Copyright 2023, CSIRO"
__credits__ = [
    "Chris Jackett <chris.jackett@csiro.au>",
    "Kevin Barnard <kbarnard@mbari.org>"
    "Nick Mortimer <nick.mortimer@csiro.au>",
    "David Webb <david.webb@csiro.au>",
    "Aaron Tyndall <aaron.tyndall@csiro.au>",
    "Franzis Althaus <franzis.althaus@csiro.au>",
    "Bec Gorton <bec.gorton@csiro.au>",
    "Ben Scoulding <ben.scoulding@csiro.au>",
]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"

marimba = typer.Typer(
    name="MarImBA - Marine Imagery Batch Actions",
    help="""MarImBA - Marine Imagery Batch Actions\n
        A Python CLI for batch processing, transforming and FAIR-ising large volumes of marine imagery.""",
    short_help="MarImBA - Marine Imagery Batch Actions",
    no_args_is_help=True,
)

marimba.add_typer(new.app, name="new")

logger = get_collection_logger()


@marimba.callback()
def global_options(
        level: LogLevel = typer.Option(LogLevel.INFO, help="Logging level."),
):
    """
    Global options for MarImBA CLI.
    """
    get_rich_handler().setLevel(logging.getLevelName(level.value))
    # logger.info(f"Initialized MarImBA CLI v{__version__}")


@marimba.command()
def qc(
        source_path: str = typer.Argument(..., help="Source path of files."),
        recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
):
    """
    Run quality control on files to check for anomalies and generate datasets statistics.
    """

    run_qc(source_path, recursive)


@marimba.command()
def catalog(
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
def rename(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files based on the instrument class specification.
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


# @marimba.command()
# def chunk(
#     source_path: str = typer.Argument(..., help="Source path of files."),
#     destination_path: str = typer.Argument(..., help="Destination path to output files."),
#     chunk_length: int = typer.Argument(10, help="Video chunk length in number of seconds."),
#     recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
#     overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
#     dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
# ):
#     """
#     Chunk video files into fixed-length videos (default 10 seconds).
#     """
#
#     chunk_files(source_path, destination_path, chunk_length)


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
def package():
    """
    Package up a MarImBA collection ready for distribution.
    """
    return


@marimba.command()
def process(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process the MarImBA collection based on the instrument class specification.
    """
    process_files(collection_path, instrument_id, dry_run)


@marimba.command()
def report():
    """
    Generate reports for a MarImBA collection, instrument or deployment.
    """
    return


# @marimba.command()
# def test():
#     """
#     Test the marimba package.
#     """
#
#     noop_instrument = NoopInstrument(get_instrument_path("noopinstrument"), {}, {})


if __name__ == "__main__":
    marimba()
