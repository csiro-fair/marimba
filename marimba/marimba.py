#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import typer

import marimba.commands.new as new
from marimba.core.collection import run_command
from marimba.utils.log import LogLevel, get_collection_logger, get_rich_handler

__author__ = "MarImBA Development Team"
__copyright__ = "Copyright 2023, CSIRO"
__credits__ = [
    "Chris Jackett <chris.jackett@csiro.au>",
    "Kevin Barnard <kbarnard@mbari.org>" "Nick Mortimer <nick.mortimer@csiro.au>",
    "David Webb <david.webb@csiro.au>",
    "Aaron Tyndall <aaron.tyndall@csiro.au>",
    "Franzis Althaus <franzis.althaus@csiro.au>",
    "Bec Gorton <bec.gorton@csiro.au>",
    "Ben Scoulding <ben.scoulding@csiro.au>",
]
__license__ = "MIT"
__version__ = "0.2"
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
    logger.info(f"Initialised MarImBA CLI v{__version__}")


@marimba.command("catalog")
def catalog_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
    exiftool_path: str = typer.Option("exiftool", help="Path to exiftool"),
    file_extension: str = typer.Option("JPG", help="extension to catalog"),
    glob_path: str = typer.Option("**", help="masked used in glob"),
    overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
):
    """
    Create an exif catalog of files stored in .exif_{extension}.
    """

    run_command(
        "run_catalog",
        collection_path,
        instrument_id,
        deployment_name,
        dry_run=dry_run,
        exiftool_path=exiftool_path,
        file_extension=file_extension,
        glob_path=glob_path,
        overwrite=overwrite,
    )


@marimba.command("init")
def init_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(..., help="MarImBA instrument ID."),
    card_paths: list[str] = typer.Argument(None, help="List of paths to SD cards to be initialised."),
    all: bool = typer.Option(False, help="."),
    days: int = typer.Option(0, help='Add an offset to the import date (e.g. "+1" to set the date to tomorrow).'),
    overwrite: bool = typer.Option(False, help="Overwrite import.yaml file on SD cards if they already exist."),
    card_size: int = typer.Option(512, help="Maximum card size (GB)."),
    format_type: str = typer.Option("exfat", help="Card filesystem format."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Initialise SD cards with an import.yaml file
    """

    run_command(
        "run_init",
        collection_path,
        instrument_id,
        None,
        extra,
        card_paths=card_paths,
        all=all,
        days=days,
        overwrite=overwrite,
        card_size=card_size,
        format_type=format_type,
        dry_run=dry_run,
    )


@marimba.command("import")
def import_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(..., help="MarImBA instrument ID."),
    card_paths: list[str] = typer.Argument(None, help="List of paths to SD cards to be initialised."),
    all: bool = typer.Option(False, help="."),
    exiftool_path: str = typer.Option("exiftool", help="Path to exiftool"),
    copy: bool = typer.Option(True, help="Clean source"),
    move: bool = typer.Option(False, help="Move source"),
    file_extension: str = typer.Option("MP4", help="extension to catalog"),
    card_size: int = typer.Option(512, help="Maximum card size (GB)."),
    format_type: str = typer.Option("exfat", help="Card filesystem format."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Import SD cards to working directory
    """
    run_command(
        "run_import",
        collection_path,
        instrument_id,
        None,
        extra,
        card_paths=card_paths,
        all=all,
        exiftool_path=exiftool_path,
        copy=copy,
        move=move,
        file_extension=file_extension,
        card_size=card_size,
        format_type=format_type,
        dry_run=dry_run,
    )


@marimba.command("doit")
def doit_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID."),
    doit_commands: list[str] = typer.Argument(None, help="MarImBA instrument ID."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Import SD cards to working directory
    """

    run_command(
        "run_doit",
        collection_path,
        instrument_id,
        doit_commands,
    )


# TODO: This could be implemented within the MarImBA process command
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
#     chunk_command(source_path, destination_path, chunk_length)
#     run_command('chunk', collection_path, instrument_id, dry_run=dry_run, chunk_length=chunk_length)


# TODO: This could be implemented within the MarImBA process command
# @marimba.command()
# def convert(
#         collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
#         instrument_id: str = typer.Argument(None, help="MarImBA instrument ID."),
#         dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
#         destination_path: str = typer.Argument(..., help="Destination path to output files."),
#         overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
# ):
#     """
#     Convert images and videos to standardised formats using Pillow and ffmpeg.
#     """
#
#     run_command('convert', collection_path, instrument_id, dry_run=dry_run, destination_path=destination_path, overwrite=overwrite)


# TODO: This could be implemented within the MarImBA process command
# @marimba.command()
# def extract(
#         collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
#         instrument_id: str = typer.Argument(None, help="MarImBA instrument ID."),
#         dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
#         destination_path: str = typer.Argument(..., help="Destination path to output files."),
#         chunk_length: int = typer.Argument(None, help="Video chunk length in number of seconds."),
#         overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
# ):
#     """
#     Extract frames from videos using ffmpeg.
#     """
#
#     run_command('extract', collection_path, instrument_id, dry_run=dry_run, destination_path=destination_path, chunk_length=chunk_length, overwrite=overwrite)


@marimba.command("metadata")
def metadata_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process metadata including merging nav data files, writing metadata into image EXIF tags, and writing iFDO files.
    """

    run_command("run_metadata", collection_path, instrument_id, deployment_name, extra, dry_run=dry_run)


@marimba.command("package")
def package_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Package up a MarImBA collection ready for distribution.
    """
    run_command("run_package", collection_path, instrument_id, deployment_name, extra, dry_run=dry_run)


@marimba.command("process")
def process_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process the MarImBA collection based on the instrument specification.
    """

    run_command("run_process", collection_path, instrument_id, deployment_name, extra, dry_run=dry_run)


@marimba.command("rename")
def rename_command(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files based on the instrument specification.
    """

    run_command("run_rename", collection_path, instrument_id, deployment_name, extra, dry_run=dry_run)


@marimba.command()
def report(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
    deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Generate reports for a MarImBA collection or instrument.
    """

    run_command("report", collection_path, instrument_id, deployment_name, extra, dry_run=dry_run)


if __name__ == "__main__":
    marimba()
