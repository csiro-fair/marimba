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
    logger.info(f"Initialised MarImBA CLI v{__version__}")


@marimba.command()
def catalog(
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
        'catalog',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run,
        exiftool_path=exiftool_path,
        file_extension=file_extension,
        glob_path=glob_path,
        overwrite=overwrite
    )


@marimba.command()
def metadata(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
        deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
        extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process metadata including merging nav data files, writing metadata into image EXIF tags, and writing iFDO files.
    """

    run_command(
        'metadata',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run
    )


@marimba.command()
def package(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
        deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
        extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Package up a MarImBA collection ready for distribution.
    """
    run_command(
        'package',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run
    )


@marimba.command()
def process(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
        deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
        extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process the MarImBA collection based on the instrument specification.
    """

    run_command(
        'process',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run
    )


@marimba.command()
def rename(
        collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
        instrument_id: str = typer.Argument(None, help="MarImBA instrument ID for targeted processing."),
        deployment_name: str = typer.Argument(None, help="MarImBA deployment name for targeted processing."),
        extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files based on the instrument specification.
    """

    run_command(
        'rename',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run
    )


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

    run_command(
        'report',
        collection_path,
        instrument_id,
        deployment_name,
        extra,
        dry_run=dry_run
    )


if __name__ == "__main__":
    marimba()
