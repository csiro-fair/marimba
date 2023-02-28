#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from logging.config import dictConfig

import typer

from commands.convert import convert_files
from commands.chunk import chunk_files
from commands.extract import extract_frames
from commands.copy import copy_files
from commands.config import create_config, ConfigLevel
from commands.qc import run_qc
from commands.metadata import merge_metadata
from commands.rename import rename_files

parent_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_directory)

from utils.logger_config import LoggerConfig

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"

dictConfig(LoggerConfig.richConfig)

marimba = typer.Typer(
    name="MarImBA - Marine Imagery Batch Actions",
    no_args_is_help=True,
    help="""MarImBA - Marine Imagery Batch Actions\n
        A Python CLI for batch processing, transforming and FAIR-ising large volumes of marine imagery.""",
    short_help="MarImBA - Marine Imagery Batch Actions",
)


@marimba.command()
def qc(
        source_path: str = typer.Argument(..., help="Source path of files."),
        recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
):
    """
    Run quality control code on files to check for anomalies and generate datasets statistics.
    """

    run_qc(source_path)


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
        source_path: str = typer.Argument(..., help="Source path to rename files."),
        config_path: str = typer.Argument(None, help="Optional path to minimal survey/deployment config file. Source directory will be searched for valid config file if not provided."),
        destination_path: str = typer.Option(None, help="Destination path to output files."),
        recursive: bool = typer.Option(True, help="Recursively process entire directory structure."),
        overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
        dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files and construct folder structure based on instrument specification file identified in the input config.
    """

    rename_files(source_path, config_path, destination_path, recursive, overwrite, dry_run)


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

    chunk_files(source_path, destination_path, chunk_length, recursive, overwrite, dry_run)


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


if __name__ == "__main__":
    marimba()
