#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import List, Optional

import typer
from rich import print

import marimba.commands.new as new
from marimba.utils.log import LogLevel, get_logger, get_rich_handler
from marimba.utils.rich import MARIMBA, error_panel, success_panel
from marimba.wrappers.project import ProjectWrapper

__author__ = "Marimba Development Team"
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
    name="Marimba - Marine Imagery Batch Actions",
    help="""Marimba - Marine Imagery Batch Actions\n
        A Python CLI for batch processing, transforming and FAIR-ising large volumes of marine imagery.""",
    short_help="Marimba - Marine Imagery Batch Actions",
    no_args_is_help=True,
)

marimba.add_typer(new.app, name="new")

logger = get_logger(__name__)


@marimba.callback()
def global_options(
    level: LogLevel = typer.Option(LogLevel.INFO, help="Logging level."),
):
    """
    Global options for Marimba CLI.
    """
    get_rich_handler().setLevel(logging.getLevelName(level.value))
    logger.info(f"Initialised Marimba CLI v{__version__}")


# @marimba.command("catalog")
# def catalog_command(
#     pipeline_name: str = typer.Argument(None, help="Marimba pipeline name for targeted processing."),
#     deployment_name: str = typer.Argument(None, help="Marimba deployment name for targeted processing."),
#     project_dir: Optional[Path] = typer.Option(
#         None,
#         help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
#     ),
#     extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
#     dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
#     exiftool_path: str = typer.Option("exiftool", help="Path to exiftool"),
#     file_extension: str = typer.Option("JPG", help="extension to catalog"),
#     glob_path: str = typer.Option("**", help="masked used in glob"),
#     overwrite: bool = typer.Option(False, help="Overwrite output files if they contain the same filename."),
# ):
#     """
#     Create an exif catalog of files stored in .exif_{extension}.
#     """
#     project_dir = new.find_project_dir_or_exit(project_dir)
#     project_wrapper = ProjectWrapper(project_dir)

#     project_wrapper.run_command(
#         "run_catalog",
#         pipeline_name,
#         deployment_name,
#         dry_run=dry_run,
#         exiftool_path=exiftool_path,
#         file_extension=file_extension,
#         glob_path=glob_path,
#         overwrite=overwrite,
#     )


@marimba.command("init")
def init_command(
    pipeline_name: str = typer.Argument(..., help="Marimba pipeline name."),
    card_paths: list[str] = typer.Argument(None, help="List of paths to SD cards to be initialised."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
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
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.run_command(
        "run_init",
        pipeline_name,
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
    pipeline_name: str = typer.Argument(..., help="Marimba pipeline name."),
    card_paths: list[str] = typer.Argument(None, help="List of paths to SD cards to be initialised."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
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
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.run_command(
        "run_import",
        pipeline_name,
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


@marimba.command("metadata")
def metadata_command(
    pipeline_name: str = typer.Argument(None, help="Marimba pipeline name for targeted processing."),
    deployment_name: str = typer.Argument(None, help="Marimba deployment name for targeted processing."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process metadata including merging nav data files, writing metadata into image EXIF tags, and writing iFDO files.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.run_command("run_metadata", pipeline_name, deployment_name, extra, dry_run=dry_run)


@marimba.command("package")
def package_command(
    package_name: str = typer.Argument(..., help="Marimba package name."),
    pipeline_name: str = typer.Argument(..., help="Marimba pipeline name to package."),
    deployment_names: Optional[List[str]] = typer.Argument(
        None, help="Marimba deployment names to package. If none are specified, all deployments will be packaged together."
    ),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
    copy: bool = typer.Option(True, help="Copy files to package directory. Set to False to move files instead."),
    extra: List[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Package up a Marimba collection ready for distribution.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    if not deployment_names:  # If no deployment names are specified, package all deployments
        deployment_names = list(project_wrapper.deployments.keys())

    try:
        # Compose the dataset
        ifdo, path_mapping = project_wrapper.compose(pipeline_name, deployment_names, extra, dry_run=dry_run)

        # Package it
        package_wrapper = project_wrapper.package(package_name, ifdo, path_mapping, copy=copy)
    except Exception as e:
        logger.error(e)
        print(error_panel(str(e)))
        raise typer.Exit()

    print(success_panel(f"Created {MARIMBA} package {package_name} in {package_wrapper.root_dir}"))


@marimba.command("process")
def process_command(
    pipeline_name: str = typer.Argument(None, help="Marimba pipeline name for targeted processing."),
    deployment_name: str = typer.Argument(None, help="Marimba deployment name for targeted processing."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Process the Marimba collection based on the pipeline specification.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.run_command("run_process", pipeline_name, deployment_name, extra, dry_run=dry_run)


@marimba.command("rename")
def rename_command(
    pipeline_name: str = typer.Argument(None, help="Marimba pipeline name for targeted processing."),
    deployment_name: str = typer.Argument(None, help="Marimba deployment name for targeted processing."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Rename files based on the pipeline specification.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.run_command("run_rename", pipeline_name, deployment_name, extra, dry_run=dry_run)


@marimba.command("update")
def update_command(
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Update (pull) all Marimba pipelines.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.update_pipelines()


if __name__ == "__main__":
    marimba()
