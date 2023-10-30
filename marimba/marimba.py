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


@marimba.command("import")
def import_command(
    deployment_name: str = typer.Argument(..., help="Marimba deployment name for targeted processing."),
    source_paths: List[Path] = typer.Argument(..., help="Paths to source files/directories to provide for import."),
    parent_deployment_name: Optional[str] = typer.Option(None, help="Name of the parent deployment. If unspecified, use the last deployment."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
    overwrite: bool = typer.Option(False, help="Overwrite an existing deployment with the same name."),
    extra: list[str] = typer.Option([], help="Extra key-value pass-through arguments."),
    dry_run: bool = typer.Option(False, help="Execute the command and print logging to the terminal, but do not change any files."),
):
    """
    Import data in a source directory into a new or existing Marimba deployment.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    # Get the deployment (create if appropriate)
    deployment_wrapper = project_wrapper.deployment_wrappers.get(deployment_name, None)
    if deployment_wrapper is None:
        try:
            deployment_config = project_wrapper.prompt_deployment_config(parent_deployment_name=parent_deployment_name)
            deployment_wrapper = project_wrapper.create_deployment(deployment_name, deployment_config)
        except ProjectWrapper.NameError as e:
            error_message = f"Invalid deployment name: {e}"
            logger.error(error_message)
            print(error_panel(error_message))
            raise typer.Exit()
    elif not overwrite:
        error_message = f"Deployment {deployment_name} already exists, and the overwrite flag is not set."
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    # Run the import
    try:
        project_wrapper.run_import(deployment_name, source_paths, extra_args=extra, dry_run=dry_run)
    except Exception as e:
        error_message = f"Error during import: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    pretty_source_paths = "\n".join([f"  - {source_path.resolve().absolute()}" for source_path in source_paths])
    print(success_panel(f"Imported data to deployment {deployment_name} from source paths:\n{pretty_source_paths}"))


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
    # pipeline_name: str = typer.Argument(..., help="Marimba pipeline name to package."),
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
        deployment_names = list(project_wrapper.deployment_wrappers.keys())

    try:
        # Compose the dataset
        dataset_mapping = project_wrapper.compose(deployment_names, extra, dry_run=dry_run)

        # Package it
        package_wrapper = project_wrapper.package(package_name, dataset_mapping, copy=copy)
    except ProjectWrapper.NoSuchPipelineError as e:
        error_message = f"No such pipeline: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()
    except ProjectWrapper.NoSuchDeploymentError as e:
        error_message = f"No such deployment: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()
    except FileExistsError as e:
        error_message = f"Package already exists: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()
    # except Exception as e:
    #     logger.error(e)
    #     print(error_panel(f"Could not package collection: {e}"))
    #     raise typer.Exit()

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

    try:
        project_wrapper.update_pipelines()
    except Exception as e:
        logger.error(e)
        print(error_panel(f"Could not update pipelines: {e}"))
        raise typer.Exit()


@marimba.command("install")
def install_command(
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Install Python dependencies from requirements.txt files defined by a project's pipelines.
    """
    project_dir = new.find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    project_wrapper.install_pipelines()


if __name__ == "__main__":
    marimba()
