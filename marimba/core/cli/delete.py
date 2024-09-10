"""
Marimba CLI Module - delete and manage Marimba project components.

This module provides a command-line interface for deleting Marimba projects, pipelines, collections, and
distribution targets. It utilizes Typer for defining CLI commands and Rich for formatting output.


Imports:
    - os: Provides access to operating system functionality.
    - pathlib: Provides classes for working with file paths.
    - typing: Provides type hinting classes.
    - typer: A library for building command line interfaces.
    - rich: A library for rich text formatting in the terminal.
    - marimba.core.utils.constants: Provides constants used in the Marimba project.
    - marimba.core.utils.log: Provides logging functionality.
    - marimba.core.utils.prompt: Provides functionality for prompting the user for input.
    - marimba.core.utils.rich: Provides utility functions for formatting output using Rich.
    - marimba.core.wrappers.project: Provides a wrapper class for working with Marimba projects.
    - marimba.core.wrappers.target: Provides a wrapper class for working with Marimba distribution targets.

Classes:
    - ProjectWrapper: A wrapper class for working with Marimba projects.
        - NameError: Raised when an invalid name is provided for a project entity.
        - NoSuchCollectionError: Raised when a specified parent collection does not exist.
        - CreateCollectionError: Raised when an error occurs while creating a collection.
    - DistributionTargetWrapper: A wrapper class for working with Marimba distribution targets.

Functions:
    - project: Creates a new Marimba project.
    - pipeline: Creates and configures a new Marimba pipeline in a project.
    - collection: Creates and configures a new Marimba collection in a project.
    - target: Creates and configures a new distribution target in a project.
"""

from pathlib import Path
from typing import Optional

import typer
from rich import print  # noqa: A004

from marimba.core.utils.constants import PROJECT_DIR_HELP
from marimba.core.utils.log import get_logger
from marimba.core.utils.paths import find_project_dir_or_exit
from marimba.core.utils.rich import MARIMBA, error_panel, format_command, format_entity, success_panel
from marimba.core.wrappers.project import ProjectWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Delete a Marimba project, pipeline or collection.",
    no_args_is_help=True,
)


@app.command()
def project(
    project_dir: Optional[Path] = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """
    Delete a Marimba project.
    """
    logger.info(f"Executing the {MARIMBA} {format_command('delete project')} command.")

    # Try to create the new project
    try:
        project_dir = find_project_dir_or_exit(project_dir)
        project_wrapper = ProjectWrapper(project_dir, dry_run=dry_run)
        root_path = project_wrapper.delete_project()
        logger.info(f'Project Deleted {MARIMBA} {format_entity("project")} "{project_wrapper.root_dir}"')
    except ProjectWrapper.InvalidStructureError:
        error_message = f'A {MARIMBA} {format_entity("project")} not valid project: "{project_dir}"'
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)
    print(success_panel(f'Deleted {MARIMBA} {format_entity("project")} "{root_path}"'))


@app.command()
def pipeline(
    pipeline_name: str = typer.Argument(..., help="Name of the pipeline."),
    project_dir: Optional[Path] = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False, help="Execute the command and print logging to the terminal, but do not change any files."
    ),
) -> None:
    """
    Delete a Marimba pipeline from a project.
    """
    logger.info(f"Executing the {MARIMBA} {format_command('delete pipeline')} command.")

    try:
        # iterate through the pipelines
        project_dir = find_project_dir_or_exit(project_dir)
        project_wrapper = ProjectWrapper(project_dir)
        root_path = project_wrapper.delete_pipeline(pipeline_name, dry_run=dry_run)
    except ProjectWrapper.InvalidNameError as e:
        error_message = f"Invalid pipeline name: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)
    except Exception as e:
        error_message = f"Could not delete pipeline: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)
    print(success_panel(f'Deleted {MARIMBA} {format_entity("pipeline")} "{pipeline_name}" at: "{root_path}"'))


@app.command()
def collection(
    collection_name: str = typer.Argument(..., help="Name of the collection to delete"),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False, help="Execute the command and print logging to the terminal, but do not change any files."
    ),
) -> None:
    """
    Delete Marimba collection in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)
    logger.info(f"Executing the {MARIMBA} {format_command('delete collection')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)
        root_path = project_wrapper.delete_collection(collection_name, dry_run=dry_run)
    except ProjectWrapper.InvalidNameError as e:
        logger.error(e)
        print(error_panel(f"Invalid collection name: {e}"))
        raise typer.Exit(code=1)
    except ProjectWrapper.NoSuchCollectionError as e:
        logger.error(e)
        print(error_panel(f"No such parent collection: {e}"))
        raise typer.Exit(code=1)
    except Exception as e:
        error_message = f"Could not delete collection: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    print(success_panel(f'Deleted {MARIMBA} {format_entity("collection")} "{collection_name}" at: ' f'"{root_path}"'))


@app.command()
def target(
    target_name: str = typer.Argument(..., help="Name of the distribution target."),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
) -> None:
    """
    Delete distribution target from a Marimba project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('delete target')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)
        project_wrapper.delete_target(target_name)
    except ProjectWrapper.InvalidNameError as e:
        error_message = f"Invalid target name: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)
    except FileExistsError:
        error_message = f'A {MARIMBA} {format_entity("target")} not found: "{project_dir / target_name}"'
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)
    except Exception as e:
        error_message = f"Could not delete target: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    print(
        success_panel(
            f'Deleted {MARIMBA} {format_entity("target")} "{target_name}" at: ' f'"{project_wrapper.targets_dir}"'
        )
    )
