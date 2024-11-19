"""
Marimba CLI Module - create new Marimba project components.

This module provides the Marimba command line interface for creating and configuring new Marimba projects,
pipelines, collections, and distribution targets. It uses Typer for defining the CLI commands and Rich for
formatting the output.

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
    - find_project_dir: Finds the project root directory from a given path.
    - find_project_dir_or_exit: Finds the project root directory or exits with an error.
    - project: Creates a new Marimba project.
    - pipeline: Creates and configures a new Marimba pipeline in a project.
    - collection: Creates and configures a new Marimba collection in a project.
    - target: Creates and configures a new distribution target in a project.
"""

import json
from os import R_OK, access
from pathlib import Path

import typer
from rich import print

from marimba.core.utils.constants import PROJECT_DIR_HELP
from marimba.core.utils.log import get_logger
from marimba.core.utils.rich import (
    MARIMBA,
    error_panel,
    format_command,
    format_entity,
    success_panel,
)
from marimba.core.wrappers.project import ProjectWrapper
from marimba.core.wrappers.target import DistributionTargetWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new Marimba project, pipeline or collection.",
    no_args_is_help=True,
)


def find_project_dir(path: str | Path) -> Path | None:
    """
    Find the project root directory from a given path.

    Args:
        path: The path to start searching from.

    Returns:
        The project root directory, or None if no project root directory was found.
    """
    path = Path(path)
    while access(path, R_OK) and path != path.parent:
        if (path / ".marimba").is_dir():
            return path
        path = path.parent
    return None


def find_project_dir_or_exit(project_dir: str | Path | None = None) -> Path:
    """
    Find the project root directory from a given path, or exit with an error if no project root directory was found.

    Args:
        project_dir: The path to start searching from. If None, the current working directory will be used.

    Returns:
        The project root directory.

    Raises:
        typer.Exit: If no project root directory was found.
    """
    # Convert project_dir to Path if it is not None, otherwise use current working directory
    project_dir = Path(project_dir) if project_dir else Path.cwd()

    # Attempt to find the project directory
    found_project_dir = find_project_dir(project_dir)

    # Check if a project directory was found
    if found_project_dir is None:
        error_message = f"Could not find a {MARIMBA} project."
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    return found_project_dir


@app.command()
def project(
    project_dir: Path = typer.Argument(..., help="Root path to create new Marimba project."),
) -> None:
    """
    Create a new Marimba project.
    """
    logger.info(f"Executing the {MARIMBA} {format_command('new project')} command.")

    # Try to create the new project
    try:
        project_wrapper = ProjectWrapper.create(project_dir)
    except FileExistsError as e:
        error_message = f'A {MARIMBA} {format_entity("project")} already exists at: "{project_dir}"'
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e

    print(success_panel(f'Created new {MARIMBA} {format_entity("project")} at: "{project_wrapper.root_dir}"'))


@app.command()
def pipeline(
    pipeline_name: str = typer.Argument(..., help="Name of the pipeline."),
    url: str = typer.Argument(..., help="URL of the pipeline git repository."),
    project_dir: Path = typer.Option(
        None,
        help=PROJECT_DIR_HELP,
    ),
    config: str = typer.Option(
        None,
        help="A custom configuration in JSON format to be merged with the prompted pipeline configuration.",
    ),
) -> None:
    """
    Create and configure a new Marimba pipeline in a project.
    """
    try:
        config_dict = json.loads(config) if config else {}
    except json.JSONDecodeError as e:
        error_message = f"Error parsing configuration JSON: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit from e

    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('new pipeline')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Create the pipeline
        pipeline_wrapper = project_wrapper.create_pipeline(pipeline_name, url)
    except ProjectWrapper.InvalidNameError as e:
        error_message = f"Invalid pipeline name: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e
    except Exception as e:
        error_message = f"Could not create pipeline: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e

    # Configure the pipeline from the command line
    pipeline_config = pipeline_wrapper.prompt_pipeline_config(config_dict)
    pipeline_wrapper.save_config(pipeline_config)

    print(
        success_panel(
            f'Created new {MARIMBA} {format_entity("pipeline")} "{pipeline_name}" at: "{pipeline_wrapper.root_dir}"',
        ),
    )


@app.command()
def collection(
    collection_name: str = typer.Argument(..., help="Name of the collection."),
    parent_collection_name: str | None = typer.Argument(
        None,
        help="Name of the parent collection. If unspecified, use the last collection.",
    ),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
    config: str = typer.Option(
        None,
        help="A custom configuration in JSON format to be merged with the prompted collection configuration.",
    ),
) -> None:
    """
    Create and configure a new Marimba collection in a project.
    """
    try:
        config_dict = json.loads(config) if config else {}
    except json.JSONDecodeError as e:
        error_message = f"Error parsing configuration JSON: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit from e

    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('new collection')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Configure the collection from the resolved schema
        collection_config = project_wrapper.prompt_collection_config(
            parent_collection_name=parent_collection_name,
            config=config_dict,
        )

        # Create the collection
        collection_wrapper = project_wrapper.create_collection(collection_name, collection_config)
    except ProjectWrapper.InvalidNameError as e:
        logger.exception(e)
        print(error_panel(f"Invalid collection name: {e}"))
        raise typer.Exit(code=1) from e
    except ProjectWrapper.NoSuchCollectionError as e:
        logger.exception(e)
        print(error_panel(f"No such parent collection: {e}"))
        raise typer.Exit(code=1) from e
    except ProjectWrapper.CreateCollectionError as e:
        logger.exception(e)
        print(error_panel(f"Could not create collection: {e}"))
        raise typer.Exit(code=1) from e
    except Exception as e:
        error_message = f"Could not create collection: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e

    print(
        success_panel(
            f'Created new {MARIMBA} {format_entity("collection")} "{collection_name}" at: '
            f'"{collection_wrapper.root_dir}"',
        ),
    )


@app.command()
def target(
    target_name: str = typer.Argument(..., help="Name of the distribution target."),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
) -> None:
    """
    Create and configure a new distribution target in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('new target')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Prompt for the target config
        target_type, target_config = DistributionTargetWrapper.prompt_target()

        # Create the distribution target
        distribution_target_wrapper = project_wrapper.create_target(target_name, target_type, target_config)
    except ProjectWrapper.InvalidNameError as e:
        error_message = f"Invalid target name: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e
    except FileExistsError as e:
        error_message = f'A {MARIMBA} {format_entity("target")} already exists at: "{project_dir / target_name}"'
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e
    except Exception as e:
        error_message = f"Could not create target: {e}"
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e

    print(
        success_panel(
            f'Created new {MARIMBA} {format_entity("target")} "{target_name}" at: '
            f'"{distribution_target_wrapper.config_path}"',
        ),
    )
