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
    - project: Creates a new Marimba project.
    - pipeline: Creates and configures a new Marimba pipeline in a project.
    - collection: Creates and configures a new Marimba collection in a project.
    - target: Creates and configures a new distribution target in a project.
"""

import json
from pathlib import Path

import typer
from rich import print

from marimba.core.utils.constants import PROJECT_DIR_HELP
from marimba.core.utils.log import get_logger
from marimba.core.utils.paths import find_project_dir_or_exit
from marimba.core.utils.rich import (
    MARIMBA,
    error_panel,
    format_command,
    format_entity,
    success_panel,
    warning_panel,
)
from marimba.core.wrappers.project import ProjectWrapper
from marimba.core.wrappers.target import DistributionTargetWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new Marimba project, pipeline or collection.",
    no_args_is_help=True,
)


@app.command()
def project(
    project_dir: Path = typer.Argument(..., help="Root path to create new Marimba project."),
) -> None:
    """
    Create a new Marimba project.
    """
    logger.info(f"Executing the {MARIMBA} {format_command('new project')} command")

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

    logger.info(f"Executing the {MARIMBA} {format_command('new pipeline')} command")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Create the pipeline
        pipeline_wrapper = project_wrapper.create_pipeline(pipeline_name, url, config_dict)
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

    # Use warning panel if no pipeline implementation found
    if pipeline_wrapper is None:
        print(
            warning_panel(
                f'Repository cloned at "{pipeline_wrapper.root_dir}", but no Pipeline implementation found. '
                f'Add a Pipeline implementation before using "{pipeline_name}" to process data.',
            ),
        )
    else:
        print(
            success_panel(
                f'Created new {MARIMBA} {format_entity("pipeline")} "{pipeline_name}" at: '
                f'"{pipeline_wrapper.root_dir}"',
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

    logger.info(f"Executing the {MARIMBA} {format_command('new collection')} command")

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

    logger.info(f"Executing the {MARIMBA} {format_command('new target')} command")

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
