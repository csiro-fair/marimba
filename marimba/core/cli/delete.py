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

from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import typer
from rich import print
from rich.progress import Progress, SpinnerColumn

from marimba.core.utils.constants import PROJECT_DIR_HELP
from marimba.core.utils.log import get_logger
from marimba.core.utils.paths import find_project_dir_or_exit
from marimba.core.utils.rich import (
    MARIMBA,
    error_panel,
    format_command,
    format_entity,
    get_default_columns,
    success_panel,
)
from marimba.core.wrappers.project import ProjectWrapper

logger = get_logger(__name__)
app = typer.Typer(
    help="Delete a Marimba project, pipeline or collection.",
    no_args_is_help=True,
)

T = TypeVar("T")


def batch_delete_operation(
    items: list[str],
    delete_func: Callable[[str, bool], T],
    entity_type: str,
    description: str,
    dry_run: bool,
) -> tuple[list[tuple[str, T]], list[tuple[str, str]]]:
    """
    Generic function to handle batch deletion operations.

    Args:
        items: List of item names to delete
        delete_func: Function that performs the deletion
        entity_type: Type of entity being deleted (for logging)
        description: Progress bar description
        dry_run: Whether to perform a dry run

    Returns:
        Tuple of (successful operations, failed operations)
    """
    success_items: list[tuple[str, T]] = []
    errors: list[tuple[str, str]] = []

    def attempt_delete(item_name: str) -> tuple[str, T | None, str | None]:
        try:
            result = delete_func(item_name, dry_run)
        except Exception as e:
            logger.exception(f"Error deleting {entity_type} {item_name}")
            return item_name, None, str(e)
        else:
            return item_name, result, None

    with Progress(
        SpinnerColumn(),
        *get_default_columns(),
        transient=True,
    ) as progress:
        tracked_items = progress.track(items, description=description)
        for item_name, result, error in (attempt_delete(item) for item in tracked_items):
            if error is None and result is not None:
                success_items.append((item_name, result))
            else:
                error_msg = error if error is not None else "Unexpected None result"
                errors.append((item_name, error_msg))

    return success_items, errors


def print_results(
    success_items: list[tuple[str, Path]],
    errors: list[tuple[str, str]],
    entity_type: str,
) -> None:
    """Print the results of a batch deletion operation."""
    if success_items:
        for name, path in success_items:
            print(success_panel(f'Deleted {MARIMBA} {format_entity(entity_type)} "{name}" at: "{path}"'))

    if errors:
        for name, error_msg in errors:
            print(error_panel(f'Failed to delete {entity_type} "{name}": {error_msg}'))
        raise typer.Exit(code=1)


@app.command()
def project(
    project_dir: Path | None = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """Delete a Marimba project."""
    logger.info(f"Executing the {MARIMBA} {format_command('delete project')} command.")

    try:
        project_dir = find_project_dir_or_exit(project_dir)
        project_wrapper = ProjectWrapper(project_dir, dry_run=dry_run)
        root_path = project_wrapper.delete_project()
        logger.info(f'Project Deleted {MARIMBA} {format_entity("project")} "{project_wrapper.root_dir}"')
        print(success_panel(f'Deleted {MARIMBA} {format_entity("project")} "{root_path}"'))
    except ProjectWrapper.InvalidStructureError as e:
        error_message = f'A {MARIMBA} {format_entity("project")} not valid project: "{project_dir}"'
        logger.exception(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1) from e


@app.command()
def pipeline(
    pipeline_names: list[str] = typer.Argument(..., help="Names of the pipelines to delete."),
    project_dir: Path | None = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """Delete one or more Marimba pipelines from a project."""
    logger.info(f"Executing the {MARIMBA} {format_command('delete pipeline')} command.")
    project_dir = find_project_dir_or_exit(project_dir)
    project_wrapper = ProjectWrapper(project_dir)

    success_items, errors = batch_delete_operation(
        pipeline_names,
        project_wrapper.delete_pipeline,
        "pipeline",
        "Deleting pipelines...",
        dry_run,
    )

    print_results(success_items, errors, "pipeline")


@app.command()
def collection(
    collection_names: list[str] = typer.Argument(..., help="Names of the collections to delete"),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """Delete one or more Marimba collections in a project."""
    project_dir = find_project_dir_or_exit(project_dir)
    logger.info(f"Executing the {MARIMBA} {format_command('delete collection')} command.")
    project_wrapper = ProjectWrapper(project_dir)

    success_items, errors = batch_delete_operation(
        collection_names,
        project_wrapper.delete_collection,
        "collection",
        "Deleting collections...",
        dry_run,
    )

    print_results(success_items, errors, "collection")


@app.command()
def target(
    target_names: list[str] = typer.Argument(..., help="Names of the distribution targets to delete."),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """Delete one or more distribution targets from a Marimba project."""
    project_dir = find_project_dir_or_exit(project_dir)
    logger.info(f"Executing the {MARIMBA} {format_command('delete target')} command.")
    project_wrapper = ProjectWrapper(project_dir)

    success_items, errors = batch_delete_operation(
        target_names,
        project_wrapper.delete_target,
        "target",
        "Deleting targets...",
        dry_run,
    )

    print_results(success_items, errors, "target")


@app.command()
def dataset(
    dataset_names: list[str] = typer.Argument(..., help="Names of the datasets to delete"),
    project_dir: Path = typer.Option(None, help=PROJECT_DIR_HELP),
    dry_run: bool = typer.Option(
        False,
        help="Execute the command and print logging to the terminal, but do not change any files.",
    ),
) -> None:
    """Delete one or more datasets from a Marimba project."""
    project_dir = find_project_dir_or_exit(project_dir)
    logger.info(f"Executing the {MARIMBA} {format_command('delete dataset')} command.")
    project_wrapper = ProjectWrapper(project_dir)

    success_items, errors = batch_delete_operation(
        dataset_names,
        project_wrapper.delete_dataset,
        "dataset",
        "Deleting datasets...",
        dry_run,
    )

    print_results(success_items, errors, "dataset")
