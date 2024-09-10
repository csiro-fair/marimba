"""
Marimba Path Utilities.

This module provides utility functions to work with the directory structure of a Marimba project, including locating
the project root directory and managing subdirectories.


Imports:
    - os: Provides access to operating system functionality.
    - pathlib: Provides classes for working with file paths.
    - typer: A library for building command line interfaces.
    - rich: A library for rich text formatting in the terminal.
    - marimba.core.utils.log: Provides logging functionality.
    - marimba.core.utils.rich: Provides utility functions for formatting output using Rich.

Functions:
    - find_project_dir: Locates the project root directory starting from a specified path.
    - find_project_dir_or_exit: Locates the project root directory or exits with an error if not found.
    - remove_all_subdirectories: Deletes all subdirectories within a specified directory, with optional dry-run and root
    directory removal features.
"""

import shutil
from os import R_OK, access
from pathlib import Path
from typing import Optional, Union

import typer

from marimba.core.utils.log import get_logger
from marimba.core.utils.rich import MARIMBA, error_panel, format_entity

logger = get_logger(__name__)


def find_project_dir(path: Union[str, Path]) -> Optional[Path]:
    """
    Locate the project root directory starting from a specified path.

    Args:
        path (Union[str, Path]): The starting path for the search.

    Returns:
        Optional[Path]: The project root directory, or None if not found.
    """
    path = Path(path)
    while access(path, R_OK) and path != path.parent:
        if (path / ".marimba").is_dir():
            return path
        path = path.parent
    return None


def find_project_dir_or_exit(project_dir: Optional[Union[str, Path]] = None) -> Path:
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
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    return found_project_dir


def remove_directory_tree(directory: Union[str, Path], entity: str, dry_run: bool) -> None:
    """
    Recursively delete the provided directory and all of its contents.

    Args:
        directory (Union[str, Path]): The directory to delete.
        entity (str): A description of the operation being performed.
        dry_run (bool): If True, only logs the deletion without actually performing it.

    Raises:
        typer.Exit: If the specified directory is not valid or an error occurs during deletion.
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        error_message = f"Invalid directory: {dir_path}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    try:
        if not dry_run:
            shutil.rmtree(dir_path)
        logger.info(f'Deleting {MARIMBA} {format_entity(entity)} at: "{dir_path}"')

    except Exception as e:
        error_message = f"Error occurred while deleting the directory: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit(code=1)

    logger.info("Successfully deleted directory.")
