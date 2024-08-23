"""
Marimba path utilities Module.

This module provides functions for working with the directory structure of a marimba project
formatting the output.

Imports:
    - os: Provides access to operating system functionality.
    - pathlib: Provides classes for working with file paths.
    - typer: A library for building command line interfaces.
    - rich: A library for rich text formatting in the terminal.
    - marimba.core.utils.log: Provides logging functionality.
    - marimba.core.utils.rich: Provides utility functions for formatting output using Rich.


Functions:
    - find_project_dir: Finds the project root directory from a given path.
    - find_project_dir_or_exit: Finds the project root directory or exits with an error.
"""


from os import R_OK, access
from pathlib import Path
from typing import Optional, Union
from marimba.core.utils.log import get_logger
import typer
from marimba.core.utils.rich import MARIMBA, error_panel,format_entity
import shutil

logger = get_logger(__name__)


def find_project_dir(path: Union[str, Path]) -> Optional[Path]:
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

def remove_all_subdirectories(directory,entity,remove_head,dry_run):
    """
    Find all the subdirectories of this directory and delete them

    Args:
        directory: The path to start searching from.
        entity : the type of operation
        remove_head: remove the head directory
        dry_run: if true don't delete

    
    Raises:
        typer.Exit: If no project root directory was found.
    """    
    dir_path = Path(directory)
    if dir_path.is_dir():
        for sub_dir in dir_path.iterdir():
            if sub_dir.is_dir():
                if not dry_run:
                    shutil.rmtree(sub_dir)
                logger.info(f'Deleting {MARIMBA} {format_entity(entity)} at: "{sub_dir}"')
        if remove_head:
            logger.info(f'Deleting {MARIMBA} {format_entity(entity)} at: "{dir_path}"')
            if not dry_run:
                shutil.rmtree(dir_path)
                