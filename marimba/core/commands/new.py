from os import R_OK, access
from pathlib import Path
from typing import Optional, Union

import typer
from rich import print

from marimba.core.utils.log import get_logger
from marimba.core.utils.prompt import prompt_schema
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
    # If the project directory is specified, check it and use it
    if project_dir is not None:
        return find_project_dir(project_dir)

    # Otherwise, search for a project directory in the current working directory and its parents
    project_dir = find_project_dir(Path.cwd())

    # If no project directory was found, exit with an error
    if project_dir is None:
        error_message = f"Could not find a {MARIMBA} project."
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    return project_dir


@app.command()
def project(
    project_dir: Path = typer.Argument(..., help="Root path to create new Marimba project."),
):
    """
    Create a new Marimba project.
    """
    logger.info(f"Executing the {MARIMBA} {format_command('new project')} command.")

    # Try to create the new project
    try:
        project_wrapper = ProjectWrapper.create(project_dir)
    except FileExistsError:
        error_message = f'A {MARIMBA} {format_entity("project")} already exists at: "{project_dir}"'
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} {format_entity("project")} at: "{project_wrapper.root_dir}"'))


@app.command()
def pipeline(
    pipeline_name: str = typer.Argument(..., help="Name of the pipeline."),
    url: str = typer.Argument(..., help="URL of the pipeline git repository."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create and configure a new Marimba pipeline in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('new pipeline')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Create the pipeline
        pipeline_wrapper = project_wrapper.create_pipeline(pipeline_name, url)
    except ProjectWrapper.NameError as e:
        error_message = f"Invalid pipeline name: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()
    except Exception as e:
        error_message = f"Could not create pipeline: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} {format_entity("pipeline")} "{pipeline_name}" at: "{pipeline_wrapper.root_dir}"'))

    # Configure the pipeline from the command line
    pipeline = pipeline_wrapper.get_instance()
    pipeline_config_schema = pipeline.get_pipeline_config_schema()
    pipeline_config = prompt_schema(pipeline_config_schema)
    pipeline_wrapper.save_config(pipeline_config)


@app.command()
def collection(
    collection_name: str = typer.Argument(..., help="Name of the collection."),
    parent_collection_name: Optional[str] = typer.Argument(None, help="Name of the parent collection. If unspecified, use the last collection."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create and configure a new Marimba collection in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} {format_command('new collection')} command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Configure the collection from the resolved schema
        collection_config = project_wrapper.prompt_collection_config(parent_collection_name=parent_collection_name)

        # Create the collection
        collection_wrapper = project_wrapper.create_collection(collection_name, collection_config)
    except ProjectWrapper.NameError as e:
        logger.error(e)
        print(error_panel(f"Invalid collection name: {e}"))
        raise typer.Exit()
    except ProjectWrapper.NoSuchCollectionError as e:
        logger.error(e)
        print(error_panel(f"No such parent collection: {e}"))
        raise typer.Exit()
    except ProjectWrapper.CreateCollectionError as e:
        logger.error(e)
        print(error_panel(f"Could not create collection: {e}"))
        raise typer.Exit()
    except Exception as e:
        error_message = f"Could not create collection: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} {format_entity("collection")} "{collection_name}" at: "{collection_wrapper.root_dir}"'))


@app.command()
def target(
    target_name: str = typer.Argument(..., help="Name of the distribution target."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
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
    except ProjectWrapper.NameError as e:
        error_message = f"Invalid target name: {e}"
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()
    except FileExistsError:
        error_message = f'A {MARIMBA} {format_entity("target")} already exists at: "{distribution_target_wrapper.config_path}"'
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} {format_entity("target")} "{target_name}" at: "{distribution_target_wrapper.config_path}"'))
