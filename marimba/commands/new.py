from os import R_OK, access
from pathlib import Path
from typing import Optional, Union

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.log import get_logger
from marimba.utils.prompt import prompt_schema
from marimba.utils.rich import MARIMBA, error_panel, success_panel
from marimba.wrappers.project import ProjectWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new Marimba collection, pipeline or deployment.",
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
    logger.info(f"Executing the {MARIMBA} [steel_blue3]new project[/steel_blue3] command.")

    # Try to create the new project
    try:
        project_wrapper = ProjectWrapper.create(project_dir)
    except FileExistsError:
        error_message = f'A {MARIMBA} [light_pink3]project[/light_pink3] already exists at: "{project_dir}"'
        logger.error(error_message)
        print(error_panel(error_message))
        raise typer.Exit()

    print(
        Panel(
            f'Created new {MARIMBA} [light_pink3]project[/light_pink3] at: "{project_wrapper.root_dir}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )


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
    Create a new Marimba pipeline in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} [steel_blue3]new pipeline[/steel_blue3] command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Create the pipeline
        pipeline_wrapper = project_wrapper.create_pipeline(pipeline_name, url)
    except Exception as e:
        logger.error(e)
        print(error_panel(str(e), title=f"Error - {e.__class__.__name__}"))
        raise typer.Exit()

    print(
        success_panel(
            f'Created new {MARIMBA} [light_pink3]pipeline[/light_pink3] "{pipeline_name}" at: "{project_wrapper.pipeline_dir / pipeline_name}"'
        )
    )

    # Configure the pipeline from the command line
    pipeline = pipeline_wrapper.get_instance()
    pipeline_config_schema = pipeline.get_pipeline_config_schema()
    pipeline_config = prompt_schema(pipeline_config_schema)
    pipeline_wrapper.save_config(pipeline_config)


@app.command()
def deployment(
    deployment_name: str = typer.Argument(..., help="Name of the deployment."),
    parent_deployment_name: Optional[str] = typer.Argument(None, help="Name of the parent deployment. If unspecified, use the last deployment."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to Marimba project root. If unspecified, Marimba will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create a new Marimba deployment in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} [steel_blue3]new deployment[/steel_blue3] command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Configure the deployment from the resolved schema
        deployment_config = project_wrapper.prompt_deployment_config(parent_deployment_name=parent_deployment_name)

        # Create the deployment
        deployment_wrapper = project_wrapper.create_deployment(deployment_name, deployment_config)
    except ProjectWrapper.NoSuchDeploymentError as e:
        logger.error(e)
        print(error_panel(f"No such parent deployment: {e}"))
        raise typer.Exit()
    except ProjectWrapper.CreateDeploymentError as e:
        logger.error(e)
        print(error_panel(f"Could not create deployment: {e}"))
        raise typer.Exit()
    except Exception as e:
        logger.error(e)
        print(error_panel(str(e), title=f"Error - {e.__class__.__name__}"))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} [light_pink3]deployment[/light_pink3] "{deployment_name}" at: "{deployment_wrapper.root_dir}"'))
