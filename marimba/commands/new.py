from os import R_OK, access
from pathlib import Path
from typing import Optional, Union

import typer
from rich import print
from rich.panel import Panel
from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt

from marimba.utils.log import get_logger
from marimba.utils.rich import MARIMBA, error_panel, success_panel
from marimba.wrappers.project import ProjectWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new MarImBA collection, pipeline or deployment.",
    no_args_is_help=True,
)


def prompt_schema(schema: dict) -> Optional[dict]:
    """
    Prompt the user for values for each field in the schema.

    The schema is given as a dictionary of field names to defaults.
    The user will be prompted for a value for each field, and the default will be used if no value is entered.
    User values will be converted into the appropriate type as given by the schema default.

    Supported types:
    - str
    - int
    - float
    - bool

    Args:
        schema: The schema to prompt the user for.

    Returns:
        The user values as a dictionary, or None if the input was interrupted.

    Raises:
        NotImplementedError: If the schema contains a type that is not supported.
    """
    user_values = schema.copy()

    try:
        for key, default_value in schema.items():
            value_type = type(default_value)
            if value_type == bool:
                value = Confirm.ask(key, default=default_value)
            elif value_type == int:
                value = IntPrompt.ask(key, default=default_value)
            elif value_type == float:
                value = FloatPrompt.ask(key, default=default_value)
            elif value_type == str:
                value = Prompt.ask(key, default=default_value)
            else:
                raise NotImplementedError(f"Unsupported type: {value_type.__name__}")
            if value is not None:
                user_values[key] = value
    except KeyboardInterrupt:
        return None

    return user_values


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
    project_dir: Path = typer.Argument(..., help="Root path to create new MarImBA project."),
):
    """
    Create a new MarImBA project.
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
        help="Path to MarImBA project root. If unspecified, MarImBA will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create a new MarImBA pipeline in a project.
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
    pipeline = pipeline_wrapper.load_pipeline()
    pipeline_config_schema = pipeline.get_pipeline_config_schema()
    pipeline_config = prompt_schema(pipeline_config_schema)
    pipeline_wrapper.save_config(pipeline_config)


@app.command()
def deployment(
    deployment_name: str = typer.Argument(..., help="Name of the deployment."),
    parent: Optional[str] = typer.Argument(None, help="Name of the parent deployment. If unspecified, use the last deployment."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to MarImBA project root. If unspecified, MarImBA will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create a new MarImBA deployment in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info(f"Executing the {MARIMBA} [steel_blue3]new deployment[/steel_blue3] command.")

    try:
        # Create project wrapper instance
        project_wrapper = ProjectWrapper(project_dir)

        # Create the deployment
        deployment_wrapper = project_wrapper.create_deployment(deployment_name)
    except Exception as e:
        logger.error(e)
        print(error_panel(str(e), title=f"Error - {e.__class__.__name__}"))
        raise typer.Exit()

    print(success_panel(f'Created new {MARIMBA} [light_pink3]deployment[/light_pink3] "{deployment_name}" at: "{deployment_wrapper.root_dir}"'))

    # Get the union of all pipeline-specific deployment config schemas
    resolved_deployment_schema = {}
    for pipeline_name, pipeline_wrapper in project_wrapper.pipeline_wrappers.items():
        pipeline = pipeline_wrapper.load_pipeline()
        deployment_config_schema = pipeline.get_deployment_config_schema()
        resolved_deployment_schema.update(deployment_config_schema)

    def get_last_deployment_name(project_wrapper: ProjectWrapper) -> Optional[str]:  # TODO: Make this less clunky
        deployment_wrappers = project_wrapper.deployment_wrappers.copy()
        deployment_wrappers.pop(deployment_name, None)
        if len(deployment_wrappers) == 0:
            return None
        return max(deployment_wrappers, key=lambda k: deployment_wrappers[k].root_dir.stat().st_mtime)

    # Use the last deployment if no parent is specified
    if parent is None:
        parent = get_last_deployment_name(project_wrapper)  # may be None

    # Update the schema with the parent deployment
    if parent is not None:
        parent_deployment_wrapper = project_wrapper.deployment_wrappers.get(parent, None)

        if parent_deployment_wrapper is None:
            print(error_panel(f'Parent deployment "{parent}" does not exist.'))
            raise typer.Exit(f'Parent deployment "{parent}" does not exist.')

        parent_deployment_config = parent_deployment_wrapper.load_config()
        resolved_deployment_schema.update(parent_deployment_config)

    # Configure the deployment from the resolved schema
    deployment_config = prompt_schema(resolved_deployment_schema)
    deployment_wrapper.save_config(deployment_config)
