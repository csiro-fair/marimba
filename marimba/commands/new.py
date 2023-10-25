from os import R_OK, access
from pathlib import Path
from typing import Optional, Union

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.log import get_logger
from marimba.wrappers.project import ProjectWrapper

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new MarImBA collection, instrument or deployment.",
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
    """
    user_values = schema.copy()

    try:
        for key, default_value in schema.items():
            value_type = type(default_value)
            value = typer.prompt(f"{key} [{value_type.__name__}]", default=default_value, type=value_type)
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
        error_message = "Could not find a MarImBA project."
        logger.error(error_message)
        print(
            Panel(
                error_message,
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    return project_dir


@app.command()
def project(
    project_dir: Path = typer.Argument(..., help="Root path to create new MarImBA project."),
):
    """
    Create a new MarImBA project.
    """
    logger.info("Executing the [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [steel_blue3]new project[/steel_blue3] command.")

    # Try to create the new project
    try:
        project_wrapper = ProjectWrapper.create(project_dir)
    except FileExistsError:
        error_message = f'A [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]project[/light_pink3] already exists at: "{project_dir}"'
        logger.error(error_message)
        print(
            Panel(
                error_message,
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    print(
        Panel(
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]project[/light_pink3] at: "{project_wrapper.root_dir}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )


@app.command()
def instrument(
    instrument_name: str = typer.Argument(..., help="Name of the instrument."),
    url: str = typer.Argument(..., help="URL of the instrument git repository."),
    project_dir: Optional[Path] = typer.Option(
        None,
        help="Path to MarImBA project root. If unspecified, MarImBA will search for a project root directory in the current working directory and its parents.",
    ),
):
    """
    Create a new MarImBA instrument in a project.
    """
    project_dir = find_project_dir_or_exit(project_dir)

    logger.info("Executing the [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [steel_blue3]new instrument[/steel_blue3] command.")

    # Create project wrapper instance
    project_wrapper = ProjectWrapper(project_dir)

    # Create the instrument
    try:
        instrument_wrapper = project_wrapper.create_instrument(instrument_name, url)
    except ProjectWrapper.CreateInstrumentError as e:
        logger.error(e)
        print(
            Panel(
                str(e),
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    print(
        Panel(
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]instrument[/light_pink3] "{instrument_name}" at: "{project_wrapper.instruments_dir / instrument_name}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )

    # Configure the instrument from the command line
    instrument = instrument_wrapper.load_instrument()
    instrument_config_schema = instrument.get_instrument_config_schema()
    instrument_config = prompt_schema(instrument_config_schema)
    instrument_wrapper.save_config(instrument_config)


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

    logger.info("Executing the [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [steel_blue3]new deployment[/steel_blue3] command.")

    # Create project wrapper instance
    project_wrapper = ProjectWrapper(project_dir)

    # Create the deployment
    try:
        deployment_wrapper = project_wrapper.create_deployment(deployment_name, parent=parent)
    except ProjectWrapper.CreateDeploymentError as e:
        logger.error(e)
        print(
            Panel(
                str(e),
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    print(
        Panel(
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]deployment[/light_pink3] "{deployment_name}" at: "{deployment_wrapper.root_dir}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )

    # Configure the deployment from the command line
    for instrument_name, instrument_wrapper in project_wrapper.instrument_wrappers.items():
        instrument = instrument_wrapper.load_instrument()
        deployment_config_schema = instrument.get_deployment_config_schema()
        print(f"Instrument {instrument_name}")
        deployment_config = prompt_schema(deployment_config_schema)
        deployment_wrapper.save_instrument_config(instrument_name, deployment_config)
