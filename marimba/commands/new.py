from pathlib import Path
from typing import Optional

import typer
from rich import print
from rich.panel import Panel

from marimba.core.project import Project
from marimba.utils.log import get_logger

logger = get_logger(__name__)

app = typer.Typer(
    help="Create a new MarImBA collection, instrument or deployment.",
    no_args_is_help=True,
)


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
        project = Project.create(project_dir)
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
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]project[/light_pink3] at: "{project.root_dir}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )


@app.command()
def instrument(
    project_dir: Path = typer.Argument(..., help="Path to MarImBA project root."),
    name: str = typer.Argument(..., help="Name of the instrument."),
    template_name: str = typer.Argument(..., help="Name of predefined MarImBA project template."),
):
    """
    Create a new MarImBA instrument in a project.
    """
    logger.info("Executing the [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [steel_blue3]new instrument[/steel_blue3] command.")

    # Create project instance
    project = Project(project_dir)

    # Create the instrument
    try:
        project.create_instrument(name, template_name)
    except Project.CreateInstrumentError as e:
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
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]instrument[/light_pink3] "{name}" at: "{project.instruments_dir / name}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )


@app.command()
def deployment(
    project_dir: Path = typer.Argument(..., help="Path to MarImBA project root."),
    name: str = typer.Argument(..., help="Name of the deployment."),
    parent: Optional[str] = typer.Argument(None, help="Name of the parent deployment. If unspecified, use the last deployment."),
):
    """
    Create a new MarImBA deployment in a project.
    """
    logger.info("Executing the [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [steel_blue3]new deployment[/steel_blue3] command.")

    # Create project instance
    project = Project(project_dir)

    # Create the deployment
    try:
        project.create_deployment(name, parent=parent)
    except Project.CreateDeploymentError as e:
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
            f'Created new [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]deployment[/light_pink3] "{name}" at: "{project.deployments_dir / name}"',
            title="Success",
            title_align="left",
            border_style="green",
        )
    )
