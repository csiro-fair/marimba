# import os
# from datetime import datetime
# from pathlib import Path
#
# from cookiecutter.main import cookiecutter
#
# from marimba.utils.log import get_collection_logger
#
# logger = get_collection_logger()
#
#
# def create_template(component, output_path, template_name):
#     template_path = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, "templates", template_name)
#     logger.info(f"Template name: {template_name}")
#     cookiecutter(template=template_path, output_dir=output_path, extra_context={"datestamp": datetime.today().strftime("%Y-%m-%d")})


import os
from datetime import datetime
from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

from cookiecutter.main import cookiecutter

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()

# TODO: import from somewhere else
from enum import Enum
class New(str, Enum):
    collection = "collection"
    instrument = "instrument"
    deployment = "deployment"



def create_template(component, output_path, template_name, instrument_id):

    if component == New.deployment and not instrument_id:
        print(
            Panel(
                f"The [bold]instrument_id[/bold] must be provided when adding a new MarImBA [bold]{component}[/bold].",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    base_templates_path = Path(os.path.abspath(__file__)).parent.parent.parent / "templates"
    logger.info(f"Setting MarImBA base templates path to: {base_templates_path}")

    logger.info(f"Checking that the provided MarImBA [bold]{component}[/bold] template exists...")
    template_path = base_templates_path / template_name / component

    if os.path.isdir(template_path):
        logger.info(f"MarImBA [bold]{component}[/bold] template [bold]{Path(template_name) / Path(component)}[/bold] exists!")
    else:
        print(
            Panel(
                f"The provided [bold]{component}[/bold] template name [bold]{Path(template_name) / Path(component)}[/bold] does not exists at {template_path}",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    logger.info(f"Checking that the provided MarImBA [bold]{component}[/bold] output path exists...")
    marimba_output_path = Path(output_path)

    # TODO: Probably should redesign the 'new' command API to require collection_name and instrument_id for new instruments and deployments to figure out the correct directory structures
    if component == "instrument":
        # Append an instruments directory if not present as the lowest level directory
        if str(marimba_output_path.parts[-1]) != "instruments":
            marimba_output_path = marimba_output_path / "instruments"
    elif component == "deployment":
        # Append a work directory if not present as the lowest level directory
        if str(marimba_output_path.parts[-1]) != "work":
            marimba_output_path = marimba_output_path / "work"

    if os.path.isdir(marimba_output_path):
        logger.info(f"MarImBA [bold]{component}[/bold] output path [bold]{marimba_output_path}[/bold] exists!")
    else:
        print(
            Panel(
                f"The provided [bold]{component}[/bold] output path [bold]{marimba_output_path}[/bold] does not exists.\nPerhaps the collection path should be one of the subdirectories of [bold]{output_path}[/bold]?",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    cookiecutter(
        template=str(template_path),
        output_dir=marimba_output_path,
        extra_context={"datestamp": datetime.today().strftime("%Y-%m-%d")}
    )
