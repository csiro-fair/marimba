import logging
import os
from enum import Enum

import typer
from rich import print
from rich.panel import Panel


class ConfigLevel(str, Enum):
    survey = "survey"
    deployment = "deployment"


def check_input_args(
    output_path: str,
):
    # Check if source_path is valid
    if not os.path.isdir(output_path):
        print(Panel(f"The output_path argument [bold]{output_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def create_survey_config(
    output_path: str,
    overwrite: bool,
    dry_run: bool
):
    logging.info(f"Creating survey-level config file at: {output_path}")


def create_deployment_config(
    output_path: str,
    overwrite: bool,
    dry_run: bool
):
    logging.info(f"Creating deployment-level config file at: {output_path}")


def create_config(
    level: ConfigLevel,
    output_path: str,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(output_path)
    
    if level == ConfigLevel.survey:
        create_survey_config(output_path, overwrite, dry_run)
    elif level == ConfigLevel.deployment:
        create_deployment_config(output_path, overwrite, dry_run)

