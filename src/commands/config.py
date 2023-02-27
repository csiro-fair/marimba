import logging
import os
from enum import Enum

import typer
from rich import print
from rich.panel import Panel

from utils.config import save_config


class ConfigLevel(str, Enum):
    survey = "survey"
    deployment = "deployment"
    

SURVEY_KEY_PROMPTS = [
    ("survey-id", "Please enter survey ID (e.g. IN2018_V06)"),
    ("image-platform", "Please enter image platform (e.g. Zeiss Axio Observer)"),
    ("image-item-identification-scheme", "Please enter image item identification scheme (e.g. <project>_<event>_<sensor>_<date>_<time>.<ext>)")
]
DEPLOYMENT_KEY_PROMPTS = [
    ("deployment-id", "Please enter deployment ID (e.g. IN2018_V06_001)"),
    ("start-timestamp", "Please enter start timestamp in ISO8601 (e.g. 2018-01-01T00:00:00Z)"),
    ("end-timestamp", "Please enter end timestamp in ISO8601 (e.g. 2018-01-01T23:59:59Z)"),
]


def check_input_args(
    output_path: str,
):
    # Check if source_path is valid
    if not os.path.isdir(output_path):
        print(Panel(f"The output_path argument [bold]{output_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def create_survey_config(
    output_dir: str,
    overwrite: bool,
    dry_run: bool
) -> dict:
    output_path = os.path.join(output_dir, "survey_config.json")
    
    logging.info(f"Creating survey-level config file at: {output_path}")
    
    survey_config = {}
    for key, prompt in SURVEY_KEY_PROMPTS:
        survey_config[key] = typer.prompt(prompt)
    
    return survey_config


def create_deployment_config(
    output_dir: str,
    overwrite: bool,
    dry_run: bool
) -> dict:
    output_path = os.path.join(output_dir, "deployment_config.json")
    
    logging.info(f"Creating deployment-level config file at: {output_path}")
    
    deployment_config = {}
    for key, prompt in DEPLOYMENT_KEY_PROMPTS:
        deployment_config[key] = typer.prompt(prompt)
    
    save_config()
    
    return deployment_config


def create_config(
    level: ConfigLevel,
    output_dir: str,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(output_dir)
    
    if level == ConfigLevel.survey:
        create_survey_config(output_dir, overwrite, dry_run)
    elif level == ConfigLevel.deployment:
        create_deployment_config(output_dir, overwrite, dry_run)

