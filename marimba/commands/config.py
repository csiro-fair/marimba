import logging
import os
from enum import Enum

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config, save_config


class ConfigLevel(str, Enum):
    survey = "survey"
    deployment = "deployment"


SURVEY_KEY_PROMPTS = [
    ("image-platform", "Please enter image platform (e.g. Zeiss Axio Observer)"),
    ("image-item-identification-scheme", "Please enter image item identification scheme (e.g. <project>_<event>_<sensor>_<date>_<time>.<ext>)")
]
DEPLOYMENT_KEY_PROMPTS = [
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
):
    output_path = os.path.join(output_dir, "survey_config.yml")

    survey_id = typer.prompt("Please enter survey ID (e.g. IN2018_V06)")
    survey_config = {}
    for key, prompt in SURVEY_KEY_PROMPTS:
        survey_config[key] = typer.prompt(prompt)

    if os.path.isfile(output_path):

        logging.info(f"Survey config file already exists: {output_path}")
        config = load_config(output_path)

        existing_surveys = config.get("surveys")
        if survey_id in existing_surveys:
            logging.info(f"Found matching survey ID - replacing config...")
            for key, _ in SURVEY_KEY_PROMPTS:
                existing_surveys[survey_id][key] = survey_config[key]

        else:
            logging.info(f"No matching survey ID - appending config...")
            survey_config["deployments"] = {}
            existing_surveys[survey_id] = survey_config

        save_config(output_path, config)
    else:
        logging.info(f"Creating new survey-level config file at: {output_path}")
        config = {
            "surveys": {
                survey_id: {
                    "deployments": {},
                    **survey_config
                }
            }
        }
        save_config(output_path, config)


def create_deployment_config(
        output_dir: str,
):
    output_path = os.path.join(output_dir, "survey_config.yml")

    if not os.path.isfile(output_path):
        print(Panel(f"The is no survey-level config at [bold]{output_path}[/bold]", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()
    else:
        config = load_config(output_path)
        existing_surveys = config.get("surveys")

        survey_id = typer.prompt("Please enter survey ID (e.g. IN2018_V06)")

        if survey_id in existing_surveys:
            logging.info(f"Found matching survey ID")

            deployment_id = typer.prompt("Please enter deployment ID (e.g. IN2018_V06_001)")
            deployment_config = {}
            for key, prompt in DEPLOYMENT_KEY_PROMPTS:
                deployment_config[key] = typer.prompt(prompt)

            existing_deployments = existing_surveys[survey_id]["deployments"]
            if deployment_id in existing_deployments:
                logging.info(f"Found matching deployment ID - replacing config...")
                for key, _ in DEPLOYMENT_KEY_PROMPTS:
                    existing_surveys[survey_id]["deployments"][deployment_id][key] = deployment_config[key]

            else:
                logging.info(f"No matching deployment ID - appending config...")
                existing_deployments[deployment_id] = deployment_config

            save_config(output_path, config)
            logging.info(f"Saving deployment-level config file at: {output_path}")

        else:
            print(Panel(f"The is no survey-level config at [bold]{output_path}[/bold]", title="Error", title_align="left", border_style="red"))
            raise typer.Exit()


def create_config(
        level: ConfigLevel,
        output_dir: str
):
    check_input_args(output_dir)

    if level == ConfigLevel.survey:
        create_survey_config(output_dir)
    elif level == ConfigLevel.deployment:
        create_deployment_config(output_dir)
