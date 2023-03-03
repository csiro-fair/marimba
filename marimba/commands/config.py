import os
from enum import Enum

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config, save_config
from marimba.utils.log import get_collection_logger
from marimba.utils.registry import Registry

logger = get_collection_logger()


class ConfigLevel(str, Enum):
    """
    Configuration level option.
    """

    survey = "survey"
    deployment = "deployment"


SURVEY_KEY_PROMPTS = [
    ("image-platform", "Please enter image platform (e.g. Zeiss Axio Observer)"),
    # ("image-item-identification-scheme", "Please enter image item identification scheme (e.g. <project>_<event>_<sensor>_<date>_<time>.<ext>)")
]
DEPLOYMENT_KEY_PROMPTS = [
    # Note: If you need start and end timestamps to time-filter deployments, add them to prompt_config() in instrument implementation
    # ("start-timestamp", "Please enter start timestamp in ISO8601 (e.g. 2018-01-01T00:00:00Z)"),
    # ("end-timestamp", "Please enter end timestamp in ISO8601 (e.g. 2018-01-01T23:59:59Z)"),
]


def check_input_args(
    output_path: str,
):
    """
    Check the input arguments for the config command.

    Args:
        output_path: The path to the directory where the config file will be saved.
    """
    # Check if source_path is valid
    if not os.path.isdir(output_path):
        print(
            Panel(
                f"The output_path argument [bold]{output_path}[/bold] is not a valid directory path",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()


def get_instrument_config(image_platform: str) -> dict:
    """
    Prompt for the instrument configuration for a given image platform.
    Looks up the instrument class in the registry and uses the class method to get the prompts.

    Args:
        image_platform: The image platform to get the instrument config for.

    Returns:
        The provided instrument config as a dictionary.
    """
    try:
        instrument_class = Registry.get(image_platform)
    except ValueError:
        print(Panel(f"Image platform '{image_platform}' is not a valid option.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    instrument_config = {}
    for key, prompt_str in instrument_class.prompt_config():
        value = typer.prompt(prompt_str)
        instrument_config[key] = value

    return instrument_config


def create_survey_config(
    output_dir: str,
):
    """
    Create the survey-level config file.

    Args:
        output_dir: The path to the directory where the config file will be saved.
    """
    output_path = os.path.join(output_dir, "survey_config.yml")

    survey_id = typer.prompt("Please enter survey ID (e.g. IN2018_V06)")
    survey_config = {}
    for key, prompt in SURVEY_KEY_PROMPTS:
        survey_config[key] = typer.prompt(prompt)

    add_survey_level_config = typer.confirm("Do you have survey-level config?")
    if add_survey_level_config:
        survey_config["config"] = get_instrument_config(survey_config["image-platform"])

    if os.path.isfile(output_path):
        logger.info(f"Survey config file already exists: {output_path}")
        config = load_config(output_path)

        existing_surveys = config.get("surveys")
        if survey_id in existing_surveys:
            logger.info(f"Found matching survey ID - replacing config...")
            for key, _ in SURVEY_KEY_PROMPTS:
                existing_surveys[survey_id][key] = survey_config[key]

        else:
            logger.info(f"No matching survey ID - appending config...")
            survey_config["deployments"] = {}
            existing_surveys[survey_id] = survey_config

        save_config(output_path, config)
    else:
        logger.info(f"Creating new survey-level config file at: {output_path}")
        config = {"surveys": {survey_id: {"deployments": {}, **survey_config}}}
        save_config(output_path, config)


def create_deployment_config(
    output_dir: str,
):
    """
    Add a deployment-level config to the survey-level config file.

    Args:
        output_dir: The path to the directory where the config file exists.
    """
    output_path = os.path.join(output_dir, "survey_config.yml")

    if not os.path.isfile(output_path):
        print(Panel(f"There is no survey-level config at [bold]{output_path}[/bold]", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()
    else:
        config = load_config(output_path)
        existing_surveys = config.get("surveys")

        # TODO: Dynamically fetch examples from survey IDs in survey config
        survey_id = typer.prompt("Please enter survey ID (e.g. IN2018_V06)")

        if survey_id in existing_surveys:
            logger.info(f"Found matching survey ID")

            deployment_id = typer.prompt("Please enter deployment ID (e.g. IN2018_V06_001)")
            deployment_config = {}
            for key, prompt in DEPLOYMENT_KEY_PROMPTS:
                deployment_config[key] = typer.prompt(prompt)

            # If no survey level config, ...
            if not existing_surveys[survey_id].get("config"):
                instrument_config = get_instrument_config(existing_surveys[survey_id]["image-platform"])
                if instrument_config:
                    deployment_config["config"] = instrument_config
            else:
                add_deployment_level_config = typer.confirm("Do you have deployment-level config?")
                if add_deployment_level_config:
                    deployment_config["config"] = get_instrument_config(existing_surveys[survey_id]["image-platform"])

            existing_surveys[survey_id]["deployments"][deployment_id] = deployment_config
            save_config(output_path, config)
            logger.info(f"Saving deployment-level config file at: {output_path}")

        else:
            print(
                Panel(
                    f'There is no survey ID "{survey_id}" in config at [bold]{output_path}[/bold]',
                    title="Error",
                    title_align="left",
                    border_style="red",
                )
            )
            raise typer.Exit()


def create_config(level: ConfigLevel, output_dir: str):
    """
    Demux the config command to the appropriate function.

    Args:
        level: The level of the config to create.
        output_dir: The path to the directory where the config file will be saved (or already exists).
    """
    check_input_args(output_dir)

    if level == ConfigLevel.survey:
        create_survey_config(output_dir)
    elif level == ConfigLevel.deployment:
        create_deployment_config(output_dir)
