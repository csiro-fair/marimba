import os
import os.path
from abc import ABC
from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config
from marimba.utils.log import LogMixin, get_collection_logger, get_instrument_file_handler

collection_logger = get_collection_logger()


def get_instrument_config(instrument_path) -> dict:
    """
    Return the instrument config as a dictionary.

    Args:
        instrument_path: The path to the MarImBA instrument.

    Returns:
        The instrument config data as a dictionary.
    """

    # Check that this is a valid MarImBA instrument

    if not os.path.isdir(instrument_path):
        print(Panel(f"There are no instruments associated with this MarImBA collection.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    instrument_config_path = os.path.join(os.path.join(instrument_path, "instrument.yml"))

    if not os.path.isfile(instrument_config_path):
        print(Panel(f"Cannot find instrument.yml in MarImBa instrument - this is not a MarImBA instrument.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    return load_config(instrument_config_path)


class Instrument(ABC, LogMixin):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        # Add the instrument file handler to the logger
        try:
            self.logger.addHandler(get_instrument_file_handler(os.path.basename(root_path)))
            self.logger.info(f'Initialising instrument-level logging for {instrument_config.get("id")}')
        except Exception as e:
            collection_logger.error(f"Failed to add instrument file handler: {e}")

        # Root and work paths for the instrument
        self.root_path = root_path
        self.work_path = os.path.join(self.root_path, "work")

        # Collection and instrument configuration
        self.collection_config = collection_config
        self.instrument_config = instrument_config

    def process_all_deployments(self, command_name, kwargs):
        """
        Process all the deployments within the instrument work directory.

        Args:
            command_name: Name of the MarImBA command to be executed.
            kwargs: Keyword arguments.
        """

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):
            self.process_single_deployment(deployment.path, command_name, kwargs)

    def process_single_deployment(self, deployment_path, command_name, kwargs):
        """
        Process a single deployment for the given deployment directory.

        Args:
            deployment_path: The path to the MarImBA deployment.
            command_name: Name of the MarImBA command to be executed.
            kwargs: Keyword arguments.
        """

        # TODO: Move this to the instrument logger
        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if kwargs.get("dry_run") else ""

        # Get deployment name and config path
        deployment_name = deployment_path.split("/")[-1]
        deployment_config_path = Path(deployment_path) / Path(deployment_name + ".yml")

        # Check if deployment metadata file exists and skip deployment if not present
        if not deployment_config_path.is_file():
            self.logger.warning(f'{dry_run_log_string}SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment_path}"')
            return
        else:
            # TODO: Need to validate deployment metadata file here
            self.logger.debug(f'{dry_run_log_string}Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment_path}"')
            command = getattr(self, command_name)
            command(deployment_path, **kwargs)

    def catalog(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]catalog[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def metadata(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]metadata[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def package(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]package[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def process(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def rename(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def report(self, deployment_path: str, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')
