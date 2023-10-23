from abc import ABC
from pathlib import Path
from typing import Union

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config
from marimba.utils.log import (
    LogMixin,
    get_collection_logger,
    get_instrument_file_handler,
)

collection_logger = get_collection_logger()


def get_instrument_config(instrument_path: Union[str, Path]) -> dict:
    """
    Return the instrument config as a dictionary.

    Args:
        instrument_path: The path to the MarImBA instrument.

    Returns:
        The instrument config data as a dictionary.
    """
    instrument_path = Path(instrument_path)

    # Check that this is a valid MarImBA instrument

    if not instrument_path.is_dir():
        print(Panel(f"There are no instruments associated with this MarImBA collection.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    instrument_config_path = instrument_path / "instrument.yml"

    if not instrument_config_path.is_file():
        print(
            Panel(
                f"Cannot find instrument.yml in MarImBa instrument - this is not a MarImBA instrument.",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    return load_config(instrument_config_path)


class Instrument(ABC, LogMixin):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, root_path: Union[str, Path], collection_config: dict, instrument_config: dict):
        root_path = Path(root_path)

        # Add the instrument file handler to the logger
        try:
            self.logger.addHandler(get_instrument_file_handler(root_path.name))
            self.logger.info(f'Initialising instrument-level logging for {instrument_config.get("id")}')
        except Exception as e:
            collection_logger.error(f"Failed to add instrument file handler: {e}")

        # Root and work paths for the instrument
        self.root_path = root_path
        self.work_path = root_path / "work"

        # Collection and instrument configuration
        self.collection_config = collection_config
        self.instrument_config = instrument_config

    def process_all_deployments(self, command_name: str, kwargs: dict):
        """
        Process all the deployments within the instrument work directory.

        Args:
            command_name: Name of the MarImBA command to be executed.
            kwargs: Keyword arguments.
        """

        # Loop through each deployment subdirectory in the instrument work directory
        # TODO: Implement new flexible deployment paths here
        for deployment in self.work_path.iterdir():
            if deployment.is_dir():
                self.process_single_deployment(deployment, command_name, kwargs)

    def process_single_deployment(self, deployment_path: Union[str, Path], command_name: str, kwargs: dict):
        """
        Process a single deployment for the given deployment directory.

        Args:
            deployment_path: The path to the MarImBA deployment.
            command_name: Name of the MarImBA command to be executed.
            kwargs: Keyword arguments.
        """
        deployment_path = Path(deployment_path)

        # Get deployment name and config path
        deployment_name = deployment_path.name
        deployment_config_path = deployment_path / (deployment_name + ".yml")

        # Check if deployment metadata file exists and skip deployment if not present
        if not deployment_config_path.is_file():
            self.logger.warning(
                f'SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment_path}"'
            )
            return
        else:  # Invoke the command for the deployment
            # TODO: Need to validate deployment metadata file here
            self.logger.debug(f'Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment_path}"')
            command = getattr(self, command_name)
            command(deployment_path, **kwargs)

    def run_init_or_import(self, command_name, kwargs):
        command = getattr(self, command_name)
        command(**kwargs)

    def run_catalog(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]catalog[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_metadata(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]metadata[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_package(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]package[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_process(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_rename(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_report(self, deployment_path: str, dry_run: bool):
        self.logger.warning(
            f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )
