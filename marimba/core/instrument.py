import logging
from abc import ABC
from pathlib import Path
from typing import Union

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config
from marimba.utils.log import LogMixin, get_file_handler


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
        print(Panel("There are no instruments associated with this MarImBA collection.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    instrument_config_path = instrument_path / "instrument.yml"

    if not instrument_config_path.is_file():
        print(
            Panel(
                "Cannot find instrument.yml in MarImBa instrument - this is not a MarImBA instrument.",
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

    class InvalidStructureError(Exception):
        """
        Raised when the instrument file structure is invalid.
        """

        pass

    def __init__(self, root_dir: Union[str, Path], config: dict, dry_run: bool):
        self._root_dir = Path(root_dir)
        self._config = config
        self._dry_run = dry_run

        self._check_file_structure()
        self._setup_logging()

    def _check_file_structure(self):
        """
        Check that the instrument file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            Instrument.InvalidStructureError: If the instrument file structure is invalid.
        """

        def check_file_exists(path: Path):
            if not path.is_file():
                raise Instrument.InvalidStructureError(f"Cannot find file at path: {path}")

        check_file_exists(self.root_dir / "instrument.py")
        # TODO: Check everything here. Note that file logging is not yet set up at this point.

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `instrument.log`.
        """
        # Create a file handler for this instance
        file_handler = get_file_handler(self.root_dir, self.name, self._dry_run, level=logging.DEBUG)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    @property
    def root_dir(self) -> Path:
        """
        The root directory of the instrument.
        """
        return self._root_dir

    @property
    def name(self) -> str:
        """
        The name of the instrument.
        """
        self._root_dir.name

    def process_single_deployment(self, deployment_path: Union[str, Path], command_name: str, kwargs: dict):
        """
        Process a single deployment for the given deployment directory.

        Args:
            deployment_path: The path to the MarImBA deployment.
            command_name: Name of the MarImBA command to be executed.
            kwargs: Keyword arguments.
        """
        # TODO: Take this out of the instrument class. MarImBA should be reponsible for calling into the instrument for a specific command for a specific deployment.

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

    def run_catalog(self):
        self.logger.warning(
            f'There is no MarImBA [bold]catalog[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_metadata(self):
        self.logger.warning(
            f'There is no MarImBA [bold]metadata[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_package(self):
        self.logger.warning(
            f'There is no MarImBA [bold]package[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_process(self):
        self.logger.warning(
            f'There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    def run_rename(self):
        self.logger.warning(
            f'There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
        )

    # def run_report(self):
    #     self.logger.warning(
    #         f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
    #     )
