import os
import os.path
from abc import ABC

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

    def catalog(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]catalog[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def metadata(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]metadata[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def package(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]package[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def process(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def rename(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')

    def report(self, dry_run: bool):
        self.logger.warning(f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]')


