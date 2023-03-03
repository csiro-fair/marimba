import os

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config


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
