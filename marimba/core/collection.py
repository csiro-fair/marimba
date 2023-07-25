import os

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config


def get_collection_config(collection_path) -> dict:
    """
    Return the collection config as a dictionary.

    Args:
        collection_path: The path to the MarImBA collection.

    Returns:
        The collection config data as a dictionary.
    """

    # Check that this is a valid MarImBA collection and

    if not os.path.isdir(collection_path):
        print(Panel(f"MarImBA collection path does not exist.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    collection_config_path = os.path.join(os.path.join(collection_path, "collection.yml"))

    if not os.path.isfile(collection_config_path):
        print(Panel(f"Cannot find collection.yml in MarImBa collection - this is not a MarImBA collection.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    return load_config(collection_config_path)
