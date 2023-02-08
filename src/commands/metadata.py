import logging
import os
import pathlib

import typer
from rich import print
from rich.panel import Panel


def check_input_args(
    source_path: str,
    ifdo_path: str
):

    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Check if ifdo_path is valid
    if not os.path.isfile(ifdo_path):
        print(Panel(f"The ifdo_path argument [bold]{ifdo_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    # Check if ifdo_path file has the correct extension
    if pathlib.Path(ifdo_path).suffix.lower() != ".ifdo":
        print(Panel(f'The ifdo_path argument [bold]{ifdo_path}[/bold] does not have the correct extension (".ifdo")', title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def merge_metadata(
    source_path: str,
    ifdo_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(source_path, ifdo_path)

    logging.info(f"Merging metadata from source directory: {source_path}")

