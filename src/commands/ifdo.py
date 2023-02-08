import logging
import os

import typer
from rich import print
from rich.panel import Panel


def check_input_args(
    output_path: str,
):
    # Check if source_path is valid
    if not os.path.isdir(output_path):
        print(Panel(f"The output_path argument [bold]{output_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def create_base_ifdo(
    output_path: str,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(output_path)

    logging.info(f"Creating base-level iFDO file at: {output_path}")

