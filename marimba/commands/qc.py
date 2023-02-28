import logging
import os

import typer
from rich import print
from rich.panel import Panel


def check_input_args(
    input_path: str,
):
    # Check if source_path is valid
    if not os.path.isdir(input_path):
        print(Panel(f"The output_path argument [bold]{input_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def run_qc(
    input_path: str,
    recursive: bool,
):
    check_input_args(input_path)

    logging.info(f"Running QC on files at: {input_path}")

