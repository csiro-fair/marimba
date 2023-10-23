import os
import shlex
import signal  # Import signal module
import subprocess

import numpy as np
import psutil
import typer
from typer.testing import CliRunner

from marimba.marimba import marimba


def main(
    collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
    instrument_id: str = typer.Argument(..., help="MarImBA instrument ID."),
    max_processes: int = typer.Option(4, help="Number of concurrent transfers"),
    format_type: str = typer.Option("exfat", help="Card format type"),
    days: int = typer.Option(0, help="Day delta for import directory"),
    overwrite: bool = typer.Option(False, help="Overwrite import.yaml"),
    debug: bool = typer.Option(False, help="Card format type"),
):
    if debug:
        runner = CliRunner()
    for i in psutil.disk_partitions():
        if i.fstype == format_type:
            print(i)
            p = psutil.disk_usage(i.mountpoint)
            if np.ceil(p.total / 1000000000) <= 512:
                if debug:
                    runner.invoke(marimba, ["initalise", collection_path, instrument_id, i.mountpoint, "--days", "1"])
                else:
                    if overwrite:
                        command = f"marimba initalise {collection_path} {instrument_id}   {i.mountpoint} --days {days} --overwrite "
                    else:
                        command = f"marimba initalise {collection_path} {instrument_id}   {i.mountpoint} --days {days} --overwrite "
                    process = subprocess.Popen(shlex.split(command))
                    process.wait()


if __name__ == "__main__":
    typer.run(main)
