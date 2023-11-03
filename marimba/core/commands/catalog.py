import shutil
import subprocess
from pathlib import Path
from typing import List, Union

import typer
from rich import print
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, track


def check_input_args(source_path: Union[str, Path], exiftool_path: str):
    """
    Check the input arguments for the catalog command.

    Args:
        source_path: The path to the directory containing the files to be catalogd.
        exiftool_path: The path to the exiftool executable.
    """
    # Check if source_path is valid
    source_path = Path(source_path)
    if not source_path.is_dir():
        print(
            Panel(
                f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()
    # check exiftool path is valid
    if not shutil.which(exiftool_path):
        print(Panel("Need a path to Exiftool", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


def catalog_command(source_path: Union[str, Path], file_extension: str, exiftool_path: str, glob_path: str, overwrite: bool):
    """
    Catalogue files using exiftool.

    Args:
        source_path: The path to the directory containing the files to be catalogd.
        file_extension: The file extension of the files to be catalogd.
        exiftool_path: The path to the exiftool executable.
        glob_path: The glob path to the files to be catalogd.
        overwrite: Whether to overwrite existing output files.
    """
    # Check input arguments and update iFDO file path if found automatically
    source_path = Path(source_path)
    check_input_args(source_path, exiftool_path)

    # find all the directories with files that match the mask
    dirstoprocess: List[Path] = []
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
        progress.add_task(description="Preparing...", total=None)
        for item in source_path.glob(glob_path):
            if not item.is_dir():  # only check directories
                continue

            # Glob for files with the given extension, either using the exact file extension case, or the upper or lower case
            exact_matches = list(item.glob(f"*.{file_extension}"))
            lower_matches = list(item.glob(f"*.{file_extension.lower()}"))
            upper_matches = list(item.glob(f"*.{file_extension.upper()}"))

            if exact_matches or lower_matches or upper_matches:
                dirstoprocess.append(item)

    # step through each directory and run exif tool on the directory if the file mask
    for dir_to_process in track(dirstoprocess, description="Cataloguing..."):
        # Glob for any files with the given extension, either using the exact file extension case, or the upper or lower case
        matches = set()
        matches.update(dir_to_process.glob(f"*.{file_extension}"))
        matches.update(dir_to_process.glob(f"*.{file_extension.lower()}"))
        matches.update(dir_to_process.glob(f"*.{file_extension.upper()}"))

        # If no matches found, skip
        if not matches:
            continue

        # Create the output file path
        file = dir_to_process / f".exif_{file_extension.lower()}.json"

        # Run exiftool
        command = f'"{exiftool_path}" -api largefile=1 -json -ext {file_extension} "{dir_to_process.absolute()}" > "{file.absolute()}"'
        if overwrite or not file.exists():
            subprocess.run(command, shell=True, check=True)
