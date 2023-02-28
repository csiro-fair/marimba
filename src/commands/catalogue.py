import glob
import logging
import os
import subprocess
import shutil
import typer
import utils.file_system as fs
from rich import print
from rich.panel import Panel
from rich.progress import track
from rich.progress import Progress, SpinnerColumn, TextColumn


def check_input_args(source_path: str, exiftool_path:str):

    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()
    #check exiftool path is valid
    if not shutil.which(exiftool_path):
        print(Panel(f"Need a path to Exiftool", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()




def catalogue_files(
    source_path: str,
    file_extension: str,
    exiftool_path: str,
    glob_path: str,
    overwrite: bool
):
    # Check input arguments and update iFDO file path if found automatically
    check_input_args(source_path,exiftool_path)
    
    #find all the directories with files that match the mask
    dirstoprocess =[]
    with Progress(SpinnerColumn(),TextColumn("[progress.description]{task.description}"),
        transient=True,) as progress:
        progress.add_task(description="Preparing...", total=None)
        for item in glob.glob(os.path.join(source_path,glob_path,'.'),recursive=True):
            if glob.glob(os.path.join(source_path,os.path.dirname(item),f'*.{file_extension.upper()}')) or glob.glob(os.path.join(source_path,os.path.dirname(item),f'*.{file_extension.lower()}')):
                dirstoprocess.append(item)
    #step through each directory and run exif tool on the directory if the file mask
    for value in track(range(len(dirstoprocess)), description="Cataloguing..."):
        source = os.path.join(source_path,os.path.dirname(dirstoprocess[value]),f'*.{file_extension}')
        if glob.glob(source):
            file = os.path.normpath(os.path.join(dirstoprocess[value],f'.exif_{file_extension}.json'))
            command =f'"{exiftool_path}" -api largefile=1 -json -ext {file_extension} "{dirstoprocess[value]}" > "{file}"'
            if overwrite or not os.path.exists(file):
                subprocess.run(command, shell=True, check=True)




