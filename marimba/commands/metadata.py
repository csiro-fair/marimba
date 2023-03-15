#This file needs to process input csv nav data file(s) and match the extracted nav data to the given video or still imagery dataset at a deployment level. The matched data is then added to the exif data of the images (or video), and added to the ifdo

import logging
import os
import pathlib
import yaml
import pandas as pd
import glob
from datetime import datetime as dt
import subprocess

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def invert_map(my_map):
    return {v: k for k, v in my_map.items()}

def check_input_args(source_path: str, ifdo_path: str):
    """
    Check the input arguments for the copy command.

    Args:
        source_path: The path to the directory where the files will be copied from.
        ifdo_path: The path to the configuration file.
    """
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(
            Panel(
                f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path",
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    # Check if config_path is valid
    if not os.path.isfile(ifdo_path):
        print(
            Panel(f"The ifdo_path argument [bold]{ifdo_path}[/bold] is not a valid file", title="Error", title_align="left", border_style="red")
        )
        raise typer.Exit()

    # Check if config_path file has the correct extension
    if pathlib.Path(ifdo_path).suffix.lower() != ".ifdo":
        print(
            Panel(
                f'The ifdo_path argument [bold]{ifdo_path}[/bold] does not have the correct extension (".ifdo")',
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def merge_metadata(
    source_path: str,
    ifdo_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):

    """
    Merge metadata for files in a directory.

    Args:
        source_path: The path to the directory where the files to be merged are located.
        ifdo_path: The path to the configuration file.
        recursive: Whether to merge metadata recursively.
        overwrite: Whether to overwrite existing metadata files.
        dry_run: Whether to run the command without actually merging the metadata.
    """
    check_input_args(source_path, ifdo_path)

    #load existing ifdo to get images and timestamps
    with open(ifdo_path) as file:
        try:
            ifdo_dict = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    #convert to pandas for merging with nav data
    ifdo_df = pd.DataFrame.from_dict(ifdo_dict['image-set-items'], orient='index').reset_index()

    #load metadata config for mapping nav data
    with open('src/config/metadata.yml') as file:
        try:
            metadata_config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    #load nav data
    for file in glob.iglob(f'{source_path}/*.CSV'):
        nav_df = pd.read_csv(file)


    logger.info(f"Merging metadata from source directory: {source_path}")#rename datetime column from config for merge
    nav_df_renamed = nav_df.rename(
        columns = {
            metadata_config['ifdo-image-set-items']['image-datetime']:'image-datetime'
        }
    )

    #merge nav data into ifdo
    ifdo_df_merged = ifdo_df.merge(nav_df_renamed, on='image-datetime', how='left')

    #dropping duplicates in case there's not a 1-1 match. Could probably think of a more intelligent way to select from duplicates, or leave for user to make sure they provide cleaned data
    #alternatively may want to raise exception if there are duplicates
    ifdo_df_renamed = (
        ifdo_df_merged.iloc[ifdo_df_merged['index'].drop_duplicates().index]
        .rename(
            columns=invert_map(metadata_config['ifdo-image-set-items'])
        )
        .rename(
            columns=invert_map(metadata_config['additional-image-set-items']) #leaving non-ifdo fields as separate in case we want to handle them differently later
        )
    )

    #update image set items to include nav data
    ifdo_dict['image-set-items'] = ifdo_df_renamed.set_index('index').to_dict('index')


    #overwrite ifdo file with updated info
    with open(ifdo_path, 'w') as file:
        documents = yaml.dump(ifdo_dict, file)

    ################# generate exif config and add exif data to images ###############
    #write exif.config file from metadata config

    #needs tidying / refactoring - may want to consider defining data_types in metadata.yml
    #TODO: redo the way exif items get hex name, at the moment it will probably only handle 10 items as index rolls through values 0-9
    conf_str = """%Image::ExifTool::UserDefined = (
    'Image::ExifTool::Exif::Main' => {"""

    for index, (k, v) in enumerate(metadata_config['ifdo-image-set-items'].items()):
        if k == 'image-datetime':
            data_type = 'string'
        else:
            data_type = 'rational64s'
        conf_str = conf_str + f"""
        0xd00{index} => {{
            Name => '{k}',
            Writable => '{data_type}',
            WriteGroup => 'IFD0'
        }},"""

    last_ifdo_index = index + 1
    for index, (k, v) in enumerate(metadata_config['additional-image-set-items'].items()):
        conf_str = conf_str + f"""
        0xd00{index+last_ifdo_index} => {{
            Name => '{k}',
            Writable => 'rational64s',
            WriteGroup => 'IFD0'
        }},"""

    conf_str = conf_str + """
      },
    );
    """

    with open('src/config/exif.config', 'w') as f:
        f.write(conf_str)

    #loop through images and generate command string for writing data to exif

    for img_file in glob.iglob(f'{source_path}*.JPG'):
        cmd = 'exiftool -config src/config/exif.config'
        img_name = img_file.split('\\')[-1]
        for k, v in ifdo_dict['image-set-items'][img_name].items():
            if k == 'image-datetime':
                cmd = cmd + " -EXIF:" + k + '="' + v + '"'
            else:
                cmd = cmd + " -EXIF:" + k + "=" + str(v)

        cmd = cmd + " " + img_file
        subprocess.call(cmd)
