#Purpose: This file needs to process input json or csv nav data file(s) and match the extracted nav data to the given video or still imagery dataset at a deployment level. The matched data is then added to the exif data of the images (or video), and exported as a csv file that contains the image name and nav data

#What do we need in the input file as a base?: time, location (lat, lon, depth)

#Step 1: Read nav data files - where are they? source_path/*.json (or source_path/*.csv)

#Step 2; Check that nav files contain the necessary columns (i.e., timestamp, latitude, longitude, depth). Do these need to be configured? - Yes, different platform have different variable outputs, so read in column names from metadata.yml

#Step 3: merge nav files together (should we have options at this stage? - i.e. forward fill, interpolate, nearest, etc.) at a) still image level, b) video level

#Step 4: write data to iFDO file, output csv

import logging
import os
import pathlib
import yaml
import pandas as pd
import glob
from datetime import datetime as dt

import typer
from rich import print
from rich.panel import Panel

def invert_map(my_map):
    return {v: k for k, v in my_map.items()}

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

    #load existing ifdo
    with open(ifdo_path) as file:
        try:
            ifdo_dict = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    ifdo_df = pd.DataFrame.from_dict(ifdo_dict['image-set-items'],orient='index').reset_index()

    #load metadata config
    with open('src/config/metadata.yml') as file:
        try:
            metadata_config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    #load nav data
    for file in glob.iglob(f'{source_path}/*.CSV'):
        df = pd.read_csv(file)

    #merge nav data into ifdo
    logging.info(f"Merging metadata from source directory: {source_path}")

    df_cleaned = df.rename(columns={metadata_config['ifdo-image-set-items']['image-datetime']:'image-datetime'})
    ifdo_merge = ifdo_df.merge(df_cleaned, on='image-datetime', how='left')
    ifdo_cleaned = (
        ifdo_merge.iloc[ifdo_merge['index'].drop_duplicates().index]
        .rename(
            columns=invert_map(metadata_config['ifdo-image-set-items'])
        )
        .rename(
            columns=invert_map(metadata_config['additional-image-set-items'])
        )
    )
    new_ifdo_dict = ifdo_cleaned.set_index('index').to_dict('index')
    ifdo_dict['image-set-items'] = new_ifdo_dict

    #overwrite ifdo file with updated info
    with open(ifdo_path, 'w') as file:
        documents = yaml.dump(ifdo_dict, file)

