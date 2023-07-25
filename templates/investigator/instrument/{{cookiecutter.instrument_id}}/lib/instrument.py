"""
MNF Deep Towed Camera instrument specification
"""

import glob
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyexiv2
import typer
import yaml
from PIL import Image
from rich import print
from rich.panel import Panel

from marimba.core.instrument import Instrument
from marimba.utils.config import load_config
from marimba.utils.exif_tags import TAGS, get_key

__author__ = "David Webb"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = ["Chris Jackett"]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "David Webb"
__email__ = "david.webb@csiro.au"
__status__ = "Development"


def invert_map(map):
    return {v: k for k, v in map.items()}


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
    if Path(ifdo_path).suffix.lower() != ".ifdo":
        print(
            Panel(
                f'The ifdo_path argument [bold]{ifdo_path}[/bold] does not have the correct extension (".ifdo")',
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()


class MNFDeepTowedCamera(Instrument):

    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        super().__init__(root_path, collection_config, instrument_config)

        # Define instrument filetypes and data files
        self.image_filetypes = ["jpg"]
        self.video_filetypes = ["mp4"]

    # TODO: Make dry_run and dry_run_log_string member variables of the instrument class and remove from here
    def traverse_and_rename_images(self, deployment_stills_path, camera_direction, deployment_config, dry_run, dry_run_log_string):

        # for file in os.scandir(deployment_stills_path):
        for i, file in enumerate(sorted(os.scandir(deployment_stills_path), key=lambda file: file.name), start=1):

            # Define regex to match any of the filetypes to be renamed
            extensions_pattern = f'({"|".join(re.escape(extension) for extension in self.image_filetypes)})$'
            file_path = file.path

            # Match case-insensitive regex expression in file name
            if re.search(extensions_pattern, file_path, re.IGNORECASE):

                # Get the output filename and path
                output_file_name = self.get_image_output_file_name(deployment_config, file_path, i, camera_direction)
                output_file_path = deployment_stills_path / output_file_name

                # Check if input and output file paths are the same
                if file_path == output_file_path:
                    self.logger.info(f'{dry_run_log_string}SKIPPING FILE - input and output file names are identical: "{file_path}"')
                # Check if output file path already exists and the overwrite argument is not set
                # elif output_file_path.is_file() and not overwrite:
                elif output_file_path.is_file():
                    self.logger.info(
                        f'{dry_run_log_string}Output file already exists and overwrite argument is not set: "{output_file_path}"')
                # Perform file renaming
                else:
                    # Only rename files if not in --dry-run mode
                    self.logger.info(f'{dry_run_log_string}Renaming file "{file.name}" to: "{output_file_path}"')
                    if not dry_run:
                        try:
                            # Rename file
                            os.rename(file_path, output_file_path)
                        # TODO: Check this is the correct exception to catch
                        except FileExistsError:
                            self.logger.error(f"Error renaming file {file_path} to {output_file_path}")

    def traverse_and_rename_videos(self, deployment_stills_path, deployment_config, dry_run, dry_run_log_string):

        # for file in os.scandir(deployment_stills_path):
        for i, file in enumerate(sorted(os.scandir(deployment_stills_path), key=lambda file: file.name), start=1):

            # Define regex to match any of the filetypes to be renamed
            extensions_pattern = f'({"|".join(re.escape(extension) for extension in self.video_filetypes)})$'
            file_path = file.path

            # Match case-insensitive regex expression in file name
            if re.search(extensions_pattern, file_path, re.IGNORECASE):

                # Get the output filename and path
                output_file_name = self.get_video_output_file_name(deployment_config, file_path, i)
                output_file_path = deployment_stills_path / output_file_name

                # Check if input and output file paths are the same
                if file_path == output_file_path:
                    self.logger.info(f'{dry_run_log_string}SKIPPING FILE - input and output file names are identical: "{file_path}"')
                # Check if output file path already exists and the overwrite argument is not set
                # elif output_file_path.is_file() and not overwrite:
                elif output_file_path.is_file():
                    self.logger.info(
                        f'{dry_run_log_string}Output file already exists and overwrite argument is not set: "{output_file_path}"')
                # Perform file renaming
                else:
                    # Only rename files if not in --dry-run mode
                    self.logger.info(f'{dry_run_log_string}Renaming file "{file.name}" to: "{output_file_path}"')
                    if not dry_run:
                        try:
                            # Rename file
                            os.rename(file_path, output_file_path)
                        # TODO: Check this is the correct exception to catch
                        except FileExistsError:
                            self.logger.error(f"Error renaming file {file_path} to {output_file_path}")

    def rename(self, dry_run: bool):
        """
        Implementation of the MarImBA rename command for the MNF Deep Towed Camera
        """

        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):

            # Get deployment name and config path
            deployment_name = deployment.path.split("/")[-1]
            deployment_path = Path(deployment.path)
            deployment_config_path = deployment_path / Path(deployment_name + ".yml")
            deployment_stills_port_path = deployment_path / "stills" / "port"
            deployment_stills_starboard_path = deployment_path / "stills" / "starboard"
            deployment_video_path = deployment_path / "video"

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'{dry_run_log_string}SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment.path}"')
                continue
            else:
                # TODO: Need to validate deployment metadata file here and load deployment config
                self.logger.info(f'{dry_run_log_string}Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment.path}"')
                deployment_config = load_config(deployment_config_path)

                # Loop through each file in the deployment port and starboard image directories
                self.traverse_and_rename_images(deployment_stills_port_path, "DSP", deployment_config, dry_run, dry_run_log_string)
                self.traverse_and_rename_images(deployment_stills_starboard_path, "DSS", deployment_config, dry_run, dry_run_log_string)
                self.traverse_and_rename_videos(deployment_video_path, deployment_config, dry_run, dry_run_log_string)

    def get_image_output_file_name(self, deployment_config: dict, file_path: str, index: int, camera_direction) -> str:

        try:
            image = Image.open(file_path)

            # Check if image has EXIF data
            if hasattr(image, '_getexif'):
                exif_data = image._getexif()
                if exif_data is not None:
                    # Loop through EXIF tags
                    for tag, value in exif_data.items():
                        tag_name = TAGS.get(tag, tag)
                        if tag_name == "DateTime":
                            date = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                            # Convert to ISO 8601 format
                            iso_timestamp = date.strftime("%Y%m%dT%H%M%SZ")

                            # Construct and return new filename
                            return (
                                f'{self.instrument_config.get("id")}_'
                                f'{camera_direction}_'
                                f'{self.collection_config.get("voyage-id").split("_")[0]}_'
                                f'{self.collection_config.get("voyage-id").split("_")[1]}_'
                                f'{deployment_config.get("deployment-id").split("_")[2]}_'
                                f'{iso_timestamp}_'
                                f"{index:05d}"
                                f".JPG"
                            )
            else:
                self.logger.error(f"No EXIF DateTime tag found in image {file_path}")

        except IOError:
            self.logger.error(f"Error: Unable to open {file_path}. Are you sure it's an image?")

    # TODO: Currently this is not getting real starting video timestamps
    def get_video_output_file_name(self, deployment_config: dict, file_path: str, index: int) -> str:

        date = datetime.now()
        # Convert to ISO 8601 format
        iso_timestamp = date.strftime("%Y%m%dT%H%M%SZ")

        # Construct and return new filename
        return (
            f'{self.instrument_config.get("id")}_'
            f'{self.collection_config.get("voyage-id").split("_")[0]}_'
            f'{self.collection_config.get("voyage-id").split("_")[1]}_'
            f'{deployment_config.get("deployment-id").split("_")[2]}_'
            f'{iso_timestamp}_'
            f"{index:05d}"
            f".MP4"
        )

    # Function to extract EXIF data
    # TODO: Clean up this method, make DateTimeOriginal EXIF element more explicit
    def get_exif_data(self, img_path):
        image = Image.open(img_path)
        exif_data = image._getexif()
        # EXIF tags: https://www.exiv2.org/tags.html
        # For DateTimeOriginal, the key is 36867 in the dictionary
        timestamp = exif_data.get(36867)
        # Convert to datetime object if timestamp is found
        if timestamp:
            return datetime.strptime(timestamp, "%Y:%m:%d %H:%M:%S")

    # Function to iterate over JPG files in a directory
    def iterate_over_images_in_dir(self, directory, image_data):
        # TODO: Fix this to be case insensitive
        for jpg_file in directory.glob("*.JPG"):
            timestamp = self.get_exif_data(jpg_file)
            image_data.append({"filename": str(jpg_file), "image-datetime": timestamp})

    # Function to iterate over JPG files in a directory
    def add_custom_exif_data(self, directory, ifdo_dict, dry_run):

        dry_run_log_string = "DRY_RUN - " if dry_run else ""

        # print(ifdo_dict[1])
        for image_file, image_data in ifdo_dict[1].get("image-set-items").items():

            self.logger.info(f'{dry_run_log_string}Adding custom metadata to "{image_file}"')

            image_path = str(directory / image_file)

            with pyexiv2.Image(image_path) as image:
                exif_data = image.read_exif()
                # print(exif_data)

                for tag_id, tag_value in image_data.items():
                    # print(get_key(tag_id), tag_id, str(tag_value))
                    if get_key(tag_id) != None and tag_value != None:
                        # print(get_key(tag_id), str(tag_value))
                        if not dry_run:
                            image.modify_exif({f"Exif.Image.{hex(get_key(tag_id))}": tag_value})

    def metadata(self, dry_run: bool):
        """
        Implementation of the MarImBA metadata command for the MNF Deep Towed Camera
        """

        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):

            # Get deployment name and config path
            deployment_name = deployment.path.split("/")[-1]
            deployment_path = Path(deployment.path)
            deployment_config_path = deployment_path / Path(deployment_name + ".yml")
            deployment_stills_port_path = deployment_path / "stills" / "port"
            deployment_stills_starboard_path = deployment_path / "stills" / "starboard"
            deployment_video_path = deployment_path / "video"

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'{dry_run_log_string}SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment.path}"')
                continue
            else:
                # TODO: Need to validate deployment metadata file here and load deployment config
                self.logger.info(f'{dry_run_log_string}Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment.path}"')
                deployment_config = load_config(deployment_config_path)

                # Initialize an empty list to store the image data
                image_data = []

                # Iterate over JPG files in the directories
                self.iterate_over_images_in_dir(deployment_stills_port_path, image_data)
                self.iterate_over_images_in_dir(deployment_stills_starboard_path, image_data)

                # Create a pandas DataFrame from the image data
                df = pd.DataFrame(image_data)

                # check_input_args(source_path, ifdo_path)

                ifdo_path = deployment_path / Path("DTC_" + deployment_name + ".ifdo")

                # load metadata config for mapping nav data
                with open(f'{self.root_path}/metadata.yml') as file:
                    try:
                        metadata_config = yaml.safe_load(file)
                    except yaml.YAMLError as exc:
                        print(exc)

                # Match and load nav data
                matching_files = list(glob.iglob(f'{deployment_path}/DTC_{deployment_name}*.CSV'))

                if len(matching_files) > 1:
                    self.logger.error(f"Multiple matching files found for pattern: DTC_{deployment_name}*.CSV")
                    print(
                        Panel(
                            f"Multiple matching files found for pattern: DTC_{deployment_name}*.CSV",
                            title="Error",
                            title_align="left",
                            border_style="red",
                        )
                    )
                    raise typer.Exit()
                elif len(matching_files) == 0:
                    self.logger.error(f"No matching file found for pattern: DTC_{deployment_name}*.CSV")
                    print(
                        Panel(
                            f"No matching file found for pattern: DTC_{deployment_name}*.CSV",
                            title="Error",
                            title_align="left",
                            border_style="red",
                        )
                    )
                    raise typer.Exit()
                else:
                    nav_df = pd.read_csv(matching_files[0])

                self.logger.info(f"Merging metadata from source directory: {deployment_path}/DTC_{deployment_name}*.CSV")  # rename datetime column from config for merge
                nav_df_renamed = nav_df.rename(
                    columns={
                        metadata_config['ifdo-image-set-items']['image-datetime']: 'image-datetime'
                    }
                )
                nav_df_renamed['image-datetime'] = pd.to_datetime(nav_df_renamed['image-datetime'])

                # merge nav data into ifdo
                df_merged = df.merge(nav_df_renamed, on='image-datetime', how='left')
                df_merged['image-datetime'] = df_merged['image-datetime'].dt.strftime("%Y%m%dT%H%M%SZ")
                df_merged['filename'] = df_merged['filename'].str.replace(str(deployment_path) + "/", '')

                # dropping duplicates in case there's not a 1-1 match. Could probably think of a more intelligent way to select from duplicates, or leave for user to make sure they provide cleaned data
                # alternatively may want to raise exception if there are duplicates
                df_renamed = (
                    # df_merged.iloc[df_merged['index'].drop_duplicates().index]
                    df_merged
                    .rename(columns=invert_map(metadata_config['ifdo-image-set-items']))
                    # leaving non-ifdo fields as separate in case we want to handle them differently later
                    .rename(columns=invert_map(metadata_config['additional-image-set-items']))
                )

                ifdo_dict = []
                # TODO: Parameterise the iFDO header so that it works for all deployments
                ifdo_dict.append({"image-set-header": {"image-set-name": f"DTC_IN2022_V09_001"}})
                ifdo_dict.append({"image-set-items": df_renamed.set_index('filename').to_dict('index')})

                # Iterate over JPG files in the directories
                self.add_custom_exif_data(deployment_path, ifdo_dict, dry_run)

                # overwrite ifdo file with updated info
                self.logger.info(f"Writing iFDO file to: {ifdo_path}")
                with open(ifdo_path, 'w') as file:
                    yaml.dump(ifdo_dict, file)
