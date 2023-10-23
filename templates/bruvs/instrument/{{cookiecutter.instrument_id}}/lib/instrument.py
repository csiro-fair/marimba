"""
Baited Remote Underwater Video Systems (BRUVS) instrument specification
"""

import json
import os
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import typer
import yaml
from jinja2 import Environment, FileSystemLoader
from rich import print
from rich.panel import Panel

from marimba.core.instrument import Instrument
from marimba.utils.config import load_config
from marimba.utils.file_system import list_sd_cards

__author__ = "Candice Untiedt"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = ["Chris Jackett"]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Candice Untiedt"
__email__ = "candice.untiedt@csiro.au"
__status__ = "Development"


class BRUVS(Instrument):

    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        super().__init__(root_path, collection_config, instrument_config)

        # Define instrument filetypes and data files
        self.video_filetypes = ["mp4"]
        self.exif_filename = [".exif_MP4.json"]

    @staticmethod
    def get_sorted_directory_file_list(directory):
        """Return a list of files with a case-insensitive .mp4 extension in the given directory."""
        files = [filename for filename in os.listdir(str(directory)) if filename.lower().endswith('.mp4')]
        return sorted(files, key=lambda s: s.lower())

    @staticmethod
    def parse_exif_from_json(filename):
        """Open and parse a JSON file containing EXIF metadata."""
        with open(str(filename), 'r') as file:
            data = json.load(file)
        return data

    @staticmethod
    def fetch_from_list(filename, dictionaries):
        """Fetch a dictionary from a list where the filename matches."""
        for dic in dictionaries:
            if dic['FileName'] == filename:
                return dic
        return None

    def move_ancillary_files(self, directory, dry_run, dry_run_log_string):

        files_to_move = []

        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            # Check if it's a file and doesn't match our conditions
            if os.path.isfile(file_path) and not (filename.lower().endswith('.mp4') or filename == '.exif_MP4.json'):
                files_to_move.append(file_path)

        if files_to_move:
            misc_dir = os.path.join(directory, 'misc')

            # Create misc directory if it doesn't exist
            if not os.path.exists(misc_dir) and not dry_run:
                os.makedirs(misc_dir)

            for file_path in files_to_move:
                self.logger.info(f'{dry_run_log_string}Moving file "{os.path.basename(file_path)}" to misc directory "{misc_dir}"')
                if not dry_run:
                    try:
                        shutil.move(file_path, os.path.join(misc_dir, os.path.basename(file_path)))
                    except Exception:
                        self.logger.error(f"Error renaming file {file_path} to {misc_dir}")

    def recursive_replace(self, data, old_string, new_string):
        """
        Recursively replace a string in the nested data (lists, dictionaries, strings).
        """
        if isinstance(data, dict):
            return {key: self.recursive_replace(value, old_string, new_string) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.recursive_replace(item, old_string, new_string) for item in data]
        elif isinstance(data, str):
            return data.replace(old_string, new_string)
        else:
            return data

    def replace_string_in_json(self, filename, old_string, new_string):
        # Read the JSON file
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Replace the string recursively
        updated_data = self.recursive_replace(data, old_string, new_string)

        # Write the updated data back to the JSON file
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(updated_data, file, indent=4)

    def run_rename(self, dry_run: bool):
        """
        Implementation of the MarImBA rename command for the BRUVS
        """

        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):

            # Get deployment name and config path
            deployment_path = Path(deployment.path)
            deployment_name = deployment_path.name
            deployment_config_path = deployment_path / Path(deployment_name + ".yml")
            deployment_video_port_path = deployment_path / "video" / "port"
            deployment_video_starboard_path = deployment_path / "video" / "starboard"

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'{dry_run_log_string}SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment.path}"')
                continue
            else:
                # TODO: Need to validate deployment metadata file here and load deployment config
                self.logger.info(f'{dry_run_log_string}Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment.path}"')
                deployment_config = load_config(deployment_config_path)

                # Rename pair-wise sorted
                deployment_video_port_list = self.get_sorted_directory_file_list(deployment_video_port_path)
                deployment_video_starboard_list = self.get_sorted_directory_file_list(deployment_video_starboard_path)

                # Ensure that both directories have the same number of .mp4 files
                if len(deployment_video_port_list) != len(deployment_video_starboard_list):
                    self.logger.error("The port and starboard directories do not contain the same number of .MP4 files.")
                    return

                # Parse the json file
                try:
                    deployment_video_port_json = self.parse_exif_from_json(deployment_video_port_path / ".exif_MP4.json")
                    deployment_video_starboard_json = self.parse_exif_from_json(deployment_video_starboard_path / ".exif_MP4.json")
                except FileNotFoundError:
                    self.logger.error(f"Cannot find .exif_MP4.json - perhaps the data has not yet been copied into the directory?")

                for idx, (port_video, starboard_video) in enumerate(zip(deployment_video_port_list, deployment_video_starboard_list), 1):

                    port_video_path = deployment_video_port_path / port_video
                    starboard_video_path = deployment_video_starboard_path / starboard_video

                    port_json_dict = self.fetch_from_list(port_video, deployment_video_port_json)
                    starboard_json_dict = self.fetch_from_list(starboard_video, deployment_video_starboard_json)

                    if not port_json_dict or not starboard_json_dict:
                        self.logger.error("Could not match filename in port or starboard .exif_MP4.json file.")
                        continue

                    timestamp = port_json_dict.get("FileCreateDate")
                    formatted_timestamp = timestamp.replace(':', '-', 2)
                    local_timestamp = datetime.fromisoformat(formatted_timestamp)
                    iso_timestamp = local_timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                    # Find match for GX code
                    port_match = re.search(r'GX\d{2}', port_json_dict.get("FileName"))
                    port_file_id = port_match.group(0) if port_match else None
                    starboard_match = re.search(r'GX\d{2}', starboard_json_dict.get("FileName"))
                    starboard_file_id = starboard_match.group(0) if starboard_match else None

                    if not port_match or not starboard_match:
                        self.logger.info('Cannot find "GX" string in filename, skipping...')
                        continue

                    output_file_name_port = os.path.join(deployment_video_port_path, self.get_video_output_file_name(deployment_config, "VCP", iso_timestamp, port_file_id))
                    output_file_name_starboard = os.path.join(deployment_video_starboard_path, self.get_video_output_file_name(deployment_config, "VCS", iso_timestamp, starboard_file_id))

                    # Check if input and output file paths are the same
                    if str(port_video_path) == str(output_file_name_port) and str(starboard_video_path) == str(output_file_name_starboard):
                        self.logger.info(f'{dry_run_log_string}SKIPPING FILE - input and output file names are identical: "{port_video_path}"')
                        self.logger.info(f'{dry_run_log_string}SKIPPING FILE - input and output file names are identical: "{starboard_video_path}"')
                    else:
                        # Only rename files if not in --dry-run mode
                        self.logger.info(f'{dry_run_log_string}Renaming file "{os.path.basename(port_video_path)}" to: "{output_file_name_port}"')
                        self.logger.info(f'{dry_run_log_string}Renaming file "{os.path.basename(starboard_video_path)}" to: "{output_file_name_starboard}"')
                        if not dry_run:
                            try:
                                # Rename file
                                os.rename(port_video_path, output_file_name_port)
                                os.rename(starboard_video_path, output_file_name_starboard)
                            # TODO: Check this is the correct exception to catch
                            except FileExistsError:
                                self.logger.error(f"Error renaming file {os.path.basename(port_video_path)} to {output_file_name_port}")
                                self.logger.error(f"Error renaming file {os.path.basename(starboard_video_path)} to {output_file_name_starboard}")

                            try:
                                # Replace filename in json file
                                self.replace_string_in_json(str(deployment_video_port_path / ".exif_MP4.json"), str(os.path.basename(port_video_path)), str(os.path.basename(output_file_name_port)))
                                self.replace_string_in_json(str(deployment_video_starboard_path / ".exif_MP4.json"), str(os.path.basename(starboard_video_path)),
                                                            str(os.path.basename(output_file_name_starboard)))
                            except Exception as e:
                                self.logger.error(f"Error replacing new filename in json file: {e}")

                # Move all remaining files into a 'misc' directory
                self.move_ancillary_files(deployment_video_port_path, dry_run, dry_run_log_string)
                self.move_ancillary_files(deployment_video_starboard_path, dry_run, dry_run_log_string)

    def get_video_output_file_name(self, deployment_config: dict, camera_direction: str, iso_timestamp: str, file_id: str) -> str:

        # Construct and return new filename
        return (
            f'{self.instrument_config.get("id")}_'
            f'{deployment_config.get("unit_id")}_'
            f'{camera_direction}_'
            f'{deployment_config.get("deployment_id")}_'
            f'{iso_timestamp}_'
            f'{file_id}'
            f".MP4"
        )

    def run_init(self, card_paths: list, dry_run: bool, days: int, overwrite: bool):
        """
        Implementation of the MarImBA init command for BRUVS
        """

        def make_xml(file_path):
            if (os.path.exists(file_path)) and (not overwrite):
                self.logger.warning(f"SKIPPING - SD card already initialised: {file_path}")
            else:
                env = Environment(loader=FileSystemLoader(self.root_path), trim_blocks=True, lstrip_blocks=True)
                template = env.get_template('import.yml')
                fill = {
                    "instrument_path": self.root_path, "instrument": self.instrument_config['id'],
                    "import_date": f"{datetime.now() + timedelta(days=days):%Y-%m-%d}",
                    "import_token": str(uuid.uuid4())[0:8]
                }
                self.logger.info(f'Making import file "{file_path}"')
                if not dry_run:
                    with open(file_path, "w") as file:
                        file.write(template.render(fill))

        if isinstance(card_paths, list):
            [make_xml(f"{file}/import.yml") for file in card_paths]
        else:
            make_xml(f"{card_paths}/import.yml")

    # Function to execute shell commands
    def execute_command(self, command, dry_run):
        if not dry_run:
            process = subprocess.Popen(shlex.split(command))
            process.wait()

    # Function to load YAML files safely
    def load_yaml(self, file_path):
        with file_path.open('r') as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError:
                self.logger.error(f"Possible corrupt YAML file at: {file_path}")
                return None

    # Main function for importing files
    def run_import(self, card_paths, all, exiftool_path, copy, move, file_extension, card_size, format_type, dry_run: bool):

        # Try to automatically find SD cards if card_paths is not defined
        if all and not card_paths:
            card_paths = list_sd_cards(format_type, card_size)

        # Check if card_paths have been provided or automatically found
        if not card_paths:
            print(
                Panel(
                    f"The card_paths argument was not provided and unable to be automatically found.",
                    title="Error",
                    title_align="left",
                    border_style="red",
                )
            )
            raise typer.Exit()

        # Loop through each card path provided
        for card in card_paths:
            card_path = Path(card)

            # Load import details from YAML file
            import_yml = card_path / "import.yml"
            import_details = self.load_yaml(import_yml)

            # Define the video path and search for files with the given extension
            video_path = card_path / "DCIM" / "100GOPRO"
            files = list(video_path.glob(f"*.{file_extension}"))

            # If video files are found, proceed with import
            if files:
                # Load bar numbers and relevant details
                bar_path = Path(self.root_path) / "work" / "camera_bars.csv"
                bar_numbers = pd.read_csv(bar_path, parse_dates=['BarStartDate', 'BarEndDate'])

                # Fetch metadata from video files
                command = f"{exiftool_path} -api largefilesupport=1 -u -json -ext {file_extension} -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {video_path}"
                process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _ = process.communicate()
                data = StringIO(out.decode('utf-8'))

                # Convert metadata to a DataFrame
                cameras = pd.read_json(data)
                cameras['Duration'] = pd.to_timedelta(cameras.Duration)
                cameras['CreateDate'] = pd.to_datetime(cameras['CreateDate'], format='%Y:%m:%d %H:%M:%S')

                # Log errors for corrupt video files
                for index, error in cameras[cameras.CameraSerialNumber.isnull()].iterrows():
                    self.logger.error(f"Possible corrupt video file {error.SourceFile}")

                # Drop rows with missing data
                cameras.dropna(inplace=True)

                # Merge camera DataFrame with bar numbers DataFrame
                cameras = cameras.merge(bar_numbers.loc[bar_numbers.Active], on='CameraSerialNumber', how='left')

                # Log errors for unmatched serial numbers
                if cameras.GoProNumber.isna().any():
                    self.logger.error(f"Camera serial number not found {cameras[cameras.GoProNumber.isna()].CameraSerialNumber}")

                # Log warnings for multiple cameras or captures in a single directory
                if len(cameras.CameraSerialNumber.unique()) > 1:
                    self.logger.warning(f"Multiple cameras in directory {cameras.CameraSerialNumber.unique()} ---> {video_path}")
                if len(cameras.CreateDate.unique()) > 1:
                    self.logger.warning(f"Multiple captures in directory {cameras.CreateDate.unique()} ---> {video_path}")

                # Filter cameras by dates and log a warning if any serial numbers are unmatched
                matched = cameras[(cameras.CreateDate > cameras.BarStartDate) & (cameras.CreateDate < cameras.BarEndDate)]
                if len(matched) != len(cameras):
                    self.logger.warning(f"Warning unmatched camera serial numbers {video_path} in please serial numbers in {bar_path} ")

                # Select the last record for import
                last = cameras.loc[cameras.CreateDate == cameras.CreateDate.max()].sort_values('SourceFile').iloc[-1]

                # Update import details with new metadata
                import_details.update({
                    'instrument_path': self.root_path,
                    'bruv_frame': last.Frame,
                    'housing_label': last.GoProNumber,
                    'camera_serial_number': last.CameraSerialNumber,
                    'camera_create_date': last.CreateDate
                })

                # Define destination path and log the copy operation
                destination = Path(import_details["import_template"].format(**import_details))
                self.logger.info(f'Copy {card} --> {destination}')

                # Execute move command if applicable
                if move:
                    command = f"rclone move {video_path.resolve()} {destination.resolve()} --progress --delete-empty-src-dirs"
                    self.logger.info(f"Using Rclone to move {video_path.resolve()} to {destination.resolve()}")
                    destination.mkdir(parents=True, exist_ok=True)
                    self.execute_command(command, dry_run)
                    return

                # Execute copy command if applicable
                if copy:
                    command = f"rclone copy {video_path.resolve()} {destination.resolve()} --progress --low-level-retries 1 "
                    self.logger.info(f"Using Rclone to copy {video_path.resolve()} to {destination.resolve()}")
                    destination.mkdir(parents=True, exist_ok=True)
                    self.execute_command(command, dry_run)

            # Log a warning if no video files are found
            else:
                self.logger.warning(f"No {file_extension} files found at {video_path}")

    def run_doit(self, doit_commands, dry_run: bool):
        pass
