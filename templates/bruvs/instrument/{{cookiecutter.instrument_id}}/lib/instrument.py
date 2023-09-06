"""
Baited Remote Underwater Video Systems (BRUVS) instrument specification
"""

import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader

from marimba.core.instrument import Instrument
from marimba.utils.config import load_config
import uuid
import glob
import pandas as pd
import subprocess
import shlex
from io import StringIO
import platform

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

    def rename(self, dry_run: bool):
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
    def initalise(self,card_path,dry_run: bool):
        """
        Implementation of the MarImBA initalise command for the BRUVS
        """

        def make_xml(file_path):
            if os.path.exists(file_path):
                self.logger.error(f"Error SDCard already initalised {file_path}")
            else:
                env = Environment(loader = FileSystemLoader(self.root_path),   trim_blocks=True, lstrip_blocks=True)
                template = env.get_template('import.yml')
                fill = {"instrumentPath" : self.root_path, "instrument" : self.instrument_config['id'],
                        "importdate" : f"{datetime.now():%Y-%m-%d}",
                        "importtoken" : str(uuid.uuid4())[0:8]}
                self.logger.info(f'{dry_run_log_string}Making import file "{file_path}"')
                if not dry_run:
                    with open(file_path, "w") as file:
                        file.write(template.render(fill))
        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if dry_run else ""    
        if isinstance(card_path,list):
            [make_xml(f"{file}/import.yaml") for file in card_path]
        else:
            make_xml(f"{card_path}/import.yaml")

    def import_command(self,card_path,clean,exiftool_path,file_extension,dry_run: bool):
        """
        Implementation of the MarImBA initalise command for the BRUVS
        """
        dry_run_log_string = "DRY_RUN - " if dry_run else ""
        importyml =f"{card_path}/import.yaml"
        with open(importyml, 'r') as stream:
            try:
                importdetails=yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                self.logger.error(f"Error possible corrupt yaml {importyml}")

        videopath = f'{card_path}/DCIM/100GOPRO'
        files = glob.glob(f'{videopath}/*.{file_extension}')
        if files:
            barpath = f'{self.root_path}/camerabars.csv'
            barnumbers = pd.read_csv(barpath,parse_dates=['StartDate','EndDate']) 
            command = f"{exiftool_path} -api largefilesupport=1 -u  -json -ext {file_extension} -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {videopath}"
            process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            s=str(out,'utf-8')
            data = StringIO(s)  
            cameras =pd.read_json(data)
            cameras['Duration'] =pd.to_timedelta(cameras.Duration)
            cameras['CreateDate'] = pd.to_datetime(cameras['CreateDate'],format='%Y:%m:%d %H:%M:%S')
            for index,error in  cameras[cameras.CameraSerialNumber.isnull()].iterrows():
                self.logger.error(f"Error possible corrupt video file {error.SourceFile}")
            cameras=cameras.dropna()
            cameras =cameras.merge(barnumbers, on='CameraSerialNumber', how='inner')

            if len(cameras.CameraSerialNumber.unique())>1:
                self.logger.warning(f"Warning multiple cameras in directory {cameras.CameraSerialNumber.unique()} ---> {videopath}")
            if len(cameras.CreateDate.unique())>1:
                self.logger.warning(f"Warning multiple captures in directory {cameras.CreateDate.unique()} ---> {videopath}")
            #get the last record as it's probably the best one!
            log=cameras.groupby('CreateDate').agg({'CameraSerialNumber':'first', 'Duration': 'sum'})
            matched =cameras[(cameras.CreateDate>cameras.StartDate) & (cameras.CreateDate<cameras.EndDate)]
            if len(matched)!=len(cameras):
                self.logger.warning(f"Warning unmatched camera serial numbers  {videopath} in please serial numbers in  {barpath} ")
            last =cameras[cameras.CreateDate==cameras.CreateDate.unique().max()].sort_values('SourceFile').iloc[-1]
            importdetails['instrumentPath'] = self.root_path
            importdetails['bruvframe'] = last.Frame
            importdetails['housinglabel'] = last.GoProNumber
            importdetails['cameraserialNumber'] = last.CameraSerialNumber
            destination =importdetails["importtemplate"].format(**importdetails)
            self.logger.info(f'{dry_run_log_string}  Copy  {card_path} --> {destination}')
            if not dry_run:
                os.makedirs(destination,exist_ok=True)
                command =f"rclone sync {card_path} {destination} --progress --exclude=/**/*.THM --exclude=/**/*.LRV"
                self.logger.info(f'{dry_run_log_string}  {command}')
                process = subprocess.Popen(shlex.split(command))
                process.wait()
            if clean==True:
                if platform.system() == "Linux":
                    command =f'find {card_path} -type f \( -name "*.LRV" -o -name "*.THM")-exec rm \123{}\125 \;'
                    process = subprocess.Popen(shlex.split(command))
                    process.wait()
                os.makedirs(destination,exist_ok=True)
                command =f"rclone move {card_path} {destination} --progress --delete-empty-src-dirs --exclude=/**/*.THM --exclude=/**/*.LRV"
                self.logger.info(f'{dry_run_log_string}  {command}')
                process = subprocess.Popen(shlex.split(command))
                process.wait()



               

           


        
