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
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import typer
import yaml
from doit.cmd_base import ModuleTaskLoader
from doit.doit_cmd import DoitMain
from jinja2 import Environment, FileSystemLoader
from rich import print
from rich.panel import Panel

from marimba.core.instrument import Instrument
from marimba.utils.config import load_config
from marimba.utils.file_system import list_sd_cards

__author__ = "Candice Untiedt"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = [
    "Chris Jackett",
    "Nick Mortimer",
]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Candice Untiedt"
__email__ = "candice.untiedt@csiro.au"
__status__ = "Development"


class BRUVS(Instrument):
    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict, dry_run: bool):
        super().__init__(root_path, collection_config, instrument_config, dry_run)

        # Define instrument filetypes and data files
        self.video_filetypes = ["mp4"]
        self.exif_filename = [".exif_MP4.json"]

    @staticmethod
    def get_sorted_directory_file_list(directory):
        """Return a list of files with a case-insensitive .mp4 extension in the given directory."""
        files = [filename for filename in os.listdir(str(directory)) if filename.lower().endswith(".mp4")]
        return sorted(files, key=lambda s: s.lower())

    @staticmethod
    def parse_exif_from_json(filename):
        """Open and parse a JSON file containing EXIF metadata."""
        with open(str(filename), "r") as file:
            data = json.load(file)
        return data

    @staticmethod
    def fetch_from_list(filename, dictionaries):
        """Fetch a dictionary from a list where the filename matches."""
        for dic in dictionaries:
            if dic["FileName"] == filename:
                return dic
        return None

    def move_ancillary_files(self, directory):
        files_to_move = []

        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            # Check if it's a file and doesn't match our conditions
            if os.path.isfile(file_path) and not (filename.lower().endswith(".mp4") or filename == ".exif_MP4.json"):
                files_to_move.append(file_path)

        if files_to_move:
            misc_dir = os.path.join(directory, "misc")

            # Create misc directory if it doesn't exist
            if not os.path.exists(misc_dir) and not self.dry_run:
                os.makedirs(misc_dir)

            for file_path in files_to_move:
                self.logger.info(f'Moving file "{os.path.basename(file_path)}" to misc directory "{misc_dir}"')
                if not self.dry_run:
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
        with open(filename, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Replace the string recursively
        updated_data = self.recursive_replace(data, old_string, new_string)

        # Write the updated data back to the JSON file
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(updated_data, file, indent=4)

    # TODO: Change this to single deployment path processing
    def run_rename(self, deployment_path: Path):
        """
        Implementation of the MarImBA rename command for the BRUVS
        """

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
                    f'SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment.path}"'
                )
                continue
            else:
                # TODO: Need to validate deployment metadata file here and load deployment config
                self.logger.info(f'Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment.path}"')
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
                    formatted_timestamp = timestamp.replace(":", "-", 2)
                    local_timestamp = datetime.fromisoformat(formatted_timestamp)
                    iso_timestamp = local_timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                    # Find match for GX code
                    port_match = re.search(r"GX\d{2}", port_json_dict.get("FileName"))
                    port_file_id = port_match.group(0) if port_match else None
                    starboard_match = re.search(r"GX\d{2}", starboard_json_dict.get("FileName"))
                    starboard_file_id = starboard_match.group(0) if starboard_match else None

                    if not port_match or not starboard_match:
                        self.logger.info('Cannot find "GX" string in filename, skipping...')
                        continue

                    output_file_name_port = os.path.join(
                        deployment_video_port_path, self.get_video_output_file_name(deployment_config, "VCP", iso_timestamp, port_file_id)
                    )
                    output_file_name_starboard = os.path.join(
                        deployment_video_starboard_path, self.get_video_output_file_name(deployment_config, "VCS", iso_timestamp, starboard_file_id)
                    )

                    # Check if input and output file paths are the same
                    if str(port_video_path) == str(output_file_name_port) and str(starboard_video_path) == str(output_file_name_starboard):
                        self.logger.info(f'SKIPPING FILE - input and output file names are identical: "{port_video_path}"')
                        self.logger.info(f'SKIPPING FILE - input and output file names are identical: "{starboard_video_path}"')
                    else:
                        # Only rename files if not in --dry-run mode
                        self.logger.info(f'Renaming file "{os.path.basename(port_video_path)}" to: "{output_file_name_port}"')
                        self.logger.info(f'Renaming file "{os.path.basename(starboard_video_path)}" to: "{output_file_name_starboard}"')
                        if not self.dry_run:
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
                                self.replace_string_in_json(
                                    str(deployment_video_port_path / ".exif_MP4.json"),
                                    str(os.path.basename(port_video_path)),
                                    str(os.path.basename(output_file_name_port)),
                                )
                                self.replace_string_in_json(
                                    str(deployment_video_starboard_path / ".exif_MP4.json"),
                                    str(os.path.basename(starboard_video_path)),
                                    str(os.path.basename(output_file_name_starboard)),
                                )
                            except Exception as e:
                                self.logger.error(f"Error replacing new filename in json file: {e}")

                # Move all remaining files into a 'misc' directory
                self.move_ancillary_files(deployment_video_port_path)
                self.move_ancillary_files(deployment_video_starboard_path)

    def get_video_output_file_name(self, deployment_config: dict, camera_direction: str, iso_timestamp: str, file_id: str) -> str:
        # Construct and return new filename
        return (
            f'{self.instrument_config.get("id")}_'
            f'{deployment_config.get("unit_id")}_'
            f"{camera_direction}_"
            f'{deployment_config.get("deployment_id")}_'
            f"{iso_timestamp}_"
            f"{file_id}"
            f".MP4"
        )

    def run_init(self, card_paths: list, all: bool, days: int, overwrite: bool, card_size: int, format_type: str):
        """
        Implementation of the MarImBA init command for BRUVS
        """

        def make_xml(file_path):
            if (os.path.exists(file_path)) and (not overwrite):
                self.logger.warning(f"SKIPPING - SD card already initialised: {file_path}")
            else:
                env = Environment(loader=FileSystemLoader(self.root_path), trim_blocks=True, lstrip_blocks=True)
                template = env.get_template("import.yml")
                fill = {
                    "instrument_path": self.root_path,
                    "instrument": self.instrument_config["id"],
                    "import_date": f"{datetime.now() + timedelta(days=days):%Y-%m-%d}",
                    "import_token": str(uuid.uuid4())[0:8],
                }
                self.logger.info(f'Making import file "{file_path}"')
                if not self.dry_run:
                    with open(file_path, "w") as file:
                        file.write(template.render(fill))

        if isinstance(card_paths, list):
            [make_xml(f"{file}/import.yml") for file in card_paths]
        else:
            make_xml(f"{card_paths}/import.yml")

    # Function to execute shell commands
    def execute_command(self, command):
        if not self.dry_run:
            process = subprocess.Popen(shlex.split(command))
            process.wait()

    # Function to load YAML files safely
    def load_yaml(self, file_path):
        with file_path.open("r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError:
                self.logger.error(f"Possible corrupt YAML file at: {file_path}")
                return None

    # Stage files
    def stage_files(self, card_paths, all, exiftool_path, copy, move, file_extension, card_size, format_type):
        # Attempt to automatically find SD cards if card_paths is not defined
        if all and not card_paths:
            card_paths = list_sd_cards(format_type, card_size)

        # TODO: Change this to raise panel method...
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
                bar_numbers = pd.read_csv(bar_path, parse_dates=["BarStartDate", "BarEndDate"])

                # Fetch metadata from video files
                command = f"{exiftool_path} -api largefilesupport=1 -u -json -ext {file_extension} -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {video_path}"
                process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _ = process.communicate()
                data = StringIO(out.decode("utf-8"))

                # Convert metadata to a DataFrame
                cameras = pd.read_json(data)
                cameras["Duration"] = pd.to_timedelta(cameras.Duration)
                cameras["CreateDate"] = pd.to_datetime(cameras["CreateDate"], format="%Y:%m:%d %H:%M:%S")

                # Log errors for corrupt video files
                for index, error in cameras[cameras.CameraSerialNumber.isnull()].iterrows():
                    self.logger.error(f"Possible corrupt video file {error.SourceFile}")

                # Drop rows with missing data
                cameras.dropna(inplace=True)

                # Merge camera DataFrame with bar numbers DataFrame
                cameras = cameras.merge(bar_numbers.loc[bar_numbers.Active], on="CameraSerialNumber", how="left")

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
                last = cameras.loc[cameras.CreateDate == cameras.CreateDate.max()].sort_values("SourceFile").iloc[-1]

                # Update import details with new metadata
                import_details.update(
                    {
                        "instrument_path": self.root_path,
                        "bruv_frame": last.Frame,
                        "housing_label": last.GoProNumber,
                        "camera_serial_number": last.CameraSerialNumber,
                        "camera_create_date": last.CreateDate,
                    }
                )

                # Define destination path and log the copy operation
                destination = Path(import_details["import_template"].format(**import_details))
                self.logger.info(f"Copy {card} --> {destination}")

                # Execute move command if applicable
                if move:
                    command = f"rclone move {video_path.resolve()} {destination.resolve()} --progress --delete-empty-src-dirs"
                    self.logger.info(f"Using Rclone to move {video_path.resolve()} to {destination.resolve()}")
                    destination.mkdir(parents=True, exist_ok=True)
                    self.execute_command(command)
                    return

                # Execute copy command if applicable
                if copy:
                    command = f"rclone copy {video_path.resolve()} {destination.resolve()} --progress --low-level-retries 1 "
                    self.logger.info(f"Using Rclone to copy {video_path.resolve()} to {destination.resolve()}")
                    destination.mkdir(parents=True, exist_ok=True)
                    self.execute_command(command)

                if not move and not copy:
                    self.logger.warning(f"Neither move or copy operations were set so no files have been staged.")

            # Log a warning if no video files are found
            else:
                self.logger.warning(f"No {file_extension} files found at {video_path}")

    # Main function for importing files
    def run_import(self, card_paths, all, exiftool_path, copy, move, file_extension, card_size, format_type, stage=True, doit=True):
        if stage:
            self.logger.info(f"Running Marimba import staging files")
            self.stage_files(card_paths, all, exiftool_path, copy, move, file_extension, card_size, format_type)

        if doit:
            self.logger.info(f"Running Marimba import doit")
            # Create an instance of the task class
            my_task_class = MyDoitClass

            # Inject the Marimba logger
            my_task_class.logger = self.logger

            # Execute doit tasks
            DoitMain(ModuleTaskLoader(vars(my_task_class))).run([])


import glob
import os
import shutil
import sqlite3
from pathlib import Path

import jinja2
import pandas as pd
import yaml
from doit import create_after
from doit import get_var
from doit.task import clean_targets
from doit.tools import run_once


class MyDoitClass:
    cfg = None
    CATALOG_DIR = None
    DOIT_CONFIG = {"check_file_uptodate": "timestamp", "continue": True}
    format_type = "exfat"

    def geturl(key):
        global cfg
        global CATALOG_DIR
        environment = jinja2.Environment()
        template = environment.from_string(cfg["paths"][key])
        return template.render(CATALOG_DIR=CATALOG_DIR)

    def task_config():
        def loadconfig(config):
            global cfg
            global CATALOG_DIR
            global COLLECTION_DIR
            with open(config, "r") as ymlfile:
                cfg = yaml.load(ymlfile, yaml.SafeLoader)
            CATALOG_DIR = os.path.dirname(os.path.abspath(config))
            CATALOG_DIR = os.path.join(os.path.dirname(CATALOG_DIR), "work")
            COLLECTION_DIR = Path(CATALOG_DIR).resolve().parents[2]

        config = {"config": get_var("config", f"{os.path.split(__file__)[0]}/config.yml")}
        loadconfig(config["config"])

    def task_create_json():
        for path in glob.glob(os.path.join(MyDoitClass.geturl("cardstore"), "**", "../work"), recursive=True):
            path = os.path.abspath(path)
            if glob.glob(os.path.join(path, "*.MP4")):
                target = os.path.join(path, cfg["paths"]["exifname"])
                file_dep = glob.glob(os.path.join(path, "*.MP4"))
                command = f"exiftool -api largefilesupport=1 -m -u -q -q -n -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView -json -ext MP4 {path} > {target} || :"
                if file_dep:
                    yield {
                        "name": path,
                        "file_dep": file_dep,
                        "actions": [command],
                        "targets": [target],
                        "uptodate": [run_once],
                        "clean": True,
                    }

    @create_after(executed="create_json", target_regex=".*\.json")
    def task_concat_json():
        def concat(dependencies, targets):
            data = pd.concat([pd.read_json(dep) for dep in dependencies])
            data["Bad"] = data["CreateDate"].isna()
            data["SourceFile"] = data.apply(lambda x: f"{{CATALOG_DIR}}/{os.path.relpath(x['SourceFile'], CATALOG_DIR)}", axis=1)
            data["Directory"] = data["SourceFile"].apply(lambda x: os.path.split(x)[0])
            data["FileName"] = data["SourceFile"].apply(os.path.basename)
            data[["ItemId", "GroupId"]] = data.FileName.str.extract("(?P<item>\d\d)(?P<group>\d\d\d\d).MP4")
            data = data.sort_values(["SourceFile"])
            data["CreateDate"] = pd.to_datetime(data.CreateDate, format="%Y:%m:%d  %H:%M:%S")
            # ok lets try and fix missing data from bad videos
            data["RunTime"] = data.groupby(["CameraSerialNumber", "GroupId"])["Duration"].cumsum()
            data = data.sort_values(["SourceFile"])
            data.to_csv(targets[0], index=False)

        exiffiles = glob.glob(os.path.join(MyDoitClass.geturl("cardstore"), "**", cfg["paths"]["exifname"]), recursive=True)
        if exiffiles:
            return {
                "file_dep": exiffiles,
                "actions": [concat],
                "targets": [MyDoitClass.geturl("exifstore")],
                "uptodate": [True],
                "clean": True,
            }

    @create_after(executed="concat_json", target_regex=".*\.json")
    def task_checkbars():
        def checkbars(dependencies, targets):
            data = pd.read_csv(dependencies[0], parse_dates=["CreateDate"])
            stats = data.groupby("CameraSerialNumber")["CreateDate"].agg([("CreateStart", "min"), ("CreateEnd", "max")]).reset_index()
            barnumbers = pd.read_csv(targets[0], parse_dates=["BarStartDate", "BarEndDate"]).drop(
                ["CreateStart", "CreateEnd"], axis=1, errors="ignore"
            )
            barnumbers = pd.merge(barnumbers, stats)
            barnumbers.to_csv(targets[0], index=False)

        return {
            "file_dep": [MyDoitClass.geturl("exifstore")],
            "actions": [checkbars],
            "targets": [MyDoitClass.geturl("barstore")],
            "uptodate": [True],
            "clean": True,
        }

    @create_after(executed="checkbars", target_regex=".*\.json")
    def task_make_autodeployments():
        def deployments(dependencies, targets):
            data = pd.read_csv(MyDoitClass.geturl("exifstore"), parse_dates=["CreateDate"])
            totaltime = (
                pd.to_datetime(data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])["Duration"].sum(), unit="s")
                .dt.strftime("%H:%M:%S")
                .rename("TotalTime")
            )
            totalfilesize = (data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])["FileSize"].sum() / 1000000000).rename(
                "TotalSize"
            )
            maxid = data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])["ItemId"].max().rename("MaxId")
            minid = data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])["ItemId"].min().rename("MinId")
            filecount = data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])["ItemId"].count().rename("FileCount")
            groups = data.groupby(["Directory", "CreateDate", "CameraSerialNumber", "GroupId"])[["SourceFile", "FieldOfView"]].first()
            output = groups.join(filecount).join(minid).join(maxid).join(totalfilesize).join(totaltime)
            barpath = f"{CATALOG_DIR}/camerabars.csv"
            barnumbers = pd.read_csv(barpath, parse_dates=["BarStartDate", "BarEndDate"])
            result = matchbars(output.reset_index(), barnumbers)
            result["CreateDate"] = pd.to_datetime(result["CreateDate"])
            result["DeploymentId"] = result.apply(
                lambda x: f"{x.CreateDate.strftime('%Y%m%dT%H%M%S')}_{x.Frame}_{x.GoProNumber}_{x.CameraSerialNumber}_{x.GroupId:02}", axis=1
            )

            manualfile = MyDoitClass.geturl("timecorrection")
            manual = result.loc[:, ["DeploymentId", "TotalTime", "CreateDate", "SourceFile"]]
            manual = manual.set_index("DeploymentId")
            if os.path.exists(manualfile):
                old = pd.read_csv(manualfile, index_col="DeploymentId")
                manual = manual.join(old["CorrectedTime"])
                manual.loc[manual.CorrectedTime.isnull(), "CorrectedTime"] = manual.loc[manual.CorrectedTime.isnull(), "CreateDate"]
            else:
                manual["CorrectedTime"] = manual["CreateDate"]
            manual["SourceFile"] = manual["SourceFile"].apply(lambda x: f'=HYPERLINK("file://{x}", "{os.path.basename(x)}")')
            manual.sort_values("DeploymentId").to_csv(manualfile)
            manual.sort_values("DeploymentId").to_excel(manualfile.replace(".csv", ".xlsx"))
            result.to_csv(targets[0], index=False)

        target = MyDoitClass.geturl("autodeployment")
        return {
            "file_dep": [MyDoitClass.geturl("exifstore")],
            "actions": [deployments],
            "targets": [target, MyDoitClass.geturl("timecorrection")],
            "uptodate": [True],
            "clean": True,
        }

    def matchbars(deployments, barnumbers, datecolumn="CreateDate"):
        conn = sqlite3.connect(":memory:")
        # write the tables
        barnumbers.to_sql("bars", conn, index=False)
        deployments[deployments.columns[~deployments.columns.isin(["Frame", "HousingNumber", "GoProNumber", "BarStartDate", "BarEndDate"])]].to_sql(
            "deployments", conn, index=False
        )
        qry = f"""
            select  
                deployments.*,
                bars.Frame,
                bars.HousingNumber,
                bars.GoProNumber,
                bars.BarStartDate,
                bars.BarEndDate,
                bars.CalibrationDate
            from
                deployments join bars on
                (deployments.{datecolumn} between bars.BarStartDate and bars.BarEndDate) and
                (deployments.CameraSerialNumber = bars.CameraSerialNumber)
            """
        result = pd.read_sql_query(qry, conn)
        result["CreateDate"] = pd.to_datetime(result["CreateDate"])
        result["DeploymentId"] = result.apply(
            lambda x: f"{x.CreateDate.strftime('%Y%m%dT%H%M%S')}_{x.Frame}_{x.GoProNumber}_{x.CameraSerialNumber}", axis=1
        )
        return result

    @create_after(executed="make_autodeployments", target_regex=".*\.json")
    def task_make_matchbars():
        def stagedeployments(dependencies, targets):
            def calculatetimes(df):
                df = df.sort_values("ItemId")
                if len(df) > 1:
                    start = df.Duration.cumsum().shift(+1)
                    start.iloc[0] = 0
                    start = pd.to_timedelta(start, unit="S")
                    df["CalculatedStartTime"] = (df["CorrectedTime"] + start).dt.round("1S")
                else:
                    df["CalculatedStartTime"] = df["CorrectedTime"]
                return df

            def makedeploymentkey(df):
                def makedirs(row):
                    left = 0
                    right = 0
                    if leftcam in row.keys():
                        left = row[leftcam]
                    if rightcam in row.keys():
                        right = row[rightcam]
                    if right == left:
                        result = f"{row['StageId']}_{int(left):02}"
                    else:
                        result = f"{row['StageId']}_{int(left):02}_{int(right):02}"
                    return row["StageId"], result

                leftcam = "L" + df.Frame.min()[-2:]
                rightcam = "R" + df.Frame.min()[-2:]
                left = (
                    df[df.GoProNumber == leftcam]
                    .groupby("CorrectedTime")
                    .first()
                    .reset_index()[["CorrectedTime", "GoProNumber", "Frame"]]
                    .add_suffix("_Left")
                )
                left["MatchTime"] = left["CorrectedTime_Left"]
                right = (
                    df[df.GoProNumber == rightcam]
                    .groupby("CorrectedTime")
                    .first()
                    .reset_index()[["CorrectedTime", "GoProNumber", "Frame"]]
                    .add_suffix("_Right")
                )
                right["MatchTime"] = right["CorrectedTime_Right"]
                merged_df = pd.merge_asof(
                    right,
                    left,
                    left_on="MatchTime",
                    right_on="MatchTime",
                    direction="nearest",
                    tolerance=pd.Timedelta(minutes=30),
                    suffixes=("_right", "_left"),
                )
                merged_df = pd.concat([merged_df, left[~left.CorrectedTime_Left.isin(merged_df.CorrectedTime_Left.unique())]])
                merged_df.loc[merged_df.CorrectedTime_Right.isna(), "Frame_Right"] = merged_df.loc[merged_df.CorrectedTime_Right.isna(), "Frame_Left"]
                merged_df.loc[merged_df.CorrectedTime_Left.isna(), "CorrectedTime_Left"] = merged_df.loc[
                    merged_df.CorrectedTime_Left.isna(), "CorrectedTime_Right"
                ]
                starttime = merged_df.MatchTime.dt.strftime("%Y%m%dT%H%M%S")
                merged_df["StageId"] = merged_df.Frame_Right + "_" + starttime
                stageId = pd.concat(
                    (
                        merged_df[["CorrectedTime_Left", "StageId"]].rename(columns={"CorrectedTime_Left": "CorrectedTime"}),
                        merged_df[["CorrectedTime_Right", "StageId"]].rename(columns={"CorrectedTime_Right": "CorrectedTime"}),
                    )
                ).dropna()
                df = pd.merge(df, stageId)
                totals = (
                    df.groupby(["StageId", "GoProNumber"])
                    .size()
                    .reset_index()
                    .pivot_table(index="StageId", values=0, columns="GoProNumber")
                    .reset_index()
                    .fillna(0)
                )
                totals = totals.apply(makedirs, axis=1).apply(pd.Series)
                totals.columns = ["StageId", "StageDir"]
                df = df.merge(totals)
                return df

            dep = pd.read_csv(MyDoitClass.geturl("autodeployment"), parse_dates=["CreateDate"])
            exifdata = pd.read_csv(MyDoitClass.geturl("exifstore"), parse_dates=["CreateDate"]).set_index(
                ["CreateDate", "CameraSerialNumber", "GroupId"]
            )
            correcttimes = pd.read_csv(MyDoitClass.geturl("timecorrection"), parse_dates=["CreateDate", "CorrectedTime"])
            dep = pd.merge(dep, correcttimes[["DeploymentId", "CorrectedTime"]], on="DeploymentId", how="left").set_index(
                ["CreateDate", "CameraSerialNumber", "GroupId"]
            )
            combined = dep.join(exifdata, rsuffix="_exif").reset_index()
            combined = combined.drop_duplicates(subset=["CameraSerialNumber", "CreateDate", "GroupId", "ItemId"], keep="last")
            combined = combined.sort_values(["CorrectedTime", "GroupId", "ItemId"])
            combined = combined.groupby(["CreateDate", "CameraSerialNumber", "GroupId"], group_keys=False).apply(calculatetimes).reset_index()
            barpath = f"{CATALOG_DIR}/camerabars.csv"
            barnumbers = pd.read_csv(barpath, parse_dates=["BarStartDate", "BarEndDate"])
            result = matchbars(combined, barnumbers, datecolumn="CalculatedStartTime")
            result["CalculatedStartTime"] = pd.to_datetime(result["CalculatedStartTime"])
            result["CorrectedTime"] = pd.to_datetime(result["CorrectedTime"])
            result["StageName"] = result.apply(
                lambda x: f'{x.Frame}_{x.GoProNumber}_{x.CalculatedStartTime.strftime("%Y%m%dT%H%M%S")}_{x.CameraSerialNumber}_{int(x.GroupId):02d}_{int(x.ItemId):02d}.MP4',
                axis=1,
            )
            result = result.drop_duplicates(subset=["CameraSerialNumber", "CreateDate", "GroupId", "ItemId"], keep="last")
            result = result.groupby("Frame").apply(makedeploymentkey)
            result.to_csv(targets[0], index=False)

        return {
            "file_dep": [MyDoitClass.geturl("autodeployment"), MyDoitClass.geturl("exifstore"), MyDoitClass.geturl("barstore")],
            "actions": [stagedeployments],
            "targets": [MyDoitClass.geturl("stage")],
            "uptodate": [True],
            "clean": True,
        }

    @create_after(executed="make_autodeployments", target_regex=".*\.json")
    def task_stage_data():
        def hardlink(dependencies, targets):
            stage = pd.read_csv(MyDoitClass.geturl("stage"))
            stage["target"] = stage.apply(lambda x: os.path.join(CATALOG_DIR, "stage", x.StageDir, x.StageName), axis=1)
            stage["SourceFile"] = stage["SourceFile"].apply(lambda x: x.format(**{"CATALOG_DIR": CATALOG_DIR}))
            for index, row in stage.iterrows():
                if not os.path.exists(row.target):
                    dir = os.path.split(row.target)[0]
                    os.makedirs(dir, exist_ok=True)
                    os.link(row.SourceFile, row.target)

        def delete_empty_folders(dryrun):
            for dirpath, dirnames, filenames in os.walk(os.path.join(CATALOG_DIR, "stage"), topdown=False):
                for dirname in dirnames:
                    full_path = os.path.join(dirpath, dirname)
                    if not os.listdir(full_path):
                        if dryrun:
                            print(f"Remove dir {full_path}")
                        else:
                            os.rmdir(full_path)

        if os.path.exists(MyDoitClass.geturl("stage")):
            stage = pd.read_csv(MyDoitClass.geturl("stage"))
            targets = stage.apply(lambda x: os.path.join(CATALOG_DIR, "stage", x.StageDir, x.StageName), axis=1).unique().tolist()
            return {
                "file_dep": [MyDoitClass.geturl("stage")],
                "actions": [hardlink],
                "targets": targets,
                "uptodate": [True],
                "clean": [clean_targets, delete_empty_folders],
            }

    @create_after(executed="stage_data", target_regex=".*\.json")
    def task_update_stationinformation():
        def finalnames(dependencies, targets):
            stage = pd.read_csv(MyDoitClass.geturl("stage"), index_col="StageId")
            stage = stage.groupby("StageId")["CalculatedStartTime"].agg([("VideoStart", "min"), ("VideoEnd", "max")]).reset_index()
            if os.path.exists(MyDoitClass.geturl("stationinfo")):
                stations = pd.read_csv(MyDoitClass.geturl("stationinfo"), index_col="StageId")
            else:
                stations = pd.DataFrame(
                    columns=[
                        "StartTime",
                        "FinishTime",
                        "CollectionId",
                        "Station",
                        "Operation",
                        "Latitude",
                        "Longitude",
                        "Depth",
                        "VideoStart",
                        "VideoEnd",
                    ]
                )
            stations = stations.join(stage.groupby("StageId").first(), how="outer", rsuffix="_Stage")
            stations["Frame"] = stations.index
            stations[["Frame", "CameraTime"]] = stations.Frame.str.split("_", expand=True)
            stations.loc[stations.VideoStart_Stage.isna(), "VideoStart_Stage"] = stations.loc[stations.VideoStart_Stage.isna(), "VideoStart"]
            stations.loc[stations.VideoStart_Stage.isna(), "VideoEnd_Stage"] = stations.loc[stations.VideoEnd_Stage.isna(), "VideoEnd"]
            stations[["VideoStart", "VideoEnd"]] = stations[["VideoStart_Stage", "VideoEnd_Stage"]]
            stations.drop(["VideoStart_Stage", "VideoEnd_Stage"], axis=1).sort_values("VideoStart").to_csv(MyDoitClass.geturl("stationinfo"))

        return {
            "file_dep": [MyDoitClass.geturl("stage")],
            "targets": [MyDoitClass.geturl("stationinfo")],
            "actions": [finalnames],
            "uptodate": [run_once],
            "clean": True,
        }

    @create_after(executed="update_stationinformation", target_regex=".*\.json")
    def task_process_names():
        def hardlink(dependencies, targets):
            stage = pd.read_csv(MyDoitClass.geturl("stage"))
            targets = stage.apply(lambda x: os.path.join(CATALOG_DIR, "deployments", x.StageDir, x.StageName), axis=1).unique().tolist()
            station = pd.read_csv(MyDoitClass.geturl("stationinfo"))
            comb = pd.merge(stage, station).dropna(subset=["CollectionId"])
            comb.CalculatedStartTime = pd.to_datetime(comb.CalculatedStartTime)
            comb.CorrectedTime = pd.to_datetime(comb.CorrectedTime)
            comb["target"] = comb.apply(
                lambda x: os.path.join(
                    CATALOG_DIR,
                    "deployments",
                    f'{x.CorrectedTime.strftime("%Y%m%d")}',
                    f"{x.CollectionId}_{x.Station}_{x.StageId}",
                    f'{x.CollectionId}_{x.Station}_{x.Frame}_{x.GoProNumber}_{x.CalculatedStartTime.strftime("%Y%m%dT%H%M%S")}_{x.CameraSerialNumber}_{int(x.GroupId):02d}_{int(x.ItemId):02d}.MP4',
                ),
                axis=1,
            )
            comb["SourceFile_exif"] = comb["SourceFile_exif"].apply(lambda x: x.format(**{"CATALOG_DIR": CATALOG_DIR}))
            comb.to_csv(MyDoitClass.geturl("renamed"), index=False)
            for index, row in comb.iterrows():
                if not os.path.exists(row.target):
                    dir = os.path.split(row.target)[0]
                    os.makedirs(dir, exist_ok=True)
                    os.link(row.SourceFile_exif, row.target)

        if os.path.exists(MyDoitClass.geturl("stage")):
            stage = pd.read_csv(MyDoitClass.geturl("stage"))
            targets = stage.apply(lambda x: os.path.join(CATALOG_DIR, "deployments", x.StageDir, x.StageName), axis=1).unique().tolist()
            station = pd.read_csv(MyDoitClass.geturl("stationinfo"))
            comb = pd.merge(stage, station).dropna(subset=["CollectionId"])
            comb.CalculatedStartTime = pd.to_datetime(comb.CalculatedStartTime)
            comb.CorrectedTime = pd.to_datetime(comb.CorrectedTime)
            targets = (
                comb.apply(
                    lambda x: os.path.join(
                        MyDoitClass.geturl("deployments"),
                        f'{x.CorrectedTime.strftime("%Y%m%d")}',
                        f"{x.CollectionId}_{x.Station}_{x.StageId}",
                        f'{x.CollectionId}_{x.Station}_{x.Frame}_{x.GoProNumber}_{x.CalculatedStartTime.strftime("%Y%m%dT%H%M%S")}_{x.CameraSerialNumber}_{int(x.GroupId):02d}_{int(x.ItemId):02d}.MP4',
                    ),
                    axis=1,
                )
                .unique()
                .tolist()
            )
            MyDoitClass.geturl("stationinfo")
            return {
                "file_dep": [MyDoitClass.geturl("stationinfo"), MyDoitClass.geturl("stage")],
                "targets": targets,
                "actions": [hardlink],
                "uptodate": [run_once],
                "clean": True,
            }

    @create_after(executed="process_names", target_regex=".*\.json")
    def task_make_calfiles():
        def cals(dependencies, targets):
            for file in targets:
                if not os.path.exists(file):
                    shutil.copy(os.path.join(MyDoitClass.geturl("calstore"), "GoPro9WideWater_default.CamCAL"), file)

        if os.path.exists(MyDoitClass.geturl("barstore")):
            bars = pd.read_csv(MyDoitClass.geturl("barstore"), parse_dates=["CalibrationDate"])
            targets = bars.apply(
                lambda x: f"{MyDoitClass.geturl('calstore')}/{x.GoProNumber}_{x.CalibrationDate.strftime('%Y%m%d')}_{x.CameraSerialNumber}.CamCAL",
                axis=1,
            ).tolist()
            return {
                "file_dep": [MyDoitClass.geturl("barstore")],
                "targets": targets,
                "actions": [cals],
                "uptodate": [run_once],
                "clean": True,
            }

    @create_after(executed="make_calfiles", target_regex=".*\.json")
    def task_move_calfiles():
        def movecals(dependencies, targets):
            renamed = pd.read_csv(MyDoitClass.geturl("renamed"), parse_dates=["CalibrationDate"])
            renamed["DeploymentPath"] = renamed.target.apply(lambda x: os.path.split(x)[0])
            renamed = renamed.groupby(["DeploymentPath", "GoProNumber"]).first().reset_index()
            renamed["CalibrationSouce"] = renamed.apply(
                lambda x: f"{MyDoitClass.geturl('calstore')}/{x.GoProNumber}_{x.CalibrationDate.strftime('%Y%m%d')}_{x.CameraSerialNumber}.CamCAL",
                axis=1,
            ).tolist()
            for index, row in renamed.iterrows():
                shutil.copy(row.CalibrationSouce, os.path.join(row.DeploymentPath, os.path.basename(row.CalibrationSouce)))

        return {
            "file_dep": [MyDoitClass.geturl("barstore"), MyDoitClass.geturl("renamed")],
            "actions": [movecals],
            "uptodate": [run_once],
            "clean": True,
        }

    def delete_empty_folders(dryrun):
        for dirpath, dirnames, filenames in os.walk(os.path.join(CATALOG_DIR, "stage"), topdown=False):
            for dirname in dirnames:
                full_path = os.path.join(dirpath, dirname)
                if not os.listdir(full_path):
                    if dryrun:
                        print(f"Remove dir {full_path}")
                    else:
                        os.rmdir(full_path)
