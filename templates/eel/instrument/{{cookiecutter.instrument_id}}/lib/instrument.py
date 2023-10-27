"""
Drop Camera Fusion 360 (DCF360) instrument specification
"""

import csv
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

from marimba.core.pipeline import BasePipeline
from marimba.utils.config import load_config

__author__ = "Carlie Devine"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = ["Chris Jackett"]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "carlie.devine@csiro.au"
__status__ = "Development"


class DropCameraFusion360(BasePipeline):
    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict, dry_run: bool):
        super().__init__(root_path, collection_config, instrument_config, dry_run)

        # Define instrument filetypes and data files
        self.video_filetypes = ["mp4"]

    def process(self, deployment_path: Path):
        """
        Implementation of the Marimba process command for the DCF360
        """

        self.logger.info("Process command")

    @staticmethod
    def get_sorted_directory_file_list(directory):
        """Return a list of files with a case-insensitive .mp4 extension in the given directory."""
        files = [filename for filename in os.listdir(str(directory)) if filename.lower().endswith(".mp4")]
        return sorted(files, key=lambda s: s.lower())

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

    def process_directory(self, deployment_video_path, deployment_config, camera_direction):
        deployment_video_list = self.get_sorted_directory_file_list(deployment_video_path)

        for video in deployment_video_list:
            video_path = deployment_video_path / video

            parser = createParser(str(video_path))
            metadata = extractMetadata(parser)

            # TODO: Try except block here
            for line in metadata.exportPlaintext():
                if "Creation date" in line:
                    creation_date_str = line.split(": ", 1)[1]
                    creation_date = datetime.strptime(creation_date_str, "%Y-%m-%d %H:%M:%S")
                    iso_timestamp = creation_date.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

            # Find match for file ID starting with 'G' and ending with 4 numbers
            match = re.search(r"G.*\d{4}$", Path(video).stem)
            file_id = match.group(0) if match else None

            output_file_name = deployment_video_path / self.get_video_output_file_name(deployment_config, camera_direction, iso_timestamp, file_id)

            # Check if input and output file paths are the same
            if str(video_path) == str(output_file_name):
                self.logger.info(f'SKIPPING FILE - input and output file names are identical: "{video_path}"')
            else:
                # Only rename files if not in --dry-run mode
                self.logger.info(f'Renaming file "{os.path.basename(video_path)}" to: "{output_file_name}"')
                if not self.dry_run:
                    try:
                        # Rename file
                        os.rename(video_path, output_file_name)
                    # TODO: Check this is the correct exception to catch
                    except FileExistsError:
                        self.logger.error(f"Error renaming file {os.path.basename(video_path)} to {output_file_name}")

    def add_video_annotation_rows(self, annotations_file_path, video_path):
        video_list = self.get_sorted_directory_file_list(video_path)

        # Loop through all files in the directory
        for filename in video_list:
            # Check if the file is an mp4 video
            if filename.endswith(".MP4"):
                filename_split = filename.split("_")
                if len(filename_split) != 6:
                    self.logger.warning(f"Filename does not appear to have all the metatdata elements: {filename}")
                    continue

                # Create a row for this video
                instrument_id, camera_direction, survey_id, deployment_id, iso_timestamp, file_id = filename.split("_")
                row = [instrument_id, camera_direction, survey_id, deployment_id, filename, iso_timestamp, "", "", "", "", "", "", ""]

                # Append the row to the CSV file
                with open(annotations_file_path, "a", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(row)

    def get_video_output_file_name(self, deployment_config: dict, camera_direction: str, iso_timestamp: str, file_id: str) -> str:
        # Construct and return new filename
        return (
            f'{self.instrument_config.get("id")}_'
            f"{camera_direction}_"
            f'{deployment_config.get("deployment_id")}_'
            f"{iso_timestamp}_"
            f"{file_id}"
            f".MP4"
        )

    def rename(self, deployment_path: Path):
        """
        Implementation of the Marimba rename command for the DCF360
        """

        self.logger.info(f'Renaming files in Marimba deployment: "{deployment_path}"')

        # Get deployment name and load deployment config
        deployment_name = Path(deployment_path).name
        deployment_config_path = Path(deployment_path) / Path(deployment_name + ".yml")
        deployment_config = load_config(deployment_config_path)
        deployment_video_front_path = Path(deployment_path) / "video" / "front"
        deployment_video_back_path = Path(deployment_path) / "video" / "back"

        # Rename images in both front and back video paths
        self.process_directory(deployment_video_front_path, deployment_config, "VCF")
        self.process_directory(deployment_video_back_path, deployment_config, "VCB")

        # Move all remaining files into a 'misc' directory
        self.move_ancillary_files(deployment_video_front_path)
        self.move_ancillary_files(deployment_video_back_path)

        # Generate an annotation template based on the renamed video files
        annotations_file_path = Path(deployment_path) / "eel_annotations.csv"
        annotations_columns = [
            "Instrument ID",
            "Camera Direction",
            "Survey ID",
            "Deployment Op",
            "Filename",
            "Timestamp",
            "Eel Count",
            "Elapsed Time",
            "Camera Movement",
            "Comment",
            "Longitude",
            "Latitude",
            "Depth",
        ]

        # Create the CSV file but do not overwrite if it already exists
        if annotations_file_path.exists():
            self.logger.info(f'SKIPPING FILE - Annotation file already exists: "{annotations_file_path}"')
        else:
            if not self.dry_run:
                with open(annotations_file_path, "w", newline="") as annotations_file:
                    writer = csv.writer(annotations_file)
                    writer.writerow(annotations_columns)

                # Add a single annotation row for each video
                self.add_video_annotation_rows(annotations_file_path, deployment_video_front_path)
                self.add_video_annotation_rows(annotations_file_path, deployment_video_back_path)
