"""
Canon EOS 600D and 700D instrument specification
"""

import datetime
import os
import re
import shutil
from pathlib import Path

import cv2 as cv
import numpy as np
import pandas as pd
import stitching
from PIL import Image
from PIL.ExifTags import TAGS
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

import marimba.utils.file_system as fs
from marimba.core.instrument import Instrument

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2023, Environment, CSIRO"
__credits__ = [
    "Carlie Devine",
]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"


class CanonEOS(Instrument):
    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        super().__init__(root_path, collection_config, instrument_config)

        # Define instrument filetypes and data files
        self.filetypes = ["jpg"]

    @staticmethod
    def get_iso_timestamp(image_path):
        try:
            img = Image.open(image_path)
            exif_data = img._getexif()

            if exif_data is None:
                print(f"No EXIF data found in {image_path}")
                return None

            exif = {TAGS[k]: v for k, v in exif_data.items() if k in TAGS}

            if "DateTimeOriginal" in exif:
                iso_timestamp = exif["DateTimeOriginal"]
                return iso_timestamp
            else:
                print(f"No ISO timestamp found in {image_path}")
                return None

        except Exception as e:
            print(f"Error processing {image_path}: {e}")
            return None

    def get_output_file_name(self, file_path: str) -> str:
        iso_timestamp = self.get_iso_timestamp(file_path).replace(":", "").replace(" ", "T") + "Z"

        if iso_timestamp:
            return iso_timestamp + ".JPG"
        else:
            return None

    def get_output_directory_path(self, file_path: str) -> str:
        iso_timestamp = self.get_iso_timestamp(file_path)

        if iso_timestamp:
            date_part, _ = iso_timestamp.split(" ", 1)
            year, month, day = date_part.split(":")
            return Path(year) / Path(month) / Path(day)
        else:
            return None

    def rename(self, dry_run: bool):
        """
        Implementation of the MarImBA rename command for the Canon EOS 600D and 700D cameras
        """

        # Set dry run log string to prepend to logging
        dry_run_prefix = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):
            # Get deployment name and config path
            deployment_name = deployment.path.split("/")[-1]
            deployment_config_path = Path(deployment.path) / Path(deployment_name + ".yml")

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'{dry_run_prefix}SKIPPING DEPLOYMENT - Cannot find deployment metadata file in deployment directory at path: "{deployment.path}/{deployment_name}.yml"'
                )
                continue
            else:
                self.logger.info(f'{dry_run_prefix}Found MarImBA deployment file at path: "{deployment.path}/{deployment_name}.yml"')

                # Define regex to match any of the filetypes to be renamed
                extensions_pattern = f'({"|".join(re.escape(extension) for extension in self.filetypes)})$'

                # Traverse the deployment raw directory
                for root, dirs, files in os.walk(Path(deployment.path) / "raw"):
                    # Loop through each file
                    for file in files:
                        # Construct file path
                        file_path = Path(root) / file

                        # Match case-insensitive regex expression in file path
                        if re.search(extensions_pattern, str(file_path), re.IGNORECASE):
                            # Get the output filename, directory and construct file path
                            output_file_name = self.get_output_file_name(file_path)
                            output_directory_path = Path(deployment.path) / "processed" / "images" / self.get_output_directory_path(file_path)
                            output_file_path = output_directory_path / output_file_name

                            # Check if input and output file paths are the same
                            if file_path == output_file_path:
                                self.logger.info(f'{dry_run_prefix}SKIPPING FILE - input and output filenames are identical: "{file_path}"')
                            # Check if output file path already exists and the overwrite argument is not set
                            elif output_file_path.is_file():
                                self.logger.info(f'{dry_run_prefix}Output file already exists: "{output_file_path}"')
                            # Perform file renaming
                            else:
                                self.logger.info(f'{dry_run_prefix}Renaming file "{file}" to: "{output_file_path}"')
                                # Only rename files if not in --dry-run mode
                                if not dry_run:
                                    try:
                                        # Create directory path and rename file
                                        fs.create_directory_if_necessary(output_directory_path)
                                        shutil.copy(file_path, output_file_path)
                                    except Exception as e:
                                        self.logger.error(f"Error renaming file {file_path}: {e}")

    def process(self, dry_run: bool):
        """
        Implementation of the MarImBA process command for the Canon EOS 600D and 700D cameras.
        This implementation
        """

        # Set dry run log string to prepend to logging
        dry_run_prefix = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):
            # Get deployment name and config path
            deployment_name = deployment.path.split("/")[-1]
            deployment_config_path = Path(deployment.path) / Path(deployment_name + ".yml")

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'{dry_run_prefix}SKIPPING DEPLOYMENT - Cannot find deployment metadata file in deployment directory at path: "{deployment.path}/{deployment_name}.yml"'
                )
                continue
            else:
                self.logger.info(f'{dry_run_prefix}Found MarImBA deployment file at path: "{deployment.path}/{deployment_name}.yml"')

                # Traverse the deployment porcessed images directory
                for root, dirs, files in os.walk(Path(deployment.path) / "processed" / "images"):
                    # Check this is a bottom-level directory that contains files
                    if len(dirs) == 0 and len(files) > 0:
                        # Filter JPG files, create a sorted and re-indexed dataframe
                        jpg_files = [file for file in files if file.lower().endswith(".jpg")]
                        images_df = pd.DataFrame(jpg_files).sort_values(0).reset_index(drop=True)

                        # Convert timestamps to POSIX timestamps
                        images_df["posix_timestamp"] = images_df[0].apply(
                            lambda x: datetime.datetime.strptime(x.split(".")[0], "%Y%m%dT%H%M%SZ").timestamp()
                        )

                        # Reshape and standardise the data for clustering
                        X = np.array(images_df["posix_timestamp"]).reshape(-1, 1)
                        scaler = StandardScaler()
                        X_scaled = scaler.fit_transform(X)

                        # Perform clustering using DBSCAN
                        # eps_in_minutes = 5
                        # eps_in_seconds = eps_in_minutes * 60
                        eps_in_seconds = 60
                        # TODO: min_samples could be 24
                        dbscan = DBSCAN(eps=eps_in_seconds / scaler.scale_[0], min_samples=20, metric="euclidean")
                        images_df["cluster"] = dbscan.fit_predict(X_scaled)

                        # Process each cluster of images into a panorama
                        for cluster_num in range(images_df["cluster"].max() + 1):
                            # Subset each cluster of images
                            cluster_df = images_df[images_df["cluster"] == cluster_num]
                            file_path = Path(root) / cluster_df[0].iloc[0]

                            # Get the output filename, directory and construct file path
                            output_file_name = cluster_df[0].iloc[0]
                            output_directory_path = (
                                Path(deployment.path) / "processed" / "panoramas" / self.get_output_directory_path(file_path).parent
                            )
                            output_file_path = output_directory_path / output_file_name

                            # Check if input and output file paths are the same
                            if file_path == output_file_path:
                                self.logger.info(f'{dry_run_prefix}SKIPPING FILE - input and output filenames are identical: "{file_path}"')
                            # Check if output file path already exists and the overwrite argument is not set
                            elif output_file_path.is_file():
                                self.logger.info(f'{dry_run_prefix}Output file already exists: "{output_file_path}"')
                            # Perform file renaming
                            else:
                                # Instantiate the stitcher and create a list of images to stitch
                                settings = {
                                    "confidence_threshold": 0.5,
                                    "nfeatures": 100,
                                    # "adjuster": "no",
                                    # "crop": False
                                }
                                stitcher = stitching.Stitcher(**settings)
                                # stitcher = stitching.Stitcher()
                                images = [str((Path(root) / file).absolute()) for file in cluster_df[0].tolist()]
                                print(len(images))
                                print(images[0])

                                # Try to stitch the panorama and report a warning if not possible, mostly caused by degraded imagery from bad weather
                                try:
                                    panorama = stitcher.stitch(images)
                                    fs.create_directory_if_necessary(output_directory_path)
                                    cv.imwrite(str(output_file_path.absolute()), panorama)
                                    self.logger.info(f'{dry_run_prefix}Successfully created panorama for "{str(output_file_path.absolute())}"')
                                except Exception as e:
                                    self.logger.error(f'{dry_run_prefix}Could not create panorama for "{str(output_file_path.absolute())}\n{e}"')
