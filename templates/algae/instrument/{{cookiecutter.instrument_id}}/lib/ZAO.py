"""
Zeiss Axio Observer instrument specification
"""

import os
import re
from pathlib import Path

import czifile
import dateutil.parser
import pandas as pd

import marimba.utils.file_system as fs
from marimba.core.instrument import Instrument
from marimba.utils.config import load_config

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2022, Environment, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"


class ZeissAxioObserver(Instrument):

    def __init__(self, root_path: str, collection_config: dict, instrument_config: dict):
        super().__init__(root_path, collection_config, instrument_config)

        # Define instrument filetypes and data files
        self.filetypes = ["czi"]
        # TODO: See if we need these here...
        self.strain_list_df = pd.read_csv(f"{root_path}/lib/anacc_strain_list.csv")
        self.cs_code_list = self.strain_list_df["CS Number"].str.replace("-", "", 1).str.replace("/", "-").unique()

        # Dictionary of available imaging systems
        self.imaging_systems = {
            "IFC": "CytoBuoy CytoSense Imaging Flow Cytometer",
            "YFC": "Yokogawa FlowCam",
            "ZAO": "ZEISS Axio Observer",
            "ZAP": "ZEISS Axio Plan",
        }

        # Dictionary of available contrast settings
        self.contrast_settings = {
            "BF": "Bright Field",
            "DIC": "Differential Interference Contrast",
            "FL": "Fluorescence",
            "PC": "Phase Contrast",
        }

        # Dictionary of available biological stains
        self.biological_stains = {
            "DPI": "Dapi",
            "GTD": "Glutaraldehyde",
            "IDN": "Iodine (Lugols)",
            "TYL": "Tylose (or Carboxyl Methyl Cellulose)",
            "NA": "Not Applicable",
        }

    def rename(self, deployment_path: str, dry_run: bool):
        """
        Implementation of the MarImBA rename command for the Zeiss Axio Observer
        """

        self.logger.info(f'Renaming files in MarImBA deployment: "{deployment_path}"')

        # Get deployment name and load deployment config
        deployment_name = deployment_path.split("/")[-1]
        deployment_config_path = Path(deployment_path) / Path(deployment_name + ".yml")
        deployment_config = load_config(deployment_config_path)

        # Loop through each file in the deployment directory
        for file in os.scandir(deployment_path):

            # Define regex to match any of the filetypes to be renamed
            extensions_pattern = f'({"|".join(re.escape(extension) for extension in self.filetypes)})$'
            file_path = file.path

            # Match case-insensitive regex expression in file name
            if re.search(extensions_pattern, file_path, re.IGNORECASE):

                # Get the output filename and path
                output_file_name = self.get_output_file_name(deployment_config, file_path)
                output_file_path = Path(deployment_path) / output_file_name

                # Check if input and output file paths are the same
                if file_path == output_file_path:
                    self.logger.info(f'SKIPPING FILE - input and output file names are identical: "{file_path}"')
                # Check if output file path already exists and the overwrite argument is not set
                # elif output_file_path.is_file() and not overwrite:
                elif output_file_path.is_file():
                    self.logger.info(
                        f'Output file already exists and overwrite argument is not set: "{output_file_path}"')
                # Perform file renaming
                else:
                    # Only rename files if not in --dry-run mode
                    self.logger.info(f'Renaming file "{file.name}" to: "{output_file_path}"')
                    if not dry_run:
                        try:
                            # Rename file
                            os.rename(file_path, output_file_path)
                        # TODO: Check this is the correct exception to catch
                        except FileExistsError:
                            self.logger.error(f"Error renaming file {file_path} to {output_file_path}")

    def get_output_file_name(self, deployment_config: dict, file_path: str) -> str:
        # Read CZI file and fetch file metadata as dictionary
        with czifile.CziFile(file_path) as czi:
            metadata = czi.metadata(raw=False)

            # Get all remaining filename identifiers from metadata
            # TODO: This isn't currently correct for the Zeiss Axoiplan (missing factor of 10)
            magnification_factor = f"X{metadata['ImageDocument']['Metadata']['Information']['Image']['MicroscopeSettings']['EyepieceSettings']['TotalMagnification']}"
            channel_id = "RGB"
            object_id = "NA"
            acquisition_date_and_time = metadata["ImageDocument"]["Metadata"]["Information"]["Image"]["AcquisitionDateAndTime"]
            iso_timestamp = dateutil.parser.isoparse(acquisition_date_and_time).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "").replace("-", "")

            # TODO: Check if any of the filename identifiers are None

            # Construct and return new filename
            return (
                f'{deployment_config.get("strain_id")}_'
                f'{self.instrument_config.get("id")}_'
                f"{magnification_factor}_"
                f'{deployment_config.get("contrast_id")}_'
                f"{channel_id}_"
                f'{deployment_config.get("biological_stain_id")}_'
                f"{object_id}_"
                f"{iso_timestamp}"
                f".CZI"
            )

    def process(self, dry_run: bool):
        """
        Implementation of the MarImBA process command for the Zeiss Axio Observer
        """

        # TODO: Move this to the instrument logger
        # Set dry run log string to prepend to logging
        dry_run_log_string = "DRY_RUN - " if dry_run else ""

        # Loop through each deployment subdirectory in the instrument work directory
        for deployment in os.scandir(self.work_path):

            # Get deployment name and config path
            deployment_name = deployment.path.split("/")[-1]
            deployment_config_path = Path(deployment.path) / Path(deployment_name + ".yml")

            # Check if deployment metadata file exists and skip deployment if not present
            if not deployment_config_path.is_file():
                self.logger.warning(
                    f'SKIPPING DEPLOYMENT - Cannot find deployment metadata file "{deployment_name}.yml" in deployment directory at path: "{deployment.path}"')
                continue
            else:
                # TODO: Need to validate deployment metadata file here and load deployment config
                self.logger.info(f'Found valid MarImBA deployment with "{deployment_name}.yml" at path: "{deployment.path}"')
                deployment_config = load_config(deployment_config_path)

                # Loop through each file in the deployment directory
                for file in os.scandir(deployment.path):

                    # Define regex to match any of the filetypes to be renamed
                    extensions_pattern = f'({"|".join(re.escape(extension) for extension in self.filetypes)})$'
                    file_path = file.path

                    # Match case-insensitive regex expression in file name
                    if re.search(extensions_pattern, file_path, re.IGNORECASE):

                        # Extract the CZI files
                        # self.extract()
                        print(f"Extracting...{file_path}")
                        self.extract(file_path)

    def already_extracted(self, new_mlai_directory_path, file_name):
        """
        Check if the CZI file has previously been extracted int JPGs

        Note: This assumes that if the first video frame already exists, then the entire video sequence has previously been extracted

        :param new_anacc_file_path:
        :param new_mlai_directory_path:
        :param file_name:
        """

        # Define new MLAI image and video frame paths
        new_mlai_image_path = os.path.join(new_mlai_directory_path, file_name) + ".JPG"
        new_mlai_video_frame_path = os.path.join(new_mlai_directory_path, file_name) + "_001.JPG"

        if os.path.isfile(new_mlai_image_path) or os.path.isfile(new_mlai_video_frame_path):
            return True
        else:
            return False


    def construct_new_directory_paths(self, iso_timestamp, strain_id, imaging_system_id, magnification_factor, contrast_id, biological_stain_id):
        # Copy to ANACC image archive
        self.logger.debug("Calculating new ANACC and MLAI image archive paths...")

        # # Fetch class name from ANACC strain list
        # if strain_id == "MSA":
        #     new_anacc_directory_path = os.path.join(self.anacc_image_directory, "Mixed")
        # else:
        #     strain_row_index = self.annac_naming_standard.strain_list_df.index[
        #         self.annac_naming_standard.strain_list_df["CS Number"] == strain_id[:2] + "-" + strain_id.replace("-", "/", 1)[2:]
        #     ].tolist()[0]
        #     class_vernacular = self.annac_naming_standard.strain_list_df.loc[strain_row_index, "Class_Vernacular"]
        #     new_anacc_directory_path = os.path.join(self.anacc_image_directory, class_vernacular, strain_id)
        #
        # # Construct new ANACC file path and check directory path exists, creating new directories if necessary
        # new_anacc_file_path = os.path.join(new_anacc_directory_path, file_name) + ".JPG"
        # fs.create_directory_path_if_not_existing(new_anacc_directory_path)

        # Construct new MLAI file path and check directory path exists, creating new directories if necessary
        split_iso_timestamp = iso_timestamp.split("T")[0]
        year = split_iso_timestamp[0:4]
        month = split_iso_timestamp[4:6]
        day = split_iso_timestamp[6:8]
        new_mlai_directory_path = os.path.join(self.mlai_image_directory, year, month, day, strain_id, imaging_system_id, magnification_factor, contrast_id, biological_stain_id)
        fs.create_directory_if_necessary(new_mlai_directory_path)

        return new_mlai_directory_path

    def extract_frames(self, image, file_name, new_mlai_directory_path):
        # If CZI file has stacked images, fetch number of images
        if len(image.shape) == 4:

            # Squeeze empty image dimensions
            single_image = image.squeeze()

            # Write new JPG image to MLAI archive
            new_mlai_file_path = os.path.join(new_mlai_directory_path, file_name) + ".JPG"
            self.write_image_to_disk(new_mlai_file_path, single_image, "MLAI")

        elif len(image.shape) == 5:
            number_of_stacked_images = image.shape[0]

            for i in range(number_of_stacked_images):

                # Squeeze empty image dimensions
                stacked_image = image[i].squeeze()

                new_mlai_file_path = os.path.join(new_mlai_directory_path, file_name) + f"_{i + 1:03d}.JPG"

                # Write new JPG image to MLAI archive
                self.write_image_to_disk(new_mlai_file_path, stacked_image, "MLAI")


    # TODO: This needs to be refactored!
    def extract(self, file_path):
        """
        Extract ...
        """

        file_path = Path(file_path)

        file_name = file_path.name
        print(f'Filename: {file_name}')

        file_extension = file_path.suffix
        print(f'Extension: {file_extension}')

        # Check file is a CZI file
        if file_extension.lower() == ".czi":
            self.logger.debug("--------------------------------------------------------------------------------------------------------------------------------")
            self.logger.debug(f"Processing CZI file: {file_path}...")

            # Check filename structure conforms with ANACC standard
            if self.annac_naming_standard.is_filename_structure_correct(file_name):

                # Extract filename attributes
                (
                    strain_id,
                    imaging_system_id,
                    magnification_factor,
                    contrast_id,
                    channel_id,
                    biological_stain_id,
                    object_id,
                    iso_timestamp,
                ) = file_name.split("_")

                # Construct new directory paths
                new_mlai_directory_path = self.construct_new_directory_paths(iso_timestamp, strain_id, imaging_system_id, magnification_factor, contrast_id, biological_stain_id)

                # Check if images have previously been extracted
                if self.overwrite_images or not self.already_extracted(new_mlai_directory_path, file_name):
                    self.logger.info(f"Extracting CZI file: {file_path}...")

                    # Try to read CZI file and extract image frames
                    try:
                        image = czifile.imread(file_path)
                        # czifile.czi2tif(file_path)

                        # Extract CZI images and video frames
                        self.extract_frames(image, file_name, new_mlai_directory_path)

                    except Exception as e:
                        self.logger.error(f"Error opening file {file_path}")
                        self.logger.error(e)



    # TODO: The following identifier checking methods need to moved into a cookiecutter post-generate hook
    # TODO: https://cookiecutter.readthedocs.io/en/1.7.2/advanced/hooks.html
    # def is_strain_id_correct(self, strain_id: str) -> bool:
    #     self.logger.debug(f"Checking entered strain identifier is valid...")
    #     if strain_id in self.cs_code_list or strain_id == "MSA":
    #         self.logger.debug(f"Entered strain identifier conforms with ANACC format!")
    #         return True
    #     else:
    #         self.logger.error("Entered strain identifier does not conform with ANACC format (e.g. CS422)")
    #         return False
    #
    # def is_imaging_system_correct(self, imaging_system_id: str) -> bool:
    #     self.logger.debug(f"Checking entered imaging system identifier is valid...")
    #     if imaging_system_id in self.imaging_systems:
    #         self.logger.debug(f"Entered imaging system identifier is valid!")
    #         return True
    #     else:
    #         self.logger.error(f"Entered imaging system identifier is not one of the available options")
    #         return False
    #
    # def is_contrast_id_correct(self, contrast_id: str) -> bool:
    #     self.logger.debug(f"Checking entered contrast setting identifier is valid...")
    #     if contrast_id in self.contrast_settings:
    #         self.logger.debug(f"Entered contrast setting identifier is valid!")
    #         return True
    #     else:
    #         self.logger.error(f"Entered contrast setting identifier is not one of the available options")
    #         return False
    #
    # # TODO: Typer prompt doesn't like empty strings - probably need to change this to return boolean
    # def check_biological_stain_id(self, biological_stain_id: str) -> str:
    #     self.logger.debug(f"Checking entered biological stain identifier is valid...")
    #     if not biological_stain_id.strip():
    #         self.logger.debug(f'Entered biological stain identifier is empty - setting to "NA"')
    #         return "NA"
    #     elif biological_stain_id in self.biological_stains:
    #         self.logger.debug(f"Entered biological stain identifier is valid!")
    #         return biological_stain_id
    #     else:
    #         self.logger.error(f"Entered biological stain identifier is not one of the available options")
    #         return

    # @classmethod
    # def prompt_config(cls) -> Iterable[Tuple[str, str]]:
    #     return [
    #         # # TODO: Need to check if these exist upstream (in survey config) before querying
    #         # ("imaging-system-identifier", "Please enter the imaging system identifier"),
    #         # ("strain-identifier", "Please enter the strain identifier"),
    #         # ("contrast-identifier", "Please enter the contrast identifier"),
    #         # ("biological-stain-identifier", "Please enter the biological stain identifier")
    #     ]

    # def is_target_rename_directory(self, directory_path: str) -> bool:
    #
    #     # Check directory is bottom-level and has no subdirectories within it
    #     self.logger.debug(f"Checking directory path is a bottom-level directory...")
    #     subdirectory_list = [f.path for f in os.scandir(directory_path) if f.is_dir()]
    #     if len(subdirectory_list) > 0:
    #         self.logger.debug(f"Directory path is not a bottom-level directory")
    #         return False
    #     else:
    #         self.logger.debug(f"Directory path is a bottom-level directory!")
    #
    #     # Check at least one CZI file exists in directory
    #     self.logger.debug(f"Checking CZI files exist in bottom-level directory...")
    #     for filename in os.listdir(directory_path):
    #         if filename.lower().endswith(self.filetype):
    #             self.logger.debug(f"Found CZI files in bottom-level directory!")
    #             self.logger.info(f"Renaming files in directory {directory_path}...")
    #             return True
    #     else:
    #         self.logger.debug(f"Directory path does not contain any CZI files")
    #         return False

    # def get_manual_metadata_fields(self) -> bool:
    #
    #     # TODO: Get fields from directory name and confirm with user. Regex for directory name.
    #
    #     # Request the strain identifier
    #     strain_id = typer.prompt("Please enter ANACC strain identifier (e.g. CS422)")
    #     if not self.is_strain_id_correct(strain_id):
    #         return False
    #
    #     # Request the imaging system identifier
    #     prompt = "Please enter imaging system identifier:"
    #     prompt = prompt + "\n    Available options:\n"
    #     for key, value in self.imaging_systems.items():
    #         prompt = prompt + f"\t{key} - {value}\n"
    #     imaging_system_id = typer.prompt(prompt)
    #     if not self.is_imaging_system_correct(imaging_system_id):
    #         return False
    #
    #     # Request the contrast setting identifier
    #     prompt = "Please enter contrast setting identifier:"
    #     prompt = prompt + "\n    Available options:\n"
    #     for key, value in self.contrast_settings.items():
    #         prompt = prompt + f"\t{key} - {value}\n"
    #     contrast_id = typer.prompt(prompt)
    #     if not self.is_contrast_id_correct(contrast_id):
    #         return False
    #
    #     # Request the biological stain identifier
    #     prompt = "Please enter biological stain identifier if used:"
    #     prompt = prompt + "\n    Available options:\n"
    #     for key, value in self.biological_stains.items():
    #         prompt = prompt + f"\t{key} - {value}\n"
    #     biological_stain_id = typer.prompt(prompt)
    #     biological_stain_id = self.check_biological_stain_id(biological_stain_id)
    #     if not contrast_id:
    #         return False
    #
    #     # Print accepted filename identifiers
    #     self.logger.debug(f"Entered strain identifier: {strain_id}")
    #     self.logger.debug(f"Entered imaging system identifier: {imaging_system_id}")
    #     self.logger.debug(f"Entered contrast setting identifier: {contrast_id}")
    #     self.logger.debug(f"Entered biological stain identifier: {biological_stain_id}")
    #     self.logger.debug(f"Provided command line arguments and manually entered identifiers appear correct!")
    #
    #     self.strain_id = strain_id
    #     self.imaging_system_id = imaging_system_id
    #     self.contrast_id = contrast_id
    #     self.biological_stain_id = biological_stain_id
    #
    #     return True

    # # TODO: Improve this filename structure checking method to interrogate each element of the filename
    # def is_filename_structure_correct(self, file_name) -> bool:
    #     if len(file_name.split("_")) != 8:
    #         self.logger.warning(f"The file {file_name} does not conform to ANACC standard - skipping")
    #         return False
    #     else:
    #         return True
    #
    # def extract_filename_ids(self, filename: str) -> list:
    #     """
    #     Extract filename identifiers from input filename according to the ANNAC filename convention:
    #     Reference: https://confluence.csiro.au/x/OICEUg
    #
    #     NOTE: After splitting, the filename is structured with the following identifiers:
    #         strain_id = filename_ids[0]
    #         imaging_system_id = filename_ids[1]
    #         magnification_factor = filename_ids[2]
    #         contrast_id = filename_ids[3]
    #         channel_id = filename_ids[4]
    #         biological_stain_id = filename_ids[5]
    #         object_id = filename_ids[6]
    #         iso_timestamp = filename_ids[7]
    #
    #     :param filename: Input filename for field identifiers extraction
    #     :return: List of ANNAC filename field identifiers
    #     """
    #     # TODO: This might usefully be placed in an ANACC filename convention utils file
    #     # Split filename on underscores
    #     filename_ids = filename.strip(".JPG").split("_")
    #
    #     # If the filename does not contain a frame number (i.e. is not a video frame), append a NaN to the list
    #     if len(filename_ids) == 8:
    #         filename_ids.append(np.NaN)
    #
    #     # Return list of extracted identifiers
    #     return filename_ids
