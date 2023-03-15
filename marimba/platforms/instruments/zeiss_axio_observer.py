"""
Zeiss Axio Observer specification
"""

import os
from typing import Iterable, Tuple

import czifile
import dateutil.parser
import pandas as pd
import typer

from marimba.platforms.instruments.base import Instrument

__author__ = "Chris Jackett"
__copyright__ = "Copyright 2022, Environment, CSIRO"
__credits__ = []
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Chris Jackett"
__email__ = "chris.jackett@csiro.au"
__status__ = "Development"


class ZeissAxioObserver(Instrument):
    def __init__(self, config: dict):
        # Get info from config
        image_set_header = config.get("image-set-header")
        self.platform = image_set_header.get("image-platform")
        self.sensor = image_set_header.get("image-sensor")
        self.filetype = image_set_header.get("image-acquisition")

        self.strain_list_df = pd.read_pickle("data/anacc_strain_list.pkl")
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

    def get_output_file_name(self, file_path: str) -> str:
        # Read CZI file and fetch file metadata as dictionary
        with czifile.CziFile(file_path) as czi:
            metadata = czi.metadata(raw=False)

            # Get all remaining filename identifiers from metadata
            # TODO: This isn't currently correct for the Zeiss Axoiplan (missing factor of 10)
            magnification_factor = (
                f"X{metadata['ImageDocument']['Metadata']['Information']['Image']['MicroscopeSettings']['EyepieceSettings']['TotalMagnification']}"
            )
            channel_identifier = "RGB"
            object_identifier = "NA"
            acquisition_date_and_time = metadata["ImageDocument"]["Metadata"]["Information"]["Image"]["AcquisitionDateAndTime"]
            iso_timestamp = (
                dateutil.parser.isoparse(acquisition_date_and_time).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "-")
            )

            # Construct and return new filename
            return (
                f"{self.strain_identifier}_"
                f"{self.imaging_system_identifier}_"
                f"{magnification_factor}_"
                f"{self.contrast_identifier}_"
                f"{channel_identifier}_"
                f"{self.biological_stain_identifier}_"
                f"{object_identifier}_"
                f"{iso_timestamp}"
                f".CZI"
            )

    def get_output_file_directory(self, directory_path: str, destination_path: str) -> str:
        if destination_path:
            return destination_path
        else:
            return directory_path

    def is_target_rename_directory(self, directory_path: str) -> bool:
        # Check directory is bottom-level and has no subdirectories within it
        self.logger.debug(f"Checking directory path is a bottom-level directory...")
        subdirectory_list = [f.path for f in os.scandir(directory_path) if f.is_dir()]
        if len(subdirectory_list) > 0:
            self.logger.debug(f"Directory path is not a bottom-level directory")
            return False
        else:
            self.logger.debug(f"Directory path is a bottom-level directory!")

        # Check at least one CZI file exists in directory
        self.logger.debug(f"Checking CZI files exist in bottom-level directory...")
        for filename in os.listdir(directory_path):
            if filename.lower().endswith(self.filetype):
                self.logger.debug(f"Found CZI files in bottom-level directory!")
                self.logger.info(f"Renaming files in directory {directory_path}...")
                return True
        else:
            self.logger.debug(f"Directory path does not contain any CZI files")
            return False

    def get_manual_metadata_fields(self) -> bool:
        # TODO: Get fields from directory name and confirm with user. Regex for directory name.

        # Request the strain identifier
        strain_identifier = typer.prompt("Please enter ANACC strain identifier (e.g. CS422)")
        if not self.is_strain_identifier_correct(strain_identifier):
            return False

        # Request the imaging system identifier
        prompt = "Please enter imaging system identifier:"
        prompt = prompt + "\n    Available options:\n"
        for key, value in self.imaging_systems.items():
            prompt = prompt + f"\t{key} - {value}\n"
        imaging_system_identifier = typer.prompt(prompt)
        if not self.is_imaging_system_correct(imaging_system_identifier):
            return False

        # Request the contrast setting identifier
        prompt = "Please enter contrast setting identifier:"
        prompt = prompt + "\n    Available options:\n"
        for key, value in self.contrast_settings.items():
            prompt = prompt + f"\t{key} - {value}\n"
        contrast_identifier = typer.prompt(prompt)
        if not self.is_contrast_identifier_correct(contrast_identifier):
            return False

        # Request the biological stain identifier
        prompt = "Please enter biological stain identifier if used:"
        prompt = prompt + "\n    Available options:\n"
        for key, value in self.biological_stains.items():
            prompt = prompt + f"\t{key} - {value}\n"
        biological_stain_identifier = typer.prompt(prompt)
        biological_stain_identifier = self.check_biological_stain_identifier(biological_stain_identifier)
        if not contrast_identifier:
            return False

        # Print accepted filename identifiers
        self.logger.debug(f"Entered strain identifier: {strain_identifier}")
        self.logger.debug(f"Entered imaging system identifier: {imaging_system_identifier}")
        self.logger.debug(f"Entered contrast setting identifier: {contrast_identifier}")
        self.logger.debug(f"Entered biological stain identifier: {biological_stain_identifier}")
        self.logger.debug(f"Provided command line arguments and manually entered identifiers appear correct!")

        self.strain_identifier = strain_identifier
        self.imaging_system_identifier = imaging_system_identifier
        self.contrast_identifier = contrast_identifier
        self.biological_stain_identifier = biological_stain_identifier

        return True

    def is_strain_identifier_correct(self, strain_identifier: str) -> bool:
        self.logger.debug(f"Checking entered strain identifier is valid...")
        if strain_identifier in self.cs_code_list or strain_identifier == "MSA":
            self.logger.debug(f"Entered strain identifier conforms with ANACC format!")
            return True
        else:
            self.logger.error("Entered strain identifier does not conform with ANACC format (e.g. CS422)")
            return False

    def is_imaging_system_correct(self, imaging_system_identifier: str) -> bool:
        self.logger.debug(f"Checking entered imaging system identifier is valid...")
        if imaging_system_identifier in self.imaging_systems:
            self.logger.debug(f"Entered imaging system identifier is valid!")
            return True
        else:
            self.logger.error(f"Entered imaging system identifier is not one of the available options")
            return False

    def is_contrast_identifier_correct(self, contrast_identifier: str) -> bool:
        self.logger.debug(f"Checking entered contrast setting identifier is valid...")
        if contrast_identifier in self.contrast_settings:
            self.logger.debug(f"Entered contrast setting identifier is valid!")
            return True
        else:
            self.logger.error(f"Entered contrast setting identifier is not one of the available options")
            return False

    # TODO: Typer prompt doesn't like empty strings - probably need to change this to return boolean
    def check_biological_stain_identifier(self, biological_stain_identifier: str) -> str:
        self.logger.debug(f"Checking entered biological stain identifier is valid...")
        if not biological_stain_identifier.strip():
            self.logger.debug(f'Entered biological stain identifier is empty - setting to "NA"')
            return "NA"
        elif biological_stain_identifier in self.biological_stains:
            self.logger.debug(f"Entered biological stain identifier is valid!")
            return biological_stain_identifier
        else:
            self.logger.error(f"Entered biological stain identifier is not one of the available options")
            return

    # # TODO: Improve this filename structure checking method to interrogate each element of the filename
    # def is_filename_structure_correct(self, file_name) -> bool:
    #     if len(file_name.split("_")) != 8:
    #         logging.warning(f"The file {file_name} does not conform to ANACC standard - skipping")
    #         return False
    #     else:
    #         return True
    #
    # def extract_filename_identifiers(self, filename: str) -> list:
    #     """
    #     Extract filename identifiers from input filename according to the ANNAC filename convention:
    #     Reference: https://confluence.csiro.au/x/OICEUg
    #
    #     NOTE: After splitting, the filename is structured with the following identifiers:
    #         strain_identifier = filename_identifiers[0]
    #         imaging_system_identifier = filename_identifiers[1]
    #         magnification_factor = filename_identifiers[2]
    #         contrast_identifier = filename_identifiers[3]
    #         channel_identifier = filename_identifiers[4]
    #         biological_stain_identifier = filename_identifiers[5]
    #         object_identifier = filename_identifiers[6]
    #         iso_timestamp = filename_identifiers[7]
    #
    #     :param filename: Input filename for field identifiers extraction
    #     :return: List of ANNAC filename field identifiers
    #     """
    #     # TODO: This might usefully be placed in an ANACC filename convention utils file
    #     # Split filename on underscores
    #     filename_identifiers = filename.strip(".JPG").split("_")
    #
    #     # If the filename does not contain a frame number (i.e. is not a video frame), append a NaN to the list
    #     if len(filename_identifiers) == 8:
    #         filename_identifiers.append(np.NaN)
    #
    #     # Return list of extracted identifiers
    #     return filename_identifiers

    @classmethod
    def prompt_config(cls) -> Iterable[Tuple[str, str]]:
        return [
            # # TODO: Need to check if these exist upstream (in survey config) before querying
            # ("imaging-system-identifier", "Please enter the imaging system identifier"),
            # ("strain-identifier", "Please enter the strain identifier"),
            # ("contrast-identifier", "Please enter the contrast identifier"),
            # ("biological-stain-identifier", "Please enter the biological stain identifier")
        ]
