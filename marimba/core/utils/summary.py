"""
Imagery Dataset Summary Utilities.

This module provides functionality to generate comprehensive summaries of imagery datasets, including metadata,
image and video statistics, and other file information. It uses the ImagerySummary class to process and analyze
dataset contents, calculate various metrics, and present the information in a structured format.

Imports:
    - json: For parsing JSON data.
    - subprocess: For running external commands.
    - dataclasses: For creating data classes.
    - datetime: For handling dates and times.
    - pathlib: For working with file paths.
    - typing: For type annotations.
    - ifdo.models: For accessing ImageData model.
    - PIL: For image processing.
    - tabulate: For creating formatted tables.

Classes:
    - ImagerySummary: Represents a summary of an imagery collection, including methods for data processing and
    formatting.
"""

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, cast

from ifdo.models import ImageData
from PIL import Image
from tabulate import tabulate

if TYPE_CHECKING:
    from marimba.core.wrappers.dataset import DatasetWrapper


@dataclass
class ImagerySummary:
    """
    Summary of an imagery collection.
    """

    dataset_name: str = ""
    context: str = ""
    contributors: str = ""
    version: Optional[str] = ""
    licenses: str = ""
    contact: Optional[str] = None

    image_num: int = 0
    image_size_bytes: int = 0
    image_average_file_size: str = ""
    image_file_types: List[str] = field(default_factory=list)
    image_resolution: str = ""
    image_color_depth: str = ""
    image_latitude_extent: str = ""
    image_longitude_extent: str = ""
    image_temporal_extent: str = ""
    image_unique_directories: int = 0
    image_licenses: str = ""
    image_data_quality: str = ""

    video_num: int = 0
    video_size_bytes: int = 0
    video_average_file_size: str = ""
    video_total_duration: str = ""
    video_file_types: List[str] = field(default_factory=list)
    video_resolution: str = ""
    video_color_depth: str = ""
    video_frame_rate: str = ""
    video_encoding_details: str = ""
    video_latitude_extent: str = ""
    video_longitude_extent: str = ""
    video_temporal_extent: str = ""
    video_unique_directories: int = 0
    video_licenses: str = ""
    video_data_quality: str = ""

    other_num: int = 0
    other_size_bytes: int = 0
    other_average_file_size: str = ""
    other_file_types: List[str] = field(default_factory=list)

    @staticmethod
    def sizeof_fmt(num: float, suffix: str = "B") -> str:
        """Format a number of bytes as a human-readable size string.

        This function takes a number representing bytes and converts it to a human-readable string format using
        binary prefixes (Ki, Mi, Gi, etc.). It iterates through unit prefixes, dividing the number by 1024 until it
        finds an appropriate scale. The result is rounded to one decimal place.

        Args:
            num: The number of bytes to format.
            suffix: The suffix to append to the formatted string. Defaults to "B".

        Returns:
            A formatted string representing the size with an appropriate unit prefix.
        """
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024:
                return f"{num:.1f} {unit}{suffix}"
            num /= 1024
        return f"{num:.1f} Yi{suffix}"

    @staticmethod
    def is_video_corrupt_quick(video_path: str) -> bool:
        """Quickly check if a video file is corrupt.

        This function performs a quick check to determine if the given video file is corrupt. It uses ffprobe to
        check the video metadata and ffmpeg to perform seek tests at the start, middle, and end of the video. The
        function returns True if any of these checks fail or if an exception occurs during the process.

        Args:
            video_path (str): Path to the video file to be checked.

        Returns:
            bool: True if the video is corrupt or an error occurs, False otherwise.
        """
        try:
            # Check metadata
            probe_cmd = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                video_path,
            ]
            probe_result = subprocess.run(probe_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if probe_result.returncode != 0:
                return True

            duration = float(probe_result.stdout.decode().strip())

            # Quick seek test (start, middle, end)
            seek_times = [0, duration / 2, duration - 1]
            for seek_time in seek_times:
                seek_cmd = ["ffmpeg", "-ss", str(seek_time), "-i", video_path, "-vframes", "1", "-f", "null", "-"]
                seek_result = subprocess.run(seek_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if seek_result.returncode != 0:
                    return True

            return False
        except Exception as e:
            print(f"Error checking video {video_path}: {e}")
            return True

    @staticmethod
    def get_image_properties(image_list: List[Path]) -> Dict[str, Any]:
        """
        Get image properties from a list of image file paths.

        This function processes a list of image file paths and extracts key properties such as resolutions,
        color depths, and the number of corrupt images. It attempts to open each image file, and if successful,
        adds its resolution and color depth to the respective sets. If an image cannot be opened, it is counted as
        corrupt.

        Args:
            - image_list: A list of Path objects representing image file paths.

        Returns:
            A dictionary containing:
            - resolutions: A set of tuples representing unique image resolutions.
            - color_depths: A set of integers representing unique color depths.
            - corrupt_images: An integer count of images that could not be opened.
        """
        resolutions = set()
        color_depths = set()
        corrupt_images = 0

        for path in image_list:
            try:
                with Image.open(path) as img:
                    resolutions.add(img.size)
                    color_depths.add(len(img.getbands()) * 8)
            except Exception:
                corrupt_images += 1

        return {"resolutions": resolutions, "color_depths": color_depths, "corrupt_images": corrupt_images}

    @staticmethod
    def calculate_image_resolution(resolutions: Set[Tuple[int, int]]) -> str:
        """
        Calculate and format image resolution from a set of resolutions.

        This function takes a set of image resolutions and returns a formatted string representation. If there's only
        one resolution, it returns that resolution. If there are multiple resolutions, it returns a range from the
        smallest to the largest resolution. If the set is empty, it returns "N/A".

        Args:
            resolutions: A set of tuples, where each tuple contains two integers representing width and height of an
            image resolution.

        Returns:
            A string representing the image resolution or range of resolutions. Format can be "WxH", "W1xH1 to W2xH2",
            or "N/A".
        """
        if len(resolutions) == 1:
            width, height = resolutions.pop()
            return f"{width}x{height}"
        elif resolutions:
            min_res = min(resolutions, key=lambda x: x[0] * x[1])
            max_res = max(resolutions, key=lambda x: x[0] * x[1])
            return f"{min_res[0]}x{min_res[1]} to {max_res[0]}x{max_res[1]}"
        return "N/A"

    @staticmethod
    def calculate_image_color_depth(color_depths: Set[int]) -> str:
        """
        Determine the color depth range of an image.

        This function calculates and returns a string representation of the color depth or color depth range for an
        image based on the provided set of color depths. If there's only one color depth, it returns that value. If
        there are multiple depths, it returns a range from the minimum to the maximum depth. If the set is empty,
        it returns "N/A".

        Args:
            color_depths: A set of integers representing the color depths present in
                the image.

        Returns:
            A string describing the color depth or depth range of the image.
        """
        if len(color_depths) == 1:
            return f"{color_depths.pop()}-bit"
        elif color_depths:
            return f"{min(color_depths)}-bit to {max(color_depths)}-bit"
        return "N/A"

    def calculate_image_average_file_size(self) -> str:
        """
        Calculate and format the average file size of images.

        This method calculates the average file size of images by dividing the total image size by the number of images.
        If there are no images, it returns 0. The result is then formatted into a human-readable string representation
        of the file size.

        Returns:
            str: A formatted string representing the average file size of images (e.g., '2.5 MB').
        """
        average_size = self.image_size_bytes / self.image_num if self.image_num > 0 else 0
        return self.sizeof_fmt(average_size)

    @staticmethod
    def contributors_to_text(names: List[str]) -> str:
        """
        Convert a list of contributor names to a formatted string.

        This function takes a list of contributor names, sorts them alphabetically by last name, and returns a formatted
        string. If there are multiple names, they are joined with commas and 'and' before the last name. If there's only
        one name, it's returned as is. If the list is empty, 'N/A' is returned.

        Args:
            names: A list of strings representing contributor names.

        Returns:
            A formatted string of contributor names sorted by last name, or 'N/A' if the list is empty.
        """
        sorted_names = sorted(names, key=lambda name: name.split()[-1])
        return (
            ", ".join(sorted_names[:-1]) + " and " + sorted_names[-1]
            if len(sorted_names) > 1
            else sorted_names[0] if sorted_names else "N/A"
        )

    @staticmethod
    def context_to_text(contexts: List[str]) -> str:
        """Convert a list of strings into a comma-separated text string.

        This function takes a list of strings and joins them into a single string, separating each item with a comma
        and a space. If the input list is empty, it returns 'N/A'.

        Args:
            contexts: A list of strings to be converted into a single text string.

        Returns:
            A string containing all items from the input list separated by commas,
            or 'N/A' if the input list is empty.
        """
        if not contexts:
            return "N/A"

        if len(contexts) == 1:
            return contexts[0]

        numbered_list = [f"{i + 1}. {desc}" for i, desc in enumerate(contexts)]
        return "<br/>".join(numbered_list)

    @staticmethod
    def list_to_text(items: List[str]) -> str:
        """
        Convert a list of strings to a comma-separated text string.

        This function takes a list of strings and joins them into a single string, separating each item with a comma
        and a space. If the input list is empty, it returns "N/A" instead.

        Args:
            - items: A list of strings to be joined.

        Returns:
            - A string containing the joined items or "N/A" if the list is empty.
        """
        return ", ".join(items) if items else "N/A"

    @staticmethod
    def calculate_image_data_quality(total_images: int, corrupt_images: int) -> str:
        """
        Calculate image data quality based on total and corrupt image counts.

        This function computes the percentage of complete and corrupt images in a dataset. It takes the total number of
        images and the number of corrupt images as input, calculates the percentages, and returns a formatted string
        with the results.

        Args:
            total_images: An integer representing the total number of images in the dataset.
            corrupt_images: An integer representing the number of corrupt images in the dataset.

        Returns:
            A string containing the percentage of complete images and the percentage of corrupt images, formatted to one
            decimal place.
        """
        complete_percentage = ((total_images - corrupt_images) / total_images) * 100 if total_images > 0 else 0
        corrupt_percentage = (corrupt_images / total_images) * 100 if total_images > 0 else 0
        return f"{complete_percentage:.1f}% complete, {corrupt_percentage:.1f}% corrupt"

    @staticmethod
    def run_ffmpeg_command(command: List[str]) -> Dict[str, Any]:
        """
        Execute an FFmpeg command and returns the JSON output.

        This function runs the provided FFmpeg command using subprocess, captures the output, and returns it as a
        parsed JSON dictionary. If the command fails, it raises a RuntimeError with the error message.

        Args:
            - command: A list of strings representing the FFmpeg command and its arguments.

        Returns:
            A dictionary containing the parsed JSON output from the FFmpeg command.

        Raises:
            - RuntimeError: If the FFmpeg command fails to execute successfully.
        """
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg command failed with error: {result.stderr}")
        return cast(Dict[str, Any], json.loads(result.stdout))

    @staticmethod
    def get_video_properties(video_list: List[Path]) -> Dict[str, Any]:
        """Get video properties from a list of video files.

        This function analyzes a list of video files using ffprobe to extract various properties such as duration,
        resolution, codec, frame rate, and color depth. It also checks for corrupt videos. The function aggregates
        this information and returns a dictionary containing summary statistics.

        Args:
            video_list: A list of Path objects representing video file paths.

        Returns:
            A dictionary containing the following keys:
            - total_seconds: Total duration of all videos in seconds.
            - resolutions: Set of unique resolutions (width, height) tuples.
            - codecs: Set of unique video codecs.
            - frame_rates: Set of unique frame rates.
            - color_depths: Set of unique color depths.
            - corrupt_videos: Number of corrupt videos detected.
        """
        total_seconds: float = 0.0
        resolutions = set()
        codecs = set()
        frame_rates = set()
        color_depths = set()
        corrupt_videos = 0

        for path in video_list:
            command = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=duration,width,height,codec_name,r_frame_rate,bits_per_raw_sample",
                "-of",
                "json",
                str(path),
            ]
            output = ImagerySummary.run_ffmpeg_command(command)["streams"][0]
            total_seconds += float(output.get("duration", 0))
            resolutions.add((output.get("width"), output.get("height")))
            codecs.add(output.get("codec_name"))
            frame_rate_str = output.get("r_frame_rate", "0/1")
            num, denom = map(int, frame_rate_str.split("/"))
            frame_rates.add(num / denom)
            color_depth = output.get("bits_per_raw_sample")
            if color_depth:
                color_depths.add(int(color_depth))
            if ImagerySummary.is_video_corrupt_quick(str(path)):
                corrupt_videos += 1

        return {
            "total_seconds": total_seconds,
            "resolutions": resolutions,
            "codecs": codecs,
            "frame_rates": frame_rates,
            "color_depths": color_depths,
            "corrupt_videos": corrupt_videos,
        }

    @staticmethod
    def get_other_properties(video_list: List[Path]) -> Dict[str, Any]:
        """Get video properties from a list of other files."""
        total_seconds: float = 0.0
        resolutions = set()
        codecs = set()
        frame_rates = set()
        color_depths = set()
        corrupt_videos = 0

        for path in video_list:
            command = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=duration,width,height,codec_name,r_frame_rate,bits_per_raw_sample",
                "-of",
                "json",
                str(path),
            ]
            output = ImagerySummary.run_ffmpeg_command(command)["streams"][0]
            total_seconds += float(output.get("duration", 0))
            resolutions.add((output.get("width"), output.get("height")))
            codecs.add(output.get("codec_name"))
            frame_rate_str = output.get("r_frame_rate", "0/1")
            num, denom = map(int, frame_rate_str.split("/"))
            frame_rates.add(num / denom)
            color_depth = output.get("bits_per_raw_sample")
            if color_depth:
                color_depths.add(int(color_depth))
            if ImagerySummary.is_video_corrupt_quick(str(path)):
                corrupt_videos += 1

        return {
            "total_seconds": total_seconds,
            "resolutions": resolutions,
            "codecs": codecs,
            "frame_rates": frame_rates,
            "color_depths": color_depths,
            "corrupt_videos": corrupt_videos,
        }

    @staticmethod
    def calculate_video_total_duration(total_seconds: float) -> str:
        """
        Convert total seconds into a human-readable duration string.

        This function takes a floating-point number representing the total duration in seconds and converts it into a
        formatted string. The output is in hours, minutes, or seconds, depending on the input value. The function
        chooses the most appropriate unit to display the duration.

        Args:
            total_seconds: A float representing the total duration in seconds.

        Returns:
            A string representing the formatted duration in hours, minutes, or seconds, with two decimal places.
        """
        if total_seconds >= 3600:
            total_hours = total_seconds / 3600
            return f"{total_hours:.2f} Hours"
        elif total_seconds >= 60:
            total_minutes = total_seconds / 60
            return f"{total_minutes:.2f} Minutes"
        else:
            return f"{total_seconds:.2f} Seconds"

    @staticmethod
    def calculate_video_resolution(resolutions: Set[Tuple[int, int]]) -> str:
        """
        Determine the video resolution range based on a set of resolutions.

        This function takes a set of video resolutions and returns a string representation of the resolution or
        resolution range. If there's only one resolution, it returns that resolution. If there are multiple resolutions,
        it returns the range from the lowest to the highest resolution. If the set is empty, it returns "N/A".

        Args:
            resolutions: A set of tuples, where each tuple contains two integers representing width and height of a
            resolution.

        Returns:
            A string representing the video resolution or resolution range. Format can be "WxH", "W1xH1 to W2xH2", or
            "N/A".
        """
        if len(resolutions) == 1:
            width, height = resolutions.pop()
            return f"{width}x{height}"
        elif resolutions:
            min_res = min(resolutions, key=lambda x: x[0] * x[1])
            max_res = max(resolutions, key=lambda x: x[0] * x[1])
            return f"{min_res[0]}x{min_res[1]} to {max_res[0]}x{max_res[1]}"
        return "N/A"

    @staticmethod
    def calculate_video_encoding_details(codecs: Set[str]) -> str:
        """
        Format a set of video codecs into a comma-separated string.

        This function takes a set of video codec names and returns a formatted string where the codecs are joined
        with commas. If the input set is empty, it returns "N/A" to indicate that no codecs are available.

        Args:
            - codecs: A set of strings representing video codec names.

        Returns:
            A string containing the comma-separated list of codecs or "N/A" if empty.
        """
        return ", ".join(codecs) if codecs else "N/A"

    @staticmethod
    def calculate_video_frame_rate(frame_rates: Set[str]) -> str:
        """
        Calculate and formats the video frame rate based on a set of frame rates.

        This function takes a set of frame rates and returns a formatted string representing the frame rate or range
        of frame rates. If the set contains a single value, it returns that value. If multiple values are present,
        it returns a range from the minimum to the maximum. If the set is empty, it returns 'N/A'.

        Args:
            - frame_rates: A set of strings representing frame rates.

        Returns:
            A formatted string representing the frame rate or range of frame rates.
        """
        if len(frame_rates) == 1:
            return f"{frame_rates.pop():.2f} fps"
        elif frame_rates:
            return f"{min(frame_rates):.2f} fps to {max(frame_rates):.2f} fps"
        return "N/A"

    @staticmethod
    def calculate_video_color_depth(color_depths: Set[str]) -> str:
        """
        Calculate and format the color depth range for video content.

        This function determines the color depth or range of color depths for video content based on the provided
        set of color depths. It handles cases where there's a single color depth, multiple color depths, or no color
        depth information available.

        Args:
            color_depths: A set of strings representing different color depths.

        Returns:
            A formatted string describing the color depth or range of color depths. If there's only one color depth, it
            returns that depth with "-bit" appended. If there are multiple depths, it returns a range from the minimum
            to the maximum depth. If the set is empty, it returns "N/A".
        """
        if len(color_depths) == 1:
            return f"{color_depths.pop()}-bit"
        elif color_depths:
            return f"{min(color_depths)}-bit to {max(color_depths)}-bit"
        return "N/A"

    def calculate_video_average_file_size(self) -> str:
        """Calculate and format the average file size of videos.

        This function calculates the average file size of videos by dividing the total video size by the number of
        videos. If there are no videos, it returns 0. The result is then formatted into a human-readable string
        representation.

        Returns:
            str: A formatted string representing the average file size of videos (e.g., '1.5 MB').
        """
        average_size = self.video_size_bytes / self.video_num if self.video_num > 0 else 0
        return self.sizeof_fmt(average_size)

    @staticmethod
    def calculate_video_data_quality(total_videos: int, corrupt_videos: int) -> str:
        """
        Calculate video data quality percentages.

        Computes the percentage of complete and corrupt videos based on the total number of videos and the number of
        corrupt videos. The function returns a formatted string with the calculated percentages.

        Args:
            - total_videos: The total number of videos in the dataset.
            - corrupt_videos: The number of corrupt videos in the dataset.

        Returns:
            A formatted string containing the percentage of complete videos and the percentage of corrupt videos, both
            rounded to one decimal place.
        """
        complete_percentage = ((total_videos - corrupt_videos) / total_videos) * 100 if total_videos > 0 else 0
        corrupt_percentage = (corrupt_videos / total_videos) * 100 if total_videos > 0 else 0
        return f"{complete_percentage:.1f}% complete, {corrupt_percentage:.1f}% corrupt"

    def calculate_other_average_file_size(self) -> str:
        """Calculate and format the average file size for 'other' files.

        This method computes the average file size for files categorized as 'other' by dividing the total size of
        'other' files by the number of 'other' files. If there are no 'other' files, it returns 0. The result is
        then formatted into a human-readable string representation.

        Returns:
            str: A human-readable string representing the average file size for 'other' files (e.g., '1.5 MB').
        """
        average_size = self.other_size_bytes / self.other_num if self.other_num > 0 else 0
        return self.sizeof_fmt(average_size)

    @classmethod
    def from_dataset(
        cls, dataset_wrapper: "DatasetWrapper", image_set_items: Dict[str, "ImageData"]
    ) -> "ImagerySummary":
        """Create an ImagerySummary object from a dataset and image set items.

        This class method processes a dataset wrapper and a dictionary of image set items to create an ImagerySummary
        object. It calculates various statistics and metadata for images, videos, and other files in the dataset.

        Args:
            dataset_wrapper: A DatasetWrapper object containing dataset information.
            image_set_items: A dictionary mapping file paths to ImageData objects.

        Returns:
            An ImagerySummary object containing comprehensive statistics and metadata about the dataset's imagery.

        TODO: Implement multithreading for this method
        """
        dataset_info = cls._extract_dataset_info(dataset_wrapper)
        image_data, video_data, other_data = cls._process_files(dataset_wrapper, image_set_items)
        file_stats = cls._calculate_file_stats(image_data, video_data, other_data)

        # Ensure all data matches expected types, handle None, and combine data
        complete_data = {**dataset_info, **file_stats}

        # Define expected types based on the ImagerySummary dataclass
        expected_types = {
            "dataset_name": str,
            "context": str,
            "contributors": str,
            "version": (str, type(None)),
            "licenses": str,
            "contact": (str, type(None)),
            "image_num": int,
            "image_size_bytes": int,
            "image_average_file_size": str,
            "image_file_types": list,
            "image_resolution": str,
            "image_color_depth": str,
            "image_latitude_extent": str,
            "image_longitude_extent": str,
            "image_temporal_extent": str,
            "image_unique_directories": int,
            "image_licenses": str,
            "image_data_quality": str,
            "video_num": int,
            "video_size_bytes": int,
            "video_average_file_size": str,
            "video_total_duration": str,
            "video_file_types": list,
            "video_resolution": str,
            "video_color_depth": str,
            "video_frame_rate": str,
            "video_encoding_details": str,
            "video_latitude_extent": str,
            "video_longitude_extent": str,
            "video_temporal_extent": str,
            "video_unique_directories": int,
            "video_licenses": str,
            "video_data_quality": str,
            "other_num": int,
            "other_size_bytes": int,
            "other_average_file_size": str,
            "other_file_types": list,
        }

        # Convert or default None values
        for key, types in expected_types.items():
            if not isinstance(types, tuple):
                types = (types,)
            value = complete_data.get(key, None)
            if value is None or not isinstance(value, types):
                if str in types:
                    complete_data[key] = ""
                elif int in types:
                    complete_data[key] = 0
                elif list in types:
                    complete_data[key] = []

        # Pass the validated and type-corrected data to the constructor
        summary = cls(**complete_data)

        cls._set_dataset_properties(summary, image_data, video_data)
        cls._set_image_properties(summary, image_data)
        cls._set_video_properties(summary, video_data)
        cls._set_other_properties(summary)
        cls._set_geographical_temporal_extents(summary, image_data, video_data)

        return summary

    @staticmethod
    def _extract_dataset_info(dataset_wrapper: "DatasetWrapper") -> Dict[str, Optional[str]]:
        return {
            "dataset_name": dataset_wrapper.name,
            "version": dataset_wrapper.version,
            "contact": (
                f"{dataset_wrapper.contact_name} <{dataset_wrapper.contact_email}>"
                if dataset_wrapper.contact_name and dataset_wrapper.contact_email
                else dataset_wrapper.contact_name or dataset_wrapper.contact_email or None
            ),
        }

    @classmethod
    def _process_files(
        cls, dataset_wrapper: "DatasetWrapper", image_set_items: Dict[str, List["ImageData"]]
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        image_data: Dict[str, Any] = {"files": [], "context": set(), "contributors": set(), "licenses": set()}
        video_data: Dict[str, Any] = {"files": [], "context": set(), "contributors": set(), "licenses": set()}
        other_data: Dict[str, List[str]] = {"files": []}

        for path_str, image_data_list in image_set_items.items():
            path = dataset_wrapper.data_dir / path_str
            suffix = path.suffix.lower()
            image_info = image_data_list[0]

            cls._update_common_data(image_data, image_info)
            cls._update_common_data(video_data, image_info)

            if suffix in cls.IMAGE_EXTENSIONS:
                cls._process_image(image_data, path, image_info)
            elif suffix in cls.VIDEO_EXTENSIONS:
                cls._process_video(video_data, path, image_info)

        cls._process_other_files(dataset_wrapper, other_data)

        return image_data, video_data, other_data

    @staticmethod
    def _update_common_data(data: Dict[str, Any], image_info: "ImageData") -> None:
        data["context"].add(image_info.image_context)
        data["licenses"].add(image_info.image_license)
        data["contributors"].update(contributor.name for contributor in image_info.image_creators)

    @classmethod
    def _process_image(cls, image_data: Dict[str, Any], path: Path, image_info: "ImageData") -> None:
        image_data["files"].append(
            {
                "path": path,
                "size": path.stat().st_size,
                "type": path.suffix.lower().replace(".", ""),
                "lat": image_info.image_latitude,
                "lon": image_info.image_longitude,
                "datetime": image_info.image_datetime,
                "directory": path.parent,
            }
        )

    @classmethod
    def _process_video(cls, video_data: Dict[str, Any], path: Path, image_info: "ImageData") -> None:
        video_data["files"].append(
            {
                "path": path,
                "size": path.stat().st_size,
                "type": path.suffix.lower().replace(".", ""),
                "lat": getattr(image_info, "image_latitude", None),
                "lon": getattr(image_info, "image_longitude", None),
                "datetime": image_info.image_datetime,
                "directory": path.parent,
                "is_corrupt": cls.is_video_corrupt_quick(str(path)),
            }
        )

    @staticmethod
    def _process_other_files(dataset_wrapper: "DatasetWrapper", other_data: Dict[str, Any]) -> None:
        for path in dataset_wrapper.root_dir.glob("**/*"):
            if (
                path.is_file()
                and path.suffix.lower() not in ImagerySummary.IMAGE_EXTENSIONS | ImagerySummary.VIDEO_EXTENSIONS
            ):
                other_data["files"].append(
                    {"path": path, "size": path.stat().st_size, "type": path.suffix.lower().replace(".", "")}
                )

    @staticmethod
    def _calculate_file_stats(
        image_data: Dict[str, Any], video_data: Dict[str, Any], other_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "image_num": len(image_data["files"]),
            "image_size_bytes": sum(file["size"] for file in image_data["files"]),
            "image_file_types": list({file["type"] for file in image_data["files"]}),
            "image_unique_directories": len({file["directory"] for file in image_data["files"]}),
            "video_num": len(video_data["files"]),
            "video_size_bytes": sum(file["size"] for file in video_data["files"]),
            "video_file_types": list({file["type"] for file in video_data["files"]}),
            "video_unique_directories": len({file["directory"] for file in video_data["files"]}),
            "other_num": len(other_data["files"]),
            "other_size_bytes": sum(file["size"] for file in other_data["files"]),
            "other_file_types": list({file["type"] for file in other_data["files"]}),
        }

    @classmethod
    def _set_dataset_properties(
        cls, summary: "ImagerySummary", image_data: Dict[str, Any], video_data: Dict[str, Any]
    ) -> None:
        summary.context = summary.context_to_text(list(image_data["context"] | video_data["context"]))
        summary.contributors = summary.contributors_to_text(
            list(image_data["contributors"] | video_data["contributors"])
        )
        summary.licenses = summary.list_to_text(list(image_data["licenses"] | video_data["licenses"]))

    @classmethod
    def _set_image_properties(cls, summary: "ImagerySummary", image_data: Dict[str, Any]) -> None:
        image_props = cls.get_image_properties([file["path"] for file in image_data["files"]])
        summary.image_resolution = cls.calculate_image_resolution(image_props["resolutions"])
        summary.image_color_depth = cls.calculate_image_color_depth(image_props["color_depths"])
        summary.image_data_quality = cls.calculate_image_data_quality(
            len(image_data["files"]), image_props["corrupt_images"]
        )
        summary.image_average_file_size = summary.calculate_image_average_file_size()
        summary.image_licenses = summary.list_to_text(list(image_data["licenses"]))

    @classmethod
    def _set_video_properties(cls, summary: "ImagerySummary", video_data: Dict[str, Any]) -> None:
        video_props = cls.get_video_properties([file["path"] for file in video_data["files"]])
        summary.video_total_duration = cls.calculate_video_total_duration(video_props["total_seconds"])
        summary.video_resolution = cls.calculate_video_resolution(video_props["resolutions"])
        summary.video_encoding_details = cls.calculate_video_encoding_details(video_props["codecs"])
        summary.video_frame_rate = cls.calculate_video_frame_rate(video_props["frame_rates"])
        summary.video_color_depth = cls.calculate_video_color_depth(video_props["color_depths"])
        summary.video_average_file_size = summary.calculate_video_average_file_size()
        summary.video_data_quality = cls.calculate_video_data_quality(
            len(video_data["files"]), video_props["corrupt_videos"]
        )
        summary.video_licenses = summary.list_to_text(list(video_data["licenses"]))

    @classmethod
    def _set_other_properties(cls, summary: "ImagerySummary") -> None:
        summary.other_average_file_size = summary.calculate_other_average_file_size()

    @staticmethod
    def _set_geographical_temporal_extents(
        summary: "ImagerySummary", image_data: Dict[str, Any], video_data: Dict[str, Any]
    ) -> None:
        for data_type in ["image", "video"]:
            data = image_data if data_type == "image" else video_data
            lats = [file["lat"] for file in data["files"] if file["lat"] is not None]
            lons = [file["lon"] for file in data["files"] if file["lon"] is not None]
            datetimes = [file["datetime"] for file in data["files"] if file["datetime"] is not None]

            setattr(summary, f"{data_type}_latitude_extent", f"{min(lats):.3f} to {max(lats):.3f}" if lats else "N/A")
            setattr(summary, f"{data_type}_longitude_extent", f"{min(lons):.3f} to {max(lons):.3f}" if lons else "N/A")
            setattr(
                summary,
                f"{data_type}_temporal_extent",
                (
                    f"{min(datetimes).strftime('%d %b %Y')} - {max(datetimes).strftime('%d %b %Y')}"
                    if datetimes
                    else "N/A"
                ),
            )

    IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif"}
    VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".wmv", ".flv", ".mkv", ".webm"}

    def __str__(self) -> str:
        dataset_metadata: List[List[str]] = [
            ["Dataset Name", self.dataset_name],
            ["Creation Date", datetime.now().strftime("%d %B %Y")],
            ["Contributors", self.contributors],
            ["License" if "," not in self.licenses else "Licenses", self.licenses],
        ]

        if self.context:
            dataset_metadata.insert(1, ["Context", self.context])
        if self.version:
            dataset_metadata.append(["Dataset Version", self.version])
        if self.contact:
            dataset_metadata.append(["Contact", self.contact])

        image_file_types_str = ", ".join(sorted(self.image_file_types)).upper() if self.image_file_types else "N/A"
        image_resolution_label = "Image Resolution" if "to" not in self.image_resolution else "Image Resolution Range"
        image_color_depth_label = (
            "Image Color Depth" if "to" not in self.image_color_depth else "Image Color Depth Range"
        )
        image_licenses_label = "License" if "," not in self.image_licenses else "Licenses"

        image_files_summary: List[List[str]] = [
            ["Total Number of Images", str(self.image_num)],
            ["Total Disk Space Used", self.sizeof_fmt(self.image_size_bytes)],
            ["Average Image File Size", self.image_average_file_size],
            ["Image File Types", image_file_types_str],
            [image_resolution_label, self.image_resolution],
            [image_color_depth_label, self.image_color_depth],
            ["Latitude Extent", self.image_latitude_extent],
            ["Longitude Extent", self.image_longitude_extent],
            ["Temporal Extent", self.image_temporal_extent],
            ["Unique Image Directories", str(self.image_unique_directories)],
            [image_licenses_label, self.image_licenses],
            ["Image Data Quality", self.image_data_quality],
        ]

        video_file_types_str = ", ".join(sorted(self.video_file_types)).upper() if self.video_file_types else "N/A"
        video_resolution_label = "Video Resolution" if "to" not in self.video_resolution else "Video Resolution Range"
        video_color_depth_label = (
            "Video Color Depth" if "to" not in self.video_color_depth else "Video Color Depth Range"
        )
        video_licenses_label = "License" if "," not in self.video_licenses else "Licenses"
        video_frame_rate_label = "Video Frame Rate" if "to" not in self.video_frame_rate else "Video Frame Rate Range"

        video_files_summary: List[List[str]] = [
            ["Total Number of Videos", str(self.video_num)],
            ["Total Disk Space Used", self.sizeof_fmt(self.video_size_bytes)],
            ["Average Video File Size", self.video_average_file_size],
            ["Total Video Duration", self.video_total_duration],
            ["Video File Types", video_file_types_str],
            [video_resolution_label, self.video_resolution],
            [video_color_depth_label, self.video_color_depth],
            ["Video Encoding Details", self.video_encoding_details],
            [video_frame_rate_label, self.video_frame_rate],
            ["Latitude Extent", self.video_latitude_extent],
            ["Longitude Extent", self.video_longitude_extent],
            ["Temporal Extent", self.video_temporal_extent],
            ["Unique Video Directories", str(self.video_unique_directories)],
            [video_licenses_label, self.video_licenses],
            ["Video Data Quality", self.video_data_quality],
        ]

        other_file_types_str = ", ".join(sorted(self.other_file_types)).upper() if self.other_file_types else "N/A"

        other_files_summary: List[List[str]] = [
            ["Total Number of Files", str(self.other_num)],
            ["Total Disk Space Used", self.sizeof_fmt(self.other_size_bytes)],
            ["Average File Size", self.other_average_file_size],
            ["File Types", other_file_types_str],
        ]

        return f"# {self.dataset_name} Dataset Summary\n\n" "## Dataset Metadata\n" + tabulate(
            dataset_metadata, headers=["Attribute", "Description"], tablefmt="github"
        ) + (
            "\n\n## Image Files Summary\n"
            + tabulate(image_files_summary, headers=["Attribute", "Description"], tablefmt="github")
            if self.image_num > 1
            else ""
        ) + (
            "\n\n## Video Files Summary\n"
            + tabulate(video_files_summary, headers=["Attribute", "Description"], tablefmt="github")
            if self.video_num > 1
            else ""
        ) + (
            "\n\n## Other Files Summary\n"
            + tabulate(other_files_summary, headers=["Attribute", "Description"], tablefmt="github")
            if self.other_num > 1
            else ""
        )
