import os
import subprocess

import typer
from rich import print
from rich.console import Console
from rich.panel import Panel

import marimba.utils.file_system as fs
from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def check_input_args(source_path: str, destination_path: str):
    """
    Check the input arguments for the chunk command.

    Args:
        source_path: The path to the directory containing the files to be chunked.
        destination_path: The path to the directory where the chunked files will be saved.
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


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def chunk_files(source_path: str, destination_path: str, chunk_length: int, recursive: bool, overwrite: bool, dry_run: bool):
    """
    Chunks video files into smaller chunks of a specified length.

    Args:
        source_path: The path to the directory containing the files to be chunked.
        destination_path: The path to the directory where the chunked files will be saved.
        chunk_length: The length of each chunk.
        recursive: Whether to chunk files recursively.
        overwrite: Whether to overwrite existing output files.
        dry_run: Whether to run the command without actually doing anything.
    """
    check_input_args(source_path)

    logger.info(f"Chunking files recursively from: {source_path}")


# Get the duration of the video in milliseconds using ffprobe
def get_video_duration(file: str) -> float:
    """
    Get a video's duration in milliseconds using ffprobe.

    Args:
        file: The path to the video file.

    Returns:
        The duration of the video in milliseconds.
    """
    try:
        duration = float(
            subprocess.check_output(
                ["ffprobe", "-i", file, "-show_entries", "format=duration", "-v", "quiet", "-of", "default=noprint_wrappers=1:nokey=1"]
            )
        )
        logger.debug("get_video_duration: " + str(int(duration * 1000)))
    except:
        logger.error("\tError accessing file metadata: " + file)

    return duration


def chunk_files(input_path: str, output_path: str, chunk_length: int):
    """
    Chunks video files into smaller chunks of a specified length.

    Args:
        input_path: The path to the directory containing the files to be chunked.
        output_path: The path to the directory where the chunked files will be saved.
        chunk_length: The length of each chunk.
    """

    console = Console()

    logger.info(f"Input path is: {input_path}")
    logger.info(f"Output path is: {output_path}")

    fs.create_directory_if_necessary(output_path)

    # with console.status("[bold green]Chunking video files...") as status:
    for directory_path, _, files in os.walk(input_path):
        for file in files:
            file_path = os.path.join(directory_path, file)
            file_name, file_extension = os.path.splitext(file)

            if file_extension.lower() in [".mp4", ".mpg", ".avi"]:
                # Get video length in seconds
                duration = get_video_duration(file_path)
                intervals = int(duration / chunk_length) + 1

                logger.info(f"Video duration = {duration}")
                logger.info(f"Number of intervals = {intervals}")

                logger.info(f'Chunking video file "{file_path}"...')

                for index, i in enumerate(range(0, intervals * chunk_length, chunk_length)):
                    file_name_split = file_name.split("_")

                    if len(file_name_split) >= 2:
                        campaign_name = file_name_split[0]
                        year = file_name_split[1]
                    if len(file_name_split) >= 3:
                        site_name = file_name_split[2]
                    if len(file_name_split) >= 4:
                        part_name = file_name_split[3]

                    if len(file_name_split) == 4:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year, site_name, part_name)
                    elif len(file_name_split) == 3:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year, site_name)
                    else:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year)

                    if not os.path.isdir(new_output_path):
                        fs.create_directory_if_necessary(new_output_path)

                    output_file_path = os.path.join(new_output_path, file_name + "_C" + str(index + 1).zfill(3) + ".MP4")

                    # if not os.path.isfile(output_file_path):

                    if file_extension.lower() in [".mp4", ".mpg", ".avi"]:
                        # print(index, i, i + chunk_length, file_path, output_file_path)
                        # Ffmpeg default -crf is 23 (fairly low quality, use 18 for higher quality)
                        subprocess.check_call(
                            [
                                "ffmpeg",
                                # "-hwaccel", "auto",
                                "-y",
                                # "-map_metadata", "0:g",
                                # "-c:v", "libx264",
                                # "-preset", "slow",
                                # "-crf", "18",
                                # "-vf", "yadif,format=yuv420p",
                                # "-c:a", "aac",
                                # "-b:a", "160k",
                                # "-movflags", "faststart",
                                "-ss",
                                str(i),
                                "-to",
                                str(i + chunk_length),
                                "-i",
                                file_path,
                                # "-hide_banner",
                                # "-loglevel", "quiet",
                                output_file_path,
                            ]
                        )
                        logger.info(f'Completed chunking video "{output_file_path}"')
