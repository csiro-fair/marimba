import logging
import os
import subprocess

import typer
from rich import print
from rich.panel import Panel
from rich.console import Console

import utils.file_system as fs


def check_input_args(
    source_path: str,
    destination_path: str
):
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def chunk_files(
    source_path: str,
    destination_path: str,
    chunk_length: int,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    check_input_args(source_path)

    logging.info(f"Chunking files recursively from: {source_path}")


# Get the duration of the video in milliseconds using ffprobe
def get_video_duration(file: str) -> float:
    try:
        duration = float(subprocess.check_output(['ffprobe', '-i', file, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'default=noprint_wrappers=1:nokey=1']))
        logging.debug('get_video_duration: ' + str(int(duration * 1000)))
    except:
        logging.error('\tError accessing file metadata: ' + file)

    return duration


def chunk_files(input_path: str, output_path: str, chunk_length: int):
    """
    Chunks video into
    """

    console = Console()

    logging.info(f"Input path is: {input_path}")
    logging.info(f"Output path is: {output_path}")

    fs.create_directory_path_if_not_existing(output_path)

    # with console.status("[bold green]Chunking video files...") as status:
    for directory_path, _, files in os.walk(input_path):

        for file in files:
            file_path = os.path.join(directory_path, file)
            file_name, file_extension = os.path.splitext(file)

            if file_extension.lower() in [".mp4", ".mpg", ".avi"]:

                # Get video length in seconds
                duration = get_video_duration(file_path)
                intervals = int(duration / chunk_length) + 1

                logging.info(f'Video duration = {duration}')
                logging.info(f'Number of intervals = {intervals}')

                logging.info(f'Chunking video file "{file_path}"...')

                for index, i in enumerate(range(0, intervals * chunk_length, chunk_length)):

                    file_name_split = file_name.split("_")

                    if len(file_name_split) >= 2:
                        campaign_name = file_name_split[0]
                        year = file_name_split[1]
                    if len(file_name_split) >= 3:
                        site_name = file_name_split[2]
                    if len(file_name_split) >= 4:
                        part_name = file_name_split[3]

                    # logging.info(f"{output_path}, {campaign_name}, {year}, {site_name}, {part_name}")
                    if len(file_name_split) == 4:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year, site_name, part_name)
                    elif len(file_name_split) == 3:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year, site_name)
                    else:
                        new_output_path = os.path.join(output_path, campaign_name + "_" + year)

                    if not os.path.isdir(new_output_path):
                        fs.create_directory_path_if_not_existing(new_output_path)

                    output_file_path = os.path.join(new_output_path, file_name + "_C" + str(index + 1).zfill(3) + ".MP4")

                    # if not os.path.isfile(output_file_path):

                    if file_extension.lower() in [".mp4", ".mpg", ".avi"]:
                        # print(index, i, i + chunk_length, file_path, output_file_path)
                        # Ffmpeg default -crf is 23 (fairly low quality, use 18 for higher quality)
                        subprocess.check_call([
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
                            "-ss", str(i),
                            "-to", str(i + chunk_length),
                            "-i", file_path,
                            # "-hide_banner",
                            # "-loglevel", "quiet",
                            output_file_path
                        ])
                        logging.info(f'Completed chunking video "{output_file_path}"')
                    # else:
                    #     logging.info(f"Video file already exists {output_file_path}")
