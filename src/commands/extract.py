import logging
import os
import subprocess

import typer
from rich import print
from rich.panel import Panel

import utils.file_system as fs

def check_input_args(
    source_path: str,
    destination_path: str
):
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


# Get the duration of the video in milliseconds using ffprobe
def get_video_duration(file: str) -> float:
    try:
        duration = float(subprocess.check_output(['ffprobe', '-i', file, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'default=noprint_wrappers=1:nokey=1']))
        logging.debug('get_video_duration: ' + str(int(duration * 1000)))
    except:
        logging.error('\tError accessing file metadata: ' + file)

    return duration


def extract_frames(
    input_path: str,
    output_path: str,
    chunk_length: int,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):

    logging.info(f"Extracting video frames from: {input_path}")

    for directory_path, _, files in os.walk(input_path):

        for file in files:

            # logging.info(file)
            file_path = os.path.join(directory_path, file)
            file_name, file_extension = os.path.splitext(file)

            if file_extension.lower() in [".mp4"]:

                # Get video length in seconds
                # duration = get_video_duration(file_path)
                # intervals = int(duration / chunk_length) + 1
                # logging.info(f'Video duration = {duration}')
                # logging.info(f'Number of intervals = {intervals}')

                logging.info(f'Extracting frames from video file "{file_path}"...')

                # for index, i in enumerate(range(0, intervals * chunk_length, chunk_length)):

                if file_extension.lower() in [".mp4"]:

                    # chunk_name = "C" + str(index + 1).zfill(3)

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

                    # if not os.path.isfile(output_file_path):
                    if not os.path.isdir(new_output_path):

                        fs.create_directory_if_necessary(new_output_path)

                        output_file_path = os.path.join(new_output_path, file_name + "_F%06d.JPG")
                        # Note: Make sure the -i flag comes after the -ss and -to flags so that ffmpeg uses fast seeking
                        subprocess.check_call([
                            "ffmpeg",
                            # "-ss", str(i),
                            # "-to", str(i + chunk_length),
                            "-i", file_path,
                            # "-vf", "lensfun=make=GoPro:model=HERO4 Silver:lens_model=fixed lens:mode=geometry:target_geometry=rectilinear:interpolation=lanczos",
                            # "-vf", "v360=input=sg:ih_fov=118.2:iv_fov=69.5:output=flat:d_fov=133.6:w=2704:h=1520",
                            # "-vf", "v360=input=fisheye:ih_fov=180:iv_fov=180",
                            "-hide_banner",
                            "-loglevel", "error",
                            output_file_path
                        ])
                        logging.info(f'Completed extracting video frames for "{file_path}" to "{new_output_path}"')
                        # else:
                        #     logging.info(f"Video frame already exists {new_output_path}")
