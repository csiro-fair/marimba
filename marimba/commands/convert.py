import logging
import os
import subprocess

import typer
from rich import print
from rich.panel import Panel

import marimba.utils.file_system as fs


def check_input_args(
    source_path: str,
    destination_path: str
):
    # Check if source_path is valid
    if not os.path.isdir(source_path):
        print(Panel(f"The source_path argument [bold]{source_path}[/bold] is not a valid directory path", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()


# TODO: Do we really need a straight copy method in MarImBA? The advantage is that we could include some arguments as default, like --archive etc...
def convert_files(
    source_path: str,
    destination_path: str,
    recursive: bool,
    overwrite: bool,
    dry_run: bool,
):
    """
    Converts video to high quality MP4/h264 w/ AAC

    Flag details
        -map_metadata 0:g  => Copy all global metadata from input to output
        -c:v libx264       => use h264 codec
        -preset slow       => use a high quality compression preset
        -crf 18            => constant rate factor. higher crf = lower quality
        -vf yadif          => de-interlace and convert to progressive scan
           ,format=yuv420p => Required for playback via quicktime
        -c:a aac           => Use built-in AAC encoder for audio
        -b:a 160k          => bit rate for audio
        –movflags faststart => Move the 'moov atom' to the start of the file

    References:
     http://journal.code4lib.org/articles/9856
    """


    check_input_args(source_path, destination_path)

    logging.info(f"Converting files recursively from: {source_path}")

    logging.info(f"Input path is: {source_path}")
    logging.info(f"Output path is: {destination_path}")

    fs.create_directory_if_necessary(destination_path)

    for directory_path, _, files in os.walk(source_path):

        for file in files:
            file_path = os.path.join(directory_path, file)
            file_name, file_extension = os.path.splitext(file)

            output_file_path = os.path.join(destination_path, file_name + ".mp4")

            if not os.path.isfile(output_file_path):

                if file_extension.lower() in [".mp4", ".mpg", ".avi"]:
                    logging.info(f'Transcoding video file "{file_path}"...')
                    subprocess.check_call([
                        "ffmpeg",
                        "-hwaccel", "auto",
                        "-i", file_path,
                        "-y",
                        "-map_metadata", "0:g",
                        "-c:v", "libx264",
                        "-preset", "slow",
                        "-crf", "18",
                        "-vf", "yadif,format=yuv420p",
                        "-c:a", "aac",
                        "-b:a", "160k",
                        "-movflags", "faststart",
                        output_file_path
                    ])
                    logging.info(f"Completed transcoding video {file_path}")
            else:
                logging.info(f"Video file already exists {output_file_path}")

