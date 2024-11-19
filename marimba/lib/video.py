"""Marimba Video Utilities.

This module offers functionality to generate thumbnail images from a video file at specified intervals. It uses PyAV
for video processing and Pillow for image handling. The generated thumbnails are saved to a specified output directory
with customizable naming conventions.

Imports:
    logging: Provides a flexible framework for generating log messages.
    pathlib: Offers classes representing filesystem paths with semantics appropriate for different operating systems.
    typing: Provides support for type hints.
    av: A Pythonic binding for FFmpeg libraries.
    PIL: Python Imaging Library (Pillow fork) for opening, manipulating, and saving image files.

Functions:
    get_stream_properties: Extracts key properties from a video stream.
    generate_potential_filenames: Creates potential filenames for video frames.
    filter_existing_thumbnails: Identifies and filters existing thumbnail files.
    save_thumbnail: Converts a video frame to a thumbnail image and saves it.
    generate_video_thumbnails: Creates thumbnail images from a video file at specified intervals.
"""

import logging
from pathlib import Path

import av
from PIL import Image

logger = logging.getLogger(__name__)


def get_stream_properties(stream: av.video.stream.VideoStream) -> tuple[float, float, int]:
    """Get properties of a video stream.

    This function extracts key properties from an av.video.stream.VideoStream object, including the frame rate,
    time base, and total number of frames. It performs validation to ensure that frame rate and time base are not None.

    Args:
        stream (av.video.stream.VideoStream): The video stream object to extract properties from.

    Returns:
        Tuple[float, float, int]: A tuple containing:
            frame_rate (float): The average frame rate of the video stream.
            time_base (float): The time base of the video stream.
            total_frames (int): The total number of frames in the video stream.

    Raises:
        ValueError: If either the frame rate or time base is None.
    """
    frame_rate = stream.average_rate
    time_base = stream.time_base
    total_frames = stream.frames

    if frame_rate is None or time_base is None:
        raise ValueError("Frame rate or time base is None.")

    return float(frame_rate), float(time_base), total_frames


def generate_potential_filenames(
    video: Path,
    output_directory: Path,
    total_frames: int,
    frame_interval: int,
    suffix: str,
) -> dict[int, Path]:
    """Generate potential filenames for video frames.

    This function creates a dictionary of potential filenames for frames extracted from a video. It generates filenames
    based on the video's name, frame numbers, and a specified interval, using zero-padding for consistent naming.

    Args:
        video: Path object representing the input video file.
        output_directory: A Path object representing the directory where the thumbnails will be saved.
        total_frames: Total number of frames in the video.
        frame_interval: Interval between frames to generate filenames for.
        suffix: String to append to the filename before the extension.

    Returns:
        A dictionary where keys are frame numbers and values are Path objects representing potential output filenames.

    Raises:
        ValueError: If total_frames or frame_interval is less than or equal to 0.
        TypeError: If video is not a Path object, or if total_frames or frame_interval is not an integer.
    """
    padding_width = len(str(total_frames))
    potential_filenames = {}

    for frame_number in range(0, total_frames, frame_interval):
        zero_padded_frame_number = f"{frame_number:0{padding_width}d}"
        output_filename = f"{video.stem}_{zero_padded_frame_number}{suffix}.JPG"
        output_path = output_directory / output_filename
        potential_filenames[frame_number] = output_path

    return potential_filenames


def filter_existing_thumbnails(potential_filenames: dict[int, Path], overwrite: bool) -> list[Path]:
    """Filter existing thumbnails and return their paths.

    This function checks for existing thumbnail files based on the provided potential filenames. If the overwrite flag
    is False, it identifies existing files, logs their presence, and removes them from the potential filenames
    dictionary. The function returns a list of paths for existing thumbnails.

    Args:
        potential_filenames: A dictionary mapping frame numbers (int) to potential thumbnail file paths (Path).
        overwrite: A boolean flag indicating whether existing thumbnails should be overwritten.

    Returns:
        A list of Path objects representing existing thumbnail files.

    Raises:
        IOError: If there are issues accessing the file system.
        TypeError: If the input types are incorrect.
    """
    thumbnail_paths = []

    if not overwrite:
        existing_files = {k: v for k, v in potential_filenames.items() if v.exists()}
        for frame_number, output_path in existing_files.items():
            logger.info(f"Thumbnail already exists: {output_path}")
            thumbnail_paths.append(output_path)
            del potential_filenames[frame_number]

    return thumbnail_paths


def save_thumbnail(frame: av.video.frame.VideoFrame, output_path: Path) -> None:
    """Save a thumbnail image from a video frame.

    This function takes a video frame, converts it to an image, resizes it to a maximum size of 300x300 pixels while
    maintaining the aspect ratio, and saves it as a thumbnail image at the specified output path.

    Args:
        frame (av.video.frame.VideoFrame): The video frame to be converted and saved as a thumbnail.
        output_path (Path): The file path where the thumbnail image will be saved.

    Returns:
        None

    Raises:
        IOError: If there's an issue saving the image to the specified output path.
        TypeError: If the input frame is not of the expected type.
    """
    img = frame.to_image()
    max_size = (300, 300)
    img.thumbnail(max_size, Image.Resampling.LANCZOS)  # type: ignore[no-untyped-call]
    img.save(output_path)


def generate_video_thumbnails(
    video: Path,
    output_directory: Path,
    interval: int = 10,
    suffix: str = "_THUMB",
    *,
    overwrite: bool = False,
) -> tuple[Path, list[Path]]:
    """
    Generate thumbnail images for a video file at specified intervals using PyAV.

    Args:
        video: A Path object representing the path to the source video file.
        output_directory: A Path object representing the directory where the thumbnails will be saved.
        interval (optional): An integer representing the interval (in seconds) at which thumbnails will be generated.
        suffix (optional): A string representing the suffix to be added to the filename of the generated thumbnails.
        overwrite (optional): A boolean indicating whether to overwrite existing thumbnails. Defaults to False.

    Returns:
        A tuple containing the input video path and a list of generated thumbnail paths.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    container = av.open(str(video))  # type: ignore[attr-defined]
    stream = container.streams.video[0]

    frame_rate, time_base, total_frames = get_stream_properties(stream)
    frame_interval = int(frame_rate * interval)
    potential_filenames = generate_potential_filenames(video, output_directory, total_frames, frame_interval, suffix)
    thumbnail_paths = filter_existing_thumbnails(potential_filenames, overwrite)

    if potential_filenames:
        for packet in container.demux(stream):
            for frame in packet.decode():
                if isinstance(frame, av.video.frame.VideoFrame):
                    frame_number = int(frame.pts * time_base * frame_rate)
                    if frame_number in potential_filenames:
                        output_path = output_directory / potential_filenames[frame_number]
                        logger.info(f"Generating video thumbnail at frame {frame_number}: {output_path}")
                        save_thumbnail(frame, output_path)
                        thumbnail_paths.append(output_path)
                        del potential_filenames[frame_number]
                        if not overwrite and not potential_filenames:
                            return video, thumbnail_paths

    return video, thumbnail_paths
