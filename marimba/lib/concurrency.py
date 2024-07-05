"""
Marimba Standard Library - Concurrency.

This module provides parallelised functionality for tasks like generating thumbnails from a list of images or videos
using multithreading.

Imports:
    - Path: Represents file system paths
    - Lock: Provides thread synchronization
    - List, Optional: Type hints for function parameters
    - multithreaded: Decorator for multithreaded execution
    - generate_thumbnail: Function to create a thumbnail from an image

Functions:
    - multithreaded_generate_thumbnails: Generates thumbnails for multiple images concurrently
"""

from pathlib import Path
from threading import Lock
from typing import List, Optional, Tuple

from marimba.core.pipeline import BasePipeline
from marimba.lib.decorators import multithreaded
from marimba.lib.image import generate_image_thumbnail
from marimba.lib.video import generate_video_thumbnails


def multithreaded_generate_image_thumbnails(
    self: BasePipeline,
    image_list: List[Path],
    output_directory: Path,
    max_workers: Optional[int] = None,
) -> List[Path]:
    """
    Generate thumbnails for a list of images using multiple threads.

    Args:
        image_list (List[Path]): A list of Path objects representing the images to generate thumbnails for.
        output_directory (Path): The directory where the thumbnails will be saved.
        max_workers (Optional[int], default=None): The maximum number of worker threads to use for generating
            thumbnails. If None, the number of worker threads will be determined automatically.

    Returns:
        List[Path]: A sorted list of Path objects representing the paths to the generated thumbnails.
    """
    video_thumbnail_list = []
    list_lock = Lock()
    output_directory.mkdir(exist_ok=True)

    @multithreaded(max_workers=max_workers)
    def generate_thumbnail_task(self: BasePipeline, thread_num: str, item: Path) -> None:
        thumbnail_path = generate_image_thumbnail(item, output_directory)
        self.logger.debug(f"Thread {thread_num} - Generated thumbnail for image {item}")
        if thumbnail_path:
            with list_lock:
                video_thumbnail_list.append(thumbnail_path)

    generate_thumbnail_task(self, items=image_list)  # type: ignore

    return video_thumbnail_list


def multithreaded_generate_video_thumbnails(
    self: BasePipeline,
    video_list: List[Path],
    output_base_directory: Path,
    interval: int = 10,
    suffix: str = "_THUMB",
    max_workers: Optional[int] = None,
    overwrite: bool = False,
) -> List[Tuple[Path, List[Path]]]:
    """Generate thumbnails for multiple videos using multithreading.

    This function processes a list of video files, creating thumbnails at specified intervals for each video. It
    utilizes multithreading to improve performance when handling multiple videos concurrently. The generated thumbnails
    are saved in the specified output directory, with each video's thumbnails placed in a subdirectory named after the
    video file.

    Args:
        - self: The BasePipeline instance.
        - video_list: A list of Path objects representing the input video files.
        - output_base_directory: A Path object specifying the base directory to save the generated thumbnails.
        - interval: An integer representing the interval (in seconds) at which thumbnails will be generated. Default is
         10.
        - suffix: A string to be appended to the filename of each generated thumbnail. Default is "_THUMB".
        - max_workers: Optional integer specifying the maximum number of worker threads. If None, uses the default
         value.
        - overwrite: A boolean indicating whether to overwrite existing thumbnails. Default is False.

    Returns:
        A list of tuples, where each tuple contains a Path object representing the video file and a list of Path objects
        representing the generated thumbnail paths for that video.

    Raises:
        - OSError: If there are issues creating directories or accessing video files.
        - ValueError: If invalid arguments are provided (e.g., negative interval).
        - RuntimeError: If thumbnail generation fails for any reason.
    """
    thumbnail_path_list: List[Tuple[Path, List[Path]]] = []
    list_lock = Lock()

    @multithreaded(max_workers=max_workers)
    def generate_thumbnail_task(self: BasePipeline, thread_num: str, item: Path) -> None:
        output_thumbnails_directory = output_base_directory / item.stem
        output_thumbnails_directory.mkdir(parents=True, exist_ok=True)
        video_path, thumbnail_paths = generate_video_thumbnails(
            item, output_thumbnails_directory, interval, suffix, overwrite
        )
        self.logger.info(f"Thread {thread_num} - Generated thumbnails for video {item}")
        if video_path and thumbnail_paths:
            with list_lock:
                thumbnail_path_list.append((video_path, thumbnail_paths))

    generate_thumbnail_task(self, items=video_list)  # type: ignore

    return thumbnail_path_list