"""
Marimba Standard Library - Concurrency.

This module provides parallelised functionality for tasks like generating thumbnails from a list of images using
multithreading.

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
from typing import List, Optional

from marimba.lib.decorators import multithreaded
from marimba.lib.image import generate_thumbnail


def multithreaded_generate_thumbnails(
    image_list: List[Path],
    output_directory: Path,
    max_workers: Optional[int] = None,
) -> List[Path]:
    """Generate thumbnails for a list of images using multiple threads.

    This function takes a list of image paths, creates thumbnails for each image, and saves them in the specified
    output directory. It utilizes multithreading to improve performance when processing multiple images. The
    function ensures thread safety when appending to the list of generated thumbnail paths.

    Args:
        image_list: A list of Path objects representing the input images.
        output_directory: A Path object specifying the directory to save the generated thumbnails.
        max_workers: Optional integer specifying the maximum number of worker threads. If None, it uses the default
        value.

    Returns:
        A sorted list of Path objects representing the generated thumbnail paths.
    """
    thumbnail_path_list = []
    list_lock = Lock()
    output_directory.mkdir(exist_ok=True)

    @multithreaded(max_workers=max_workers)
    def generate_thumbnail_task(item: Path) -> None:
        thumbnail_path = generate_thumbnail(item, output_directory)
        if thumbnail_path:
            with list_lock:
                thumbnail_path_list.append(thumbnail_path)

    generate_thumbnail_task(items=image_list)  # type: ignore

    return sorted(thumbnail_path_list)
