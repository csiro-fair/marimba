"""
Marimba parallelisation utilities.

This module provides a multithreaded function to efficiently generate thumbnails for a list of images and save them
to the specified output directory.

Imports:
    - pathlib.Path: Represents filesystem paths.
    - threading.Lock: Primitive lock object for thread synchronization.
    - typing.List: Generic version of list.
    - typing.Optional: Union type containing None and another type.
    - marimba.lib.decorators.multithreaded: Decorator for executing a function across multiple threads.
    - marimba.lib.image.generate_thumbnail: Function for generating a thumbnail from an image.

Functions:
    - multithreaded_generate_thumbnails(
            image_list: List[Path],
            output_directory: Path,
            max_workers: Optional[int] = None
        ) -> List[Path]: Generates thumbnails for the provided list of images using multiple threads and saves them
            to the specified output directory.
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
