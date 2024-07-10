"""Marimba Image Utilities.

This module contains a collection of functions for various image processing tasks such as resizing, cropping, rotating,
and applying filters. It also includes utilities for image analysis and grid creation.

Imports:
    - pathlib: Provides classes for filesystem path operations.
    - shutil: Offers high-level operations on files and collections of files.
    - typing: Provides support for type hints.
    - cv2: OpenCV library for computer vision tasks.
    - numpy: Library for numerical computing in Python.
    - PIL: Python Imaging Library for opening, manipulating, and saving image files.
    - marimba.core.utils.log: Custom logging utility for the Marimba project.

Functions:
    - generate_image_thumbnail: Create a thumbnail version of an image.
    - convert_to_jpeg: Convert an image to JPEG format.
    - resize_fit: Resize an image to fit within specified dimensions.
    - resize_exact: Resize an image to exact dimensions.
    - scale: Scale an image by a given factor.
    - rotate_clockwise: Rotate an image clockwise by a specified number of degrees.
    - turn_clockwise: Turn an image clockwise in 90-degree increments.
    - flip_vertical: Flip an image vertically.
    - flip_horizontal: Flip an image horizontally.
    - is_blurry: Determine if an image is blurry.
    - crop: Crop an image to a specified size and position.
    - apply_clahe: Apply Contrast Limited Adaptive Histogram Equalization to an image.
    - gaussian_blur: Apply Gaussian blur to an image.
    - sharpen: Sharpen an image.
    - get_width_height: Get the dimensions of an image.
    - create_grid_image: Create a grid image from multiple images.
    - get_shannon_entropy: Calculate the Shannon entropy of an image.
    - get_average_image_color: Calculate the average color of an image.
"""

from pathlib import Path
from shutil import copy2
from typing import Iterable, Optional, Tuple, Union, cast

import cv2
import numpy as np
from PIL import Image

from marimba.core.utils.log import get_logger

logger = get_logger(__name__)


def generate_image_thumbnail(image: Path, output_directory: Path, suffix: str = "_THUMB") -> Path:
    """
    Generate a thumbnail image from the given image file.

    Args:
        image: A Path object representing the path to the source image file.
        output_directory: A Path object representing the directory where the thumbnail will be saved.
        suffix (optional): A string representing the suffix to be added to the filename of the generated thumbnail
        image. Defaults to "_THUMB".

    Returns:
        A Path object representing the path to the generated thumbnail image.

    """
    output_filename = image.stem + suffix + image.suffix
    output_path = output_directory / output_filename
    if not output_path.exists():
        logger.info(f"Generating image thumbnail at: {output_path}")
        resize_fit(image, 300, 300, output_path)
    return output_path


def convert_to_jpeg(path: Union[str, Path], quality: int = 95, destination: Optional[Union[str, Path]] = None) -> Path:
    """
    Convert an image to JPEG format.

    Args:
        path: The path to the image file.
        quality: The JPEG quality, from 0 to 100.
        destination:
            The path to save the converted image to. If not provided, the original file will be overwritten. The path
            extension will be forced to .jpg.

    Returns:
        The path to the converted image file. If the destination argument is provided, this will be the same as the
        destination argument with the extension forced to .jpg.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    destination = destination.with_suffix(".jpg")
    if path.suffix.lower() in (".jpg", ".jpeg"):
        copy2(path, destination)
    else:
        img = cast(Image.Image, Image.open(path))
        img.convert("RGB").save(destination, "JPEG", quality=quality)
    return destination


def _resize_fit(img: Image.Image, max_width: int, max_height: int) -> Image.Image:
    width, height = img.size
    if width > max_width or height > max_height:
        ratio = min(max_width / width, max_height / height)
        new_width = int(width * ratio)
        new_height = int(height * ratio)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    return img


def resize_fit(
    path: Union[str, Path],
    max_width: int = 1920,
    max_height: int = 1080,
    destination: Optional[Union[str, Path]] = None,
) -> None:
    """
    Resize an image to fit within a maximum width and height.

    Args:
        path: The path to the image file.
        max_width: The maximum width of the image.
        max_height: The maximum height of the image.
        destination: The path to save the resized image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = _resize_fit(img, max_width, max_height)
    img.save(destination)


def resize_exact(
    path: Union[str, Path], width: int = 1920, height: int = 1080, destination: Optional[Union[str, Path]] = None
) -> None:
    """
    Resize an image to exact dimensions.

    Args:
        path: The path to the image file.
        width: The width to resize the image to.
        height: The height to resize the image to.
        destination: The path to save the resized image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = img.resize((width, height), Image.Resampling.LANCZOS)
    img.save(destination)


def scale(path: Union[str, Path], scale_factor: float, destination: Optional[Union[str, Path]] = None) -> None:
    """
    Scale an image by a given factor.

    Args:
        path: The path to the image file.
        scale_factor: The scale factor to apply to the image, 0-1.
        destination: The path to save the scaled image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = cast(Image.Image, Image.open(path))
    width, height = img.size
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)
    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    img.save(destination)


def rotate_clockwise(
    path: Union[str, Path], degrees: int, expand: bool = False, destination: Optional[Union[str, Path]] = None
) -> None:
    """
    Rotates an image in the clockwise direction by the specified number of degrees.

    Args:
        path (Union[str, Path]): The path to the image file to rotate.
        degrees (int): The number of degrees to rotate the image in the clockwise direction.
        expand (bool, optional): Whether to expand the size of the image to fit the rotated version.
            Defaults to False.
        destination (Optional[Union[str, Path]], optional): The destination path to save the rotated image.
            If not provided, the rotated image will be overwritten on the original path. Defaults to None.

    Returns:
        None: This method does not return any value.

    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = img.rotate(-degrees, expand=expand)  # type: ignore[no-untyped-call]
    img.save(destination)


def turn_clockwise(path: Union[str, Path], turns: int = 1, destination: Optional[Union[str, Path]] = None) -> None:
    """
    Turn an image clockwise in steps of 90 degrees.

    Args:
        path: The path to the image file.
        turns: The number of 90-degree turns to make. Must be between 1 and 3 inclusive.
        destination: The path to save the turned image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    # Validate the turns value
    if turns not in [1, 2, 3]:
        raise ValueError("Turns must be an integer between 1 and 3 inclusive.")

    # Map turns to the corresponding rotation constants
    rotation_constants = {
        1: Image.Transpose.ROTATE_90,
        2: Image.Transpose.ROTATE_180,
        3: Image.Transpose.ROTATE_270,
    }

    img = cast(Image.Image, Image.open(path))
    img = img.transpose(rotation_constants[turns])
    img.save(destination)


def flip_vertical(path: Union[str, Path], destination: Optional[Union[str, Path]] = None) -> None:
    """
    Flip an image vertically.

    Args:
        path: The path to the image file.
        destination: The path to save the flipped image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = img.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
    img.save(destination)


def flip_horizontal(path: Union[str, Path], destination: Optional[Union[str, Path]] = None) -> None:
    """
    Flip an image horizontally.

    Args:
        path: The path to the image file.
        destination: The path to save the flipped image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
    img.save(destination)


def is_blurry(path: Union[str, Path], threshold: float = 100.0) -> bool:
    """
    Determine if an image is blurry.

    Args:
        path: The path to the image file.
        threshold:
            The threshold for the variance of the Laplacian. If the variance is below this threshold, the image is
            considered blurry.

    Returns:
        True if the image is blurry, False otherwise.
    """
    image = cv2.imread(str(path))
    if image is None:
        raise ValueError(f"Could not load the image from the path: {path}")

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    variance_of_laplacian = cv2.Laplacian(gray, cv2.CV_64F).var()

    # Explicitly cast the result to float for type clarity
    variance_of_laplacian = float(variance_of_laplacian)

    image_is_blurry = variance_of_laplacian < threshold

    if not isinstance(image_is_blurry, bool):
        raise TypeError("Expected image_is_blurry to be a boolean")

    return image_is_blurry


def crop(
    path: Union[str, Path], x: int, y: int, width: int, height: int, destination: Optional[Union[str, Path]] = None
) -> None:
    """
    Crop an image to a given size and position.

    Args:
        path: The path to the image file.
        x: The x-coordinate of the top-left corner of the crop.
        y: The y-coordinate of the top-left corner of the crop.
        width: The width of the crop.
        height: The height of the crop.
        destination: The path to save the cropped image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cast(Image.Image, Image.open(path))
    img = img.crop((x, y, x + width, y + height))
    img.save(destination)


def apply_clahe(
    path: Union[str, Path],
    clip_limit: float = 2.0,
    tile_grid_size: Tuple[int, int] = (8, 8),
    destination: Optional[Union[str, Path]] = None,
) -> None:
    """
    Apply Contrast Limited Adaptive Histogram Equalization (CLAHE) to an image.

    Args:
        path: The path to the image file.
        clip_limit: The threshold for contrast limiting.
        tile_grid_size:
            The size of the grid for the histogram equalization. The image will be divided into equally sized
            rectangular tiles.
        destination: The path to save the image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cv2.imread(str(path), 0)

    # Apply CLAHE to the image
    clahe = cv2.createCLAHE(clipLimit=clip_limit, tileGridSize=tile_grid_size)
    img_clahe = clahe.apply(img)

    cv2.imwrite(str(destination), img_clahe)


def gaussian_blur(
    path: Union[str, Path], kernel_size: Tuple[int, int] = (5, 5), destination: Optional[Union[str, Path]] = None
) -> None:
    """
    Blur an image.

    Args:
        path: The path to the image file.
        kernel_size: The size of the kernel to use for blurring.
        destination: The path to save the blurred image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cv2.imread(str(path))

    # Apply Gaussian blur to the image
    img_blur = cv2.GaussianBlur(img, kernel_size, 0)

    cv2.imwrite(str(destination), img_blur)


def sharpen(path: Union[str, Path], destination: Optional[Union[str, Path]] = None) -> None:
    """
    Sharpen an image.

    Args:
        path: The path to the image file.
        destination: The path to save the sharpened image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path

    img = cv2.imread(str(path))

    # Apply sharpening to the image
    kernel = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    img_sharpen = cv2.filter2D(img, -1, kernel)

    cv2.imwrite(str(destination), img_sharpen)


def get_width_height(path: Union[str, Path]) -> Tuple[int, int]:
    """
    Get the width and height of an image.

    Args:
        path: The path to the image file.

    Returns:
        A tuple containing the width and height of the image.
    """
    path = Path(path)
    img = cast(Image.Image, Image.open(path))
    size = img.size

    if not (isinstance(size, tuple) and len(size) == 2 and all(isinstance(x, int) for x in size)):
        raise ValueError("Size must be a tuple of two integers")

    return size


def create_grid_image(
    paths: Iterable[Union[str, Path]],
    destination: Union[str, Path],
    columns: int = 5,
    column_width: int = 300,
) -> None:
    """
    Create an image that represents a grid of images.

    Args:
        paths: The paths to the images to include in the grid.
        destination: The path to save the grid image to.
        columns: The number of columns in the grid.
        column_width: The width in pixels of each column in the grid.
    """
    if not paths or Path(destination).exists():
        return

    logger.info(f"Creating thumbnail overview image: {str(destination)}")

    paths = [Path(p) for p in paths]
    destination = Path(destination)

    # Calculate the number of rows in the grid
    rows = len(paths) // columns
    if len(paths) % columns > 0:
        rows += 1

    # Compute the optimal row height to remove whitespace
    row_height = column_width
    for path in paths:
        img = cast(Image.Image, Image.open(path))
        img_width, img_height = img.size
        ratio = img_width / img_height
        row_height = min(row_height, int(column_width / ratio))

    # Create the grid image
    grid_image = Image.new("RGB", (column_width * columns, row_height * rows))
    for i, path in enumerate(paths):
        # Calculate the coordinates of the image in the grid
        x = (i % columns) * column_width
        y = (i // columns) * row_height

        # Resize the image to fit in the grid
        img = cast(Image.Image, Image.open(path))
        img = _resize_fit(img, column_width, row_height)

        # Center the image in the grid tile
        img_width, img_height = img.size
        x += (column_width - img_width) // 2
        y += (row_height - img_height) // 2

        # Paste the image into the grid
        grid_image.paste(img, (x, y))

    # Save the grid image
    grid_image.save(destination)


def get_shannon_entropy(image_data: Image.Image) -> float:
    """
    Calculate the Shannon entropy of an image file.

    Args:
        image_data: The loaded image data.

    Returns:
        The Shannon entropy of the image as a float value.
    """
    # Convert to grayscale
    grayscale_image = image_data.convert("L")

    # Calculate the histogram
    histogram = np.array(grayscale_image.histogram(), dtype=np.float32)

    # Normalize the histogram to get probabilities
    probabilities = histogram / histogram.sum()

    # Filter out zero probabilities
    probabilities = probabilities[probabilities > 0]

    # Calculate Shannon entropy
    entropy = -np.sum(probabilities * np.log2(probabilities))

    return float(entropy)


def get_average_image_color(image_data: Image.Image) -> Tuple[int, ...]:
    """
    Calculate the average color of an image.

    Args:
        image_data: The loaded image data.

    Returns:
        A list of integers representing the average color of the image in RGB format. Each element in the list
        corresponds to the average intensity of the Red, Green, and Blue channels, respectively.

        Note: If the input image is None, None will be returned.
    """
    # Convert the image to numpy array
    np_image = np.array(image_data)

    # Calculate the average color for each channel
    average_color = np.mean(np_image, axis=(0, 1))

    return tuple(map(int, average_color))
