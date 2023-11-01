"""
Image utilities. Includes transcoding, resizing, cropping, etc.
"""


from pathlib import Path
from typing import Tuple, Union

import cv2
import numpy as np
from PIL import Image


def convert_to_jpeg(path: Union[str, Path], quality: int = 95, destination: Union[str, Path] = None) -> Path:
    """
    Convert an image to JPEG format.

    Args:
        path: The path to the image file.
        quality: The JPEG quality, from 0 to 100.

    Returns:
        The path to the converted image file.
    """
    path = Path(path)
    if path.suffix.lower() in (".jpg", ".jpeg"):
        return path
    else:
        new_path = path.with_suffix(".jpg")
        img = Image.open(path)
        img.convert("RGB").save(new_path, "JPEG", quality=quality)
        return new_path


def resize_fit(path: Union[str, Path], max_width: int = 1920, max_height: int = 1080, destination: Union[str, Path] = None):
    """
    Resize an image to fit within a maximum width and height.

    Args:
        path: The path to the image file.
        max_width: The maximum width of the image.
        max_height: The maximum height of the image.
        destination: The path to save the resized image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    width, height = img.size
    if width > max_width or height > max_height:
        ratio = min(max_width / width, max_height / height)
        new_width = int(width * ratio)
        new_height = int(height * ratio)
        img = img.resize((new_width, new_height), Image.LANCZOS)
    img.save(destination)


def resize_exact(path: Union[str, Path], width: int = 1920, height: int = 1080, destination: Union[str, Path] = None):
    """
    Resize an image to exact dimensions.

    Args:
        path: The path to the image file.
        width: The width to resize the image to.
        height: The height to resize the image to.
        destination: The path to save the resized image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.resize((width, height), Image.LANCZOS)
    img.save(destination)


def scale(path: Union[str, Path], scale: float, destination: Union[str, Path] = None):
    """
    Scale an image by a given factor.

    Args:
        path: The path to the image file.
        scale: The scale factor to apply to the image, 0-1.
        destination: The path to save the scaled image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    width, height = img.size
    new_width = int(width * scale)
    new_height = int(height * scale)
    img = img.resize((new_width, new_height), Image.LANCZOS)
    img.save(destination)


def rotate_clockwise(path: Union[str, Path], degrees: int, expand: bool = False, destination: Union[str, Path] = None):
    """
    Rotate an image clockwise by a given number of degrees.

    Args:
        path: The path to the image file.
        degrees: The number of degrees to rotate the image.
        destination: The path to save the rotated image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.rotate(-degrees, expand=expand)
    img.save(destination)


def turn_clockwise(path: Union[str, Path], turns: int = 1, destination: Union[str, Path] = None):
    """
    Turn an image clockwise in steps of 90 degrees.

    Args:
        path: The path to the image file.
        destination: The path to save the turned image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.transpose(Image.ROTATE_90 * turns)
    img.save(destination)


def flip_vertical(path: Union[str, Path], destination: Union[str, Path] = None):
    """
    Flip an image vertically.

    Args:
        path: The path to the image file.
        destination: The path to save the flipped image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.transpose(Image.FLIP_TOP_BOTTOM)
    img.save(destination)


def flip_horizontal(path: Union[str, Path], destination: Union[str, Path] = None):
    """
    Flip an image horizontally.

    Args:
        path: The path to the image file.
        destination: The path to save the flipped image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.transpose(Image.FLIP_LEFT_RIGHT)
    img.save(destination)


def is_blurry(path: Union[str, Path], threshold: float = 100.0) -> bool:
    """
    Determine if an image is blurry.

    Args:
        path: The path to the image file.
        threshold: The threshold for the variance of the Laplacian. If the variance is below this threshold, the image is considered blurry.

    Returns:
        True if the image is blurry, False otherwise.
    """
    image = cv2.imread(str(path))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    variance_of_laplacian = cv2.Laplacian(gray, cv2.CV_64F).var()
    return variance_of_laplacian < threshold


def crop(path: Union[str, Path], x: int, y: int, width: int, height: int, destination: Union[str, Path] = None):
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
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path
    img = Image.open(path)
    img = img.crop((x, y, x + width, y + height))
    img.save(destination)


def apply_clahe(path: Union[str, Path], clip_limit: float = 2.0, tile_grid_size: Tuple[int, int] = (8, 8), destination: Union[str, Path] = None):
    """
    Apply Contrast Limited Adaptive Histogram Equalization (CLAHE) to an image.

    Args:
        path: The path to the image file.
        clip_limit: The threshold for contrast limiting.
        tile_grid_size: The size of the grid for the histogram equalization. The image will be divided into equally sized rectangular tiles.
        destination: The path to save the image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path

    img = cv2.imread(str(path), 0)

    # Apply CLAHE to the image
    clahe = cv2.createCLAHE(clipLimit=clip_limit, tileGridSize=tile_grid_size)
    img_clahe = clahe.apply(img)

    cv2.imwrite(str(destination), img_clahe)


def gaussian_blur(path: Union[str, Path], kernel_size: Tuple[int, int] = (5, 5), destination: Union[str, Path] = None):
    """
    Blur an image.

    Args:
        path: The path to the image file.
        kernel_size: The size of the kernel to use for blurring.
        destination: The path to save the blurred image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path

    img = cv2.imread(str(path))

    # Apply Gaussian blur to the image
    img_blur = cv2.GaussianBlur(img, kernel_size, 0)

    cv2.imwrite(str(destination), img_blur)


def sharpen(path: Union[str, Path], destination: Union[str, Path] = None):
    """
    Sharpen an image.

    Args:
        path: The path to the image file.
        destination: The path to save the sharpened image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    if destination is not None:
        destination = Path(destination)
    else:
        destination = path

    img = cv2.imread(str(path))

    # Apply sharpening to the image
    kernel = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    img_sharpen = cv2.filter2D(img, -1, kernel)

    cv2.imwrite(str(destination), img_sharpen)
