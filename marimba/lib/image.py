"""Marimba Image Utilities.

This module offers a comprehensive set of functions for various image processing tasks, including resizing,
cropping, rotating, and applying filters. It also includes utilities for image analysis and grid creation.

Imports:
    - collections.abc: Provides abstract base classes for collections.
    - dataclasses: Provides a decorator and functions for automatically adding generated special methods to classes.
    - pathlib: Offers classes representing filesystem paths with semantics appropriate for different operating systems.
    - shutil: Offers a number of high-level operations on files and collections of files.
    - typing: Provides runtime support for type hints.
    - cv2: OpenCV library for computer vision tasks.
    - numpy: Fundamental package for scientific computing with Python.
    - PIL: Python Imaging Library for opening, manipulating, and saving many different image file formats.

Classes:
    - GridDimensions: Defines dimensions and configuration for grid image creation.
    - GridRow: Represents a single row in an image grid.
    - GridImageProcessor: Processes images into grid layouts.
    - OutputPathManager: Manages the creation of output paths for grid images.

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

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from typing import cast

import cv2
import numpy as np
from PIL import Image
from PIL.Image import Image as PILImage


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
        resize_fit(image, 300, 300, output_path)
    return output_path


def convert_to_jpeg(path: str | Path, quality: int = 95, destination: str | Path | None = None) -> Path:
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
    path: str | Path,
    max_width: int = 1920,
    max_height: int = 1080,
    destination: str | Path | None = None,
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
    path: str | Path,
    width: int = 1920,
    height: int = 1080,
    destination: str | Path | None = None,
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


def scale(path: str | Path, scale_factor: float, destination: str | Path | None = None) -> None:
    """
    Scale an image by a given factor.

    Args:
        path: The path to the image file.
        scale_factor: The scale factor to apply to the image, 0-1.
        destination: The path to save the scaled image to. If not provided, the original file will be overwritten.
    """
    path = Path(path)
    destination = Path(destination) if destination is not None else path
    img = cast(Image.Image, Image.open(path))
    width, height = img.size
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)
    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    img.save(destination)


def rotate_clockwise(
    path: str | Path,
    degrees: int,
    *,
    expand: bool = False,
    destination: str | Path | None = None,
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


def turn_clockwise(path: str | Path, turns: int = 1, destination: str | Path | None = None) -> None:
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


def flip_vertical(path: str | Path, destination: str | Path | None = None) -> None:
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


def flip_horizontal(path: str | Path, destination: str | Path | None = None) -> None:
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


def is_blurry(path: str | Path, threshold: float = 100.0) -> bool:
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
    path: str | Path,
    x: int,
    y: int,
    width: int,
    height: int,
    destination: str | Path | None = None,
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
    path: str | Path,
    clip_limit: float = 2.0,
    tile_grid_size: tuple[int, int] = (8, 8),
    destination: str | Path | None = None,
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
    path: str | Path,
    kernel_size: tuple[int, int] = (5, 5),
    destination: str | Path | None = None,
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


def sharpen(path: str | Path, destination: str | Path | None = None) -> None:
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


def get_width_height(path: str | Path) -> tuple[int, int]:
    """
    Get the width and height of an image.

    Args:
        path: The path to the image file.

    Returns:
        A tuple containing the width and height of the image.
    """
    expected_dimensions = 2

    path = Path(path)
    img = cast(Image.Image, Image.open(path))
    size = img.size

    if not (isinstance(size, tuple) and len(size) == expected_dimensions and all(isinstance(x, int) for x in size)):
        raise ValueError("Size must be a tuple of two integers")

    return size


@dataclass
class GridDimensions:
    """Define the dimensions and configuration for grid image creation.

    This class represents the dimensions and configuration parameters used for creating a grid-based image. It stores
    information about the number of columns, the width of each column, and the maximum height of the grid. These
    attributes are used to determine the layout and size of the resulting grid image.

    Attributes:
        columns (int): The number of columns in the grid.
        column_width (int): The width of each column in pixels.
        max_height (int): The maximum height of the grid in pixels.
    """

    columns: int
    column_width: int
    max_height: int


class GridRow:
    """
    Represents a single row in an image grid.

    This class manages a row of images within a grid layout. It handles image resizing, placement, and rendering
    onto a canvas. The row automatically adjusts the height of images to fit within the specified column width while
    maintaining aspect ratio.

    Attributes:
        images (list[tuple[PILImage, int, int]]): List of tuples containing resized images and their dimensions.
        height (int): Height of the tallest image in the row.
        dimensions (GridDimensions): Configuration for the grid layout.

    Methods:
        add_image(img: PILImage) -> bool: Adds an image to the row if space is available.
        render(canvas: PILImage, y_offset: int) -> None: Renders the row of images onto the given canvas.
        cleanup() -> None: Closes all images in the row to free up resources.
    """

    def __init__(self, dimensions: GridDimensions) -> None:
        """
        Initialize a new grid row.

        Args:
            dimensions: Configuration for the grid layout
        """
        self.images: list[tuple[PILImage, int, int]] = []  # (image, width, height)
        self.height: int = 0
        self.dimensions = dimensions

    def add_image(self, img: PILImage) -> bool:
        """
        Attempt to add an image to the row.

        Args:
            img: The image to add to the row

        Returns:
            bool: True if image was successfully added, False if row is full
        """
        if len(self.images) >= self.dimensions.columns:
            return False

        # Calculate scaled dimensions to fit column width
        img_width, img_height = img.size
        scale_factor = self.dimensions.column_width / img_width
        scaled_height = int(img_height * scale_factor)

        # Resize image
        resized_img = img.resize(
            (self.dimensions.column_width, scaled_height),
            Image.Resampling.LANCZOS,
        )

        self.images.append((resized_img, self.dimensions.column_width, scaled_height))
        self.height = max(self.height, scaled_height)
        return True

    def render(self, canvas: PILImage, y_offset: int) -> None:
        """
        Render the row onto the canvas at the specified y_offset.

        Args:
            canvas: The image canvas to render onto
            y_offset: Vertical offset for rendering
        """
        for idx, (img, _, height) in enumerate(self.images):
            x = idx * self.dimensions.column_width
            y = y_offset + (self.height - height) // 2  # Center vertically in row
            canvas.paste(img, (x, y))

    def cleanup(self) -> None:
        """Close all images in the row to free up resources."""
        for img, _, _ in self.images:
            img.close()


@dataclass
class GridImageProcessor:
    """
    Process images into grid layouts.

    This class handles the processing of images into grid layouts based on specified dimensions. It can create grids
    from a subset of image paths, process single images, and render the final grid image.

    Attributes:
        dimensions: GridDimensions object containing the grid layout specifications.

    Methods:
        process_single_image: Process a single image and add it to the row.
        create_grid: Create a single grid from a subset of image paths.
        _render_grid: Render the grid from a list of rows.

    Example:
        dimensions = GridDimensions(columns=3, column_width=200, max_height=800)
        processor = GridImageProcessor(dimensions)
        image_paths = [Path('image1.jpg'), Path('image2.jpg'), Path('image3.jpg')]
        grid_image, height, processed = processor.create_grid(image_paths)
        if grid_image:
            grid_image.save('output_grid.jpg')
    """

    dimensions: GridDimensions

    def process_single_image(self, path: Path, row: GridRow) -> bool:
        """
        Process a single image and add it to the row.

        This function opens an image file, processes it, and attempts to add it to the provided GridRow object. It
        handles potential errors that may occur during image processing and manages failed attempts.

        Args:
            path (Path): The file path to the image to be processed.
            row (GridRow): The GridRow object to which the processed image will be added.

        Returns:
            bool: True if the image was successfully processed and added to the row, False otherwise.

        Raises:
            OSError: If there is an issue opening or processing the image file.
        """
        try:
            with Image.open(path) as img:
                return row.add_image(img)
        except OSError:
            return False

    def create_grid(self, paths_subset: list[Path]) -> tuple[PILImage | None, int, int]:
        """
        Create a single grid from a subset of image paths.

        This function processes a subset of image paths to create a grid of images. It iterates through the paths,
        processing each image and adding it to the current row. When a row is full or the maximum height is reached,
        it starts a new row. The process continues until all images are processed or the grid's maximum height is
        reached.

        Args:
            paths_subset (list[Path]): A list of Path objects representing the image files to be included in the grid.

        Returns:
            tuple[PILImage | None, int, int]: A tuple containing:
                - The rendered grid as a PIL Image object, or None if no grid was created.
                - The current height of the grid.
                - The number of images processed.

        Raises:
            ValueError: If the dimensions object is not properly initialized.
            IOError: If there are issues reading or processing the image files.
        """
        rows: list[GridRow] = []
        current_height = 0
        current_row = GridRow(self.dimensions)
        images_processed = 0

        while images_processed < len(paths_subset):
            path = paths_subset[images_processed]
            if not self.process_single_image(path, current_row):
                images_processed += 1
                continue

            # If row is full, start a new one
            if len(current_row.images) >= self.dimensions.columns:
                if current_height + current_row.height <= self.dimensions.max_height:
                    current_height += current_row.height
                    rows.append(current_row)
                    current_row = GridRow(self.dimensions)
                else:
                    break

            images_processed += 1

        # Handle last row
        if current_row.images and current_height + current_row.height <= self.dimensions.max_height:
            rows.append(current_row)
            current_height += current_row.height
        else:
            current_row.cleanup()

        if not rows:
            return None, 0, images_processed

        return self._render_grid(rows), current_height, images_processed

    def _render_grid(self, rows: list[GridRow]) -> PILImage:
        """
        Render the grid from a list of rows.

        This function takes a list of GridRow objects and renders them into a single PIL Image. It calculates the total
        height of the grid based on the heights of individual rows, creates a new image with the appropriate dimensions,
        and then iterates through each row, rendering it onto the final image at the correct vertical position.

        Args:
            rows: A list of GridRow objects to be rendered.

        Returns:
            PILImage: A PIL Image object representing the rendered grid.

        Raises:
            PIL.Image.DecompressionBombError: If the resulting image dimensions exceed the maximum allowed size.
            MemoryError: If there's not enough memory to create the image.
        """
        actual_height = sum(row.height for row in rows)
        final_image = Image.new(
            "RGB",
            (self.dimensions.columns * self.dimensions.column_width, actual_height),
            color="black",
        )

        y_offset = 0
        for row in rows:
            row.render(final_image, y_offset)
            y_offset += row.height
            row.cleanup()

        return final_image


class OutputPathManager:
    """
    Manage the creation of output paths for grid images.

    This class is responsible for generating appropriate file paths for grid images based on a given base path.
    It handles the creation of sequential file names for multiple grids and ensures proper file extensions are used.

    Attributes:
        base_path (Path): The base path for output files.
        extension (str): The file extension to use if the base path doesn't have one. Defaults to '.jpg'.

    Methods:
        create_path(grid_number: int, has_more_grids: bool) -> Path:
            Create the appropriate output path based on grid number and remaining grids.
    """

    def __init__(self, base_path: Path) -> None:
        """
        Initialize the OutputPathManager with a base path for output files.

        Args:
            base_path: Base path for output files
        """
        """Initialize with base path for output files."""
        self.base_path = base_path
        self.extension = "" if base_path.suffix else ".jpg"

    def create_path(self, grid_number: int, has_more_grids: bool) -> Path:
        """
        Create the appropriate output path based on grid number and remaining grids.

        This function generates a Path object for the output file. If there are no more grids and the grid number is 0,
        it returns the base path. Otherwise, it creates a new path by appending the grid number to the base filename.

        Args:
            grid_number (int): The current grid number being processed.
            has_more_grids (bool): Indicates whether there are more grids to process after the current one.

        Returns:
            Path: A Path object representing the output file path.

        Raises:
            TypeError: If grid_number is not an integer or has_more_grids is not a boolean.
        """
        if not has_more_grids and grid_number == 0:
            return self.base_path
        grid_number += 1
        # Insert the number before any existing extension
        return (
            self.base_path.parent / f"{self.base_path.stem}_{grid_number:02d}{self.base_path.suffix or self.extension}"
        )


def create_grid_image(
    paths: Iterable[str | Path],
    destination: str | Path,
    columns: int = 5,
    column_width: int = 300,
    max_height: int = 32000,
) -> list[Path]:
    """Create one or more grid images from the provided image paths.

    Images are arranged in a grid layout, with automatic pagination when the
    maximum height is reached. Images are scaled to fit the specified column width
    while maintaining aspect ratio.

    Args:
        paths: Paths to the images to include in the grid
        destination: Base path for saving the output grid images
        columns: Number of columns in the grid
        column_width: Width in pixels of each column
        max_height: Maximum allowed height for a single grid image

    Returns:
        List of paths to the created grid images
    """
    paths_list = sorted([Path(p) for p in paths], key=lambda x: x.name)
    if not paths_list:
        return []

    dimensions = GridDimensions(columns, column_width, max_height)
    processor = GridImageProcessor(dimensions)
    path_manager = OutputPathManager(Path(destination))

    created_files: list[Path] = []
    remaining_paths = paths_list
    grid_number = 0

    while remaining_paths:
        grid_image, _, images_processed = processor.create_grid(remaining_paths)
        if grid_image is None:
            break

        remaining_paths = remaining_paths[images_processed:]
        output_path = path_manager.create_path(grid_number, bool(remaining_paths))

        grid_image.save(output_path)
        created_files.append(output_path)
        grid_image.close()

        grid_number += 1 if remaining_paths or grid_number > 0 else 0

    return created_files


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


def get_average_image_color(image_data: Image.Image) -> tuple[int, ...]:
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
