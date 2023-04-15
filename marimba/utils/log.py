import logging
import os.path
from enum import Enum
from pathlib import Path

import rich.logging

from marimba.utils.context import get_collection_path, get_instrument_path

# Global file formatter. This is used for all file handlers.
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Global Rich (console) handler. This is used for all loggers and can have its level configured with the `--level` global option.
rich_handler = rich.logging.RichHandler(
    level=logging.WARNING, log_time_format="%Y-%m-%d %H:%M:%S,%f", markup=True, show_path=True, rich_tracebacks=True, tracebacks_show_locals=False
)

# Global collection logger. This is used for all collection-level logging.
collection_logger = None

# Global instrument name -> file handler map. These are used for all instrument-level logging.
instrument_file_handlers = {}


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Get a logger with a given name and level.

    Args:
        name: The name of the logger.
        level: The level of the logger.

    Returns:
        A logger.
    """
    # Create the logger and add the Rich handler
    logger = logging.Logger(name, level)
    logger.addHandler(rich_handler)

    return logger


def get_rich_handler() -> rich.logging.RichHandler:
    """
    Get the global Rich handler.

    Returns:
        The Rich handler.
    """
    return rich_handler


def get_collection_logger() -> logging.Logger:
    """
    Get the collection-level logger. Initializes the logger if it has not been initialized.

    Returns:
        The global logger.
    """
    global collection_logger
    if collection_logger is None:
        collection_logger = get_logger("marimba")

    return collection_logger


def init_collection_file_handler():
    """
    Initialize the collection-level file handler.

    This should be called after the collection directory has been set by `set_collection_path`.
    """
    # Get the collection directory and basename
    collection_path = get_collection_path()
    # collection_basename = os.path.basename(collection_path)
    collection_basename = Path(collection_path).parts[-1]

    # Create the file handler and add it to the collection logger
    collection_file_handler = get_file_handler(collection_path, collection_basename)
    collection_file_handler.setLevel(logging.INFO)
    collection_file_handler.setFormatter(file_formatter)
    get_collection_logger().addHandler(collection_file_handler)


def get_instrument_file_handler(instrument_name: str) -> logging.FileHandler:
    """
    Get the file handler for an instrument. Initializes the file handler if it has not been initialized.

    Args:
        instrument_name: The name of the instrument.

    Returns:
        The file handler for the instrument.
    """
    if instrument_name not in instrument_file_handlers:
        init_instrument_file_handler(instrument_name)

    return instrument_file_handlers[instrument_name]


def init_instrument_file_handler(instrument_name: str):
    """
    Initialize the file handler for an instrument.

    Args:
        instrument_name: The name of the instrument.
    """
    # Get the instrument directory
    instrument_dir = get_instrument_path(instrument_name)

    # Create the file handler and add it to the collection logger
    instrument_file_handler = get_file_handler(instrument_dir, instrument_name)
    instrument_file_handler.setLevel(logging.INFO)
    instrument_file_handler.setFormatter(file_formatter)

    # Add it to the map
    global instrument_file_handlers
    instrument_file_handlers[instrument_name] = instrument_file_handler


def get_file_handler(output_dir: str, name: str, level: int = logging.INFO) -> logging.FileHandler:
    """
    Get a file handler for a given output directory and name.

    Args:
        output_dir: The output directory.
        name: The name (stem) of the file. The file extension will be added automatically.

    Returns:
        A file handler to `output_dir/name.log`.
    """
    # Ensure the directory exists
    assert os.path.exists(output_dir), f"Output directory {output_dir} does not exist."

    # Build the path as `output_dir/name.log`
    path = os.path.join(output_dir, f"{name}.log")

    # Create the handler and set the level & formatter
    handler = logging.FileHandler(path)
    handler.setLevel(level)
    handler.setFormatter(file_formatter)

    return handler


from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

logger = get_collection_logger()

from marimba.utils.context import set_collection_path

def setup_logging(collection_path):
    # Check that collection_path exists and is legit
    if not os.path.isdir(collection_path) or not os.path.isfile(Path(collection_path) / "collection.yml") or not os.path.isdir(
            Path(collection_path) / "instruments"):
        print(
            Panel(
                f'The provided root MarImBA collection path "[bold]{collection_path}[/bold]" does not appear to be a valid MarImBA collection.',
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    # Set the collection directory
    set_collection_path(collection_path)

    # Initialize the collection-level file handler
    init_collection_file_handler()

    logger.info(f"Setting up collection-level logging at: {collection_path}")

class LogLevel(str, Enum):
    """
    Enumerated log levels for MarImBA CLI.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogMixin:
    """
    Mixin for logging. Adds a `logger` property that provides a `logging.Logger` ad-hoc using the class name.
    """

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):  # Lazy initialization
            self._logger = get_logger(self.__class__.__name__)
            self._logger.addHandler(logging.NullHandler())  # Add NullHandler to avoid logs on stdout by default
        return self._logger
