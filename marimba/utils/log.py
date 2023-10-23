import logging
import os.path
from enum import Enum
from pathlib import Path
from typing import Optional, Union

import rich.logging
import typer
from rich import print
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel

from marimba.utils.context import get_collection_path, get_instrument_path, set_collection_path

# Global collection logger - this is used for all collection-level logging.
collection_logger: Optional[logging.Logger] = None

# Global file log format - this is used for all file handlers.
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Global instrument name -> file handler dictionary - this is used for all instrument-level logging.
instrument_file_handlers = {}


class DryRunRichFormatter(RichHandler):
    def __init__(self, dry_run, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = dry_run

    def emit(self, record):
        if self.dry_run:
            record.msg = f"DRY_RUN - {record.msg}"
        return super().emit(record)

    def set_dry_run(self, dry_run: bool):
        self.dry_run = dry_run


rich_handler = DryRunRichFormatter(
    dry_run=False,
    level=logging.WARNING,
    log_time_format="%Y-%m-%d %H:%M:%S,%f",
    markup=True,
    show_path=True,
    rich_tracebacks=True,
    tracebacks_show_locals=False,
)


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
    Initialise the collection-level file handler. This should be called after the collection directory has been set by `set_collection_path`.
    """
    # Get the collection directory and basename
    collection_path = get_collection_path()

    # Check collection path is set
    if collection_path is None:
        raise ValueError("Collection path is not set. Call `set_collection_path` first.")

    collection_basename = collection_path.parts[-1]

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


def get_file_handler(output_dir: Union[str, Path], name: str, level: int = logging.INFO) -> logging.FileHandler:
    """
    Get a file handler for a given output directory and name.

    Args:
        output_dir: The output directory.
        name: The name (stem) of the file. The file extension will be added automatically.
        level: The logging level.

    Returns:
        A file handler to `output_dir/name.log`.
    """
    output_dir = Path(output_dir)

    # Ensure the directory exists
    assert output_dir.is_dir(), f"Output directory {output_dir} does not exist."

    # Build the path as `output_dir/name.log`
    path = output_dir / f"{name}.log"

    # Create the handler and set the level & formatter
    handler = NoRichFileHandler(path.absolute())
    handler.setLevel(level)
    handler.setFormatter(file_formatter)

    return handler


def setup_logging(collection_path: Union[str, Path], dry_run: bool = False):
    collection_path = Path(collection_path)

    # Check that collection_path exists and is legit
    legit = collection_path.is_dir() and (collection_path / "collection.yml").is_file() and (collection_path / "instruments").is_dir()
    if not legit:
        print(
            Panel(
                f'The provided root collection path "[bold]{collection_path}[/bold]" does not appear to be a valid MarImBA collection.',
                title="Error",
                title_align="left",
                border_style="red",
            )
        )
        raise typer.Exit()

    # Set the collection directory
    set_collection_path(collection_path)

    # Set the Rich handler dry run mode and initialise the collection-level file handler
    get_rich_handler().set_dry_run(dry_run)
    init_collection_file_handler()

    logger.info(f'Setting up collection-level logging at: "{collection_path}"')


# Create a Rich console object
console = Console()


class NoRichFileHandler(logging.FileHandler):
    """
    Custom FileHandler to remove Rich styling from log entries.
    """

    def emit(self, record):
        """
        Over-ride the emit method to remove styling.
        """

        # Render the log message to a string using Rich Console
        rendered_message = Console().render_str(record.getMessage())

        # Replace the original log message with the plain text version
        record.msg = rendered_message
        record.args = ()

        # Call the original emit method to write the plain text log entry to the file
        super().emit(record)


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
        # Lazy initialization
        if not hasattr(self, "_logger"):
            self._logger = get_logger(self.__class__.__name__)
            # Add NullHandler to avoid logs on stdout by default
            self._logger.addHandler(logging.NullHandler())
        return self._logger


logger = get_collection_logger()
