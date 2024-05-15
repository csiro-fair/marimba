import logging
from enum import Enum
from pathlib import Path
from typing import Union, Optional, Any

from rich.console import Console
from rich.logging import RichHandler

# Global file log format - this is used for all file handlers.
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class DryRunRichHandler(RichHandler):
    def __init__(self, dry_run: bool, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.dry_run = dry_run

    def emit(self, record: logging.LogRecord) -> None:
        if self.dry_run:
            record.msg = f"DRY_RUN - {record.msg}"
        super().emit(record)

    def set_dry_run(self, dry_run: bool) -> None:
        self.dry_run = dry_run


rich_handler = DryRunRichHandler(
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


def get_rich_handler() -> DryRunRichHandler:
    """
    Get the global Rich handler.

    Returns:
        The Rich handler.
    """
    return rich_handler


def get_file_handler(
        output_dir: Union[str, Path],
        name: str,
        dry_run: bool,
        level: int = logging.INFO
) -> logging.FileHandler:
    """
    Get a file handler for a given output directory and name.

    Args:
        output_dir: The output directory.
        name: The name (stem) of the file. The file extension will be added automatically.
        dry_run (bool): If true, no log entries will be written to the file.
        level: The logging level.

    Returns:
        A file handler to `output_dir/name.log`.

    Raises:
        FileNotFoundError: If the output directory does not exist.
    """
    output_dir = Path(output_dir)

    # Ensure the directory exists
    if not output_dir.is_dir():
        raise FileNotFoundError(f"Output directory {output_dir} does not exist.")

    # Build the path as `output_dir/name.log`
    path = output_dir / f"{name}.log"

    # Create the handler and set the level & formatter
    handler = NoRichFileHandler(str(path.absolute()), dry_run=dry_run)
    handler.setLevel(level)
    handler.setFormatter(file_formatter)

    return handler


# Create a Rich console object
console = Console()


class NoRichFileHandler(logging.FileHandler):
    """
    Custom FileHandler to remove Rich styling from log entries.
    """

    def __init__(
            self,
            filename: str,
            mode: str = "a",
            encoding: Optional[str] = None,
            delay: bool = False,
            dry_run: bool = False
    ) -> None:
        """
        Initialize the NoRichFileHandler.

        Parameters:
        - filename (str): The filename to which the log will be written.
        - mode (str): The mode in which the file will be opened.
        - encoding (str): The encoding to use when writing to the file.
        - delay (bool): If true, the file opening is deferred until the first emit.
        - dry_run (bool): If true, no log entries will be written to the file.

        """
        super().__init__(filename, mode, encoding, delay)
        self.dry_run = dry_run

    def emit(self, record: logging.LogRecord) -> None:
        """
        Over-ride the emit method to conditionally remove styling and write log.

        If dry_run is True, the method will return early, not logging the message to file.

        Parameters:
        - record (logging.LogRecord): The log record to emit.

        """
        # Check if dry_run is set to True
        if self.dry_run:
            return

        # Render the log message to a string using Rich Console
        rendered_message = Console().render_str(record.getMessage())

        # Replace the original log message with the plain text version
        record.msg = rendered_message
        record.args = ()

        # Call the original emit method to write the plain text log entry to the file
        super().emit(record)


class LogLevel(str, Enum):
    """
    Enumerated log levels for Marimba CLI.
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
