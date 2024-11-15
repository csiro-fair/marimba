"""
Marimba Logging Utilities.

This module provides utilities for configuring and using logging in the Marimba CLI application. It includes custom
log handlers, log level enumeration, and a mixin class for easy integration of logging into other classes.

Imports:
    - logging: The Python standard library logging module.
    - enum.Enum: The enumeration class from the Python standard library.
    - pathlib.Path: The Path class from the Python standard library for handling file paths.
    - typing: Type hinting classes and utilities from the Python standard library.
    - rich.console.Console: The Console class from the Rich library for formatting console output.
    - rich.logging.RichHandler: The RichHandler class from the Rich library for integrating Rich formatting with
    logging.

Classes:
    - DryRunRichHandler: A custom log handler that extends RichHandler and adds dry run functionality.
    - NoRichFileHandler: A custom file handler that removes Rich styling from log entries.
    - LogLevel: An enumeration of log levels for the Marimba CLI.
    - LogMixin: A mixin class that adds a `logger` property for easy integration of logging into other classes.

Functions:
    - get_logger: Get a logger with a given name and level.
    - get_rich_handler: Get the global Rich handler.
    - get_file_handler: Get a file handler for a given output directory and name.
"""

import logging
from enum import Enum
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

# Global file log format - this is used for all file handlers.
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class DryRunRichHandler(RichHandler):
    """
    A class that extends the RichHandler class and adds a dry run functionality to log messages.

    Attributes:
        dry_run (bool): Flag indicating whether the dry run mode is enabled.

    Methods:
        __init__(self, dry_run: bool, *args: Any, **kwargs: Any) -> None:
            Initializes the DryRunRichHandler object.

        emit(self, record: logging.LogRecord) -> None:
            Overrides the emit method of RichHandler to prepend "DRY_RUN" to log messages when dry run mode is enabled.

        set_dry_run(self, dry_run: bool) -> None:
            Sets the dry_run attribute to the specified boolean value.
    """

    def __init__(self, dry_run: bool, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """
        Initialise the __init__ method.

        Args:
            dry_run (bool): Specifies whether the method should run in dry-run mode.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None
        """
        super().__init__(*args, **kwargs)
        self.dry_run = dry_run

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit method for RichHandler.

        Args:
            record (logging.LogRecord): The log record to be emitted.

        Returns:
            None: This method does not return anything.

        """
        if self.dry_run:
            record.msg = f"DRY_RUN - {record.msg}"
        super().emit(record)

    def set_dry_run(self, dry_run: bool) -> None:
        """
        Set dry-run string in Rich logger.

        Args:
            dry_run (bool): A boolean value indicating whether to enable dry run mode or not.

        Returns:
            None
        """
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
    logger = logging.getLogger(name)
    logger.setLevel(level)
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
    output_dir: str | Path,
    name: str,
    dry_run: bool,
    level: int = logging.DEBUG,
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


class NoRichFileHandler(logging.FileHandler):
    """
    Custom FileHandler to remove Rich styling from log entries.
    """

    def __init__(
        self,
        filename: str,
        mode: str = "a",
        encoding: str | None = None,
        *,
        delay: bool = False,
        dry_run: bool = False,
    ) -> None:
        """
        Initialise the class instance.

        Args:
            filename (str): The filename to which the log will be written.
            mode (str): The mode in which the file will be opened.
            encoding (str): The encoding to use when writing to the file.
            delay (bool): If true, the file opening is deferred until the first emit.
            dry_run (bool): If true, no log entries will be written to the file.

        """
        super().__init__(filename, mode, encoding, delay)
        self.dry_run = dry_run

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit method for RichHandler.

        Args:
            record: A logging.LogRecord object that contains information about the log record being emitted.

        Returns:
            None

        Description:
        This method is called by the logging module's Logger to emit a log record. It is overridden in a subclass
        of a logging.Handler to perform specific actions when a log record is emitted.

        The method first checks if the 'dry_run' attribute is set to True. If it is, the method returns without
        performing any further actions.

        Next, the log message of the record is rendered to a string using the Rich Console library. The rendered
        message is then assigned to the 'msg' attribute of the record, replacing the original log message. The
        'args' attribute is set to an empty tuple.

        Finally, the original 'emit' method of the superclass is called to write the plain text log entry to the file.
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


class LogPrefixFilter(logging.Filter):
    """
    A log filter that adds a prefix to log messages.

    Attributes:
        prefix (str): The prefix to add to log messages.
    """

    def __init__(self, prefix: str) -> None:
        """
        Initialise the filter with a prefix.

        Args:
            prefix (str): The prefix to add to log messages.
        """
        self.prefix = prefix
        super().__init__()

    def apply_prefix(self, record: logging.LogRecord) -> bool:
        """
        Apply the prefix to the log record's message.

        Args:
            record (logging.LogRecord): The log record.

        Returns:
            bool: Always returns True to ensure the record is not filtered out.
        """
        record.msg = f"{self.prefix} {record.msg}"
        return True


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
        """
        Returns the logger instance for the current class.

        Returns:
            logging.Logger: The logger instance.

        Notes:
            - The logger is lazily initialized, meaning that it is only created when the logger property is accessed
            for the first time.
            - The logger is named after the class name.
            - A NullHandler is added to the logger to avoid logs being outputted to stdout by default.
        """
        # Lazy initialization
        if not hasattr(self, "_logger"):
            self._logger = get_logger(self.__class__.__name__)
            # Add NullHandler to avoid logs on stdout by default
            self._logger.addHandler(logging.NullHandler())
        return self._logger
