from rich.logging import RichHandler

"""
Reference:  https://github.com/borntyping/python-colorlog
            https://github.com/Textualize/rich/discussions/2189?sort=top


from rich.logging import RichHandler

Note:
    Be careful when enabling console markup in log messages if you have configured logging for libraries not
    under your control. If a dependency writes messages containing square brackets, it may not produce the intended output.

Args:
    level (Union[int, str], optional): Log level. Defaults to logging.NOTSET.
    console (:class:`~rich.console.Console`, optional): Optional console instance to write logs.
        Default will use a global console instance writing to stdout.
    show_time (bool, optional): Show a column for the time. Defaults to True.
    omit_repeated_times (bool, optional): Omit repetition of the same time. Defaults to True.
    show_level (bool, optional): Show a column for the level. Defaults to True.
    show_path (bool, optional): Show the path to the original log call. Defaults to True.
    enable_link_path (bool, optional): Enable terminal link of path column to file. Defaults to True.
    highlighter (Highlighter, optional): Highlighter to style log messages, or None to use ReprHighlighter. Defaults to None.
    markup (bool, optional): Enable console markup in log messages. Defaults to False.
    rich_tracebacks (bool, optional): Enable rich tracebacks with syntax highlighting and formatting. Defaults to False.
    tracebacks_width (Optional[int], optional): Number of characters used to render tracebacks, or None for full width. Defaults to None.
    tracebacks_extra_lines (int, optional): Additional lines of code to render tracebacks, or None for full width. Defaults to None.
    tracebacks_theme (str, optional): Override pygments theme used in traceback.
    tracebacks_word_wrap (bool, optional): Enable word wrapping of long tracebacks lines. Defaults to True.
    tracebacks_show_locals (bool, optional): Enable display of locals in tracebacks. Defaults to False.
    tracebacks_suppress (Sequence[Union[str, ModuleType]]): Optional sequence of modules or paths to exclude from traceback.
    locals_max_length (int, optional): Maximum length of containers before abbreviating, or None for no abbreviation.
        Defaults to 10.
    locals_max_string (int, optional): Maximum length of string before truncating, or None to disable. Defaults to 80.
    log_time_format (Union[str, TimeFormatterCallable], optional): If ``log_time`` is enabled, either string for strftime or callable that formats the time. Defaults to "[%x %X] ".
    keywords (List[str], optional): List of words to highlight instead of ``RichHandler.KEYWORDS``.
    
"""


class LoggerConfig:
    """
    Logger configuration for MarImBA CLI.
    """
    standardConfig = {
        "version": 1,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
            },
        },
        "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "standard", "level": "INFO"}},
        "loggers": {"": {"handlers": ["console"], "level": "INFO"}},
    }
    richConfig = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "rich.logging.RichHandler",
                "log_time_format": "[%Y-%m-%d %H:%M:%S,%f]",
                "markup": True,
                "show_path": True,
                "rich_tracebacks": True,
                "tracebacks_show_locals": False,
            },
        },
        "loggers": {
            "": {
                "level": "DEBUG",
                "handlers": ["console"],
                "propagate": "no",
            }
        }
    }
