"""Tests for marimba.core.utils.log.

The logging convention is load-bearing per CLAUDE.md §Logging convention;
get_logger is the canonical entry point and must produce a logger with the
expected level + handler shape. These tests pin those invariants directly
rather than relying on transitive use across the codebase.
"""

import logging
from pathlib import Path

import pytest

from marimba.core.utils.log import (
    DryRunRichHandler,
    LogLevel,
    LogMixin,
    LogPrefixFilter,
    NoRichFileHandler,
    get_file_handler,
    get_logger,
    get_rich_handler,
)


class TestGetLogger:
    """Cover get_logger's name + level + handler contract."""

    @pytest.mark.unit
    def test_returns_logger_with_given_name(self) -> None:
        logger = get_logger("test_get_logger_name")

        assert logger.name == "test_get_logger_name"
        assert isinstance(logger, logging.Logger)

    @pytest.mark.unit
    def test_default_level_is_debug(self) -> None:
        logger = get_logger("test_get_logger_default_level")

        assert logger.level == logging.DEBUG

    @pytest.mark.unit
    def test_custom_level_is_applied(self) -> None:
        logger = get_logger("test_get_logger_custom_level", level=logging.WARNING)

        assert logger.level == logging.WARNING

    @pytest.mark.unit
    def test_adds_rich_handler(self) -> None:
        logger = get_logger("test_get_logger_handler")
        rich_handler = get_rich_handler()

        # The shared rich_handler is added on each get_logger call.
        assert rich_handler in logger.handlers


class TestGetRichHandler:
    """Cover get_rich_handler module-level singleton behaviour."""

    @pytest.mark.unit
    def test_returns_shared_instance(self) -> None:
        """get_rich_handler is a module-level singleton."""
        assert get_rich_handler() is get_rich_handler()

    @pytest.mark.unit
    def test_returns_dry_run_rich_handler_instance(self) -> None:
        assert isinstance(get_rich_handler(), DryRunRichHandler)


class TestGetFileHandler:
    """Cover get_file_handler construction and error paths."""

    @pytest.mark.unit
    def test_creates_handler_with_log_extension(self, tmp_path: Path) -> None:
        handler = get_file_handler(tmp_path, "myname", dry_run=False)

        try:
            assert isinstance(handler, NoRichFileHandler)
            assert handler.baseFilename.endswith("myname.log")
        finally:
            handler.close()

    @pytest.mark.unit
    def test_raises_when_output_dir_missing(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="does not exist"):
            get_file_handler(tmp_path / "nope", "myname", dry_run=False)

    @pytest.mark.unit
    def test_default_level_is_debug(self, tmp_path: Path) -> None:
        handler = get_file_handler(tmp_path, "myname", dry_run=False)
        try:
            assert handler.level == logging.DEBUG
        finally:
            handler.close()


class TestNoRichFileHandlerDryRun:
    """Cover NoRichFileHandler dry-run vs write semantics."""

    @pytest.mark.unit
    def test_dry_run_skips_emit(self, tmp_path: Path) -> None:
        log_path = tmp_path / "out.log"
        handler = NoRichFileHandler(str(log_path), dry_run=True)

        try:
            record = logging.LogRecord("x", logging.INFO, "p", 1, "hello", None, None)
            handler.emit(record)
        finally:
            handler.close()

        # In dry-run mode emit returns without writing; the file may not even exist.
        assert (not log_path.exists()) or log_path.read_text() == ""

    @pytest.mark.unit
    def test_non_dry_run_writes_rendered_message(self, tmp_path: Path) -> None:
        log_path = tmp_path / "out.log"
        handler = NoRichFileHandler(str(log_path), dry_run=False)
        # NoRichFileHandler does NOT add a formatter itself — get_file_handler
        # does. Add a minimal formatter so the test asserts content shape, not
        # default format quirks.
        handler.setFormatter(logging.Formatter("%(message)s"))

        try:
            record = logging.LogRecord("x", logging.INFO, "p", 1, "hello world", None, None)
            handler.emit(record)
        finally:
            handler.close()

        contents = log_path.read_text()
        assert "hello world" in contents


class TestLogPrefixFilter:
    """Cover LogPrefixFilter prefix mutation."""

    @pytest.mark.unit
    def test_apply_prefix_mutates_message_and_returns_true(self) -> None:
        log_filter = LogPrefixFilter("[PIPELINE_X] ")
        record = logging.LogRecord("x", logging.INFO, "p", 1, "starting work", None, None)

        result = log_filter.apply_prefix(record)

        assert result is True
        assert record.msg == "[PIPELINE_X] starting work"


class TestLogLevel:
    """Cover LogLevel enum value pinning."""

    @pytest.mark.unit
    def test_values_match_stdlib_names(self) -> None:
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"


class TestLogMixin:
    """Cover LogMixin lazy + memoised logger access."""

    @pytest.mark.unit
    def test_logger_lazily_initialised_and_named_after_class(self) -> None:
        class MyClass(LogMixin):
            pass

        instance = MyClass()
        assert not hasattr(instance, "_logger")

        logger = instance.logger

        assert isinstance(logger, logging.Logger)
        assert logger.name == "MyClass"
        assert hasattr(instance, "_logger")

    @pytest.mark.unit
    def test_logger_is_memoised_across_accesses(self) -> None:
        class MyClass2(LogMixin):
            pass

        instance = MyClass2()
        first = instance.logger
        second = instance.logger

        assert first is second
