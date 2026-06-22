"""
uv command executor.

This module provides a command executor that executes uv pip commands.

Classes:
    ExecutorResult: stdout and stderr from the executor.
    UvExecutor: Executor for uv pip commands.
"""

from __future__ import annotations

import dataclasses
import shutil
import subprocess

from marimba.core import MarimbaError


@dataclasses.dataclass
class ExecutorResult:
    """
    stdout and stderr from the executor.
    """

    output: str
    error: str


class UvExecutor:
    """
    Executor for uv pip commands.
    """

    class UvError(MarimbaError):
        """
        Exception raised when a uv pip command fails.
        """

    def __init__(self, uv_path: str) -> None:
        """
        Initialize a uv executor.

        Args:
            uv_path: Path to the uv executable.
        """
        self._uv_path = uv_path

    @classmethod
    def create(cls) -> UvExecutor:
        """
        Creates a uv executor from the uv system path.

        Returns:
            UvExecutor instance
        """
        uv_path = shutil.which("uv")
        if uv_path is None:
            msg = "uv executable not found in PATH"
            raise cls.UvError(msg)

        return cls(uv_path)

    def __call__(self, *args: str) -> ExecutorResult:
        """
        Executes a uv pip command.

        Args:
            *args: Arguments to uv pip command.

        Returns:
            ExecutorResult instance
        """
        with subprocess.Popen(
            [self._uv_path, "pip", *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            output, error = process.communicate()
            self._handle_uv_error(process.returncode, error.decode())

            return ExecutorResult(output.decode(), error.decode())

    def _handle_uv_error(self, return_code: int, stderr: str = "") -> None:
        """
        Handle uv pip command errors by raising appropriate exceptions.

        Args:
            return_code: The return code from uv pip command process
            stderr: The stderr output from the uv pip command

        Raises:
            UvError: If uv pip command fails
        """
        if return_code != 0:
            msg = f"uv pip command failed (return code {return_code}): {stderr.strip()}"
            raise self.UvError(msg)
