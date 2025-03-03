"""
pip command executor.

This module provides a command executor that executes pip commands.

Classes:
    ExecutorResult: stdout and stderr from the executor.
    PipExecutor: Executor for pip commands.
"""

from __future__ import annotations

import dataclasses
import shutil
import subprocess


@dataclasses.dataclass
class ExecutorResult:
    """
    stdout and stderr from the executor.
    """

    output: str
    error: str


class PipExecutor:
    """
    Executor for pip commands.
    """

    class PipError(Exception):
        """
        Exception raised when a pip command fails.
        """

    def __init__(self, pip_path: str) -> None:
        """
        Initialize a pip executor.

        Args:
            pip_path: Path to the pip executable.
        """
        self._pip_path = pip_path

    @classmethod
    def create(cls) -> PipExecutor:
        """
        Creates a pip executor from the pip system path.

        Returns:
            PipExecutor instance
        """
        pip_path = shutil.which("pip")
        if pip_path is None:
            raise cls.PipError("pip executable not found in PATH")

        return cls(pip_path)

    def __call__(self, *args: str) -> ExecutorResult:
        """
        Executes a pip command.

        Args:
            *args: Arguments to pip command.

        Returns:
            ExecutorResult instance
        """
        with subprocess.Popen(
            [self._pip_path, *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            output, error = process.communicate()
            self._handle_pip_error(process.returncode)

            return ExecutorResult(output.decode(), error.decode())

    def _handle_pip_error(self, return_code: int) -> None:
        """
        Handle pip installation errors by raising appropriate exceptions.

        Args:
            return_code: The return code from pip installation process

        Raises:
            PipelineWrapper.InstallError: If pip installation fails
        """
        if return_code != 0:
            raise self.PipError(
                f"pip install had a non-zero return code: {return_code}",
            )
