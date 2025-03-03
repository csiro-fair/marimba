from __future__ import annotations

import dataclasses
import shutil
import subprocess


@dataclasses.dataclass
class ExecutorResult:
    output: str
    error: str


class PipExecutor:
    class PipException(Exception):
        pass

    def __init__(self, pip_path: str):
        self._pip_path = pip_path

    @classmethod
    def create(cls) -> PipExecutor:
        pip_path = shutil.which("pip")
        if pip_path is None:
            raise Exception("pip executable not found in PATH")

        return cls(pip_path)

    def __call__(self, *args: str) -> ExecutorResult:
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
            raise self.PipException(
                f"pip install had a non-zero return code: {return_code}",
            )
