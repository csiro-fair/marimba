import logging

from pathlib import Path
from typing import Callable

from marimba.core.installer.pip_executor import ExecutorResult, PipExecutor


class PipelineInstaller:
    REQUIREMENTS_TXT = "requirements.txt"
    PY_PROJECT = "pyproject.toml"

    class InstallError(Exception):
        """
        Raised when there is an error installing pipeline dependencies.
        """

    def __init__(self, pipeline_path: Path, logger: logging.Logger, pip_executor: Callable[..., ExecutorResult]):
        self._pipeline_path = pipeline_path
        self._logger = logger
        self._executor = pip_executor

    @classmethod
    def create(cls, pipeline_path: Path, logger: logging.Logger):
        pip_executor = PipExecutor.create()
        return cls(pipeline_path, logger, pip_executor)

    @property
    def requirements_path(self) -> Path:
        """
        The path to the pipeline requirements file.
        """
        return self._pipeline_path / self.REQUIREMENTS_TXT

    @property
    def py_project_path(self):
        """
        The path to the pipeline python project.
        """
        return self._pipeline_path / self.PY_PROJECT

    def __call__(self):
        """
        Install the pipeline dependencies as provided in a requirements.txt file, if present.

        Raises:
            PipelineWrapper.InstallError: If there is an error installing pipeline dependencies.
        """
        self._logger.info(f"Started installing pipeline dependencies from {self.requirements_path}")

        try:
            self._install()

        except Exception as e:
            self._logger.exception(f"Error installing pipeline dependencies: {e}")
            raise PipelineInstaller.InstallError from e

    def _install(self):
        if self.requirements_path.is_file():
            abs_path = self.requirements_path.absolute()
            self._validate_exists(abs_path)

            result = self._executor("install", "--no-input", "-r", str(abs_path))
        elif self.py_project_path.is_file():
            abs_path = self.py_project_path.absolute()
            self._validate_exists(abs_path)

            result = self._executor("install", "--no-input", str(abs_path.parent))
        else:
            error_msg = f"Pipeline does not defines dependencies: {self.requirements_path} / {self.py_project_path}"
            self._logger.exception(error_msg)
            raise PipelineInstaller.InstallError(error_msg)

        if result.output:
            self._logger.debug(result.output)
        if result.error:
            self._logger.warning(result.error)

        self._logger.info("Pipeline dependencies installed")

    @staticmethod
    def _validate_exists(requirements_path: Path) -> None:
        """
        Validate that the requirements file exists and is accessible.

        Args:
            requirements_path: Path to the requirements.txt file

        Raises:
            PipelineWrapper.InstallError: If requirements file is not found
        """
        if not requirements_path.is_file():
            raise PipelineInstaller.InstallError(f"Requirements file not found: {requirements_path}")
