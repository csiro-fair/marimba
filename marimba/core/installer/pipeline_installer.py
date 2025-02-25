import logging

from pathlib import Path
from typing import Callable

from marimba.core.installer.pip_executor import ExecutorResult, PipExecutor


class PipelineInstaller:
    REQUIREMENTS_TXT = "requirements.txt"

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
        return self._pipeline_path / "requirements.txt"

    def __call__(self):
        """
        Install the pipeline dependencies as provided in a requirements.txt file, if present.

        Raises:
            PipelineWrapper.InstallError: If there is an error installing pipeline dependencies.
        """
        if not self.requirements_path.is_file():
            self._logger.exception(f"Requirements file does not exist: {self.requirements_path}")
            raise PipelineInstaller.InstallError(f"Requirements file does not exist: {self.requirements_path}")

        self._logger.info(f"Started installing pipeline dependencies from {self.requirements_path}")
        try:
            # Ensure the requirements path is an absolute path and exists
            requirements_path = str(self.requirements_path.absolute())
            self._validate_requirements(requirements_path)

            result = self._executor("install", "--no-input", "-r", requirements_path)
            if result.output:
                self._logger.debug(result.output)
            if result.error:
                self._logger.warning(result.error)

            self._logger.info("Pipeline dependencies installed")
        except Exception as e:
            self._logger.exception(f"Error installing pipeline dependencies: {e}")
            raise PipelineInstaller.InstallError from e

    def _validate_requirements(self, requirements_path: str) -> None:
        """
        Validate that the requirements file exists and is accessible.

        Args:
            requirements_path: Path to the requirements.txt file

        Raises:
            PipelineWrapper.InstallError: If requirements file is not found
        """
        if not Path(requirements_path).is_file():
            raise PipelineInstaller.InstallError(f"Requirements file not found: {requirements_path}")
