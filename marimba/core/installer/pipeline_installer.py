"""
Pipeline Installer.

This module provides a pipeline installer which handles the installation of pipeline dependencies.

Classes:
    PipelineInstaller: Installs pipeline dependencies defined in the requirements.txt or pyproject.toml.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from marimba.core.installer.uv_executor import ExecutorResult, UvExecutor

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable
    from pathlib import Path


class PipelineInstaller:
    """
    Installs pipeline dependencies defined in the requirements.txt or pyproject.toml.
    """

    REQUIREMENTS_TXT = "requirements.txt"
    PY_PROJECT = "pyproject.toml"

    class InstallError(Exception):
        """
        Raised when there is an error installing pipeline dependencies.
        """

    def __init__(
        self,
        pipeline_path: Path,
        logger: logging.Logger,
        uv_executor: Callable[..., ExecutorResult],
    ) -> None:
        """
        Initialize the pipeline installer.

        Args:
            pipeline_path: Path to the pipeline to install.
            logger: Logger to use.
            uv_executor: Uv executor to use.
        """
        self._pipeline_path = pipeline_path
        self._logger = logger
        self._executor = uv_executor

    @classmethod
    def create(cls, pipeline_path: Path, logger: logging.Logger) -> PipelineInstaller:
        """
        Creates a new pipeline installer with the uv executor.

        Args:
            pipeline_path: Path to the pipeline to install.
            logger: Logger to use.

        Returns:
            PipelineInstaller instance.
        """
        uv_executor = UvExecutor.create()
        return cls(pipeline_path, logger, uv_executor)

    @property
    def requirements_path(self) -> Path:
        """
        The path to the pipeline requirements file.
        """
        return self._pipeline_path / self.REQUIREMENTS_TXT

    @property
    def py_project_path(self) -> Path:
        """
        The path to the pipeline python project.
        """
        return self._pipeline_path / self.PY_PROJECT

    def __call__(self) -> None:
        """
        Install the pipeline dependencies as provided in a requirements.txt or pyproject.toml file, if present.

        Raises:
            PipelineWrapper.InstallError: If there is an error installing pipeline dependencies.
        """
        self._logger.info(
            f"Started installing pipeline dependencies from {self.requirements_path}",
        )

        try:
            self._install()

        except Exception as e:
            self._logger.exception(f"Error installing pipeline dependencies: {e}")
            raise PipelineInstaller.InstallError from e

    def _install(self) -> None:
        if self.requirements_path.is_file():
            abs_path = self.requirements_path.absolute()
            self._validate_exists(abs_path)

            result = self._executor("install", "-r", str(abs_path))
        elif self.py_project_path.is_file():
            abs_path = self.py_project_path.absolute()
            self._validate_exists(abs_path)

            result = self._executor("install", str(abs_path.parent))
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
            raise PipelineInstaller.InstallError(
                f"Requirements file not found: {requirements_path}",
            )
