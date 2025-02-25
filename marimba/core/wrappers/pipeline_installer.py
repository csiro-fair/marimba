import logging
import shutil
import subprocess

from pathlib import Path


class PipelineInstaller:
    REQUIREMENTS_TXT = 'requirements.txt'

    class InstallError(Exception):
        """
        Raised when there is an error installing pipeline dependencies.
        """

    def __init__(self, pipeline_path: Path, logger: logging.Logger):
        self._pipeline_path = pipeline_path
        self._logger = logger

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

            # Find and validate pip executable
            pip_path = self._validate_pip()

            with subprocess.Popen(
                [pip_path, "install", "--no-input", "-r", requirements_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as process:
                output, error = process.communicate()
                if output:
                    self._logger.debug(output.decode("utf-8"))
                if error:
                    self._logger.warning(error.decode("utf-8"))

                self._handle_pip_error(process.returncode)

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

    def _validate_pip(self) -> str:
        """
        Validate that pip is available in the system PATH.

        Returns:
            str: Path to pip executable

        Raises:
            PipelineWrapper.InstallError: If pip is not found
        """
        pip_path = shutil.which("pip")
        if pip_path is None:
            raise PipelineInstaller.InstallError("pip executable not found in PATH")
        return pip_path

    def _handle_pip_error(self, returncode: int) -> None:
        """
        Handle pip installation errors by raising appropriate exceptions.

        Args:
            returncode: The return code from pip installation process

        Raises:
            PipelineWrapper.InstallError: If pip installation fails
        """
        if returncode != 0:
            raise self.InstallError(
                f"pip install had a non-zero return code: {returncode}",
            )

