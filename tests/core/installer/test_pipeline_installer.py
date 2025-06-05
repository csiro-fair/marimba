import logging
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from marimba.core.installer.uv_executor import ExecutorResult, UvExecutor
from marimba.core.installer.pipeline_installer import PipelineInstaller


@pytest.fixture()
def temp_dir():
    temporary_directory = tempfile.TemporaryDirectory()
    yield Path(temporary_directory.name)
    temporary_directory.cleanup()


@pytest.fixture()
def temp_dir_with_requirements(temp_dir: Path) -> Generator[Path, None, None]:
    requirements = "\n"
    with open(temp_dir / "requirements.txt", "w") as requirements_file:
        requirements_file.write(requirements)

    yield temp_dir


def test_installer_valid(caplog: pytest.LogCaptureFixture, temp_dir_with_requirements: Path) -> None:
    logger = logging.Logger("test")

    def mock_executor(*args: str) -> ExecutorResult:
        assert args == (
            "install",
            "-r",
            str(temp_dir_with_requirements / "requirements.txt"),
        )
        return ExecutorResult("", "")

    installer = PipelineInstaller(temp_dir_with_requirements, logger, mock_executor)

    installer()


def test_installer_missing_requirements_file(caplog: pytest.LogCaptureFixture, temp_dir: Path) -> None:
    logger = logging.Logger("test")

    def mock_executor(*args: str) -> ExecutorResult:
        return ExecutorResult("", "")

    installer = PipelineInstaller(temp_dir, logger, mock_executor)

    with pytest.raises(PipelineInstaller.InstallError):
        installer()


def test_installer_executor_error(caplog: pytest.LogCaptureFixture, temp_dir_with_requirements: Path) -> None:
    logger = logging.Logger("test")

    def mock_executor(*args: str) -> ExecutorResult:
        raise UvExecutor.UvError("")

    installer = PipelineInstaller(temp_dir_with_requirements, logger, mock_executor)

    with pytest.raises(PipelineInstaller.InstallError):
        installer()
