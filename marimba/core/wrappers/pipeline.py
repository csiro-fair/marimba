"""
Marimba Core Pipeline Wrapper Module.

This module provides the PipelineWrapper class for managing pipeline directories, including creation, configuration,
dependency installation, and instance management.

The PipelineWrapper class allows for creating a new pipeline directory from a remote git repository, loading and
saving pipeline configurations, and retrieving instances of the pipeline implementation. It also provides functionality
for updating the pipeline repository and installing pipeline dependencies.

Imports:
    - logging: Python logging module for logging messages.
    - shutil: High-level operations on files and collections of files.
    - subprocess: Subprocess management module for running external commands.
    - sys: System-specific parameters and functions.
    - importlib.util: Utility functions for importing modules.
    - pathlib.Path: Object-oriented filesystem paths.
    - typing: Support for type hints.
    - git.Repo: GitPython library for interacting with Git repositories.
    - marimba.core.pipeline.BasePipeline: Base class for pipeline implementations.
    - marimba.core.utils.config: Utility functions for loading and saving configuration files.
    - marimba.core.utils.log.LogMixin: Mixin class for adding logging functionality.
    - marimba.core.utils.log.get_file_handler: Function for creating a file handler for logging.

Classes:
    - PipelineWrapper: Pipeline directory wrapper class for managing pipeline directories.
        - InvalidStructureError: Exception raised when the project file structure is invalid.
        - InstallError: Exception raised when there is an error installing pipeline dependencies.
"""

import shutil
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import TYPE_CHECKING, Any

from git import Repo

from marimba.core.parallel.pipeline_loader import load_pipeline_instance
from marimba.core.pipeline import BasePipeline
from marimba.core.utils.config import load_config, save_config
from marimba.core.utils.log import LogMixin, get_file_handler
from marimba.core.utils.prompt import prompt_schema

# Type-checking imports
if TYPE_CHECKING:
    import logging


class PipelineWrapper(LogMixin):
    """
    Pipeline directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the project file structure is invalid.
        """

    class InstallError(Exception):
        """
        Raised when there is an error installing pipeline dependencies.
        """

    def __init__(self, root_dir: str | Path, *, dry_run: bool = False) -> None:
        """
        Initialise the class instance.

        Args:
            root_dir: A string or Path object representing the root directory.
            dry_run: A boolean indicating whether the method should be executed in a dry run mode.

        """
        self._root_dir = Path(root_dir)
        self._dry_run = dry_run

        self._file_handler: logging.FileHandler | None = None
        self._pipeline_class: type[BasePipeline] | None = None

        self._check_file_structure()
        self._setup_logging()

    @property
    def root_dir(self) -> Path:
        """
        The root directory of the pipeline.
        """
        return self._root_dir

    @property
    def repo_dir(self) -> Path:
        """
        The repository directory of the pipeline.
        """
        return self.root_dir / "repo"

    @property
    def config_path(self) -> Path:
        """
        The path to the pipeline configuration file.
        """
        return self.root_dir / "pipeline.yml"

    @property
    def requirements_path(self) -> Path:
        """
        The path to the pipeline requirements file.
        """
        return self.repo_dir / "requirements.txt"

    @property
    def log_path(self) -> Path:
        """
        The path to the project log file.
        """
        return self._root_dir / f"{self.name}.log"

    @property
    def name(self) -> str:
        """
        The name of the pipeline.
        """
        return self.root_dir.name

    @property
    def dry_run(self) -> bool:
        """
        Whether the pipeline should run in dry-run mode.
        """
        return self._dry_run

    def _check_file_structure(self) -> None:
        """
        Check that the pipeline file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            PipelineDirectory.InvalidStructureError: If the pipeline file structure is invalid.

        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise PipelineWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path) -> None:
            if not path.is_file():
                raise PipelineWrapper.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.repo_dir)
        check_file_exists(self.config_path)

    def _setup_logging(self) -> None:
        """
        Set up logging. Create file handler for this instance that writes to `pipeline.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.root_dir, self.name, self._dry_run)

        # Add the file handler to the logger
        self.logger.addHandler(self._file_handler)

    @classmethod
    def create(cls, root_dir: str | Path, url: str, *, dry_run: bool = False) -> "PipelineWrapper":
        """
        Create a new pipeline directory from a remote git repository.

        Args:
            root_dir: The root directory of the pipeline.
            url: The URL of the pipeline implementation git repository.
            dry_run: Whether to run in dry-run mode.

        Raises:
            FileExistsError: If the pipeline root directory already exists.

        """
        root_dir = Path(root_dir)

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'Pipeline root directory "{root_dir}" already exists.')

        # Create the pipeline root directory
        root_dir.mkdir(parents=True)

        # Clone the pipeline repository
        repo_dir = root_dir / "repo"
        Repo.clone_from(url, repo_dir)

        # Create the pipeline configuration file (initialize as empty)
        config_path = root_dir / "pipeline.yml"
        save_config(config_path, {})

        return cls(root_dir, dry_run=dry_run)

    def load_config(self) -> dict[str, Any]:
        """
        Load the pipeline configuration.

        Returns:
            The pipeline configuration.

        """
        return load_config(self.config_path)

    def save_config(self, config: dict[Any, Any] | None) -> None:
        """
        Save the pipeline configuration.

        Args:
            config: The pipeline configuration.

        """
        if config:
            save_config(self.config_path, config)

    def get_instance(self) -> BasePipeline:
        """
        Get the pipeline instance.

        This method loads the pipeline instance using the standalone function `load_pipeline_instance()`.
        The pipeline instance is then set up for file logging if the `_file_handler` is initialized.

        Returns:
            BasePipeline: The pipeline instance.

        """
        # Use the standalone function to load the pipeline instance
        return load_pipeline_instance(
            self.root_dir,
            self.repo_dir,
            self.name,
            self.config_path,
            self.dry_run,
        )

    def get_pipeline_class(self) -> type[BasePipeline] | None:
        """
        Get the pipeline class.

        Lazy-loaded and cached. Automatically scans the repository for a pipeline implementation.

        Returns:
            The pipeline class.

        Raises:
            FileNotFoundError:
                If the pipeline implementation file cannot be found, or if there are multiple pipeline implementation
                files.
            ImportError: If the pipeline implementation file cannot be imported.

        """
        if self._pipeline_class is None:
            # Find files that end with .pipeline.py in the repository
            pipeline_module_paths = list(self.repo_dir.glob("**/*.pipeline.py"))

            # Ensure there is one result
            if len(pipeline_module_paths) == 0:
                raise FileNotFoundError(f'No pipeline implementation found in "{self.repo_dir}".')

            if len(pipeline_module_paths) > 1:
                raise FileNotFoundError(
                    f'Multiple pipeline implementations found in "{self.repo_dir}": {pipeline_module_paths}.',
                )
            pipeline_module_path = pipeline_module_paths[0]

            pipeline_module_name = pipeline_module_path.stem
            pipeline_module_spec = spec_from_file_location(
                pipeline_module_name,
                str(pipeline_module_path.absolute()),
            )

            if pipeline_module_spec is None:
                raise ImportError(f"Could not load spec for {pipeline_module_name} from {pipeline_module_path}")

            # Create the pipeline module
            pipeline_module = module_from_spec(pipeline_module_spec)

            # Enable repo-relative imports by temporarily adding the repository directory to the module search path
            sys.path.insert(0, str(self.repo_dir.absolute()))

            # Ensure that loader is not None before executing the module
            if pipeline_module_spec.loader is None:
                raise ImportError(f"Could not find loader for {pipeline_module_name} from {pipeline_module_path}")

            # Execute it
            pipeline_module_spec.loader.exec_module(pipeline_module)

            # Remove the repository directory from the module search path to avoid conflicts
            sys.path.pop(0)

            # Find any BasePipeline implementations
            for obj in pipeline_module.__dict__.values():
                if isinstance(obj, type) and issubclass(obj, BasePipeline) and obj is not BasePipeline:
                    self._pipeline_class = obj
                    break

        return self._pipeline_class

    def prompt_pipeline_config(
        self,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Prompt for and process pipeline configuration.

        This function prompts for pipeline configuration based on the provided schema and any existing configuration.
        It merges the existing configuration (if provided) with additional user input for missing configuration items.

        Args:
            config (Optional[Dict]): Existing pipeline configuration. If provided, it will be used to pre-fill
            the configuration dictionary.

        Returns:
            Dict[Any, Any]: A dictionary containing the final pipeline configuration after merging existing config
            and user input.

        Raises:
            ValueError: If the pipeline instance or configuration schema cannot be retrieved.
            TypeError: If the provided config is not a dictionary.

        """
        pipeline = self.get_instance()
        pipeline_config_schema = pipeline.get_pipeline_config_schema()

        # Prepopulate collection_config with values from provided config
        pipeline_config = {}
        if config:
            for key in list(pipeline_config_schema.keys()):
                if key in config:
                    pipeline_config[key] = config[key]
                    del pipeline_config_schema[key]  # Remove the key so it won't be prompted

        # Prompt from the remaining resolved schema
        if pipeline_config_schema:
            additional_config = prompt_schema(pipeline_config_schema)
            if additional_config:
                pipeline_config.update(additional_config)

        self.logger.debug(f"Final prompted pipeline config: {pipeline_config}")

        return pipeline_config

    def update(self) -> None:
        """
        Update the pipeline repository by issuing a git pull.
        """
        repo = Repo(self.repo_dir)
        repo.remotes.origin.pull()

    def _handle_pip_error(self, returncode: int) -> None:
        """
        Handle pip installation errors by raising appropriate exceptions.

        Args:
            returncode: The return code from pip installation process

        Raises:
            PipelineWrapper.InstallError: If pip installation fails
        """
        if returncode != 0:
            raise PipelineWrapper.InstallError(
                f"pip install had a non-zero return code: {returncode}",
            )

    def _validate_requirements(self, requirements_path: str) -> None:
        """
        Validate that the requirements file exists and is accessible.

        Args:
            requirements_path: Path to the requirements.txt file

        Raises:
            PipelineWrapper.InstallError: If requirements file is not found
        """
        if not Path(requirements_path).is_file():
            raise PipelineWrapper.InstallError(f"Requirements file not found: {requirements_path}")

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
            raise PipelineWrapper.InstallError("pip executable not found in PATH")
        return pip_path

    def install(self) -> None:
        """
        Install the pipeline dependencies as provided in a requirements.txt file, if present.

        Raises:
            PipelineWrapper.InstallError: If there is an error installing pipeline dependencies.
        """
        if not self.requirements_path.is_file():
            self.logger.exception(f"Requirements file does not exist: {self.requirements_path}")
            raise PipelineWrapper.InstallError(f"Requirements file does not exist: {self.requirements_path}")

        self.logger.info(f"Installing pipeline dependencies from {self.requirements_path}...")
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
                    self.logger.debug(output.decode("utf-8"))
                if error:
                    self.logger.warning(error.decode("utf-8"))

                self._handle_pip_error(process.returncode)

            self.logger.info("Pipeline dependencies installed successfully.")
        except Exception as e:
            self.logger.exception(f"Error installing pipeline dependencies: {e}")
            raise PipelineWrapper.InstallError from e
