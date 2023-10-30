import logging
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Union

from git import Repo

from marimba.core.pipeline import BasePipeline
from marimba.utils.config import load_config, save_config
from marimba.utils.log import LogMixin, get_file_handler


class PipelineWrapper(LogMixin):
    """
    Pipeline directory wrapper.
    """

    class InvalidStructureError(Exception):
        """
        Raised when the project file structure is invalid.
        """

        pass

    class InstallError(Exception):
        """
        Raised when there is an error installing pipeline dependencies.
        """

        pass

    def __init__(self, root_dir: Union[str, Path], dry_run: bool = False):
        self._root_dir = Path(root_dir)
        self._dry_run = dry_run

        self._file_handler = None
        self._pipeline_class = None

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
    def dry_run(self) -> bool:
        """
        Whether the pipeline should run in dry-run mode.
        """
        return self._dry_run

    def _check_file_structure(self):
        """
        Check that the pipeline file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            PipelineDirectory.InvalidStructureError: If the pipeline file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise PipelineWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        def check_file_exists(path: Path):
            if not path.is_file():
                raise PipelineWrapper.InvalidStructureError(f'"{path}" does not exist or is not a file.')

        check_dir_exists(self.root_dir)
        check_dir_exists(self.repo_dir)
        check_file_exists(self.config_path)

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `pipeline.log`.
        """
        # Create a file handler for this instance
        self._file_handler = get_file_handler(self.root_dir, "pipeline", False, level=logging.DEBUG)

        # Add the file handler to the logger
        self.logger.addHandler(self._file_handler)

    @classmethod
    def create(cls, root_dir: Union[str, Path], url: str, dry_run: bool = False):
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

    def load_config(self) -> dict:
        """
        Load the pipeline configuration.

        Returns:
            The pipeline configuration.
        """
        return load_config(self.config_path)

    def save_config(self, config: dict):
        """
        Save the pipeline configuration.

        Args:
            config: The pipeline configuration.
        """
        save_config(self.config_path, config)

    def get_instance(self) -> BasePipeline:
        """
        Get an instance of the pipeline implementation.

        Injects the pipeline configuration and logger into the instance.

        Returns:
            The pipeline instance.

        Raises:
            FileNotFoundError: If the pipeline implementation file cannot be found, or if there are multiple pipeline implementation files.
            ImportError: If the pipeline implementation file cannot be imported.
        """
        # Get the pipeline class
        pipeline_class = self.get_pipeline_class()

        # Create an instance of the pipeline
        pipeline_instance = pipeline_class(self.repo_dir, config=self.load_config(), dry_run=self.dry_run)

        # Set up pipeline file logging
        pipeline_instance.logger.addHandler(self._file_handler)

        return pipeline_instance

    def get_pipeline_class(self) -> BasePipeline:
        """
        Get the pipeline class. Lazy-loaded and cached. Automatically scans the repository for a pipeline implementation.

        Returns:
            The pipeline class.

        Raises:
            FileNotFoundError: If the pipeline implementation file cannot be found, or if there are multiple pipeline implementation files.
            ImportError: If the pipeline implementation file cannot be imported.
        """
        if self._pipeline_class is None:
            # Find files that end with .pipeline.py in the repository
            pipeline_module_paths = list(self.repo_dir.glob("**/*.pipeline.py"))

            # Ensure there is one result
            if len(pipeline_module_paths) == 0:
                raise FileNotFoundError(f'No pipeline implementation found in "{self.repo_dir}".')
            elif len(pipeline_module_paths) > 1:
                raise FileNotFoundError(f'Multiple pipeline implementations found in "{self.repo_dir}": {pipeline_module_paths}.')
            pipeline_module_path = pipeline_module_paths[0]

            pipeline_module_name = pipeline_module_path.stem
            pipeline_module_spec = spec_from_file_location(
                pipeline_module_name,
                str(pipeline_module_path.absolute()),
            )

            # Create the pipeline module
            pipeline_module = module_from_spec(pipeline_module_spec)

            # Enable repo-relative imports by temporarily adding the repository directory to the module search path
            sys.path.insert(0, str(self.repo_dir.absolute()))

            # Execute it
            pipeline_module_spec.loader.exec_module(pipeline_module)

            # Remove the repository directory from the module search path to avoid conflicts
            sys.path.pop(0)

            # Find any BasePipeline implementations
            for _, obj in pipeline_module.__dict__.items():
                if isinstance(obj, type) and issubclass(obj, BasePipeline) and obj is not BasePipeline:
                    self._pipeline_class = obj
                    break
        return self._pipeline_class

    def update(self):
        """
        Update the pipeline repository by issuing a git pull.
        """
        repo = Repo(self.repo_dir)
        repo.remotes.origin.pull()

    def install(self):
        """
        Install the pipeline dependencies as provided in a requirements.txt file, if present.

        Raises:
            PipelineWrapper.InstallError: If there is an error installing pipeline dependencies.
        """
        if self.requirements_path.is_file():
            self.logger.info(f"Installing pipeline dependencies from {self.requirements_path}...")
            try:
                process = subprocess.Popen(
                    ["pip", "install", "--no-input", "-r", str(self.requirements_path.absolute())], stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                output, error = process.communicate()
                if output:
                    self.logger.debug(output.decode("utf-8"))
                if error:
                    self.logger.warning(error.decode("utf-8"))

                if process.returncode != 0:
                    raise Exception(f"pip install had a non-zero return code: {process.returncode}")

                self.logger.info("Pipeline dependencies installed successfully.")
            except Exception as e:
                self.logger.error(f"Error installing pipeline dependencies: {e}")
                raise PipelineWrapper.InstallError from e
