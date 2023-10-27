import logging
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

    def __init__(self, root_dir: Union[str, Path]):
        self._root_dir = Path(root_dir)
        self._file_handler = None

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
        return self.root_dir / "repository"

    @property
    def config_path(self) -> Path:
        """
        The path to the pipeline configuration file.
        """
        return self.root_dir / "pipeline.yml"

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
    def create(cls, root_dir: Union[str, Path], url: str):
        """
        Create a new pipeline directory from a remote git repository.

        Args:
            root_dir: The root directory of the pipeline.
            url: The URL of the pipeline implementation git repository.

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
        repo_dir = root_dir / "repository"
        Repo.clone_from(url, repo_dir)

        # Create the pipeline configuration file (initialize as empty)
        config_path = root_dir / "pipeline.yml"
        save_config(config_path, {})

        return cls(root_dir)

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

    def load_pipeline(self) -> BasePipeline:
        """
        Dynamically load an instance of the pipeline implementation.

        Injects the pipeline configuration and logger into the instance.

        Returns:
            The pipeline instance.

        Raises:
            FileNotFoundError: If the pipeline implementation file cannot be found, or if there are multiple pipeline implementation files.
            ImportError: If the pipeline implementation file cannot be imported.
        """
        # Find files that end with .pipeline.py in the repository
        pipeline_module_paths = list(self.repo_dir.glob("**/*.pipeline.py"))

        # Ensure there is one result
        if len(pipeline_module_paths) == 0:
            raise FileNotFoundError(f'No pipeline implementation found in "{self.repo_dir}".')
        elif len(pipeline_module_paths) > 1:
            raise FileNotFoundError(f'Multiple pipeline implementations found in "{self.repo_dir}": {pipeline_module_paths}.')
        pipeline_module_path = pipeline_module_paths[0]

        pipeline_module_spec = spec_from_file_location("pipeline", str(pipeline_module_path.absolute()))

        # Load the pipeline module
        pipeline_module = module_from_spec(pipeline_module_spec)
        pipeline_module_spec.loader.exec_module(pipeline_module)

        # Find any BasePipeline implementations
        for _, obj in pipeline_module.__dict__.items():
            if isinstance(obj, type) and issubclass(obj, BasePipeline) and obj is not BasePipeline:
                # Create an instance of the pipeline
                pipeline_instance = obj(config=self.load_config(), dry_run=False)

                # Set up pipeline file logging
                pipeline_instance.logger.addHandler(self._file_handler)

                return pipeline_instance

    def update(self):
        """
        Update the pipeline repository by issuing a git pull.
        """
        repo = Repo(self.repo_dir)
        repo.remotes.origin.pull()
