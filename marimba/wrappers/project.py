import ast
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ifdo.models import ImageData

from marimba.utils.log import LogMixin, get_file_handler, get_logger
from marimba.utils.prompt import prompt_schema
from marimba.wrappers.dataset import DatasetWrapper
from marimba.wrappers.deployment import DeploymentWrapper
from marimba.wrappers.pipeline import PipelineWrapper

logger = get_logger(__name__)


def get_merged_keyword_args(kwargs: dict, extra_args: list, logger: logging.Logger) -> dict:
    """
    Merge any extra key-value arguments with other keyword arguments.

    Args:
        kwargs: The keyword arguments to merge with.
        extra_args: A list of extra key-value arguments to merge.
        logger: A logger object to log any warnings.

    Returns:
        A dictionary containing the merged keyword arguments.
    """
    extra_dict = {}
    if extra_args:
        for arg in extra_args:
            # Attempt to split the argument into a key and a value
            parts = arg.split("=")
            if len(parts) == 2:
                key, value_str = parts
                try:
                    # Convert the string value to its corresponding data type
                    value = ast.literal_eval(value_str)
                except (ValueError, SyntaxError):
                    # If evaluation fails, keep the original string value
                    value = value_str
                    logger.warning(f'Could not evaluate extra argument value: "{value_str}"')
                extra_dict[key] = value
            else:
                logger.warning(f'Invalid extra argument provided: "{arg}"')

    return {**kwargs, **extra_dict}


class ProjectWrapper(LogMixin):
    """
    Marimba project directory wrapper. Provides methods for interacting with the project.

    To create a new project, use the `create` method:
    ```python
    project_wrapper = ProjectWrapper.create("my_project")
    ```

    To wrap an existing project, use the constructor:
    ```python
    project_wrapper = ProjectWrapper("my_project")
    ```
    """

    class InvalidStructureError(Exception):
        """
        Raised when the project file structure is invalid.
        """

        pass

    class CreatePipelineError(Exception):
        """
        Raised when a pipeline cannot be created.
        """

        pass

    class CreateDeploymentError(Exception):
        """
        Raised when a deployment cannot be created.
        """

        pass

    class RunCommandError(Exception):
        """
        Raised when a command cannot be run.
        """

        pass

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline does not exist in the project.
        """

        pass

    class NoSuchDeploymentError(Exception):
        """
        Raised when a deployment does not exist in the project.
        """

        pass

    class NameError(Exception):
        """
        Raised when an invalid name is used.
        """

        pass

    def __init__(self, root_dir: Union[str, Path]):
        super().__init__()

        self._root_dir = Path(root_dir)

        self._pipeline_dir = self._root_dir / "pipelines"
        self._deployments_dir = self._root_dir / "deployments"
        self._marimba_dir = self._root_dir / ".marimba"

        self._pipeline_wrappers = {}  # pipeline name -> PipelineWrapper instance
        self._deployment_wrappers = {}  # deployment name -> DeploymentWrapper instance

        self._check_file_structure()
        self._setup_logging()

        self._load_pipelines()
        self._load_deployments()

    @classmethod
    def create(cls, root_dir: Union[str, Path]) -> "ProjectWrapper":
        """
        Create a project from a root directory.

        Args:
            root_dir: The root directory of the project.

        Returns:
            A project.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the folder structure
        root_dir = Path(root_dir)
        pipeline_dir = root_dir / "pipelines"
        deployments_dir = root_dir / "deployments"
        marimba_dir = root_dir / ".marimba"

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'"{root_dir}" already exists.')

        # Create the folder structure
        root_dir.mkdir(parents=True)
        pipeline_dir.mkdir()
        deployments_dir.mkdir()
        marimba_dir.mkdir()

        return cls(root_dir)

    def _check_file_structure(self):
        """
        Check that the project file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            ProjectWrapper.InvalidStructureError: If the project file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise ProjectWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self._root_dir)
        check_dir_exists(self._pipeline_dir)
        check_dir_exists(self._deployments_dir)
        check_dir_exists(self._marimba_dir)

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `project.log`.
        """
        # Create a file handler for this instance
        file_handler = get_file_handler(self.root_dir, self.name, False, level=logging.DEBUG)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def _load_pipelines(self):
        """
        Load pipeline wrappers from the `pipelines` directory.

        Populates the `_pipeline_wrappers` dictionary with `PipelineWrapper` instances.

        Raises:
            PipelineWrapper.InvalidStructureError: If the pipeline directory structure is invalid.
        """
        pipeline_dirs = filter(lambda p: p.is_dir(), self.pipeline_dir.iterdir())

        self._pipeline_wrappers.clear()
        for pipeline_dir in pipeline_dirs:
            self._pipeline_wrappers[pipeline_dir.name] = PipelineWrapper(pipeline_dir)

    def _load_deployments(self):
        """
        Load deployment instances from the `deployments` directory.

        Populates the `_deployment_wrappers` dictionary with `DeploymentWrapper` instances.

        Raises:
            Deployment.InvalidStructureError: If the deployment directory structure is invalid.
        """

        deployment_dirs = filter(lambda p: p.is_dir(), self._deployments_dir.iterdir())

        self._deployment_wrappers.clear()
        for deployment_dir in deployment_dirs:
            self._deployment_wrappers[deployment_dir.name] = DeploymentWrapper(deployment_dir)

    def create_pipeline(self, name: str, url: str) -> PipelineWrapper:
        """
        Create a new pipeline.

        Args:
            name: The name of the pipeline.
            url: URL of the pipeline git repository.

        Returns:
            The pipeline directory wrapper.

        Raises:
            ProjectWrapper.CreatePipelineError: If the pipeline cannot be created.
            ProjectWrapper.NameError: If the name is invalid.
        """
        self.logger.debug(f'Creating pipeline "{name}" from {url}')

        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Check that a pipeline with the same name doesn't already exist
        pipeline_dir = self._pipeline_dir / name
        if pipeline_dir.exists():
            raise ProjectWrapper.CreatePipelineError(f'A pipeline with the name "{name}" already exists.')

        # Create the pipeline directory
        pipeline_wrapper = PipelineWrapper.create(pipeline_dir, url)

        # Add the pipeline to the project
        self._pipeline_wrappers[name] = pipeline_wrapper

        return pipeline_wrapper

    def create_deployment(self, name: str, config: Dict[str, Any]) -> DeploymentWrapper:
        """
        Create a new deployment.

        Args:
            name: The name of the deployment.
            config: The deployment configuration.

        Returns:
            The deployment directory wrapper.

        Raises:
            ProjectWrapper.CreateDeploymentError: If the deployment cannot be created.
        """
        self.logger.debug(f'Creating deployment "{name}"')

        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Check that a deployment with the same name doesn't already exist
        deployment_dir = self.deployments_dir / name
        if deployment_dir.exists():
            raise ProjectWrapper.CreateDeploymentError(f'A deployment with the name "{name}" already exists.')

        # Create the deployment directory
        deployment_wrapper = DeploymentWrapper.create(deployment_dir, config)

        # Create the pipeline data directories
        for pipeline_name in self._pipeline_wrappers:
            deployment_wrapper.get_pipeline_data_dir(pipeline_name).mkdir()

        # Add the deployment to the project
        self._deployment_wrappers[name] = deployment_wrapper

        return deployment_wrapper

    def run_command(
        self,
        command_name: str,
        pipeline_name: Optional[str] = None,
        deployment_name: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        **kwargs: dict,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Run a command within the project.

        By default, this will run the command for all pipelines and deployments in the project.
        If a pipeline name is provided, it will run the command for all deployments of that pipeline.
        If a deployment name is provided, it will run the command for that deployment only.
        These can be combined to run the command for a specific deployment of a specific pipeline.

        Args:
            command_name: The name of the command to run.
            pipeline_name: The name of the pipeline to run the command for.
            deployment_name: The name of the deployment to run the command for.
            extra_args: Any extra arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Returns:
            A dictionary containing the results of the command for each deployment: {deployment_name: {pipeline_name: result}}.

        Raises:
            ProjectWrapper.RunCommandError: If the command cannot be run.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        if pipeline_name is not None:
            pipeline_wrapper = self._pipeline_wrappers.get(pipeline_name, None)
            if pipeline_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Pipeline "{pipeline_name}" does not exist within the project.')

        if deployment_name is not None:
            deployment_wrapper = self._deployment_wrappers.get(deployment_name, None)
            if deployment_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Deployment "{deployment_name}" does not exist within the project.')

        # Select the pipelines and deployments to run the command for
        pipeline_wrappers_to_run = {pipeline_name: pipeline_wrapper} if pipeline_name is not None else self._pipeline_wrappers
        deployment_wrappers_to_run = {deployment_name: deployment_wrapper} if deployment_name is not None else self._deployment_wrappers

        # Load pipeline instances
        pipeline_to_run = {pipeline_name: pipeline_wrapper.get_instance() for pipeline_name, pipeline_wrapper in pipeline_wrappers_to_run.items()}

        # Check that the command exists for all pipelines
        for run_pipeline_name, run_pipeline in pipeline_to_run.items():
            if not hasattr(run_pipeline, command_name):
                raise ProjectWrapper.RunCommandError(f'Command "{command_name}" does not exist for pipeline "{run_pipeline_name}".')

        # Invoke the command for each pipeline and deployment
        results_by_deployment = {}
        for run_deployment_name, run_deployment_wrapper in deployment_wrappers_to_run.items():
            results_by_pipeline = {}
            for run_pipeline_name, run_pipeline in pipeline_to_run.items():
                # Get the pipeline-specific data directory and config
                pipeline_deployment_data_dir = run_deployment_wrapper.get_pipeline_data_dir(run_pipeline_name)
                pipeline_deployment_config = run_deployment_wrapper.load_config()

                # Call the method
                method = getattr(run_pipeline, command_name)
                results_by_pipeline[run_pipeline_name] = method(pipeline_deployment_data_dir, pipeline_deployment_config, **merged_kwargs)

            results_by_deployment[run_deployment_name] = results_by_pipeline

        return results_by_deployment

    def compose(
        self, deployment_names: List[str], extra_args: Optional[List[str]] = None, **kwargs: dict
    ) -> Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]]:
        """
        Compose a dataset for the given deployments across all pipelines.

        Args:
            deployment_names: The names of the deployments to compose.
            extra_args: Any extra CLI arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Returns:
            A dict mapping pipeline name -> { output file path -> (input file path, image data) }

        Raises:
            ProjectWrapper.NoSuchDeploymentError: If a deployment does not exist in the project.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        # Get the deployment wrappers
        deployment_wrappers: List[DeploymentWrapper] = []
        for deployment_name in deployment_names:
            deployment_wrapper = self.deployment_wrappers.get(deployment_name, None)
            if deployment_wrapper is None:
                raise ProjectWrapper.NoSuchDeploymentError(deployment_name)
            deployment_wrappers.append(deployment_wrapper)

        # Load the deployment configs
        deployment_configs = [deployment_wrapper.load_config() for deployment_wrapper in deployment_wrappers]

        dataset_mapping = {}
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            # Get the pipeline instance
            pipeline = pipeline_wrapper.get_instance()

            # Get the deployment data directories for the pipeline
            deployment_data_dirs = [deployment_wrapper.get_pipeline_data_dir(pipeline_name) for deployment_wrapper in deployment_wrappers]

            # Compose the pipeline data mapping
            pipeline_data_mapping = pipeline.run_compose(deployment_data_dirs, deployment_configs, **merged_kwargs)

            # Add the pipeline data mapping to the dataset mapping
            dataset_mapping[pipeline_name] = pipeline_data_mapping

        return dataset_mapping

    def package(self, name: str, dataset_mapping: Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]], copy: bool = True) -> DatasetWrapper:
        """
        Create a Marimba dataset from a dataset mapping.

        Args:
            name: The name of the dataset.
            dataset_mapping: The dataset mapping to package.
            copy: Whether to copy the files (True) or move them (False).

        Returns:
            A dataset wrapper instance for the created dataset.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
            FileExistsError: If the dataset root directory already exists.
            DatasetWrapper.InvalidPathMappingError: If the path mapping is invalid.
        """
        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Create the dataset
        dataset_root_dir = self.distribution_dir / name
        dataset_wrapper = DatasetWrapper.create(dataset_root_dir)

        # Populate it
        dataset_wrapper.populate(name, dataset_mapping, copy=copy)

        return dataset_wrapper

    def run_import(
        self, deployment_name: str, source_paths: Iterable[Union[str, Path]], extra_args: Optional[List[str]] = None, **kwargs: dict
    ) -> None:
        """
        Run the import command to populate a deployment from a source data directory.

        May overwrite existing data in the deployment.

        Args:
            deployment_name: The name of the deployment to import into.
            source_paths: The source paths to import from.
            extra_args: Any extra CLI arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Raises:
            ProjectWrapper.NoSuchDeploymentError: If the deployment does not exist in the project.
        """
        source_paths = list(map(lambda p: Path(p), source_paths))

        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        # Get the deployment wrapper
        deployment_wrapper = self.deployment_wrappers.get(deployment_name, None)
        if deployment_wrapper is None:
            raise ProjectWrapper.NoSuchDeploymentError(deployment_name)

        # Import each pipeline
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            # Get the pipeline instance
            pipeline = pipeline_wrapper.get_instance()

            # Get the deployment data directory
            deployment_data_dir = deployment_wrapper.get_pipeline_data_dir(pipeline_name)

            # Load the deployment config
            deployment_config = deployment_wrapper.load_config()

            # Run the import
            pipeline.run_import(deployment_data_dir, source_paths, deployment_config, **merged_kwargs)

    def prompt_deployment_config(self, parent_deployment_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Prompt the user for a deployment configuration.

        The schema will be generated from the pipeline-specific deployment config schemas of all pipelines in the project, as well as the deployment config of the parent deployment if specified.

        Args:
            parent_deployment_name: The name of the parent deployment. If unspecified, use the last deployment by modification time.

        Returns:
            The deployment configuration as a dictionary.

        Raises:
            ProjectWrapper.NoSuchDeploymentError: If the parent deployment does not exist in the project.
        """
        # Get the union of all pipeline-specific deployment config schemas
        resolved_deployment_schema = {}
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            pipeline = pipeline_wrapper.get_instance()
            deployment_config_schema = pipeline.get_deployment_config_schema()
            resolved_deployment_schema.update(deployment_config_schema)

        def get_last_deployment_name() -> Optional[str]:
            if len(self.deployment_wrappers) == 0:
                return None
            return max(self.deployment_wrappers, key=lambda k: self.deployment_wrappers[k].root_dir.stat().st_mtime)

        # Use the last deployment if no parent is specified
        if parent_deployment_name is None:
            parent_deployment_name = get_last_deployment_name()  # may be None

        # Update the schema with the parent deployment
        if parent_deployment_name is not None:
            parent_deployment_wrapper = self.deployment_wrappers.get(parent_deployment_name, None)

            if parent_deployment_wrapper is None:
                raise ProjectWrapper.NoSuchDeploymentError(parent_deployment_name)

            parent_deployment_config = parent_deployment_wrapper.load_config()
            resolved_deployment_schema.update(parent_deployment_config)

        # Prompt from the resolved schema
        deployment_config = prompt_schema(resolved_deployment_schema)

        return deployment_config

    def update_pipelines(self):
        """
        Update all pipelines in the project.
        """
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            self.logger.info(f'Updating pipeline "{pipeline_name}"')
            try:
                pipeline_wrapper.update()
                self.logger.info(f'Successfully updated pipeline "{pipeline_name}"')
            except Exception as e:
                self.logger.error(f'Failed to update pipeline "{pipeline_name}": {e}')

    def install_pipelines(self):
        """
        Install all pipelines dependencies in the project into the current environment.
        """
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            self.logger.info(f'Installing pipeline "{pipeline_name}"')
            try:
                pipeline_wrapper.install()
                self.logger.info(f'Successfully installed pipeline "{pipeline_name}"')
            except PipelineWrapper.InstallError:
                self.logger.error(f'Failed to install pipeline "{pipeline_name}"')

    @property
    def pipeline_wrappers(self) -> Dict[str, PipelineWrapper]:
        """
        The loaded pipeline wrappers in the project.
        """
        return self._pipeline_wrappers

    @property
    def deployment_wrappers(self) -> Dict[str, DeploymentWrapper]:
        """
        The loaded deployment wrappers in the project.
        """
        return self._deployment_wrappers

    @property
    def root_dir(self) -> Path:
        """
        The root directory of the project.
        """
        return self._root_dir

    @property
    def pipeline_dir(self) -> Path:
        """
        The pipelines directory of the project.
        """
        return self._pipeline_dir

    @property
    def deployments_dir(self) -> Path:
        """
        The deployments directory of the project.
        """
        return self._deployments_dir

    @property
    def distribution_dir(self) -> Path:
        """
        The path to the distribution directory. Lazy-created on first access.
        """
        distribution_dir = self._root_dir / "dist"
        distribution_dir.mkdir(exist_ok=True)
        return distribution_dir

    @property
    def marimba_dir(self) -> Path:
        """
        The Marimba directory of the project.
        """
        return self._marimba_dir

    @property
    def name(self) -> str:
        """
        The name of the project.
        """
        return self._root_dir.name

    @staticmethod
    def check_name(name: str):
        """
        Check that a name is valid.

        Args:
            name: The name to check.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
        """
        if not name.isidentifier():
            raise ProjectWrapper.NameError(name)
