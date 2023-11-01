import ast
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ifdo.models import ImageData

from marimba.core.utils.log import LogMixin, get_file_handler, get_logger
from marimba.core.utils.prompt import prompt_schema
from marimba.core.wrappers.collection import CollectionWrapper
from marimba.core.wrappers.dataset import DatasetWrapper
from marimba.core.wrappers.pipeline import PipelineWrapper
from marimba.core.wrappers.target import DistributionTargetWrapper

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

    class CreateCollectionError(Exception):
        """
        Raised when a collection cannot be created.
        """

        pass

    class RunCommandError(Exception):
        """
        Raised when a command cannot be run.
        """

        pass

    class CompositionError(Exception):
        """
        Raised when a pipeline cannot compose its data.
        """

        pass

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline does not exist in the project.
        """

        pass

    class NoSuchCollectionError(Exception):
        """
        Raised when a collection does not exist in the project.
        """

        pass

    class NoSuchDatasetError(Exception):
        """
        Raised when a dataset does not exist in the project.
        """

        pass

    class NoSuchTargetError(Exception):
        """
        Raised when a distribution target does not exist in the project.
        """

        pass

    class NameError(Exception):
        """
        Raised when an invalid name is used.
        """

        pass

    def __init__(self, root_dir: Union[str, Path], dry_run: bool = False):
        super().__init__()

        self._root_dir = Path(root_dir)
        self._dry_run = dry_run

        self._pipeline_dir = self._root_dir / "pipelines"
        self._collections_dir = self._root_dir / "collections"
        self._marimba_dir = self._root_dir / ".marimba"

        self._pipeline_wrappers = {}  # pipeline name -> PipelineWrapper instance
        self._collection_wrappers = {}  # collection name -> CollectionWrapper instance
        self._dataset_wrappers = {}  # dataset name -> DatasetWrapper instance
        self._target_wrappers = {}  # target name -> DistributionTargetWrapper instance

        self._check_file_structure()
        self._setup_logging()

        self._load_pipelines()
        self._load_collections()
        self._load_datasets()
        self._load_targets()

    @classmethod
    def create(cls, root_dir: Union[str, Path], dry_run: bool = False) -> "ProjectWrapper":
        """
        Create a project from a root directory.

        Args:
            root_dir: The root directory of the project.
            dry_run: Whether to run in dry-run mode.

        Returns:
            A project.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the folder structure
        root_dir = Path(root_dir)
        pipeline_dir = root_dir / "pipelines"
        collections_dir = root_dir / "collections"
        marimba_dir = root_dir / ".marimba"

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'"{root_dir}" already exists.')

        # Create the folder structure
        root_dir.mkdir(parents=True)
        pipeline_dir.mkdir()
        collections_dir.mkdir()
        marimba_dir.mkdir()

        return cls(root_dir, dry_run=dry_run)

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
        check_dir_exists(self._collections_dir)
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
            self._pipeline_wrappers[pipeline_dir.name] = PipelineWrapper(pipeline_dir, dry_run=self.dry_run)

        self.logger.debug(f'Loaded {len(self._pipeline_wrappers)} pipeline(s): {", ".join(self._pipeline_wrappers.keys())}')

    def _load_collections(self):
        """
        Load collection instances from the `collections` directory.

        Populates the `_collection_wrappers` dictionary with `CollectionWrapper` instances.

        Raises:
            CollectionWrapper.InvalidStructureError: If the collection directory structure is invalid.
        """
        collection_dirs = filter(lambda p: p.is_dir(), self._collections_dir.iterdir())

        self._collection_wrappers.clear()
        for collection_dir in collection_dirs:
            self._collection_wrappers[collection_dir.name] = CollectionWrapper(collection_dir)

        self.logger.debug(f'Loaded {len(self._collection_wrappers)} collection(s): {", ".join(self._collection_wrappers.keys())}')

    def _load_datasets(self):
        """
        Load dataset instances from the `dist` directory.

        Populates the `_dataset_wrappers` dictionary with `DatasetWrapper` instances.

        Raises:
            DatasetWrapper.InvalidStructureError: If the dataset directory structure is invalid.
        """
        dataset_dirs = filter(lambda p: p.is_dir(), self.distribution_dir.iterdir())

        self._dataset_wrappers.clear()
        for dataset_dir in dataset_dirs:
            self._dataset_wrappers[dataset_dir.name] = DatasetWrapper(dataset_dir)

        self.logger.debug(f'Loaded {len(self._dataset_wrappers)} dataset(s): {", ".join(self._dataset_wrappers.keys())}')

    def _load_targets(self):
        """
        Load distribution target wrappers from the `.marimba/targets` directory.

        Populates the `_load_targets` dictionary with `DistributionTargetWrapper` instances.

        Raises:
            DistributionTargetWrapper.InvalidConfigError: If the distribution target configuration file is invalid.
        """
        target_config_paths = filter(lambda p: p.is_file(), self.targets_dir.iterdir())

        self._target_wrappers.clear()
        for target_config_path in target_config_paths:
            self._target_wrappers[target_config_path.stem] = DistributionTargetWrapper(target_config_path)

        self.logger.debug(f'Loaded {len(self._target_wrappers)} distribution target(s): {", ".join(self._target_wrappers.keys())}')

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

        # Show warning if there are already collections in the project
        if len(self.collection_wrappers) > 0:
            self.logger.warning("Creating a new pipeline in a project with existing collections.")

        # Create the pipeline directory
        pipeline_wrapper = PipelineWrapper.create(pipeline_dir, url, dry_run=self.dry_run)

        # Add the pipeline to the project
        self._pipeline_wrappers[name] = pipeline_wrapper

        self.logger.debug(f'Created pipeline "{name}" successfully')

        return pipeline_wrapper

    def create_collection(self, name: str, config: Dict[str, Any]) -> CollectionWrapper:
        """
        Create a new collection.

        Args:
            name: The name of the collection.
            config: The collection configuration.

        Returns:
            The collection directory wrapper.

        Raises:
            ProjectWrapper.CreateCollectionError: If the collection cannot be created.
        """
        self.logger.debug(f'Creating collection "{name}"')

        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Check that a collection with the same name doesn't already exist
        collection_dir = self.collections_dir / name
        if collection_dir.exists():
            raise ProjectWrapper.CreateCollectionError(f'A collection with the name "{name}" already exists.')

        # Create the collection directory
        collection_wrapper = CollectionWrapper.create(collection_dir, config)

        # Create the pipeline data directories
        for pipeline_name in self._pipeline_wrappers:
            collection_wrapper.get_pipeline_data_dir(pipeline_name).mkdir()

        # Add the collection to the project
        self._collection_wrappers[name] = collection_wrapper

        self.logger.debug(f'Created collection "{name}" successfully')

        return collection_wrapper

    def run_command(
        self,
        command_name: str,
        pipeline_name: Optional[str] = None,
        collection_name: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        **kwargs: dict,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Run a command within the project.

        By default, this will run the command for all pipelines and collections in the project.
        If a pipeline name is provided, it will run the command for all collections of that pipeline.
        If a collection name is provided, it will run the command for that collection only.
        These can be combined to run the command for a specific collection of a specific pipeline.

        Args:
            command_name: The name of the command to run.
            pipeline_name: The name of the pipeline to run the command for.
            collection_name: The name of the collection to run the command for.
            extra_args: Any extra arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Returns:
            A dictionary containing the results of the command for each collection: {collection_name: {pipeline_name: result}}.

        Raises:
            ProjectWrapper.RunCommandError: If the command cannot be run.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        if pipeline_name is not None:
            pipeline_wrapper = self._pipeline_wrappers.get(pipeline_name, None)
            if pipeline_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Pipeline "{pipeline_name}" does not exist within the project.')

        if collection_name is not None:
            collection_wrapper = self._collection_wrappers.get(collection_name, None)
            if collection_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Collection "{collection_name}" does not exist within the project.')

        # Select the pipelines and collections to run the command for
        pipeline_wrappers_to_run = {pipeline_name: pipeline_wrapper} if pipeline_name is not None else self._pipeline_wrappers
        collection_wrappers_to_run = {collection_name: collection_wrapper} if collection_name is not None else self._collection_wrappers

        # Load pipeline instances
        pipeline_to_run = {pipeline_name: pipeline_wrapper.get_instance() for pipeline_name, pipeline_wrapper in pipeline_wrappers_to_run.items()}

        # Check that the command exists for all pipelines
        for run_pipeline_name, run_pipeline in pipeline_to_run.items():
            if not hasattr(run_pipeline, command_name):
                raise ProjectWrapper.RunCommandError(f'Command "{command_name}" does not exist for pipeline "{run_pipeline_name}".')

        # Invoke the command for each pipeline and collection
        self.logger.debug(
            f'Running command "{command_name}" across pipeline(s) {", ".join(pipeline_to_run.keys())} and collection(s) {" ,".join(collection_wrappers_to_run.keys())} with kwargs {merged_kwargs}'
        )
        results_by_collection = {}
        for run_collection_name, run_collection_wrapper in collection_wrappers_to_run.items():
            results_by_pipeline = {}
            for run_pipeline_name, run_pipeline in pipeline_to_run.items():
                # Get the pipeline-specific data directory and config
                pipeline_collection_data_dir = run_collection_wrapper.get_pipeline_data_dir(run_pipeline_name)
                pipeline_collection_config = run_collection_wrapper.load_config()

                # Call the method
                method = getattr(run_pipeline, command_name)
                results_by_pipeline[run_pipeline_name] = method(pipeline_collection_data_dir, pipeline_collection_config, **merged_kwargs)

            results_by_collection[run_collection_name] = results_by_pipeline

        return results_by_collection

    def compose(
        self, collection_names: List[str], extra_args: Optional[List[str]] = None, **kwargs: dict
    ) -> Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]]:
        """
        Compose a dataset for the given collections across all pipelines.

        Args:
            collection_names: The names of the collections to compose.
            extra_args: Any extra CLI arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Returns:
            A dict mapping pipeline name -> { output file path -> (input file path, image data) }

        Raises:
            ProjectWrapper.NoSuchCollectionError: If a collection does not exist in the project.
            ProjectWrapper.CompositionError: If a pipeline cannot compose its data.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        # Get the collection wrappers
        collection_wrappers: List[CollectionWrapper] = []
        for collection_name in collection_names:
            collection_wrapper = self.collection_wrappers.get(collection_name, None)
            if collection_wrapper is None:
                raise ProjectWrapper.NoSuchCollectionError(collection_name)
            collection_wrappers.append(collection_wrapper)

        # Load the collection configs
        collection_configs = [collection_wrapper.load_config() for collection_wrapper in collection_wrappers]

        self.logger.debug(f'Composing dataset for collections {", ".join(collection_names)} with kwargs {merged_kwargs}')
        dataset_mapping = {}
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            # Get the pipeline instance
            pipeline = pipeline_wrapper.get_instance()

            # Get the collection data directories for the pipeline
            collection_data_dirs = [collection_wrapper.get_pipeline_data_dir(pipeline_name) for collection_wrapper in collection_wrappers]

            # Compose the pipeline data mapping
            try:
                pipeline_data_mapping = pipeline.run_compose(collection_data_dirs, collection_configs, **merged_kwargs)
            except Exception as e:
                raise ProjectWrapper.CompositionError(f'Pipeline "{pipeline_name}" failed to compose its data:\n{e}') from e

            # Add the pipeline data mapping to the dataset mapping
            dataset_mapping[pipeline_name] = pipeline_data_mapping

        return dataset_mapping

    def create_dataset(
        self, dataset_name: str, dataset_mapping: Dict[str, Dict[Path, Tuple[Path, List[ImageData]]]], copy: bool = True
    ) -> DatasetWrapper:
        """
        Create a Marimba dataset from a dataset mapping.

        Args:
            dataset_name: The name of the dataset.
            dataset_mapping: The dataset mapping to package.
            copy: Whether to copy the files (True) or move them (False).

        Returns:
            A dataset wrapper instance for the created dataset.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
            FileExistsError: If the dataset root directory already exists.
            DatasetWrapper.InvalidDatasetMappingError: If the dataset mapping is invalid.
        """
        self.logger.debug(f'Packaging dataset "{dataset_name}"')

        # Check the name is valid
        ProjectWrapper.check_name(dataset_name)

        # Create the dataset
        dataset_root_dir = self.distribution_dir / dataset_name
        dataset_wrapper = DatasetWrapper.create(dataset_root_dir, dry_run=self.dry_run)

        # Populate it
        dataset_wrapper.populate(
            dataset_name, dataset_mapping, self.log_path, map(lambda pw: pw.log_path, self.pipeline_wrappers.values()), copy=copy
        )

        self._dataset_wrappers[dataset_name] = dataset_wrapper

        return dataset_wrapper

    def create_target(self, target_name: str, target_type: str, target_config: dict) -> DistributionTargetWrapper:
        """
        Create a Marimba distribution target.

        Args:
            target_name: The name of the distribution target.
            target_type: The type of distribution target to create. See `DistributionTargetWrapper.CLASS_MAP` for valid types.
            target_config: The configuration for the distribution target.

        Returns:
            A distribution target wrapper instance for the created target.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
            FileExistsError: If the target already exists.
        """
        self.logger.debug(f'Creating distribution target "{target_name}"')

        # Check the name is valid
        ProjectWrapper.check_name(target_name)

        # Create the target
        target_config_path = self.targets_dir / f"{target_name}.yml"
        target_wrapper = DistributionTargetWrapper.create(target_config_path, target_type, target_config)

        self._target_wrappers[target_name] = target_wrapper

        return target_wrapper

    def distribute(self, dataset_name: str, target_name: str):
        """
        Distribute a dataset to a distribution target.

        Args:
            dataset_name: The name of the dataset to distribute.
            target_name: The name of the distribution target to distribute to.

        Raises:
            ProjectWrapper.NoSuchDatasetError: If the dataset does not exist in the project.
            ProjectWrapper.NoSuchTargetError: If the distribution target does not exist in the project.
            DistributionTargetBase.DistributionError: If the dataset cannot be distributed.
        """
        self.logger.debug(f'Distributing dataset "{dataset_name}" to target "{target_name}"')

        # Get the dataset wrapper
        dataset_wrapper = self.dataset_wrappers.get(dataset_name, None)
        if dataset_wrapper is None:
            raise ProjectWrapper.NoSuchDatasetError(dataset_name)

        # Get the distribution target wrapper
        target_wrapper = self.target_wrappers.get(target_name, None)
        if target_wrapper is None:
            raise ProjectWrapper.NoSuchTargetError(target_name)

        # Get the distribution target instance
        distribution_target = target_wrapper.get_instance()

        # Distribute the dataset
        distribution_target.distribute(dataset_wrapper)

    def run_import(
        self, collection_name: str, source_paths: Iterable[Union[str, Path]], extra_args: Optional[List[str]] = None, **kwargs: dict
    ) -> None:
        """
        Run the import command to populate a collection from a source data directory.

        May overwrite existing data in the collection.

        Args:
            collection_name: The name of the collection to import into.
            source_paths: The source paths to import from.
            extra_args: Any extra CLI arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Raises:
            ProjectWrapper.NoSuchCollectionError: If the collection does not exist in the project.
        """
        source_paths = list(map(lambda p: Path(p), source_paths))

        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        # Get the collection wrapper
        collection_wrapper = self.collection_wrappers.get(collection_name, None)
        if collection_wrapper is None:
            raise ProjectWrapper.NoSuchCollectionError(collection_name)

        # Import each pipeline
        pretty_paths = ", ".join(list(map(lambda p: str(p.resolve().absolute()), source_paths)))
        self.logger.debug(f'Running import for collection "{collection_name}" from source paths {pretty_paths} with kwargs {merged_kwargs}')
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            # Get the pipeline instance
            pipeline = pipeline_wrapper.get_instance()

            # Get the collection data directory
            collection_data_dir = collection_wrapper.get_pipeline_data_dir(pipeline_name)

            # Load the collection config
            collection_config = collection_wrapper.load_config()

            # Run the import
            pipeline.run_import(collection_data_dir, source_paths, collection_config, **merged_kwargs)

    def prompt_collection_config(self, parent_collection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Prompt the user for a collection configuration.

        The schema will be generated from the pipeline-specific collection config schemas of all pipelines in the project, as well as the collection config of the parent collection if specified.

        Args:
            parent_collection_name: The name of the parent collection. If unspecified, use the last collection by modification time.

        Returns:
            The collection configuration as a dictionary.

        Raises:
            ProjectWrapper.NoSuchCollectionError: If the parent collection does not exist in the project.
        """
        # Get the union of all pipeline-specific collection config schemas
        resolved_collection_schema = {}
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            pipeline = pipeline_wrapper.get_instance()
            collection_config_schema = pipeline.get_collection_config_schema()
            resolved_collection_schema.update(collection_config_schema)

        def get_last_collection_name() -> Optional[str]:
            if len(self.collection_wrappers) == 0:
                return None
            return max(self.collection_wrappers, key=lambda k: self.collection_wrappers[k].root_dir.stat().st_mtime)

        # Use the last collection if no parent is specified
        if parent_collection_name is None:
            parent_collection_name = get_last_collection_name()  # may be None
            if parent_collection_name is not None:
                self.logger.info(f'Using last collection "{parent_collection_name}" as parent')

        # Update the schema with the parent collection
        if parent_collection_name is not None:
            parent_collection_wrapper = self.collection_wrappers.get(parent_collection_name, None)

            if parent_collection_wrapper is None:
                raise ProjectWrapper.NoSuchCollectionError(parent_collection_name)

            parent_collection_config = parent_collection_wrapper.load_config()
            resolved_collection_schema.update(parent_collection_config)
            self.logger.debug(f'Using parent collection "{parent_collection_name}" with config: {parent_collection_config}')

        # Prompt from the resolved schema
        collection_config = prompt_schema(resolved_collection_schema)
        self.logger.debug(f"Prompted collection config: {collection_config}")

        return collection_config

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
    def collection_wrappers(self) -> Dict[str, CollectionWrapper]:
        """
        The loaded collection wrappers in the project.
        """
        return self._collection_wrappers

    @property
    def dataset_wrappers(self) -> Dict[str, DatasetWrapper]:
        """
        The loaded dataset wrappers in the project.
        """
        return self._dataset_wrappers

    @property
    def target_wrappers(self) -> Dict[str, DistributionTargetWrapper]:
        """
        The loaded distribution target wrappers in the project.
        """
        return self._target_wrappers

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
    def collections_dir(self) -> Path:
        """
        The collections directory of the project.
        """
        return self._collections_dir

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
    def targets_dir(self) -> Path:
        """
        The distribution targets directory of the project.
        """
        targets_dir = self._marimba_dir / "targets"
        targets_dir.mkdir(exist_ok=True)
        return targets_dir

    @property
    def log_path(self) -> Path:
        """
        The path to the project log file.
        """
        return self._root_dir / f"{self.name}.log"

    @property
    def name(self) -> str:
        """
        The name of the project.
        """
        return self._root_dir.name

    @property
    def dry_run(self) -> bool:
        """
        Whether the project is in dry-run mode.
        """
        return self._dry_run

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
