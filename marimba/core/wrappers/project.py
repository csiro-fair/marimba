"""
Marimba Core Project Wrapper Module.

This module provides functionality for managing Marimba project directories, including creating, wrapping,
and interacting with projects. It includes utility functions and classes to handle keyword arguments, project structure,
logging, and various wrappers for pipelines, collections, datasets, and distribution targets.

Imports:
    - ast: Abstract Syntax Trees for parsing Python syntax.
    - logging: Logging facility for Python.
    - pathlib.Path: Object-oriented filesystem paths.
    - typing: Type hints for function signatures and variables.
    - ifdo.models.ImageData: Data model for image data.
    - rich.progress.Progress, rich.progress.SpinnerColumn: Utilities for creating progress bars.
    - marimba.core.utils.log.LogMixin, marimba.core.utils.log.get_file_handler:
      Utilities for logging.
    - marimba.core.utils.prompt.prompt_schema: Utility for prompting schema.
    - marimba.core.utils.rich.get_default_columns: Utility for default columns in rich progress.
    - marimba.core.wrappers.collection.CollectionWrapper: Wrapper for collections.
    - marimba.core.wrappers.dataset.DatasetWrapper: Wrapper for datasets.
    - marimba.core.wrappers.pipeline.PipelineWrapper: Wrapper for pipelines.
    - marimba.core.wrappers.target.DistributionTargetWrapper: Wrapper for
      distribution targets.

Classes:
    - ProjectWrapper: A class to manage Marimba project directories.
        - Nested exceptions for various project-related errors.
        - Methods for creating and wrapping projects, checking file structures, setting up logging, loading pipelines,
        collections, datasets, and targets, running commands, composing datasets, creating datasets and targets,
        distributing datasets, importing collections, prompting collection configurations, updating pipelines,
        and installing pipeline dependencies.

Functions:
    - get_merged_keyword_args: Merges extra key-value arguments with other keyword arguments.
"""

import ast
import logging
import math
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from ifdo.models import ImageData
from rich.progress import Progress, SpinnerColumn

from marimba.core.parallel.pipeline_loader import load_pipeline_instance
from marimba.core.utils.constants import Operation
from marimba.core.utils.log import LogMixin, get_file_handler
from marimba.core.utils.paths import remove_directory_tree
from marimba.core.utils.prompt import prompt_schema
from marimba.core.utils.rich import get_default_columns
from marimba.core.wrappers.collection import CollectionWrapper
from marimba.core.wrappers.dataset import DatasetWrapper
from marimba.core.wrappers.pipeline import PipelineWrapper
from marimba.core.wrappers.target import DistributionTargetWrapper


def get_merged_keyword_args(
    kwargs: dict[str, Any],
    extra_args: list[str] | None,
    logger: logging.Logger,
) -> dict[str, Any]:
    """
    Merge any extra key-value arguments with other keyword arguments.

    Args:
        kwargs: The keyword arguments to merge with.
        extra_args: A list of extra key-value arguments to merge.
        logger: A logger object to log any warnings.

    Returns:
        A dictionary containing the merged keyword arguments.
    """
    # Define constant for expected key-value pair parts
    key_value_parts = 2

    extra_dict = {}
    if extra_args:
        for arg in extra_args:
            # Attempt to split the argument into a key and a value
            parts = arg.split("=")
            if len(parts) == key_value_parts:
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


def execute_import(
    pipeline_name: str,
    root_dir: Path,
    repo_dir: Path,
    config_path: Path,
    dry_run: bool,
    collection_data_dir: Path,
    collection_config: dict[str, Any],
    source_path: Path,
    log_string_prefix: str,
    merged_kwargs: dict[str, Any],
) -> str:
    """
    Execute the import process for a specified pipeline.

    This function loads a pipeline instance, runs the import process, and measures the execution time. It handles
    various configuration parameters and supports dry run mode for testing purposes.

    Args:
        pipeline_name (str): The name of the pipeline to be executed.
        root_dir (Path): The root directory of the pipeline.
        repo_dir (Path): The directory of the pipeline repository.
        config_path (Path): The path to the configuration file for the pipeline.
        dry_run (bool): If True, runs the import in dry run mode without making actual changes.
        collection_data_dir (Path): The directory where collected data will be stored.
        collection_config (dict[str, Any]): Additional configuration options for the collection process.
        source_path (Path): The source path from which data will be imported.
        log_string_prefix (str): A prefix to be added to log messages for easier identification.
        merged_kwargs (dict[str, Any]): Additional keyword arguments to be passed to the import process.

    Returns:
        str: A message indicating the completion of the import process and the elapsed time in seconds.

    Raises:
        ValueError: If any of the required parameters are missing or invalid.
        ImportError: If the specified pipeline module cannot be loaded.
        RuntimeError: If an error occurs during the import process.
    """
    start_import_time = time.time()

    # Load the pipeline instance using the standalone function
    pipeline_instance = load_pipeline_instance(
        root_dir,
        repo_dir,
        pipeline_name,
        config_path,
        dry_run,
        log_string_prefix,
    )

    # Run the import method
    pipeline_instance.run_import(collection_data_dir, source_path, collection_config, **merged_kwargs)

    end_import_time = time.time()
    import_duration = end_import_time - start_import_time

    return (
        f'Completed importing data for pipeline "{pipeline_name}" and source "{source_path}" in {import_duration:.2f} '
        f"seconds"
    )


def execute_process(
    pipeline_name: str,
    root_dir: Path,
    repo_dir: Path,
    config_path: Path,
    collection_name: str,
    collection_data_dir: Path,
    collection_config: dict[str, Any],
    dry_run: bool,
    log_string_prefix: str,
    merged_kwargs: dict[str, Any],
) -> str:
    """
    Execute a command for a given pipeline and collection.

    Args:
        pipeline_name (str): The name of the pipeline.
        root_dir (Path): The root directory of the pipeline.
        repo_dir (Path): The directory of the pipeline repository.
        config_path (Path): The configuration file path.
        dry_run (bool): Flag indicating if the command should be run in dry run mode.
        collection_name (str): The name of the collection.
        collection_data_dir (Path): The directory where collection data will be stored.
        collection_config (Dict[str, Any]): Additional configuration for the collection process.
        log_string_prefix (str): A prefix to be added to log messages for easier identification.
        merged_kwargs (Dict[str, Any]): Additional keyword arguments to be passed to the command.

    Returns:
        str: A message indicating the completion of the command execution and the elapsed time in seconds.
    """
    start_command_time = time.time()

    # Load the pipeline instance using the standalone function
    pipeline_instance = load_pipeline_instance(
        root_dir,
        repo_dir,
        pipeline_name,
        config_path,
        dry_run,
        log_string_prefix,
    )

    # Run the process method
    pipeline_instance.run_process(collection_data_dir, collection_config, **merged_kwargs)

    end_command_time = time.time()
    command_duration = end_command_time - start_command_time

    return (
        f'Completed processing for pipeline "{pipeline_name}" and collection "{collection_name}" '
        f"in {command_duration:.2f} seconds"
    )


def execute_packaging(
    pipeline_name: str,
    root_dir: Path,
    repo_dir: Path,
    collection_name: str,
    collection_data_dir: Path,
    collection_config: dict[str, Any],
    config_path: Path,
    dry_run: bool,
    log_string_prefix: str,
    merged_kwargs: dict[str, Any],
) -> tuple[dict[Path, tuple[Path, list[Any] | None, dict[str, Any] | None]], str]:
    """
    Package a pipeline's data for a given collection directory and configuration.

    Args:
        pipeline_name: The name of the pipeline.
        root_dir: The root directory of the pipeline.
        repo_dir: The directory of the pipeline repository.
        collection_name: The name of the collection.
        collection_data_dir: The directory containing the collection data.
        collection_config: The configuration for the collection.
        config_path: The configuration file path.
        dry_run: Flag indicating if the package should be run in dry run mode.
        log_string_prefix (str): A prefix to be added to log messages for easier identification.
        merged_kwargs: Additional keyword arguments to be passed to the package process.

    Returns:
        A dictionary mapping output file paths to tuples of input file paths, image data, and additional data.
    """
    start_package_time = time.time()

    # Load the pipeline instance using the standalone function
    pipeline_instance = load_pipeline_instance(
        root_dir,
        repo_dir,
        pipeline_name,
        config_path,
        dry_run,
        log_string_prefix,
    )

    # Run the package method
    pipeline_data_mapping = pipeline_instance.run_package(collection_data_dir, collection_config, **merged_kwargs)

    end_package_time = time.time()
    package_duration = end_package_time - start_package_time

    return (
        pipeline_data_mapping,
        f'Completed composing data for pipeline "{pipeline_name}" and collection "{collection_name}" in '
        f"{package_duration:.2f} seconds",
    )


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

    class CreatePipelineError(Exception):
        """
        Raised when a pipeline cannot be created.
        """

    class CreateCollectionError(Exception):
        """
        Raised when a collection cannot be created.
        """

    class RunCommandError(Exception):
        """
        Raised when a command cannot be run.
        """

    class CompositionError(Exception):
        """
        Raised when a pipeline cannot compose its data.
        """

    class NoSuchPipelineError(Exception):
        """
        Raised when a pipeline does not exist in the project.
        """

    class NoSuchCollectionError(Exception):
        """
        Raised when a collection does not exist in the project.
        """

    class NoSuchDatasetError(Exception):
        """
        Raised when a dataset does not exist in the project.
        """

    class NoSuchTargetError(Exception):
        """
        Raised when a distribution target does not exist in the project.
        """

    class InvalidNameError(Exception):
        """
        Raised when an invalid name is used.
        """

    class DeletePipelineError(Exception):
        """
        Raised when a Pipeline cannot be deleted.
        """

    class MarimbaThreadError(Exception):
        """
        Raised when an error occurs within a Marimba thread.
        """

    class MarimbaProcessError(Exception):
        """
        Raised when an error occurs within a Marimba process.
        """

    def __init__(self, root_dir: str | Path, *, dry_run: bool = False) -> None:
        """
        Initialise the class instance.

        Args:
            root_dir (Union[str, Path]): The root directory path.
            dry_run (bool, optional): Specifies whether to run the method in dry run mode. Defaults to False.
        """
        super().__init__()

        self._root_dir = Path(root_dir)
        self._dry_run = dry_run

        self._pipeline_wrappers: dict[str, PipelineWrapper] = {}  # pipeline name -> PipelineWrapper instance
        self._collection_wrappers: dict[str, CollectionWrapper] = {}  # collection name -> CollectionWrapper instance
        self._dataset_wrappers: dict[str, DatasetWrapper] = {}  # dataset name -> DatasetWrapper instance
        self._target_wrappers: dict[str, DistributionTargetWrapper] = (
            {}
        )  # target name -> DistributionTargetWrapper instance

        self._check_file_structure()
        self._setup_logging()

        self._load_pipelines()
        self._load_collections()
        self._load_datasets()
        self._load_targets()
        self.logger.debug(
            f"Loaded {len(self.pipeline_wrappers)} pipeline(s), "
            f"{len(self.collection_wrappers)} collection(s), "
            f"{len(self.dataset_wrappers)} dataset(s) and "
            f"{len(self.target_wrappers)} distribution target(s)",
        )

    @classmethod
    def create(cls, root_dir: str | Path, *, dry_run: bool = False) -> "ProjectWrapper":
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
        marimba_dir = root_dir / ".marimba"

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'"{root_dir}" already exists.')

        # Create the folder structure
        root_dir.mkdir(parents=True)
        marimba_dir.mkdir()

        return cls(root_dir, dry_run=dry_run)

    def _check_file_structure(self) -> None:
        """
        Check that the project file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            ProjectWrapper.InvalidStructureError: If the project file structure is invalid.
        """

        def check_dir_exists(path: Path) -> None:
            if not path.is_dir():
                raise ProjectWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self.root_dir)

    def _setup_logging(self) -> None:
        """
        Set up logging. Create file handler for this instance that writes to `project.log`.
        """
        # Create a file handler for this instance
        file_handler = get_file_handler(self.root_dir, "project", self._dry_run)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def _load_pipelines(self) -> None:
        """
        Load pipeline wrappers from the `pipelines` directory.

        Populates the `_pipeline_wrappers` dictionary with `PipelineWrapper` instances.

        Raises:
            PipelineWrapper.InvalidStructureError: If the pipeline directory structure is invalid.
        """
        pipeline_dirs = filter(lambda p: p.is_dir(), self.pipelines_dir.iterdir())

        self._pipeline_wrappers.clear()
        for pipeline_dir in pipeline_dirs:
            self._pipeline_wrappers[pipeline_dir.name] = PipelineWrapper(pipeline_dir, dry_run=self.dry_run)

    def _load_collections(self) -> None:
        """
        Load collection instances from the `collections` directory.

        Populates the `_collection_wrappers` dictionary with `CollectionWrapper` instances.

        Raises:
            CollectionWrapper.InvalidStructureError: If the collection directory structure is invalid.
        """
        collection_dirs = filter(lambda p: p.is_dir(), self.collections_dir.iterdir())

        self._collection_wrappers.clear()
        for collection_dir in collection_dirs:
            self._collection_wrappers[collection_dir.name] = CollectionWrapper(collection_dir)

    def _load_datasets(self) -> None:
        """
        Load dataset instances from the `dist` directory.

        Populates the `_dataset_wrappers` dictionary with `DatasetWrapper` instances.

        Raises:
            DatasetWrapper.InvalidStructureError: If the dataset directory structure is invalid.
        """
        dataset_dirs = filter(lambda p: p.is_dir(), self.datasets_dir.iterdir())

        self._dataset_wrappers.clear()
        for dataset_dir in dataset_dirs:
            self._dataset_wrappers[dataset_dir.name] = DatasetWrapper(dataset_dir)

    def _load_targets(self) -> None:
        """
        Load distribution target wrappers from the `targets` directory.

        Populates the `_load_targets` dictionary with `DistributionTargetWrapper` instances.

        Raises:
            DistributionTargetWrapper.InvalidConfigError: If the distribution target configuration file is invalid.
        """
        target_config_paths = filter(lambda p: p.is_file(), self.targets_dir.iterdir())

        self._target_wrappers.clear()
        for target_config_path in target_config_paths:
            self._target_wrappers[target_config_path.stem] = DistributionTargetWrapper(target_config_path)

    def delete_project(self) -> Path:
        """
        Delete a project.

        Returns:
            A Path to the deleted project.

        """
        if not self.dry_run:
            remove_directory_tree(self.root_dir.resolve(), "project", self.dry_run)
        return self.root_dir

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
        pipeline_dir = self.pipelines_dir / name
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

    def delete_pipeline(
        self,
        name: str,
        dry_run: bool,
    ) -> Path:
        """
        Delete a pipeline.

        Args:
            name: The name of the pipeline.
            dry_run: Whether to run in dry-run mode.

        Returns:
            The path to deleted pipeline.

        Raises:
            ProjectWrapper.DeletePipelineError: If the pipeline cannot be deleted.
            ProjectWrapper.NameError: If the name is invalid.
        """
        self.logger.debug(f'Deleting pipeline "{name}"')

        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Check that a pipeline with the same name doesn't already exist
        pipeline_dir = self.pipelines_dir / name
        if pipeline_dir.exists():
            if not dry_run:
                remove_directory_tree(pipeline_dir, "pipeline", dry_run)
        else:
            raise ProjectWrapper.DeletePipelineError(f'A pipeline with the name "{name}" does not exist.')
        return pipeline_dir

    def create_collection(self, name: str, config: dict[str, Any]) -> CollectionWrapper:
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
            collection_wrapper.create_pipeline_data_dir(pipeline_name)

        # Add the collection to the project
        self._collection_wrappers[name] = collection_wrapper

        self.logger.debug(f'Created collection "{name}" successfully')

        return collection_wrapper

    def delete_collection(
        self,
        name: str,
        dry_run: bool,
    ) -> Path:
        """
        Delete a collection.

        Args:
            name: The name of the collection.
            dry_run: Whether to run in dry-run mode.

        Returns:
            The collection directory wrapper.

        Raises:
            ProjectWrapper.CreateCollectionError: If the collection cannot be created.
        """
        self.logger.debug(f'Deleting collection "{name}"')

        # Check the name is valid
        ProjectWrapper.check_name(name)

        # Check that a collection with the same name doesn't already exist
        collection_dir = self.collections_dir / name
        if collection_dir.exists():
            remove_directory_tree(collection_dir, "collection", dry_run)
        else:
            raise ProjectWrapper.NoSuchCollectionError(f'A collection with the name "{name}" does not exist.')
        return collection_dir

    def _get_wrappers_to_run(
        self,
        pipeline_names: list[str],
        collection_names: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pipeline_wrappers_to_run = {}
        collection_wrappers_to_run = {}

        # Process pipeline names
        for pipeline_name in pipeline_names:
            pipeline_wrapper = self._pipeline_wrappers.get(pipeline_name)
            if pipeline_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Pipeline "{pipeline_name}" does not exist within the project.')
            pipeline_wrappers_to_run[pipeline_name] = pipeline_wrapper

        # Process collection names
        for collection_name in collection_names:
            collection_wrapper = self._collection_wrappers.get(collection_name)
            if collection_wrapper is None:
                raise ProjectWrapper.RunCommandError(
                    f'Collection "{collection_name}" does not exist within the project.',
                )
            collection_wrappers_to_run[collection_name] = collection_wrapper

        return pipeline_wrappers_to_run, collection_wrappers_to_run

    def _check_command_exists(self, pipelines_to_run: dict[str, Any], command_name: str) -> None:
        for run_pipeline_name, run_pipeline in pipelines_to_run.items():
            if not hasattr(run_pipeline, command_name):
                raise ProjectWrapper.RunCommandError(
                    f'Command "{command_name}" does not exist for pipeline "{run_pipeline_name}".',
                )

    def _create_command_tasks(
        self,
        executor: ProcessPoolExecutor,
        pipeline_wrappers_to_run: dict[str, Any],
        collection_wrappers_to_run: dict[str, Any],
        merged_kwargs: dict[str, Any],
    ) -> dict[Any, tuple[str, str]]:

        futures = {}
        process_index = 1
        total_processes = len(pipeline_wrappers_to_run) * len(collection_wrappers_to_run)

        process_padding_length = math.ceil(math.log10(total_processes + 1))
        pipeline_padding_length = math.ceil(math.log10(len(pipeline_wrappers_to_run) + 1))
        collection_padding_length = math.ceil(math.log10(len(collection_wrappers_to_run) + 1))

        for pipeline_index, (run_pipeline_name, run_pipeline_wrapper) in enumerate(
            pipeline_wrappers_to_run.items(),
            start=1,
        ):
            root_dir = run_pipeline_wrapper.root_dir
            repo_dir = run_pipeline_wrapper.repo_dir
            config_path = run_pipeline_wrapper.config_path
            dry_run = run_pipeline_wrapper.dry_run

            for collection_index, (run_collection_name, run_collection_wrapper) in enumerate(
                collection_wrappers_to_run.items(),
                start=1,
            ):
                collection_data_dir = run_collection_wrapper.get_pipeline_data_dir(run_pipeline_name)
                collection_config = run_collection_wrapper.load_config()

                # Zero-pad process, pipeline and collection indices
                padded_process_index = f"{process_index:0{process_padding_length}}"
                padded_pipeline_index = f"{pipeline_index:0{pipeline_padding_length}}"
                padded_collection_index = f"{collection_index:0{collection_padding_length}}"

                log_string_prefix = (
                    f"Process {padded_process_index} | "
                    f'Pipeline {padded_pipeline_index} "{run_pipeline_name}" | '
                    f'Collection {padded_collection_index} "{run_collection_name}" | '
                )

                futures[
                    executor.submit(
                        execute_process,
                        run_pipeline_name,
                        root_dir,
                        repo_dir,
                        config_path,
                        run_collection_name,
                        collection_data_dir,
                        collection_config,
                        dry_run,
                        log_string_prefix,
                        merged_kwargs,
                    )
                ] = (run_pipeline_name, log_string_prefix)
                process_index += 1

        return futures

    def run_process(
        self,
        collection_names: list[str],
        pipeline_names: list[str],
        extra_args: list[str] | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """
        Run a command within the project.

        Args:
            collection_names: The names of the collections to run the command for.
            pipeline_names: The names of the pipelines to run the command for.
            extra_args: Any extra arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Returns:
            A dictionary containing the results of the command for each collection:
                {collection_name: {pipeline_name: result}}.

        Raises:
            ProjectWrapper.RunCommandError: If the command cannot be run.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        pipeline_wrappers_to_run, collection_wrappers_to_run = self._get_wrappers_to_run(
            pipeline_names,
            collection_names,
        )

        pretty_pipelines = ", ".join(f'"{p!s}"' for p, _ in pipeline_wrappers_to_run.items())
        pretty_collections = ", ".join(f'"{c!s}"' for c, _ in collection_wrappers_to_run.items())
        pipeline_label = "pipeline" if len(pipeline_wrappers_to_run) == 1 else "pipelines"
        collection_label = "collection" if len(collection_wrappers_to_run) == 1 else "collections"
        self.logger.debug(
            f"Processing data for {pipeline_label} {pretty_pipelines} and {collection_label} {pretty_collections} "
            f"with kwargs {merged_kwargs}",
        )

        total_processes = len(pipeline_wrappers_to_run) * len(collection_wrappers_to_run)

        self.logger.debug(
            f"Setting up multiprocessing to run {total_processes} independent process"
            f"{'es' if total_processes != 1 else ''}, made up of {len(pipeline_wrappers_to_run)} pipeline"
            f"{'s' if len(pipeline_wrappers_to_run) != 1 else ''} and {len(collection_wrappers_to_run)} collection"
            f"{'s' if len(collection_wrappers_to_run) != 1 else ''} per pipeline.",
        )

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                run_pipeline_name: progress.add_task(
                    f"[green]Processing data for pipeline {run_pipeline_name}",
                    total=len(collection_wrappers_to_run),
                )
                for run_pipeline_name in pipeline_wrappers_to_run
            }

            with ProcessPoolExecutor() as executor:
                futures = self._create_command_tasks(
                    executor,
                    pipeline_wrappers_to_run,
                    collection_wrappers_to_run,
                    merged_kwargs,
                )

                for future in as_completed(futures):
                    pipeline_name, log_string_prefix = futures[future]
                    try:
                        message = future.result()
                        self.logger.debug(f"{log_string_prefix} - {message}")
                    except Exception as e:
                        raise ProjectWrapper.MarimbaProcessError(f"{log_string_prefix} - {e}") from e
                    finally:
                        progress.advance(tasks_by_pipeline_name[pipeline_name])

    def _create_composition_tasks(
        self,
        executor: ProcessPoolExecutor,
        pipeline_names: list[str],
        collection_names: list[str],
        collection_wrappers: list[Any],
        collection_configs: list[dict[str, Any]],
        merged_kwargs: dict[str, Any],
    ) -> dict[Any, tuple[str, str, str]]:

        futures = {}
        process_index = 1
        total_processes = len(pipeline_names) * len(collection_names)

        process_padding_length = math.ceil(math.log10(total_processes + 1))
        pipeline_padding_length = math.ceil(math.log10(len(pipeline_names) + 1))
        collection_padding_length = math.ceil(math.log10(len(collection_names) + 1))

        # for pipeline_index, (pipeline_name, pipeline_wrapper) in enumerate(self.pipeline_wrappers.items(), start=1):
        for pipeline_index, pipeline_name in enumerate(pipeline_names, start=1):

            # Get the pipeline wrapper
            pipeline_wrapper = self.pipeline_wrappers.get(pipeline_name, None)
            if pipeline_wrapper is None:
                raise ProjectWrapper.NoSuchPipelineError(pipeline_name)

            root_dir = pipeline_wrapper.root_dir
            repo_dir = pipeline_wrapper.repo_dir
            config_path = pipeline_wrapper.config_path
            dry_run = pipeline_wrapper.dry_run

            for collection_index, (collection_name, collection_wrapper, collection_config) in enumerate(
                zip(collection_names, collection_wrappers, collection_configs, strict=False),
                start=1,
            ):
                if collection_wrapper is not None:
                    collection_data_dir = collection_wrapper.get_pipeline_data_dir(pipeline_name)

                    # Zero-pad process, pipeline and collection indices
                    padded_process_index = f"{process_index:0{process_padding_length}}"
                    padded_pipeline_index = f"{pipeline_index:0{pipeline_padding_length}}"
                    padded_collection_index = f"{collection_index:0{collection_padding_length}}"

                    log_string_prefix = (
                        f"Process {padded_process_index} | "
                        f'Pipeline {padded_pipeline_index} "{pipeline_name}" | '
                        f'Collection {padded_collection_index} "{collection_name}" | '
                    )

                    futures[
                        executor.submit(
                            execute_packaging,
                            pipeline_name,
                            root_dir,
                            repo_dir,
                            collection_name,
                            collection_data_dir,
                            collection_config,
                            config_path,
                            dry_run,
                            log_string_prefix,
                            merged_kwargs,
                        )
                    ] = (pipeline_name, collection_name, log_string_prefix)
                    process_index += 1

        return futures

    def compose(
        self,
        dataset_name: str,
        collection_names: list[str],
        pipeline_names: list[str],
        extra_args: list[str] | None = None,
        **kwargs: dict[str, Any],
    ) -> dict[str, dict[Path, tuple[Path, ImageData | None, dict[str, Any] | None]]]:
        """
        Compose a dataset for given collections across multiple pipelines.

        This function creates a dataset by composing data from specified collections using multiple pipelines. It
        handles the process of merging configurations, setting up multiprocessing, and executing composition tasks for
        each pipeline-collection pair.

        Args:
            dataset_name: A string representing the name of the dataset to be created.
            collection_names: A list of strings containing the names of the collections to compose.
            pipeline_names: A list of strings containing the names of the pipelines to use for composition.
            extra_args: An optional list of strings containing extra CLI arguments to pass to the command.
            **kwargs: Additional keyword arguments to pass to the command.

        Returns:
            A nested dictionary where the outer key is the pipeline name, and the inner key is the output file path.
            The value is a tuple containing the input file path, image data (or None), and additional metadata
            (or None).

        Raises:
            FileExistsError: If the dataset directory already exists.
            ProjectWrapper.NoSuchCollectionError: If a specified collection does not exist in the project.
            ProjectWrapper.CompositionError: If a pipeline fails to compose its data for a collection.
        """
        # Check that the dataset directory doesn't already exist
        dataset_root_dir = self.datasets_dir / dataset_name
        if dataset_root_dir.is_dir():
            raise FileExistsError(dataset_root_dir)

        # Get the collection wrappers
        collection_wrappers: list[CollectionWrapper] = []
        for collection_name in collection_names:
            collection_wrapper = self.collection_wrappers.get(collection_name, None)
            if collection_wrapper is None:
                raise ProjectWrapper.NoSuchCollectionError(collection_name)
            collection_wrappers.append(collection_wrapper)

        # Load the collection configs and get the merged keyword arguments
        collection_configs = [collection_wrapper.load_config() for collection_wrapper in collection_wrappers]
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        pretty_pipelines = ", ".join(f'"{p!s}"' for p, _ in self.pipeline_wrappers.items())
        pretty_collections = ", ".join(f'"{c!s}"' for c in collection_names)
        pipeline_label = "pipeline" if len(self.pipeline_wrappers) == 1 else "pipelines"
        collection_label = "collection" if len(collection_wrappers) == 1 else "collections"
        self.logger.debug(
            f'Packaging dataset "{dataset_name}" for {pipeline_label} {pretty_pipelines} and {collection_label} '
            f"{pretty_collections} with kwargs {merged_kwargs}",
        )

        total_processes = len(self.pipeline_wrappers) * len(collection_wrappers)

        self.logger.debug(
            f"Setting up multiprocessing to run {total_processes} independent process"
            f"{'es' if total_processes != 1 else ''}, made up of {len(self.pipeline_wrappers)} pipeline"
            f"{'s' if len(self.pipeline_wrappers) != 1 else ''} and {len(collection_wrappers)} collection"
            f"{'s' if len(collection_wrappers) != 1 else ''} per pipeline.",
        )

        dataset_mapping: dict[str, dict[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]]] = {}

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            total_task_length = len(self.pipeline_wrappers) * len(collection_wrappers)
            task = progress.add_task("[green]Composing data (1/11)", total=total_task_length)

            with ProcessPoolExecutor() as executor:
                futures = self._create_composition_tasks(
                    executor,
                    pipeline_names,
                    collection_names,
                    collection_wrappers,
                    collection_configs,
                    merged_kwargs,
                )

                for future in as_completed(futures):
                    pipeline_name, collection_name, log_string_prefix = futures[future]
                    try:
                        (pipeline_data_mapping, message) = future.result()
                        self.logger.debug(f"{log_string_prefix} - {message}")
                        if pipeline_name not in dataset_mapping:
                            dataset_mapping[pipeline_name] = {}
                        dataset_mapping[pipeline_name].update(pipeline_data_mapping)
                    except Exception as e:
                        raise ProjectWrapper.CompositionError(
                            f"{log_string_prefix} - "
                            f'Pipeline "{pipeline_name}" failed to compose its data for collection '
                            f'"{collection_name}":\n{e}',
                        ) from e
                    finally:
                        progress.advance(task)

        return dataset_mapping

    def create_dataset(
        self,
        dataset_name: str,
        dataset_mapping: dict[str, dict[Path, tuple[Path, list[ImageData] | None, dict[str, Any] | None]]],
        operation: Operation = Operation.copy,
        version: str | None = "1.0",
        contact_name: str | None = None,
        contact_email: str | None = None,
        zoom: int | None = None,
    ) -> DatasetWrapper:
        """
        Create a Marimba dataset from a dataset mapping.

        Args:
            dataset_name: The name of the dataset to be created.
            dataset_mapping: A dictionary containing the dataset mapping information.
            operation: The operation to perform on files (copy, move or link). Defaults to Operation.copy.
            version: The version of the dataset. Defaults to '1.0'.
            contact_name: The name of the contact person for the dataset. Defaults to None.
            contact_email: The email of the contact person for the dataset. Defaults to None.
            zoom: The zoom level for the dataset. Defaults to None.

        Returns:
            A DatasetWrapper instance representing the created dataset.

        Raises:
            - ProjectWrapper.NameError: If the provided dataset name is invalid.
            - FileExistsError: If the dataset root directory already exists.
            - DatasetWrapper.InvalidDatasetMappingError: If the provided dataset mapping is invalid.
            - DatasetWrapper.ManifestError: If the dataset is inconsistent with its manifest.
        """
        # Check the name is valid
        ProjectWrapper.check_name(dataset_name)

        # Create the dataset
        dataset_root_dir = self.datasets_dir / dataset_name
        dataset_wrapper = DatasetWrapper.create(
            dataset_root_dir,
            version=version,
            contact_name=contact_name,
            contact_email=contact_email,
            dry_run=self.dry_run,
        )

        # Populate it
        dataset_wrapper.populate(
            dataset_name,
            dataset_mapping,
            self.pipelines_dir,
            self.log_path,
            (pw.log_path for pw in self.pipeline_wrappers.values()),
            operation=operation,
            zoom=zoom,
        )

        # Validate it
        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            globbed_files = list(dataset_wrapper.root_dir.glob("**/*"))
            task = progress.add_task("[green]Validating dataset (11/11)", total=len(globbed_files))
            dataset_wrapper.validate(dataset_name, progress, task)
            progress.advance(task)

        self._dataset_wrappers[dataset_name] = dataset_wrapper

        return dataset_wrapper

    def delete_dataset(
        self,
        dataset_name: str,
        dry_run: bool,
    ) -> Path:
        """
        Delete a Marimba dataset.

        Args:
            dataset_name: The name of the dataset.
            dry_run: Whether to run in dry-run mode.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
            FileExistsError: If the dataset root directory does not exist.
        """
        # Check the name is valid
        ProjectWrapper.check_name(dataset_name)

        # Create the dataset
        dataset_root_dir = self.datasets_dir / dataset_name
        if dataset_root_dir.exists():
            if not self.dry_run:
                remove_directory_tree(dataset_root_dir, "dataset", dry_run)
        else:
            raise FileExistsError(f'"{dataset_root_dir}" dataset does not exist.')
        return dataset_root_dir

    def create_target(
        self,
        target_name: str,
        target_type: str,
        target_config: dict[str, Any],
    ) -> DistributionTargetWrapper:
        """
        Create a Marimba distribution target.

        Args:
            target_name: The name of the distribution target.
            target_type:
                The type of distribution target to create. See `DistributionTargetWrapper.CLASS_MAP` for valid types.
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

        # Ensure that instance is of type DistributionTargetWrapper
        if target_wrapper is None or not isinstance(target_wrapper, DistributionTargetWrapper):
            raise ValueError("Expected a DistributionTargetWrapper instance")

        self._target_wrappers[target_name] = target_wrapper

        return target_wrapper

    def delete_target(
        self,
        target_name: str,
        dry_run: bool,
    ) -> Path:
        """
        Delete a Marimba distribution target file.

        Args:
            target_name: The name of the distribution target to delete.
            dry_run: Whether to run in dry-run mode.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
            FileExistsError: If the target not found.
        """
        # Check the name is valid
        ProjectWrapper.check_name(target_name)
        target_config_path = self.targets_dir / f"{target_name}.yml"
        if target_config_path.exists():
            if not dry_run:
                target_config_path.unlink()
        else:
            raise FileExistsError(f'"{target_config_path}"target does not exist.')
        self.logger.debug(f'Deleting distribution target "{target_name}"')
        return target_config_path

    def distribute(self, dataset_name: str, target_name: str) -> None:
        """
        Distribute a dataset to a distribution target.

        Args:
            dataset_name: The name of the dataset to distribute.
            target_name: The name of the distribution target to distribute to.

        Raises:
            ProjectWrapper.NoSuchDatasetError: If the dataset does not exist in the project.
            ProjectWrapper.NoSuchTargetError: If the distribution target does not exist in the project.
            DatasetWrapper.ManifestError: If the dataset is inconsistent with its manifest.
            DistributionTargetBase.DistributionError: If the dataset cannot be distributed.
        """
        self.logger.debug(f'Distributing dataset "{dataset_name}" to target "{target_name}"')

        # Get the dataset wrapper
        dataset_wrapper = self.dataset_wrappers.get(dataset_name, None)
        if dataset_wrapper is None:
            raise ProjectWrapper.NoSuchDatasetError(dataset_name)

        # Validate the dataset
        dataset_wrapper.validate(dataset_name)

        # Get the distribution target wrapper
        target_wrapper = self.target_wrappers.get(target_name, None)
        if target_wrapper is None:
            raise ProjectWrapper.NoSuchTargetError(target_name)

        # Get the distribution target instance
        distribution_target = target_wrapper.get_instance()

        # Check if distribution_target is not None
        if distribution_target is None:
            self.logger.exception("Failed to get a valid distribution target instance.")
            return

        # Distribute the dataset
        distribution_target.distribute(dataset_wrapper)

    def run_import(
        self,
        collection_name: str,
        source_paths: list[Path],
        pipeline_names: list[str],
        extra_args: list[str] | None = None,
        operation: Operation = Operation.copy,
    ) -> None:
        """
        Run the import command to populate a collection from a source data directory.

        May overwrite existing data in the collection.

        Args:
            collection_name: The name of the collection to import into.
            source_paths: The source paths to import from.
            pipeline_names: Names of the pipelines to run
            extra_args: Any extra CLI arguments to pass to the command.
            operation: The operation to perform on files (copy, move or link). Defaults to Operation.copy.

        Raises:
            ProjectWrapper.NoSuchCollectionError: If the collection does not exist in the project.
        """
        operation_dict = {"operation": operation.value}
        merged_kwargs = get_merged_keyword_args(operation_dict, extra_args, self.logger)

        # Get the collection wrapper
        collection_wrapper = self.collection_wrappers.get(collection_name, None)
        if collection_wrapper is None:
            raise ProjectWrapper.NoSuchCollectionError(collection_name)

        pretty_paths = ", ".join(str(Path(p).resolve().absolute()) for p in source_paths)
        self.logger.debug(
            f'Importing data for collection "{collection_name}" from source path(s) '
            f"{pretty_paths} with kwargs {merged_kwargs}",
        )

        pipeline_wrappers_to_run, _ = self._get_wrappers_to_run(pipeline_names, [])

        num_pipelines = len(pipeline_wrappers_to_run)
        num_sources = len(source_paths)
        total_processes = num_pipelines * num_sources

        self.logger.debug(
            f"Setting up multiprocessing to run {total_processes} independent process"
            f"{'es' if total_processes != 1 else ''}, made up of {num_pipelines} pipeline"
            f"{'s' if num_pipelines != 1 else ''} and {num_sources} source"
            f"{'s' if num_sources != 1 else ''} per pipeline.",
        )

        process_padding_length = math.ceil(math.log10(total_processes + 1))
        pipeline_padding_length = math.ceil(math.log10(num_pipelines + 1))
        source_padding_length = math.ceil(math.log10(num_sources + 1))

        with Progress(SpinnerColumn(), *get_default_columns()) as progress:
            tasks_by_pipeline_name = {
                pipeline_name: progress.add_task(
                    f"[green]Importing data for pipeline {pipeline_name}",
                    total=num_pipelines,
                )
                for pipeline_name in pipeline_wrappers_to_run
            }

            with ProcessPoolExecutor() as executor:
                futures = {}
                process_index = 1

                for pipeline_index, (pipeline_name, pipeline_wrapper) in enumerate(
                    pipeline_wrappers_to_run.items(),
                    start=1,
                ):
                    root_dir = pipeline_wrapper.root_dir
                    repo_dir = pipeline_wrapper.repo_dir
                    config_path = pipeline_wrapper.config_path
                    dry_run = pipeline_wrapper.dry_run
                    collection_data_dir = collection_wrapper.get_pipeline_data_dir(pipeline_name)
                    collection_config = collection_wrapper.load_config()

                    for source_index, source_path in enumerate(source_paths, start=1):
                        # Zero-pad process, pipeline and source indices
                        padded_process_index = f"{process_index:0{process_padding_length}}"
                        padded_pipeline_index = f"{pipeline_index:0{pipeline_padding_length}}"
                        padded_source_index = f"{source_index:0{source_padding_length}}"

                        log_string_prefix = (
                            f"Process {padded_process_index} | "
                            f'Pipeline {padded_pipeline_index} "{pipeline_name}" | '
                            f"Source {padded_source_index} | "
                        )

                        futures[
                            executor.submit(
                                execute_import,
                                pipeline_name,
                                root_dir,
                                repo_dir,
                                config_path,
                                dry_run,
                                collection_data_dir,
                                collection_config,
                                Path(source_path),
                                log_string_prefix,
                                merged_kwargs,
                            )
                        ] = (pipeline_name, log_string_prefix)
                        process_index += 1

                for future in as_completed(futures):
                    pipeline_name, log_string_prefix = futures[future]
                    try:
                        message = future.result()
                        self.logger.debug(f"{log_string_prefix} - {message}")
                    except Exception as e:
                        raise ProjectWrapper.MarimbaThreadError(f"{log_string_prefix} - {e}") from e
                    finally:
                        progress.advance(tasks_by_pipeline_name[pipeline_name])

    def prompt_collection_config(
        self,
        parent_collection_name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Prompt the user for a collection configuration using predefined schemas and optional parent collection settings.

        Args:
            parent_collection_name (Optional[str]): The name of the parent collection. Defaults to the last modified
            collection if unspecified.
            config (Optional[Dict[str, Any]]): Initial configuration values, if any.

        Returns:
            Dict[str, Any]: The complete collection configuration.

        Raises:
            NoSuchCollectionError: Raised if the specified parent collection does not exist in the project.
        """
        resolved_collection_schema = self._get_unified_collection_schema()
        parent_collection_name = self._resolve_parent_collection_name(parent_collection_name)
        self._update_schema_with_parent_config(resolved_collection_schema, parent_collection_name)
        return self._collect_final_config(resolved_collection_schema, config)

    def _get_unified_collection_schema(self) -> dict[str, Any]:
        """Aggregate collection config schemas from all pipelines in the project."""
        schema = {}
        for pipeline_wrapper in self.pipeline_wrappers.values():
            pipeline = pipeline_wrapper.get_instance()
            schema.update(pipeline.get_collection_config_schema())
        return schema

    def _resolve_parent_collection_name(self, parent_collection_name: str | None) -> str | None:
        """Determine the appropriate parent collection name if not specified."""
        if parent_collection_name is None:
            parent_collection_name = self._get_last_modified_collection_name()
            if parent_collection_name:
                self.logger.info(f'Using last collection "{parent_collection_name}" as parent')
        return parent_collection_name

    def _get_last_modified_collection_name(self) -> str | None:
        """Fetch the name of the last modified collection."""
        if not self.collection_wrappers:
            return None
        return max(self.collection_wrappers, key=lambda k: self.collection_wrappers[k].root_dir.stat().st_mtime)

    def _update_schema_with_parent_config(self, schema: dict[str, Any], parent_collection_name: str | None) -> None:
        """Update the schema based on the configuration of the parent collection, if applicable."""
        if parent_collection_name:
            parent_wrapper = self.collection_wrappers.get(parent_collection_name)
            if parent_wrapper is None:
                raise ProjectWrapper.NoSuchCollectionError(parent_collection_name)
            parent_config = parent_wrapper.load_config()
            schema.update(parent_config)
            self.logger.debug(f'Using parent collection "{parent_collection_name}" with config: {parent_config}')

    def _collect_final_config(
        self,
        schema: dict[str, Any],
        provided_config: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Combine the user-provided config with additional prompted entries from the schema."""
        final_config = provided_config or {}
        # Prepopulate with existing config and remove keys that will not be prompted
        for key in list(schema.keys()):
            if key in final_config:
                del schema[key]  # Remove the key so it won't be prompted

        # Prompt for additional configuration and update
        if schema:
            additional_config = prompt_schema(schema)
            if additional_config:  # Ensure additional_config is not None
                final_config.update(additional_config)

        self.logger.debug(f"Final prompted collection config: {final_config}")
        return final_config

    def update_pipelines(self) -> None:
        """
        Update all pipelines in the project.
        """
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            self.logger.info(f'Updating pipeline "{pipeline_name}"')
            try:
                pipeline_wrapper.update()
                self.logger.info(f'Successfully updated pipeline "{pipeline_name}"')
            # TODO @<cjackett>: Raise these exceptions and handle in marimba.py
            except (OSError, ValueError) as e:
                self.logger.exception(f'Failed to update pipeline "{pipeline_name}" due to an I/O or value error: {e}')
            except Exception as e:
                self.logger.exception(f'Failed to update pipeline "{pipeline_name}" due to an unexpected error: {e}')

    def install_pipelines(self) -> None:
        """
        Install all pipelines dependencies in the project into the current environment.
        """
        for pipeline_name, pipeline_wrapper in self.pipeline_wrappers.items():
            self.logger.info(f'Installing pipeline "{pipeline_name}"')
            try:
                pipeline_wrapper.install()
                self.logger.info(f'Successfully installed pipeline "{pipeline_name}"')
            except PipelineWrapper.InstallError:
                self.logger.exception(f'Failed to install pipeline "{pipeline_name}"')

    @property
    def pipeline_wrappers(self) -> dict[str, PipelineWrapper]:
        """
        The loaded pipeline wrappers in the project.
        """
        return self._pipeline_wrappers

    @property
    def collection_wrappers(self) -> dict[str, CollectionWrapper]:
        """
        The loaded collection wrappers in the project.
        """
        return self._collection_wrappers

    @property
    def dataset_wrappers(self) -> dict[str, DatasetWrapper]:
        """
        The loaded dataset wrappers in the project.
        """
        return self._dataset_wrappers

    @property
    def target_wrappers(self) -> dict[str, DistributionTargetWrapper]:
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
    def pipelines_dir(self) -> Path:
        """
        The pipelines directory of the project.
        """
        pipelines_dir = self.root_dir / "pipelines"
        pipelines_dir.mkdir(exist_ok=True)
        return pipelines_dir

    @property
    def collections_dir(self) -> Path:
        """
        The collections directory of the project.
        """
        collections_dir = self.root_dir / "collections"
        collections_dir.mkdir(exist_ok=True)
        return collections_dir

    @property
    def datasets_dir(self) -> Path:
        """
        The datasets directory of the project.
        """
        distributions_dir = self.root_dir / "datasets"
        distributions_dir.mkdir(exist_ok=True)
        return distributions_dir

    @property
    def marimba_dir(self) -> Path:
        """
        The Marimba directory of the project.
        """
        marimba_dir = self.root_dir / ".marimba"
        marimba_dir.mkdir(exist_ok=True)
        return marimba_dir

    @property
    def targets_dir(self) -> Path:
        """
        The distribution targets directory of the project.
        """
        targets_dir = self.root_dir / "targets"
        targets_dir.mkdir(exist_ok=True)
        return targets_dir

    @property
    def log_path(self) -> Path:
        """
        The path to the project log file.
        """
        return self.root_dir / "project.log"

    @property
    def name(self) -> str:
        """
        The name of the project.
        """
        return self.root_dir.name

    @property
    def dry_run(self) -> bool:
        """
        Whether the project is in dry-run mode.
        """
        return self._dry_run

    @staticmethod
    def check_name(name: str) -> None:
        """
        Check that a name is valid.

        Valid names include only alphanumeric, underscore and dash characters.

        Args:
            name: The name to check.

        Raises:
            ProjectWrapper.NameError: If the name is invalid.
        """
        for char in name:
            if not (char.isalnum() or char in ("_", "-")):
                raise ProjectWrapper.InvalidNameError(name)
