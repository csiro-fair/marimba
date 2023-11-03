from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from marimba.core.metadata import BaseMetadata
from marimba.core.utils.log import LogMixin
from marimba.core.utils.rich import format_command, format_entity


class BasePipeline(ABC, LogMixin):
    """
    Marimba pipeline abstract base class. All pipelines should inherit from this class.
    """

    def __init__(self, root_path: Union[str, Path], config: Optional[dict] = None, dry_run: bool = False):
        self._root_path = root_path
        self._config = config
        self._dry_run = dry_run

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        """
        Returns the pipeline configuration schema.

        The returned dictionary should be a flat map of key -> default value.
        All keys must be strings, and values must be YAML-serializable.
        Nested dictionaries and lists are not supported
        The value types will be used to infer the type of the configuration value when prompting the user for input.
        For example:
        ```python
        {
            "name": "my_camera",        # str
            "my_internal_id": 0,        # int
            "focal_length_x": 1.0,      # float
            ...
        }
        ```

        The pipeline configuration schema represents values that are static for the pipeline across all collections.
        Use `get_collection_config_schema` for values that are specific to a collection.

        Returns:
            The pipeline configuration schema.
        """
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict:
        """
        Returns the collection configuration schema.

        The collection configuration schema represents the values that the pipeline requires that are specific to a collection.
        Use `get_pipeline_config_schema` for values that are static for the pipeline across all collections.

        See `get_pipeline_config_schema` for more details.
        """
        return {}

    @property
    def config(self) -> Optional[dict]:
        """
        The pipeline static configuration.
        """
        return self._config

    @property
    def dry_run(self) -> bool:
        """
        Whether or not to perform a dry run.
        """
        return self._dry_run

    @property
    def class_name(self) -> str:
        """
        The name of the pipeline class.
        """
        return self.__class__.__name__

    def run_import(self, data_dir: Path, source_paths: List[Path], config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the import command. Delegate to the private implementation method `_import`.

        Do not override this method. Override `_import` instead.

        Args:
            data_dir: The data directory.
            source_paths: The source paths.
            config: The collection configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running {format_command('import')} command for pipeline {format_entity(self.class_name)} with args: {data_dir=}, {source_paths=}, {config=}, {kwargs=}"
        )
        return self._import(data_dir, source_paths, config, **kwargs)

    def run_process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the process command. Delegate to the private implementation method `_process`.

        Do not override this method. Override `_process` instead.

        Args:
            data_dir: The data directory.
            config: The collection configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running {format_command('process')} command for pipeline {format_entity(self.class_name)} with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._process(data_dir, config, **kwargs)

    def run_compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Dict[Path, Tuple[Path, List[BaseMetadata]]]:
        """
        Compose a dataset from the given data directories and their corresponding collection configurations.

        Args:
            data_dirs: The data directories to compose.
            configs: The collection configurations for the data directories.
            kwargs: Additional keyword arguments.

        Returns:
            The pipeline data mapping.
        """
        self.logger.debug(
            f"Running {format_command('compose')} command for pipeline {format_entity(self.class_name)} with args: {data_dirs=}, {configs=}, {kwargs=}"
        )
        return self._compose(data_dirs, configs, **kwargs)

    def _import(self, data_dir: Path, source_paths: List[Path], config: Dict[str, Any], **kwargs: dict):
        """
        `run_import` implementation; override this to implement the import command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no Marimba {format_command('import')} command implemented for pipeline {format_entity(self.class_name)}")

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_process` implementation; override this to implement the process command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no Marimba {format_command('process')} command implemented for pipeline {format_entity(self.class_name)}")

    @abstractmethod
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Dict[Path, Tuple[Path, List[BaseMetadata]]]:
        """
        `run_compose` implementation; override this.

        TODO: Add docs on how to implement this method.
        """
        raise NotImplementedError
