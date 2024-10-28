"""
Marimba Pipeline Abstract Base Class Module.

The `BasePipeline` class is an abstract base class that all Marimba pipelines should inherit from. It provides a
standard interface for implementing pipelines and includes methods for running import, process, and compose commands.

Imports:
    - ABC: Abstract base class from the `abc` module.
    - abstractmethod: Decorator for declaring abstract methods from the `abc` module.
    - Path: Class for representing file system paths from the `pathlib` module.
    - Any, Dict, List, Optional, Tuple, Union: Type hinting classes from the `typing` module.
    - ImageData: Class for representing image data from the `ifdo.models` module.
    - LogMixin: Mixin class for logging from the `marimba.core.utils.log` module.
    - format_command, format_entity: Functions for formatting command and entity names from the
      `marimba.core.utils.rich` module.

Classes:
    - BasePipeline: Abstract base class for Marimba pipelines.

"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from ifdo.models import ImageData

from marimba.core.utils.log import LogMixin
from marimba.core.utils.rich import format_command, format_entity


class BasePipeline(ABC, LogMixin):
    """
    Marimba pipeline abstract base class. All pipelines should inherit from this class.
    """

    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Initialise the class instance.

        Args:
            root_path (Union[str, Path]): The root path where the object will work.
            config (Optional[Dict[str, Any]]): The configuration settings for the object. Defaults to None.
            dry_run (bool): Whether to perform a dry run or not. Defaults to False.
        """
        self._root_path = root_path
        self._config = config
        self._dry_run = dry_run

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        """
        Return the pipeline configuration schema.

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
    def get_collection_config_schema() -> dict[str, Any]:
        """
        Return the collection configuration schema.

        The collection configuration schema represents the values that the pipeline requires that are specific to a
        collection. Use `get_pipeline_config_schema` for values that are static for the pipeline across all collections.

        See `get_pipeline_config_schema` for more details.
        """
        return {}

    @property
    def config(self) -> dict[str, Any] | None:
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

    def run_import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: dict[str, Any]) -> None:
        """
        Public interface for the import command. Delegate to the private implementation method `_import`.

        Do not override this method. Override `_import` instead.

        Args:
            data_dir: The data directory.
            source_path: The source path.
            config: The collection configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running {format_command('import')} command for pipeline {format_entity(self.class_name)} with args: "
            f"{data_dir=}, {source_path=}, {config=}, {kwargs=}",
        )

        # Check for the existence of the source_path directory
        if not source_path.is_dir():
            self.logger.exception(f"Source path {source_path} is not a directory")
            return None

        return self._import(data_dir, source_path, config, **kwargs)

    def run_process(self, data_dir: Path, config: dict[str, Any], **kwargs: dict[str, Any]) -> None:
        """
        Public interface for the process command. Delegate to the private implementation method `_process`.

        Do not override this method. Override `_process` instead.

        Args:
            data_dir: The data directory.
            config: The collection configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running {format_command('process')} command for pipeline {format_entity(self.class_name)} with args: "
            f"{data_dir=}, {config=}, {kwargs=}",
        )
        return self._process(data_dir, config, **kwargs)

    def run_package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> dict[Path, tuple[Path, ImageData | None, dict[str, Any] | None]]:
        """
        Package a dataset from the given data directories and their corresponding collection configurations.

        Return an [iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) instance that represents the composed
        dataset and a dictionary that maps files within the provided data directories to relative paths for the
        resulting distributable dataset.

        Args:
            data_dir: The data directory to compose.
            config: The collection configuration for the data directory.
            kwargs: Additional keyword arguments.

        Returns:
            The iFDO and path mapping dict.
        """
        self.logger.debug(
            f"Running {format_command('package')} command for pipeline {format_entity(self.class_name)} with args: "
            f"{data_dir=}, {config=}, {kwargs=}",
        )
        return self._package(data_dir, config, **kwargs)

    def _import(
        self,
        data_dir: Path,  # noqa: ARG002
        source_path: Path,  # noqa: ARG002
        config: dict[str, Any],  # noqa: ARG002
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """
        `run_import` implementation; override this to implement the import command.
        """
        self.logger.warning(
            f"There is no Marimba {format_command('import')} command implemented for pipeline "
            f"{format_entity(self.class_name)}",
        )

    def _process(
        self,
        data_dir: Path,  # noqa: ARG002
        config: dict[str, Any],  # noqa: ARG002
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """
        `run_process` implementation; override this to implement the process command.
        """
        self.logger.warning(
            f"There is no Marimba {format_command('process')} command implemented for pipeline "
            f"{format_entity(self.class_name)}",
        )

    @abstractmethod
    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> dict[Path, tuple[Path, ImageData | None, dict[str, Any] | None]]:
        """
        `run_compose` implementation; override this.

        TODO @<cjackett>: Add docs on how to implement this method.
        """
        raise NotImplementedError
