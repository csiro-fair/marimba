from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ifdo import iFDO

from marimba.utils.log import LogMixin


class BasePipeline(ABC, LogMixin):
    """
    MarImBA instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, config: Optional[dict] = None, dry_run: bool = False):
        self._config = config
        self._dry_run = dry_run

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        """
        Returns the instrument configuration schema.

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

        The instrument configuration schema represents values that are static for the instrument across all deployments.
        Use `get_deployment_config_schema` for values that are specific to a deployment.

        Returns:
            The instrument configuration schema.
        """
        return {}

    @staticmethod
    def get_deployment_config_schema() -> dict:
        """
        Returns the deployment configuration schema.

        The deployment configuration schema represents the values that the instrument requires that are specific to a deployment.
        Use `get_instrument_config_schema` for values that are static for the instrument across all deployments.

        See `get_instrument_config_schema` for more details.
        """
        return {}

    @property
    def config(self) -> Optional[dict]:
        """
        The instrument static configuration.
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
        The name of the instrument class.
        """
        return self.__class__.__name__

    def run_init(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the init command. Delegate to the private implementation method `_init`.

        Do not override this method. Override `_init` instead.

        Args:
            data_dir: The data directory.
            config: The deployment configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running [bold]init[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._init(self, data_dir, config, **kwargs)

    def run_import(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the import command. Delegate to the private implementation method `_import`.

        Do not override this method. Override `_import` instead.

        Args:
            data_dir: The data directory.
            config: The deployment configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running [bold]import[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._import(data_dir, config, **kwargs)

    def run_rename(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the rename command. Delegate to the private implementation method `_rename`.

        Do not override this method. Override `_rename` instead.

        Args:
            data_dir: The data directory.
            config: The deployment configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running [bold]rename[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._rename(data_dir, config, **kwargs)

    def run_process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the process command. Delegate to the private implementation method `_process`.

        Do not override this method. Override `_process` instead.

        Args:
            data_dir: The data directory.
            config: The deployment configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running [bold]process[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._process(data_dir, config, **kwargs)

    def run_metadata(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        Public interface for the metadata command. Delegate to the private implementation method `_metadata`.

        Do not override this method. Override `_metadata` instead.

        Args:
            data_dir: The data directory.
            config: The deployment configuration.
            kwargs: Additional keyword arguments.
        """
        self.logger.debug(
            f"Running [bold]metadata[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dir=}, {config=}, {kwargs=}"
        )
        return self._metadata(data_dir, config, **kwargs)

    def run_compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Tuple[iFDO, Dict[Path, Path]]:
        """
        Compose a dataset from the given data directories and their corresponding deployment configurations.

        Return an [iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) instance that represents the composed dataset and a dictionary that maps files within the provided data directories to relative paths for the resulting distributable dataset.

        Args:
            data_dirs: The data directories to compose.
            configs: The deployment configurations for the data directories.
            kwargs: Additional keyword arguments.

        Returns:
            The iFDO and path mapping dict.
        """
        self.logger.debug(
            f"Running [bold]compose[/bold] command for instrument [bold]{self.class_name}[/bold] with args: {data_dirs=}, {configs=}, {kwargs=}"
        )
        return self._compose(data_dirs, configs, **kwargs)

    def _init(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_init` implementation; override this to implement the init command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no MarImBA [bold]init[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def _import(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_import` implementation; override this to implement the import command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no MarImBA [bold]import[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def _rename(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_rename` implementation; override this.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_process` implementation; override this to implement the process command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def _metadata(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        """
        `run_metadata` implementation; override this to implement the metadata command.

        TODO: Add docs on how to implement this method.
        """
        self.logger.warning(f"There is no MarImBA [bold]init[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    @abstractmethod
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Tuple[iFDO, Dict[Path, Path]]:
        """
        `run_compose` implementation; override this.

        TODO: Add docs on how to implement this method.
        """
        raise NotImplementedError
