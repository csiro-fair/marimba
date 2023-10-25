from abc import ABC
from typing import Optional

from marimba.utils.log import LogMixin


class BaseInstrument(ABC, LogMixin):
    """
    MarImBA instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, config: Optional[dict] = None, dry_run: bool = False):
        self._config = config
        self._dry_run = dry_run

    @staticmethod
    def get_instrument_config_schema() -> dict:
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

    def run_init_or_import(self, command_name, kwargs):
        command = getattr(self, command_name)
        command(**kwargs)

    def run_catalog(self, data_dir, config, **kwargs):
        self.logger.warning(f"There is no MarImBA [bold]catalog[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def run_metadata(self, data_dir, config, **kwargs):
        self.logger.warning(f"There is no MarImBA [bold]metadata[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def run_package(self, data_dir, config, **kwargs):
        self.logger.warning(f"There is no MarImBA [bold]package[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def run_process(self, data_dir, config, **kwargs):
        self.logger.warning(f"There is no MarImBA [bold]process[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    def run_rename(self, data_dir, config, **kwargs):
        self.logger.warning(f"There is no MarImBA [bold]rename[/bold] command implemented for instrument [bold]{self.class_name}[/bold]")

    # def run_report(self):
    #     self.logger.warning(
    #         f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
    #     )
