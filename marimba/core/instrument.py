from abc import ABC

from marimba.utils.log import LogMixin


class Instrument(ABC, LogMixin):
    """
    Instrument abstract base class. All instruments should inherit from this class.
    """

    def __init__(self, config: dict, dry_run: bool):
        self._config = config
        self._dry_run = dry_run

    @property
    def config(self) -> dict:
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

    @property
    def class_name(self) -> str:
        """
        The name of the instrument.
        """
        return self.__class__.__name__

    # def run_report(self):
    #     self.logger.warning(
    #         f'There is no MarImBA [bold]report[/bold] command implemented for instrument [bold]{self.instrument_config.get("id")}[/bold]'
    #     )
