from inspect import getfullargspec
from pathlib import Path
from typing import Tuple, Union

from rich.prompt import Prompt

from marimba.core.distribution.bases import DistributionTargetBase
from marimba.core.distribution.dap import CSIRODapDistributionTarget
from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.utils.config import load_config, save_config


class DistributionTargetWrapper:
    CLASS_MAP = {
        "s3": S3DistributionTarget,
        "dap": CSIRODapDistributionTarget,
    }

    class InvalidConfigError(Exception):
        """
        Raised when the configuration file is invalid.
        """

        pass

    def __init__(self, config_path: Union[str, Path]):
        self._config_path = Path(config_path)
        self._config = {}

        self._load_config()
        self._check_config()

    @classmethod
    def create(cls, config_path: Union[str, Path], target_type: str, target_args: dict) -> DistributionTargetBase:
        """
        Create a distribution target at the specified path with the specified type and arguments.

        Args:
            config_path: The path to the distribution target configuration file.
            target_type: The type of distribution target to create.
            target_args: The arguments to pass to the distribution target constructor.

        Returns:
            The distribution target wrapper instance.

        Raises:
            InvalidConfigError: If the configuration is invalid.
            FileExistsError: If a distribution target already exists at the specified path.
        """
        config_path = Path(config_path)

        # Check that the distribution target doesn't already exist
        if config_path.exists():
            raise FileExistsError(config_path)

        # Structure the config dict
        config = {"type": target_type, "config": target_args}

        # Save the config
        save_config(config_path, config)

        return cls(config_path)

    @staticmethod
    def prompt_target() -> Tuple[str, dict]:
        """
        Use Rich to prompt for a distribution target configuration.

        Returns:
            A tuple of the distribution target type and arguments.
        """
        # Prompt for the distribution target type
        target_type = Prompt.ask("Distribution target type", choices=DistributionTargetWrapper.CLASS_MAP.keys())

        # Get the distribution target class
        target_class = DistributionTargetWrapper.CLASS_MAP.get(target_type)

        # Get the distribution target __init__ positional and keyword arguments
        arg_spec = getfullargspec(target_class.__init__)
        positional_args = arg_spec.args[1:]  # exclude 'self'
        keyword_args = arg_spec.kwonlyargs
        defaults = arg_spec.defaults if arg_spec.defaults else []

        # Prepare the default values for the keyword arguments
        keyword_defaults = dict(zip(keyword_args[::-1], defaults[::-1]))

        def map_arg_name(arg_name: str) -> str:
            """
            Map an argument name to a Rich prompt choice.

            Args:
                arg_name: The argument name to map.

            Returns:
                The mapped argument name.
            """
            return arg_name.replace("_", " ").capitalize()

        target_args = {}
        # Prompt for values for the positional arguments
        for arg_name in positional_args:
            target_args[arg_name] = Prompt.ask(map_arg_name(arg_name))

        # Prompt for values for the keyword arguments, using the default values
        for arg_name in keyword_args:
            default_value = keyword_defaults.get(arg_name)
            target_args[arg_name] = Prompt.ask(map_arg_name(arg_name), default=default_value)

        return target_type, target_args

    def _check_config(self):
        """
        Validate the distribution target configuration.

        Raises:
            InvalidConfigError: If the configuration is invalid.
        """
        target_type = self.config.get("type", None)
        if target_type is None:
            raise DistributionTargetWrapper.InvalidConfigError("The distribution target configuration must specify a 'type'.")

        if target_type not in DistributionTargetWrapper.CLASS_MAP.keys():
            raise DistributionTargetWrapper.InvalidConfigError(
                f"Invalid distribution target type: {target_type}. Must be one of: {', '.join(DistributionTargetWrapper.CLASS_MAP.keys())}"
            )

        target_args = self.config.get("config", None)
        if target_args is None:
            raise DistributionTargetWrapper.InvalidConfigError("The distribution target configuration must specify a 'config'.")

    def _load_config(self):
        """
        Load the distribution target configuration file.
        """
        self._config = load_config(self.config_path)

    @property
    def config_path(self) -> Path:
        """
        The path to the distribution target configuration file.
        """
        return self._config_path

    @property
    def config(self) -> dict:
        """
        The distribution target configuration.
        """
        return self._config

    def get_instance(self) -> DistributionTargetBase:
        """
        Get an instance of the distribution target as specified by the config.

        Returns:
            The distribution target instance of the implementation specified by the config.
        """
        target_type = self.config.get("type")
        target_args = self.config.get("config")

        return DistributionTargetWrapper.CLASS_MAP.get(target_type)(**target_args)
