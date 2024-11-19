"""
Marimba Core Target Wrapper Module.

The distribution_target_wrapper module provides a wrapper class for creating and managing distribution targets. It
allows for the creation of distribution target instances based on a given configuration file, as well as interactive
prompting for creating a new distribution target configuration.

Imports:
    - getfullargspec, isclass from inspect: Used for introspecting distribution target classes.
    - Path from pathlib: Used for handling file paths.
    - FunctionType from types: Used for type checking.
    - Any, Dict, Optional, Tuple, Union, cast from typing: Used for type hinting.
    - Prompt from rich.prompt: Used for interactive prompting.
    - DistributionTargetBase from marimba.core.distribution.bases: Base class for distribution targets.
    - CSIRODapDistributionTarget from marimba.core.distribution.dap: CSIRO DAP distribution target implementation.
    - S3DistributionTarget from marimba.core.distribution.s3: S3 distribution target implementation.
    - load_config, save_config from marimba.core.utils.config: Used for loading and saving configuration files.

Classes:
    - DistributionTargetWrapper: A wrapper class for creating and managing distribution targets.
        - InvalidConfigError: Raised when the configuration file is invalid.

Functions:
    - prompt_target: Use Rich to prompt for a distribution target configuration.
"""

from inspect import getfullargspec, isclass
from pathlib import Path
from types import FunctionType
from typing import Any, ClassVar, cast

from rich.prompt import Prompt

from marimba.core.distribution.bases import DistributionTargetBase
from marimba.core.distribution.dap import CSIRODapDistributionTarget
from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.utils.config import load_config, save_config


class DistributionTargetWrapper:
    """
    A wrapper class for creating and managing distribution targets.

    The DistributionTargetWrapper class provides methods for creating and managing distribution targets. It allows
    for the creation of distribution target instances based on a given configuration file, as well as interactive
    prompting for creating a new distribution target configuration.

    Attributes:
        CLASS_MAP (Dict[str, Type[DistributionTargetBase]]): A mapping of distribution target types to their
        respective classes.

    Raises:
        InvalidConfigError: Raised when the configuration file is invalid.

    """

    CLASS_MAP: ClassVar[dict[str, type]] = {
        "s3": S3DistributionTarget,
        "dap": CSIRODapDistributionTarget,
    }

    class InvalidConfigError(Exception):
        """
        Raised when the configuration file is invalid.
        """

    def __init__(self, config_path: str | Path) -> None:
        """
        Initialise the class instance.

        Args:
            config_path (Union[str, Path]): The path to the configuration file.
        """
        self._config_path = Path(config_path)
        self._config: dict[str, Any] = {}

        self._load_config()
        self._check_config()

    @classmethod
    def create(
        cls,
        config_path: str | Path,
        target_type: str,
        target_args: dict[str, Any],
    ) -> "DistributionTargetWrapper":
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
    def prompt_target() -> tuple[str, dict[str, Any]]:
        """
        Use Rich to prompt for a distribution target configuration.

        Returns:
            A tuple of the distribution target type and arguments.
        """
        # Convert dict_keys to list for choices
        choices = list(DistributionTargetWrapper.CLASS_MAP.keys())

        # Prompt for the distribution target type
        target_type = Prompt.ask("Distribution target type", choices=choices)

        # Get the distribution target class
        target_class = DistributionTargetWrapper.CLASS_MAP.get(target_type)
        if target_class is None:
            raise ValueError(f"No target class found for type {target_type}")

        # Ensure that target_class is indeed a class
        if not isclass(target_class):
            raise TypeError(f"Target class for type {target_type} is not a class")

        # Check if __init__ method exists
        if not hasattr(target_class, "__init__"):
            raise TypeError(f"Target class {target_type} does not have an __init__ method")

        # Ensure that target_class.__init__ is a method
        if not isinstance(target_class.__init__, FunctionType):  # type: ignore[attr-defined]
            raise TypeError(f"__init__ of target class {target_type} is not a method")

        # Get the distribution target __init__ positional and keyword arguments
        arg_spec = getfullargspec(target_class)
        positional_args = arg_spec.args[1:]  # exclude 'self'
        keyword_args = arg_spec.kwonlyargs
        defaults = arg_spec.defaults if arg_spec.defaults else []

        # Prepare the default values for the keyword arguments
        keyword_defaults = dict(zip(keyword_args[::-1], defaults[::-1], strict=False))

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
            default_value = str(keyword_defaults.get(arg_name))
            target_args[arg_name] = Prompt.ask(map_arg_name(arg_name), default=default_value)

        return target_type, target_args

    def _check_config(self) -> None:
        """
        Validate the distribution target configuration.

        Raises:
            InvalidConfigError: If the configuration is invalid.
        """
        target_type = self.config.get("type", None)
        if target_type is None:
            raise DistributionTargetWrapper.InvalidConfigError(
                "The distribution target configuration must specify a 'type'.",
            )

        if target_type not in DistributionTargetWrapper.CLASS_MAP:
            raise DistributionTargetWrapper.InvalidConfigError(
                f"Invalid distribution target type: {target_type}. Must be one of: "
                f"{', '.join(DistributionTargetWrapper.CLASS_MAP.keys())}",
            )

        target_args = self.config.get("config", None)
        if target_args is None:
            raise DistributionTargetWrapper.InvalidConfigError(
                "The distribution target configuration must specify a 'config'.",
            )

    def _load_config(self) -> None:
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
    def config(self) -> dict[str, Any]:
        """
        The distribution target configuration.
        """
        return self._config

    def get_instance(self) -> DistributionTargetBase | None:
        """
        Get an instance of the distribution target as specified by the config.

        Returns:
            The distribution target instance of the implementation specified by the config.
        """
        target_type = self.config.get("type")
        target_args = self.config.get("config")

        if isinstance(target_type, str) and isinstance(target_args, dict):
            target_class = DistributionTargetWrapper.CLASS_MAP.get(target_type)
            if target_class:
                # Use cast to assure Mypy of the return type
                return cast(DistributionTargetBase, target_class(**target_args))
        return None
