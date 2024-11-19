"""
Loads a pipeline instance from a repository directory.

This module provides functionality to load a pipeline instance from a given repository directory. It searches for a
pipeline implementation file (ending with .pipeline.py) within the repository, imports the pipeline module, and
creates an instance of the pipeline class.

Imports:
    - sys: Provides access to system-specific parameters and functions.
    - module_from_spec, spec_from_file_location (from importlib.util): Utilities for loading modules from specs.
    - Path (from pathlib): Represents filesystem paths.
    - Type (from typing): Defines the Type[] type hint.
    - BasePipeline (from marimba.core.pipeline): The base class for pipeline implementations.
    - load_config (from marimba.core.utils.config): Loads the pipeline configuration.

Functions:
    - load_pipeline_instance(repo_dir: Path, config_path: Path, dry_run: bool) -> BasePipeline: Loads the pipeline
      instance from the given repository directory.
"""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from marimba.core.pipeline import BasePipeline
from marimba.core.utils.config import load_config
from marimba.core.utils.log import LogPrefixFilter, get_file_handler


def load_pipeline_instance(
    root_dir: Path,
    repo_dir: Path,
    pipeline_name: str,
    config_path: Path,
    dry_run: bool,
    log_string_prefix: str | None = None,
) -> BasePipeline:
    """
    Load a pipeline instance from a given repository directory.

    This function searches for a pipeline implementation file in the specified repository directory, imports the module,
    and instantiates the pipeline class. It also configures logging for the pipeline instance.

    Args:
        root_dir: The root directory for the pipeline.
        repo_dir: The repository directory containing the pipeline implementation.
        pipeline_name: The name of the pipeline.
        config_path: The path to the pipeline configuration file.
        dry_run: Boolean flag indicating whether to run in dry-run mode.
        log_string_prefix: Optional prefix for log messages.

    Returns:
        BasePipeline: An instance of the pipeline class.

    Raises:
        FileNotFoundError: If no pipeline implementation file is found or if multiple implementations are found.
        ImportError: If the pipeline module or class cannot be imported or instantiated.
    """
    # Find files that end with .pipeline.py in the repository
    pipeline_module_paths = list(repo_dir.glob("**/*.pipeline.py"))

    # Ensure there is one result
    if len(pipeline_module_paths) == 0:
        raise FileNotFoundError(f'No pipeline implementation found in "{repo_dir}".')
    if len(pipeline_module_paths) > 1:
        raise FileNotFoundError(f'Multiple pipeline implementations found in "{repo_dir}": {pipeline_module_paths}.')
    pipeline_module_path = pipeline_module_paths[0]

    pipeline_module_name = pipeline_module_path.stem
    pipeline_module_spec = spec_from_file_location(
        pipeline_module_name,
        str(pipeline_module_path.absolute()),
    )

    if pipeline_module_spec is None:
        raise ImportError(f"Could not load spec for {pipeline_module_name} from {pipeline_module_path}")

    # Create the pipeline module
    pipeline_module = module_from_spec(pipeline_module_spec)

    # Enable repo-relative imports by temporarily adding the repository directory to the module search path
    sys.path.insert(0, str(repo_dir.absolute()))

    # Ensure that loader is not None before executing the module
    if pipeline_module_spec.loader is None:
        raise ImportError(f"Could not find loader for {pipeline_module_name} from {pipeline_module_path}")

    # Execute it
    pipeline_module_spec.loader.exec_module(pipeline_module)

    # Remove the repository directory from the module search path to avoid conflicts
    sys.path.pop(0)

    # Find any BasePipeline implementations
    pipeline_class: type[BasePipeline] | None = None
    for obj in pipeline_module.__dict__.values():
        try:
            if isinstance(obj, type) and issubclass(obj, BasePipeline) and obj is not BasePipeline:
                pipeline_class = obj
                break
        except TypeError:
            continue

    if pipeline_class is None:
        raise ImportError("Pipeline class has not been set or could not be found.")

    # Create an instance of the pipeline
    pipeline_instance = pipeline_class(repo_dir, config=load_config(config_path), dry_run=dry_run)

    # Add the log string prefix to the logger
    if log_string_prefix:
        prefix_filter = LogPrefixFilter(log_string_prefix)
        pipeline_instance.logger.addFilter(prefix_filter.apply_prefix)

    # Add a file handler to accumulate pipeline logs
    file_handler = get_file_handler(root_dir, pipeline_name, dry_run)
    pipeline_instance.logger.addHandler(file_handler)

    return pipeline_instance
