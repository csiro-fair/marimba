"""
Load a pipeline instance from a repository directory.

This module provides functionality to load and configure a pipeline instance from a given repository directory.
It includes methods for finding the pipeline implementation file, loading the module, instantiating the pipeline
class, and setting up logging for the pipeline instance.

Imports:
    sys: Provides access to some variables used or maintained by the Python interpreter.
    types: Provides runtime support for type hints.
    importlib.machinery: Provides the low-level import machinery used by importlib.
    importlib.util: Utility code for implementers of the import system.
    pathlib.Path: Offers classes representing filesystem paths.
    marimba.core.pipeline.BasePipeline: Base class for pipeline implementations.
    marimba.core.utils.config.load_config: Function to load configuration from a file.
    marimba.core.utils.log: Module containing logging utilities.

Functions:
    _find_pipeline_module_path: Find the pipeline implementation file in the repository.
    _log_empty_repo_warning: Log a warning message for an empty repository case.
    _load_pipeline_module: Load the pipeline module from the given path.
    _is_valid_pipeline_class: Check if an object is a valid pipeline class.
    _find_pipeline_class: Find the pipeline class in the module.
    _configure_pipeline_logging: Configure logging for the pipeline instance.
    load_pipeline_instance: Load a pipeline instance from a given repository directory.
"""

import sys
import types
from importlib import machinery
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from marimba.core.pipeline import BasePipeline
from marimba.core.utils.config import load_config
from marimba.core.utils.log import LogPrefixFilter, get_file_handler, get_logger


def _find_pipeline_module_path(
    repo_dir: Path,
    *,
    allow_empty: bool = False,
) -> Path | None:
    """Find the pipeline implementation file in the repository."""
    pipeline_module_paths = list(repo_dir.glob("**/*.pipeline.py"))

    if not pipeline_module_paths:
        if allow_empty:
            _log_empty_repo_warning(repo_dir)
            return None
        raise FileNotFoundError(
            f'No pipeline implementation found in "{repo_dir}". '
            f"The repository must contain a .pipeline.py file with a class that inherits from BasePipeline.",
        )

    if len(pipeline_module_paths) > 1:
        raise FileNotFoundError(
            f'Multiple pipeline implementations found in "{repo_dir}": {pipeline_module_paths}',
        )

    return pipeline_module_paths[0]


def _log_empty_repo_warning(repo_dir: Path) -> None:
    """Log warning message for empty repository case."""
    logger = get_logger("marimba.core.pipeline")
    logger.warning(
        f'Pipeline repository cloned to "{repo_dir}", '
        "but no Marimba Pipeline implementation was found.\n\n"
        "To implement your Pipeline:\n"
        "1. Create a file in your Pipeline repository ending in .pipeline.py\n"
        "2. Copy and paste into your new Pipeline file the Pipeline template from: "
        "https://raw.githubusercontent.com/csiro-fair/marimba/main/docs/templates/template.pipeline.py\n"
        "3. Rename the PipelineTemplate class to the name of your Pipeline\n"
        "4. Implement these required methods:\n"
        "   - get_pipeline_config_schema()\n"
        "   - get_collection_config_schema()\n"
        "   - _import()\n"
        "   - _process()\n"
        "   - _package()\n\n"
        "For detailed implementation instructions, please see the Pipeline Implementation Guide: "
        "https://github.com/csiro-fair/marimba/blob/main/docs/pipeline.md\n"
        "Your Pipeline will not be able to process data until these steps are completed.",
    )


def _load_pipeline_module(
    module_path: Path,
) -> tuple[str, types.ModuleType, machinery.ModuleSpec]:
    """Load the pipeline module from the given path."""
    module_name = module_path.stem
    module_spec = spec_from_file_location(
        module_name,
        str(module_path.absolute()),
    )

    if module_spec is None:
        raise ImportError(f"Could not load spec for {module_name} from {module_path}")

    if module_spec.loader is None:
        raise ImportError(f"Could not find loader for {module_name} from {module_path}")

    module = module_from_spec(module_spec)
    sys.modules[module_name] = module  # Register the module in sys.modules
    return module_name, module, module_spec


def _is_valid_pipeline_class(obj: type[object]) -> bool:
    """Check if an object is a valid pipeline class."""
    try:
        return isinstance(obj, type) and issubclass(obj, BasePipeline) and obj is not BasePipeline
    except TypeError:
        return False


def _find_pipeline_class(module: types.ModuleType) -> type[BasePipeline]:
    """Find the pipeline class in the module."""
    if not hasattr(module, "__dict__"):
        raise ImportError("Invalid module: module has no __dict__ attribute")

    for obj in module.__dict__.values():
        if isinstance(obj, type) and _is_valid_pipeline_class(obj):
            return obj  # type: ignore[return-value]  # We know it's a Type[BasePipeline] due to _is_valid_pipeline_class

    raise ImportError("Pipeline class has not been set or could not be found")


def _configure_pipeline_logging(
    pipeline_instance: BasePipeline,
    root_dir: Path,
    pipeline_name: str,
    dry_run: bool,
    log_string_prefix: str | None = None,
) -> None:
    """Configure logging for the pipeline instance."""
    # First remove any existing handlers to prevent duplication
    pipeline_instance.logger.handlers = []

    if log_string_prefix:
        prefix_filter = LogPrefixFilter(log_string_prefix)
        pipeline_instance.logger.addFilter(prefix_filter.apply_prefix)

    # Check if this handler already exists before adding
    file_handler = get_file_handler(root_dir, pipeline_name, dry_run)
    handler_paths = [h.baseFilename for h in pipeline_instance.logger.handlers if hasattr(h, "baseFilename")]
    if not any(h == file_handler.baseFilename for h in handler_paths):
        pipeline_instance.logger.addHandler(file_handler)


def load_pipeline_instance(
    root_dir: Path,
    repo_dir: Path,
    pipeline_name: str,
    config_path: Path,
    dry_run: bool,
    log_string_prefix: str | None = None,
    *,
    allow_empty: bool = False,
) -> BasePipeline | None:
    """
    Load a pipeline instance from a given repository directory.

    Args:
        root_dir: The root directory for the pipeline.
        repo_dir: The repository directory containing the pipeline implementation.
        pipeline_name: The name of the pipeline.
        config_path: The path to the pipeline configuration file.
        dry_run: Boolean flag indicating whether to run in dry-run mode.
        log_string_prefix: Optional prefix for log messages.
        allow_empty: If True, return None for empty repos instead of raising an error.

    Returns:
        Optional[BasePipeline]: An instance of the pipeline class, or None if repository is empty and allow_empty=True.

    Raises:
        FileNotFoundError: If no pipeline implementation found or if multiple implementations are found.
        ImportError: If the pipeline module or class cannot be imported or instantiated.
    """
    # Find the pipeline implementation file
    module_path = _find_pipeline_module_path(repo_dir, allow_empty=allow_empty)
    if module_path is None:
        return None

    # Load the pipeline module
    module_name, module, module_spec = _load_pipeline_module(module_path)

    # Enable repo-relative imports
    sys.path.insert(0, str(repo_dir.absolute()))
    try:
        if module_spec.loader is None:
            raise ImportError(f"Module loader is None for {module_name}")
        module_spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)

    # Find and instantiate the pipeline class
    pipeline_class = _find_pipeline_class(module)
    pipeline_instance = pipeline_class(
        repo_dir,
        config=load_config(config_path),
        dry_run=dry_run,
    )

    # Configure logging
    _configure_pipeline_logging(
        pipeline_instance,
        root_dir,
        pipeline_name,
        dry_run,
        log_string_prefix,
    )

    return pipeline_instance
