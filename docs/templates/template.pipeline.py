"""
Example Pipeline implementation template.

This template shows how to implement a Marimba Pipeline by inheriting from BasePipeline.
Rename the PipelineTemplate class to something appropriate for your Pipeline's purpose.
"""

from pathlib import Path
from typing import Any

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline


class PipelineTemplate(BasePipeline):
    """
    Template Pipeline implementation. Rename this class and customize for your needs.
    """

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        """
        Return the Pipeline configuration schema.

        The returned dictionary should be a flat map of key -> default value.
        All keys must be strings, and values must be YAML-serializable.
        Nested dictionaries and lists are not supported.

        Returns:
            Dictionary mapping configuration keys to their default values.
        """
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        """
        Return the collection configuration schema.

        The collection configuration schema represents values specific to a collection.
        Use get_pipeline_config_schema() for values that are static across collections.

        Returns:
            Dictionary mapping configuration keys to their default values.
        """
        return {}

    def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> None:
        """
        Import data from source_path into data_dir.

        Args:
            data_dir: The destination directory for imported data.
            source_path: The source directory containing data to import.
            config: The collection configuration dictionary.
            kwargs: Additional keyword arguments.
        """
        return

    def _process(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> None:
        """
        Process data in data_dir.

        Args:
            data_dir: The directory containing data to process.
            config: The collection configuration dictionary.
            kwargs: Additional keyword arguments.
        """
        return

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> dict[Path, tuple[Path, ImageData | None, dict[str, Any] | None]]:
        """
        Package data from data_dir for distribution.

        Args:
            data_dir: The directory containing data to package.
            config: The collection configuration dictionary.
            kwargs: Additional keyword arguments.

        Returns:
            Dictionary mapping source paths to tuples of (destination path, ImageData, metadata).
        """
        return {}
