"""
Example Pipeline implementation template.

This template shows how to implement a Marimba Pipeline by inheriting from BasePipeline.
Rename the PipelineTemplate class to something appropriate for your Pipeline's purpose.
"""

from pathlib import Path
from typing import Any

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.header.base import BaseMetadataHeader
from marimba.core.schemas.ifdo import iFDOMetadata


class PipelineTemplate(BasePipeline):
    """
    Template Pipeline implementation. Rename this class and customize for your needs.
    """

    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize a new Pipeline instance.

        Args:
            root_path (str | Path): Base directory path where the pipeline will store its data and configuration files.
            config (dict[str, Any] | None, optional): Pipeline configuration dictionary. If None, default configuration
             will be used. Defaults to None.
            dry_run (bool, optional): If True, prevents any filesystem modifications. Useful for validation and testing.
             Defaults to False.

        Note:
            This class inherits from BasePipeline and uses iFDOMetadata as its metadata class.
        """
        super().__init__(
            root_path,
            config,
            dry_run=dry_run,
            metadata_class=iFDOMetadata,
        )

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
    ) -> tuple[
        dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]],
        dict[type[BaseMetadata], BaseMetadataHeader[object]],
    ]:
        """
        Package data from data_dir for distribution.

        Args:
            data_dir: The directory containing data to package.
            config: The collection configuration dictionary.
            kwargs: Additional keyword arguments.

        Returns:
            Dictionary mapping source paths to tuples of (destination path, BaseMetadata list, metadata).
        """
        data_mapping: tuple[
            dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]],
            dict[type[BaseMetadata], BaseMetadataHeader[object]],
        ] = (
            {},
            {},
        )
        return data_mapping
