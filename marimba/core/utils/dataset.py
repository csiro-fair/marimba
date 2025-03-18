"""
Dataset mapping processors decorators.

This module provides decorators for applying dataset mapping processors at different project levels. It also provides a
factory function for retrieving a decorator based on the MetadataGenerationLevelOptions enum.
"""

from collections.abc import Callable
from functools import reduce
from pathlib import Path
from typing import Any, TypeAlias

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import MetadataGenerationLevelOptions

PIPELINE_DATASET_MAPPING_TYPE: TypeAlias = dict[
    str,
    dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]],
]
DATASET_MAPPING_TYPE: TypeAlias = dict[str, PIPELINE_DATASET_MAPPING_TYPE]

MAPPING_PROCESSOR_TYPE: TypeAlias = Callable[[PIPELINE_DATASET_MAPPING_TYPE, str | None], dict[str, list[BaseMetadata]]]
DECORATOR_TYPE: TypeAlias = Callable[[MAPPING_PROCESSOR_TYPE, DATASET_MAPPING_TYPE], dict[str, list[BaseMetadata]]]


def get_mapping_processor_decorator(
    level: MetadataGenerationLevelOptions,
) -> DECORATOR_TYPE:
    """
    Returns a decorator that applies a mapping processor to the given level.

    Args:
        level: Metadata generation level.

    Returns:
        Decorator for a mapping processor.
    """
    if level == MetadataGenerationLevelOptions.project:
        return _run_mapping_processor
    if level == MetadataGenerationLevelOptions.pipeline:
        return _run_mapping_processor_per_pipeline
    if level == MetadataGenerationLevelOptions.collection:
        return _run_mapping_processor_per_pipline_and_collection

    raise TypeError(f"Unknown mapping processor type: {level}")


def _run_mapping_processor(
    dataset_mapping_processor: MAPPING_PROCESSOR_TYPE,
    dataset_mapping: DATASET_MAPPING_TYPE,
) -> dict[str, list[BaseMetadata]]:
    pipeline_dataset_mapping = _reduce_dataset_mapping(dataset_mapping)
    return dataset_mapping_processor(pipeline_dataset_mapping, None)


def _reduce_dataset_mapping(dataset_mapping: DATASET_MAPPING_TYPE) -> PIPELINE_DATASET_MAPPING_TYPE:
    return {
        pipeline_name: reduce(lambda x, y: x | y, pipeline_data.values(), {})
        for pipeline_name, pipeline_data in dataset_mapping.items()
    }


def _run_mapping_processor_per_pipeline(
    dataset_mapping_processor: MAPPING_PROCESSOR_TYPE,
    dataset_mapping: DATASET_MAPPING_TYPE,
) -> dict[str, list[BaseMetadata]]:
    dataset_items: dict[str, list[BaseMetadata]] = {}
    for pipeline_name, pipeline_data in dataset_mapping.items():
        pipeline_mapping = _reduce_dataset_mapping({pipeline_name: pipeline_data})
        collection_dataset_items = dataset_mapping_processor(pipeline_mapping, f"{pipeline_name}")
        dataset_items = dataset_items | collection_dataset_items

    return dataset_items


def _run_mapping_processor_per_pipline_and_collection(
    dataset_mapping_processor: MAPPING_PROCESSOR_TYPE,
    dataset_mapping: DATASET_MAPPING_TYPE,
) -> dict[str, list[BaseMetadata]]:
    dataset_items: dict[str, list[BaseMetadata]] = {}
    for pipeline_name, pipeline_mapping in dataset_mapping.items():
        for collection_name, collection_data in pipeline_mapping.items():
            collection_mapping = {pipeline_name: collection_data}
            collection_dataset_items = dataset_mapping_processor(
                collection_mapping,
                f"{collection_name}.{pipeline_name}",
            )
            dataset_items = dataset_items | collection_dataset_items

    return dataset_items
