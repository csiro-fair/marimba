"""
Dataset mapping processors decorators.

This module provides decorators for applying dataset mapping processors at different project levels. It also provides a
factory function for retrieving a decorator based on the MetadataGenerationLevelOptions enum.
"""

from collections import defaultdict
from collections.abc import Callable
from functools import reduce
from pathlib import Path
from typing import Any, TypeAlias, TypeVar

from marimba.core.pipeline import PackageEntry
from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import MetadataGenerationLevelOptions

PIPELINE_DATASET_MAPPING_TYPE: TypeAlias = dict[
    str,
    dict[Path, PackageEntry],
]
DATASET_MAPPING_TYPE: TypeAlias = dict[str, PIPELINE_DATASET_MAPPING_TYPE]

PIPELINE_MAPPED_DATASET_ITEMS = dict[str, dict[str, list[BaseMetadata]]]
MAPPED_DATASET_ITEMS = dict[str, PIPELINE_MAPPED_DATASET_ITEMS]

PIPLINE_MAPPED_GROUPED_ITEMS = dict[
    str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]
]
MAPPED_GROUPED_ITEMS = dict[str, PIPLINE_MAPPED_GROUPED_ITEMS]

MAPPING_PROCESSOR_TYPE: TypeAlias = Callable[
    [dict[type[BaseMetadata], dict[str, list[BaseMetadata]]], str | None],
    None,
]
DECORATOR_TYPE: TypeAlias = Callable[
    [MAPPING_PROCESSOR_TYPE, MAPPED_GROUPED_ITEMS], None
]

T = TypeVar("T")
S = TypeVar("S")
R = TypeVar("R")


def flatten_middle_mapping(
    mapping: dict[str, dict[str, dict[T, S]]],
) -> dict[str, dict[T, S]]:
    """
    Flattens the middle level of a mapping structure.

    Args:
        mapping: Mapping to flatten.

    Returns:
        flattened mapping structure.
    """
    return {
        pipeline_name: flatten_mapping(pipeline_data)
        for pipeline_name, pipeline_data in mapping.items()
    }


def flatten_mapping(mapping: dict[str, dict[T, S]]) -> dict[T, S]:
    """
    Flattens a mapping structure for one level.

    Args:
        mapping: Mapping to flatten.

    Returns:
        flattened mapping structure.
    """
    return reduce(lambda x, y: x | y, mapping.values(), {})


def flatten_middle_list_mapping(
    mapping: dict[str, dict[str, dict[T, dict[S, R]]]],
) -> dict[str, dict[T, dict[S, R]]]:
    """
    Flattens the middle level of a mapping structure.

    Args:
        mapping: Mapping to flatten.

    Returns:
        flattened mapping structure.
    """
    return {
        pipeline_name: flatten_list_mapping(pipeline_data)
        for pipeline_name, pipeline_data in mapping.items()
    }


def flatten_list_mapping(
    mapping: dict[str, dict[T, dict[S, R]]],
) -> dict[T, dict[S, R]]:
    """
    Flattens the middle level of a mapping structure.

    Args:
        mapping: Mapping to flatten.

    Returns:
        flattened mapping structure.
    """
    output: defaultdict[T, dict[S, R]] = defaultdict(dict)
    for dictionary in mapping.values():
        for key, value in dictionary.items():
            output[key].update(value)

    return dict(output)


def execute_on_mapping(
    mapping: dict[str, dict[str, S]], executor: Callable[[S], T]
) -> dict[str, dict[str, T]]:
    """
    Executes a function on a mapping structure.

    Args:
        mapping: Mapping which contains data for the execution.
        executor: Function to execute.

    Returns:
        Mapping with execution results.
    """
    return {
        pipeline_name: {
            collection_name: executor(collection_mapping)
            for collection_name, collection_mapping in pipeline_mapping.items()
        }
        for pipeline_name, pipeline_mapping in mapping.items()
    }


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
    collection_dataset_mapping: MAPPED_GROUPED_ITEMS,
) -> None:
    pipeline_dataset_mapping = flatten_middle_list_mapping(collection_dataset_mapping)
    dataset_mapping = flatten_list_mapping(pipeline_dataset_mapping)
    return dataset_mapping_processor(dataset_mapping, None)


def _run_mapping_processor_per_pipeline(
    dataset_mapping_processor: MAPPING_PROCESSOR_TYPE,
    collection_dataset_mapping: MAPPED_GROUPED_ITEMS,
) -> None:
    pipeline_dataset_mapping = flatten_middle_list_mapping(collection_dataset_mapping)

    for pipeline_name, pipeline_data in pipeline_dataset_mapping.items():
        dataset_mapping_processor(pipeline_data, f"{pipeline_name}")


def _run_mapping_processor_per_pipline_and_collection(
    dataset_mapping_processor: MAPPING_PROCESSOR_TYPE,
    dataset_mapping: MAPPED_GROUPED_ITEMS,
) -> None:
    for pipeline_name, pipeline_mapping in dataset_mapping.items():
        for collection_name, collection_data in pipeline_mapping.items():
            dataset_mapping_processor(
                collection_data,
                f"{collection_name}.{pipeline_name}",
            )
