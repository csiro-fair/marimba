from pathlib import Path

import pytest

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.generic import GenericMetadata
from marimba.core.utils.constants import MetadataGenerationLevelOptions
from marimba.core.utils.dataset import (
    DATASET_MAPPING_TYPE,
    _run_mapping_processor,
    _run_mapping_processor_per_pipeline,
    _run_mapping_processor_per_pipline_and_collection,
    get_mapping_processor_decorator,
    PIPELINE_DATASET_MAPPING_TYPE,
    flatten_middle_mapping,
    MAPPED_DATASET_ITEMS,
    MAPPED_GROUPED_ITEMS,
)


def test_get_mapping_processor_decorator():
    assert get_mapping_processor_decorator(MetadataGenerationLevelOptions.project) == _run_mapping_processor
    assert (
        get_mapping_processor_decorator(MetadataGenerationLevelOptions.pipeline) == _run_mapping_processor_per_pipeline
    )
    assert (
        get_mapping_processor_decorator(MetadataGenerationLevelOptions.collection)
        == _run_mapping_processor_per_pipline_and_collection
    )

    with pytest.raises(TypeError):
        get_mapping_processor_decorator("bla")  # type: ignore


def test_flatten_mapping():
    mapping = {"a": {"b": {"c": 1}}}
    assert flatten_middle_mapping(mapping) == {"a": {"c": 1}}


def test_run_mapping_processor():
    def dataset_mapping_processor(
        dataset_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]], _: str | None
    ) -> None:
        assert dataset_mapping == {GenericMetadata: {"a": []}}

    dataset_mapping: MAPPED_GROUPED_ITEMS = {"pipeline": {"collection": {GenericMetadata: {"a": []}}}}
    _run_mapping_processor(dataset_mapping_processor, dataset_mapping)


def test_run_mapping_processor_per_pipeline():
    def dataset_mapping_processor(
        dataset_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]], collection_name: str | None
    ) -> None:
        assert dataset_mapping == {GenericMetadata: {"a": []}}
        assert collection_name == "pipeline"

    dataset_mapping: MAPPED_GROUPED_ITEMS = {"pipeline": {"collection": {GenericMetadata: {"a": []}}}}
    _run_mapping_processor_per_pipeline(dataset_mapping_processor, dataset_mapping)


def test_run_mapping_processor_per_pipline_and_collection():
    def dataset_mapping_processor(
        dataset_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]], collection_name: str | None
    ) -> None:
        assert dataset_mapping == {GenericMetadata: {"a": []}}
        assert collection_name == "collection.pipeline"

    dataset_mapping: MAPPED_GROUPED_ITEMS = {"pipeline": {"collection": {GenericMetadata: {"a": []}}}}
    _run_mapping_processor_per_pipline_and_collection(dataset_mapping_processor, dataset_mapping)
