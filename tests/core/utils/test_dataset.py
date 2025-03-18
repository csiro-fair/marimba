from pathlib import Path

import pytest

from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import MetadataGenerationLevelOptions
from marimba.core.utils.dataset import (
    DATASET_MAPPING_TYPE,
    _run_mapping_processor,
    _run_mapping_processor_per_pipeline,
    _run_mapping_processor_per_pipline_and_collection,
    get_mapping_processor_decorator,
    PIPELINE_DATASET_MAPPING_TYPE,
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


def test_run_mapping_processor():
    def dataset_mapping_processor(
        dataset_mapping: PIPELINE_DATASET_MAPPING_TYPE, _: str | None
    ) -> dict[str, list[BaseMetadata]]:
        assert dataset_mapping == {"pipeline": {Path("tmp"): (Path("tmp"), None, None)}}
        return {}

    dataset_mapping: DATASET_MAPPING_TYPE = {"pipeline": {"collection": {Path("tmp"): (Path("tmp"), None, None)}}}
    _run_mapping_processor(dataset_mapping_processor, dataset_mapping)


def test_run_mapping_processor_per_pipeline():
    def dataset_mapping_processor(
        dataset_mapping: PIPELINE_DATASET_MAPPING_TYPE, collection_name: str | None
    ) -> dict[str, list[BaseMetadata]]:
        assert dataset_mapping == {"pipeline": {Path("tmp"): (Path("tmp"), None, None)}}
        assert collection_name == "pipeline"

        return {}

    dataset_mapping: DATASET_MAPPING_TYPE = {"pipeline": {"collection": {Path("tmp"): (Path("tmp"), None, None)}}}
    _run_mapping_processor_per_pipeline(dataset_mapping_processor, dataset_mapping)


def test_run_mapping_processor_per_pipline_and_collection():
    def dataset_mapping_processor(
        dataset_mapping: PIPELINE_DATASET_MAPPING_TYPE, collection_name: str | None
    ) -> dict[str, list[BaseMetadata]]:
        assert dataset_mapping == {"pipeline": {Path("tmp"): (Path("tmp"), None, None)}}
        assert collection_name == "collection.pipeline"
        return {}

    dataset_mapping: DATASET_MAPPING_TYPE = {"pipeline": {"collection": {Path("tmp"): (Path("tmp"), None, None)}}}
    _run_mapping_processor_per_pipline_and_collection(dataset_mapping_processor, dataset_mapping)
