from typing import TYPE_CHECKING

import pytest
import pytest_mock

from marimba.core.schemas.generic import GenericMetadata

if TYPE_CHECKING:
    from marimba.core.schemas.base import BaseMetadata
from marimba.core.utils.constants import MetadataGenerationLevelOptions
from marimba.core.utils.dataset import (
    MAPPED_GROUPED_ITEMS,
    _run_mapping_processor,
    _run_mapping_processor_per_pipeline,
    _run_mapping_processor_per_pipeline_and_collection,
    execute_on_mapping,
    flatten_list_mapping,
    flatten_mapping,
    flatten_middle_list_mapping,
    flatten_middle_mapping,
    get_mapping_processor_decorator,
)


class TestGetMappingProcessorDecorator:
    """Test cases for the get_mapping_processor_decorator function."""

    @pytest.mark.unit
    def test_get_mapping_processor_decorator_valid_levels(self) -> None:
        """Test get_mapping_processor_decorator returns correct processors for valid metadata generation levels."""
        # Arrange - All valid MetadataGenerationLevelOptions
        project_level = MetadataGenerationLevelOptions.project
        pipeline_level = MetadataGenerationLevelOptions.pipeline
        collection_level = MetadataGenerationLevelOptions.collection

        # Act
        project_processor = get_mapping_processor_decorator(project_level)
        pipeline_processor = get_mapping_processor_decorator(pipeline_level)
        collection_processor = get_mapping_processor_decorator(collection_level)

        # Assert
        assert project_processor == _run_mapping_processor, "Expected project level to return _run_mapping_processor"
        assert (
            pipeline_processor == _run_mapping_processor_per_pipeline
        ), "Expected pipeline level to return _run_mapping_processor_per_pipeline"
        assert (
            collection_processor == _run_mapping_processor_per_pipeline_and_collection
        ), "Expected collection level to return _run_mapping_processor_per_pipeline_and_collection"

    @pytest.mark.unit
    def test_get_mapping_processor_decorator_invalid_string_raises_error(self) -> None:
        """Test get_mapping_processor_decorator raises TypeError with descriptive message for invalid string input.

        This test verifies that passing an invalid string value to get_mapping_processor_decorator
        raises a TypeError with a clear error message indicating the unknown processor type.
        """
        # Arrange
        invalid_level = "invalid_level"
        expected_error_pattern = r"Unknown mapping processor type: invalid_level"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            get_mapping_processor_decorator(invalid_level)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_get_mapping_processor_decorator_none_input_raises_error(self) -> None:
        """Test get_mapping_processor_decorator raises TypeError with descriptive message when given None as input.

        This test verifies that passing None to get_mapping_processor_decorator
        raises a TypeError with a clear error message indicating the unknown processor type.
        """
        # Arrange
        invalid_level = None
        expected_error_pattern = r"Unknown mapping processor type: None"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            get_mapping_processor_decorator(invalid_level)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_get_mapping_processor_decorator_integer_input_raises_error(self) -> None:
        """Test get_mapping_processor_decorator raises TypeError when given integer input.

        This test verifies that passing an integer value to get_mapping_processor_decorator
        raises a TypeError with a clear error message indicating the unknown processor type.
        """
        # Arrange
        invalid_level = 42
        expected_error_pattern = r"Unknown mapping processor type: 42"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            get_mapping_processor_decorator(invalid_level)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_get_mapping_processor_decorator_boolean_input_raises_error(self) -> None:
        """Test get_mapping_processor_decorator raises TypeError with descriptive message for boolean input.

        This test verifies that passing a boolean value to get_mapping_processor_decorator
        raises a TypeError with a clear error message indicating the unknown processor type.
        """
        # Arrange
        invalid_level = True
        expected_error_pattern = r"Unknown mapping processor type: True"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            get_mapping_processor_decorator(invalid_level)  # type: ignore[arg-type]


class TestFlattenMiddleMapping:
    """Test cases for the flatten_middle_mapping function."""

    @pytest.mark.unit
    def test_flatten_middle_mapping_basic_case(self) -> None:
        """Test flatten_middle_mapping correctly flattens middle level of nested dictionary structure.

        This test verifies that flatten_middle_mapping takes a 3-level nested dictionary
        (pipeline -> collection -> key -> value) and flattens the middle level to produce
        a 2-level structure (pipeline -> key -> value) by merging all collection mappings.
        """
        # Arrange
        mapping: dict[str, dict[str, dict[str, int]]] = {"a": {"b": {"c": 1}, "d": {"e": 1}}}
        expected: dict[str, dict[str, int]] = {"a": {"c": 1, "e": 1}}

        # Act
        result = flatten_middle_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert len(result) == 1, f"Expected 1 pipeline in result, but got {len(result)}"
        assert "a" in result, "Expected pipeline 'a' to be present in result"
        assert len(result["a"]) == 2, f"Expected 2 keys in flattened pipeline 'a', but got {len(result['a'])}"

        # Verify specific flattened content
        assert result["a"]["c"] == 1, f"Expected key 'c' to have value 1, but got {result['a']['c']}"
        assert result["a"]["e"] == 1, f"Expected key 'e' to have value 1, but got {result['a']['e']}"

    @pytest.mark.unit
    def test_flatten_middle_mapping_empty_dict(self) -> None:
        """Test flatten_middle_mapping with empty dictionary returns empty dict and maintains type consistency."""
        # Arrange
        mapping: dict[str, dict[str, dict[str, int]]] = {}
        expected: dict[str, dict[str, int]] = {}

        # Act
        result = flatten_middle_mapping(mapping)

        # Assert
        assert result == expected, f"Expected empty dict, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 0, f"Expected empty dict with length 0, but got length {len(result)}"

    @pytest.mark.unit
    def test_flatten_middle_mapping_single_pipeline(self) -> None:
        """Test flatten_middle_mapping with single pipeline containing multiple collections.

        This test verifies that flatten_middle_mapping correctly flattens the middle level
        (collections) of a 3-level nested structure, merging all collection dictionaries
        within a single pipeline while preserving the pipeline structure.
        """
        # Arrange
        mapping = {
            "pipeline1": {
                "collection1": {"key1": "value1", "key2": "value2"},
                "collection2": {"key3": "value3"},
            },
        }
        expected = {"pipeline1": {"key1": "value1", "key2": "value2", "key3": "value3"}}

        # Act
        result = flatten_middle_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert len(result) == 1, f"Expected 1 pipeline in result, but got {len(result)}"
        assert "pipeline1" in result, "Expected pipeline 'pipeline1' to be present in result"
        assert len(result["pipeline1"]) == 3, f"Expected 3 flattened keys, but got {len(result['pipeline1'])}"

        # Verify specific flattened content from both collections
        assert (
            result["pipeline1"]["key1"] == "value1"
        ), f"Expected 'key1' to have value 'value1', but got '{result['pipeline1']['key1']}'"
        assert (
            result["pipeline1"]["key2"] == "value2"
        ), f"Expected 'key2' to have value 'value2', but got '{result['pipeline1']['key2']}'"
        assert (
            result["pipeline1"]["key3"] == "value3"
        ), f"Expected 'key3' to have value 'value3', but got '{result['pipeline1']['key3']}'"

    @pytest.mark.unit
    def test_flatten_middle_mapping_multiple_pipelines(self) -> None:
        """Test flatten_middle_mapping with multiple pipelines each containing collections."""
        # Arrange
        mapping = {
            "pipeline1": {
                "collection1": {"key1": "value1"},
                "collection2": {"key2": "value2"},
            },
            "pipeline2": {
                "collection3": {"key3": "value3"},
                "collection4": {"key4": "value4"},
            },
        }
        expected = {
            "pipeline1": {"key1": "value1", "key2": "value2"},
            "pipeline2": {"key3": "value3", "key4": "value4"},
        }

        # Act
        result = flatten_middle_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert len(result) == 2, f"Expected 2 pipelines in result, but got {len(result)}"

    @pytest.mark.unit
    def test_flatten_middle_mapping_overlapping_keys_within_pipeline(self) -> None:
        """Test flatten_middle_mapping behavior when collections within a pipeline have overlapping keys."""
        # Arrange
        mapping: dict[str, dict[str, dict[str, str]]] = {
            "pipeline1": {
                "collection1": {"duplicate_key": "value1"},
                "collection2": {"duplicate_key": "value2", "unique_key": "value3"},
            },
        }
        expected: dict[str, dict[str, str]] = {
            "pipeline1": {"duplicate_key": "value2", "unique_key": "value3"},
        }

        # Act
        result = flatten_middle_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert len(result) == 1, f"Expected 1 pipeline in result, but got {len(result)}"
        assert "pipeline1" in result, "Expected 'pipeline1' to be present in result"
        assert (
            len(result["pipeline1"]) == 2
        ), f"Expected 2 keys in flattened pipeline, but got {len(result['pipeline1'])}"
        assert "duplicate_key" in result["pipeline1"], "Expected 'duplicate_key' to be present in flattened result"
        assert (
            result["pipeline1"]["duplicate_key"] == "value2"
        ), f"Expected 'value2' due to dict merge order, but got {result['pipeline1']['duplicate_key']}"
        assert (
            result["pipeline1"]["unique_key"] == "value3"
        ), f"Expected 'value3' for unique_key, but got {result['pipeline1']['unique_key']}"


class TestFlattenMapping:
    """Test cases for the flatten_mapping function."""

    @pytest.mark.unit
    def test_flatten_mapping_basic_case(self) -> None:
        """Test flatten_mapping correctly flattens two non-overlapping nested dictionaries into single level.

        This test verifies that flatten_mapping takes a 2-level nested dictionary structure
        and flattens it to a single level by merging all inner dictionaries while preserving
        keys and values from the original nested structure.
        """
        # Arrange
        mapping = {"a": {"b": 1}, "c": {"d": 1}}
        expected = {"b": 1, "d": 1}

        # Act
        result = flatten_mapping(mapping)

        # Assert
        assert result == expected, f"Expected flattened mapping {expected}, but got {result}"
        assert len(result) == 2, f"Expected exactly 2 keys in flattened result, but got {len(result)}"
        assert "b" in result, "Expected key 'b' from first nested dict to be present in result"
        assert "d" in result, "Expected key 'd' from second nested dict to be present in result"
        assert result["b"] == 1, f"Expected value 1 for key 'b', but got {result['b']}"
        assert result["d"] == 1, f"Expected value 1 for key 'd', but got {result['d']}"

    @pytest.mark.unit
    def test_flatten_mapping_empty_dict(self) -> None:
        """Test flatten_mapping with empty dictionary returns empty dict and maintains type consistency."""
        # Arrange
        mapping: dict[str, dict[str, int]] = {}
        expected: dict[str, int] = {}

        # Act
        result = flatten_mapping(mapping)

        # Assert
        assert result == expected, f"Expected empty dict, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 0, f"Expected empty dict with length 0, but got length {len(result)}"

    @pytest.mark.unit
    def test_flatten_mapping_single_nested_dict(self) -> None:
        """Test flatten_mapping correctly extracts keys from single nested dictionary.

        This test verifies that flatten_mapping properly flattens a single outer key containing
        multiple inner key-value pairs, preserving all inner keys and their values while
        discarding the outer key structure.
        """
        # Arrange
        mapping = {"outer": {"inner1": "value1", "inner2": "value2"}}
        expected = {"inner1": "value1", "inner2": "value2"}

        # Act
        result = flatten_mapping(mapping)

        # Assert
        assert result == expected, f"Expected flattened mapping {expected}, but got {result}"
        assert len(result) == 2, f"Expected exactly 2 keys in flattened result, but got {len(result)}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"

        # Verify specific key-value pairs for detailed failure messages
        assert "inner1" in result, "Expected key 'inner1' to be present in flattened result"
        assert "inner2" in result, "Expected key 'inner2' to be present in flattened result"
        assert result["inner1"] == "value1", f"Expected 'inner1' to have value 'value1', but got '{result['inner1']}'"
        assert result["inner2"] == "value2", f"Expected 'inner2' to have value 'value2', but got '{result['inner2']}'"

    @pytest.mark.unit
    def test_flatten_mapping_overlapping_keys(self) -> None:
        """Test flatten_mapping behavior when inner dicts have overlapping keys.

        The flatten_mapping function uses reduce with dict union operator (|), meaning
        later dictionaries in the mapping.values() iteration will override keys from
        earlier ones. This test verifies that behavior is consistent.
        """
        # Arrange
        mapping = {"first": {"key": "value1"}, "second": {"key": "value2"}}
        expected_value = "value2"  # Later values override earlier ones due to dict union operator (|)

        # Act
        result = flatten_mapping(mapping)

        # Assert
        # Due to dict union operator (|), later values should override earlier ones
        assert "key" in result, "Expected 'key' to be present in flattened result"
        assert (
            result["key"] == expected_value
        ), f"Expected '{expected_value}' due to dict merge order, but got {result['key']}"
        assert len(result) == 1, f"Expected exactly 1 key in flattened result, but got {len(result)} keys"

    @pytest.mark.unit
    def test_flatten_mapping_multiple_nested_dicts(self) -> None:
        """Test flatten_mapping with multiple nested dictionaries containing string values."""
        # Arrange
        mapping = {
            "first_group": {"key1": "value1", "key2": "value2"},
            "second_group": {"key3": "value3", "key4": "value4"},
            "third_group": {"key5": "value5"},
        }
        expected = {"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4", "key5": "value5"}

        # Act
        result = flatten_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert len(result) == 5, f"Expected 5 keys in result, but got {len(result)}"
        # Verify specific key-value pairs for more detailed failure messages
        for key, expected_value in expected.items():
            assert key in result, f"Expected key '{key}' to be present in result"
            assert (
                result[key] == expected_value
            ), f"Expected '{key}' to have value '{expected_value}', but got '{result[key]}'"


class TestRunMappingProcessor:
    """Test cases for the _run_mapping_processor function."""

    @pytest.mark.unit
    def test_run_mapping_processor_flattens_all_levels(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test _run_mapping_processor flattens 4-level structure to 2-level and calls processor with None.

        This test verifies that _run_mapping_processor correctly transforms a complex 4-level nested
        structure (pipeline -> collection -> metadata_type -> file -> metadata_list) into a flattened
        2-level structure (metadata_type -> file -> metadata_list) by applying both flatten_middle_list_mapping
        and flatten_list_mapping, then calls the processor with the flattened mapping and None as collection_name.
        """
        # Arrange
        mock_processor = mocker.Mock()

        # Create the complex 4-level nested structure that _run_mapping_processor expects
        # Structure: pipeline -> collection -> metadata_type -> file -> metadata_list
        dataset_mapping: MAPPED_GROUPED_ITEMS = {
            "pipeline1": {
                "collection1": {GenericMetadata: {"file1": []}},
                "collection2": {GenericMetadata: {"file2": []}},
            },
            "pipeline2": {
                "collection3": {GenericMetadata: {"file3": []}},
            },
        }

        expected_flattened_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file1": [], "file2": [], "file3": []},
        }

        # Act
        _run_mapping_processor(mock_processor, dataset_mapping)

        # Assert - Verify processor was called correctly
        mock_processor.assert_called_once()
        call_args = mock_processor.call_args[0]
        actual_mapping, actual_collection_name = call_args

        # Verify the main flattening transformation occurred correctly
        assert (
            actual_mapping == expected_flattened_mapping
        ), f"Expected flattened mapping {expected_flattened_mapping}, but got {actual_mapping}"
        assert actual_collection_name is None, f"Expected collection_name to be None, but got {actual_collection_name}"

        # Verify structure and content integrity after flattening
        assert GenericMetadata in actual_mapping, "Expected GenericMetadata type to be present in flattened mapping"
        assert (
            len(actual_mapping[GenericMetadata]) == 3
        ), f"Expected 3 files from all pipelines/collections, but got {len(actual_mapping[GenericMetadata])}"

        # Verify all original files from different pipelines and collections are preserved
        expected_files = {"file1", "file2", "file3"}
        actual_files = set(actual_mapping[GenericMetadata].keys())
        assert actual_files == expected_files, f"Expected files {expected_files} to be present, but got {actual_files}"


class TestRunMappingProcessorPerPipeline:
    """Test cases for the _run_mapping_processor_per_pipeline function."""

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_calls_processor_for_single_pipeline(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test _run_mapping_processor_per_pipeline calls processor once for single pipeline with flattened collections.

        This test verifies that when given a single pipeline with multiple collections,
        the processor is called exactly once with the pipeline name as collection_name
        and with all collections flattened into a single mapping structure.
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {
            "pipeline": {"collection": {GenericMetadata: {"a": []}}, "another": {GenericMetadata: {"b": []}}},
        }
        expected_pipeline_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"a": [], "b": []},
        }

        # Act
        _run_mapping_processor_per_pipeline(mock_processor, dataset_mapping)

        # Assert
        mock_processor.assert_called_once_with(expected_pipeline_mapping, "pipeline")

        # Verify the call arguments in detail
        call_args = mock_processor.call_args[0]
        actual_mapping, actual_collection_name = call_args

        assert (
            actual_mapping == expected_pipeline_mapping
        ), f"Expected pipeline mapping {expected_pipeline_mapping}, but got {actual_mapping}"
        assert (
            actual_collection_name == "pipeline"
        ), f"Expected collection_name 'pipeline', but got {actual_collection_name}"

        # Verify both collections were flattened into the single pipeline call
        assert GenericMetadata in actual_mapping, "Expected GenericMetadata type to be present in mapping"
        assert len(actual_mapping[GenericMetadata]) == 2, "Expected 2 flattened files from both collections"
        assert "a" in actual_mapping[GenericMetadata], "Expected 'a' from first collection to be present"
        assert "b" in actual_mapping[GenericMetadata], "Expected 'b' from second collection to be present"
        assert (
            actual_mapping[GenericMetadata]["a"] == []
        ), f"Expected 'a' to have empty list, but got {actual_mapping[GenericMetadata]['a']}"
        assert (
            actual_mapping[GenericMetadata]["b"] == []
        ), f"Expected 'b' to have empty list, but got {actual_mapping[GenericMetadata]['b']}"

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_multiple_pipelines(self, mocker: pytest_mock.MockerFixture) -> None:
        """Test _run_mapping_processor_per_pipeline calls processor once per pipeline with correct collection names.

        This test verifies that when given multiple pipelines, the processor is called
        exactly once for each pipeline, with the correct pipeline name as collection_name
        and with flattened collections within each pipeline.
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {
            "pipeline1": {"collection1": {GenericMetadata: {"file_a": []}}},
            "pipeline2": {"collection2": {GenericMetadata: {"file_b": []}}},
        }
        expected_pipeline1_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file_a": []},
        }
        expected_pipeline2_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file_b": []},
        }

        # Act
        _run_mapping_processor_per_pipeline(mock_processor, dataset_mapping)

        # Assert
        assert mock_processor.call_count == 2, f"Expected processor to be called twice, got {mock_processor.call_count}"

        # Verify calls were made with expected arguments
        expected_calls = [
            mocker.call(expected_pipeline1_mapping, "pipeline1"),
            mocker.call(expected_pipeline2_mapping, "pipeline2"),
        ]
        mock_processor.assert_has_calls(expected_calls, any_order=True)


class TestRunMappingProcessorPerPipelineAndCollection:
    """Test cases for the _run_mapping_processor_per_pipeline_and_collection function."""

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_and_collection_calls_processor_for_each_collection(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test _run_mapping_processor_per_pipeline_and_collection calls processor per collection with formatted name.

        This test verifies that the processor is called exactly once for a single collection with:
        1. The collection name formatted as 'collection.pipeline'
        2. The exact mapping data for that specific collection (no flattening across collections)
        3. The proper metadata type structure preserved
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {"pipeline": {"collection": {GenericMetadata: {"a": []}}}}
        expected_collection_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"a": []},
        }

        # Act
        _run_mapping_processor_per_pipeline_and_collection(mock_processor, dataset_mapping)

        # Assert - Verify processor called once with correct arguments
        mock_processor.assert_called_once_with(expected_collection_mapping, "collection.pipeline")

        # Extract call arguments for detailed verification
        call_args = mock_processor.call_args[0]
        actual_mapping, actual_collection_name = call_args

        # Verify mapping structure matches expected
        assert (
            actual_mapping == expected_collection_mapping
        ), f"Expected collection mapping {expected_collection_mapping}, but got {actual_mapping}"

        # Verify collection name formatting
        assert (
            actual_collection_name == "collection.pipeline"
        ), f"Expected collection_name 'collection.pipeline', but got '{actual_collection_name}'"

        # Verify metadata type and file structure integrity
        assert GenericMetadata in actual_mapping, "Expected GenericMetadata type to be present in mapping"
        assert (
            len(actual_mapping[GenericMetadata]) == 1
        ), f"Expected exactly 1 file in collection mapping, but got {len(actual_mapping[GenericMetadata])}"
        assert "a" in actual_mapping[GenericMetadata], "Expected file 'a' to be present in collection mapping"
        assert (
            actual_mapping[GenericMetadata]["a"] == []
        ), f"Expected file 'a' to have empty metadata list, but got {actual_mapping[GenericMetadata]['a']}"

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_and_collection_multiple_collections(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test _run_mapping_processor_per_pipeline_and_collection handles multiple collections correctly.

        This test verifies that when given multiple collections within a single pipeline,
        the processor is called once for each collection with the correct collection names
        formatted as 'collection.pipeline' and individual collection mapping data.
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {
            "pipeline": {
                "collection1": {GenericMetadata: {"a": []}},
                "collection2": {GenericMetadata: {"b": []}},
            },
        }

        # Act
        _run_mapping_processor_per_pipeline_and_collection(mock_processor, dataset_mapping)

        # Assert
        assert (
            mock_processor.call_count == 2
        ), f"Expected processor to be called twice, but got {mock_processor.call_count} calls"

        # Verify calls were made with expected arguments
        expected_calls = [
            mocker.call({GenericMetadata: {"a": []}}, "collection1.pipeline"),
            mocker.call({GenericMetadata: {"b": []}}, "collection2.pipeline"),
        ]
        mock_processor.assert_has_calls(expected_calls, any_order=True)

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_and_collection_empty_dataset(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test _run_mapping_processor_per_pipeline_and_collection handles empty dataset mapping gracefully.

        This test verifies that when given an empty dataset mapping, the processor
        is never called and the function completes without errors. This ensures
        the function is robust when processing projects with no data.
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {}

        # Act
        _run_mapping_processor_per_pipeline_and_collection(mock_processor, dataset_mapping)

        # Assert
        mock_processor.assert_not_called()
        assert (
            mock_processor.call_count == 0
        ), f"Expected processor to never be called with empty dataset, but got {mock_processor.call_count} calls"

    @pytest.mark.unit
    def test_run_mapping_processor_per_pipeline_and_collection_multiple_pipelines(
        self,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test _run_mapping_processor_per_pipeline_and_collection handles multiple pipelines with collections.

        This test verifies that when given multiple pipelines each with collections,
        the processor is called once for each collection across all pipelines with
        correctly formatted collection names as 'collection.pipeline'.
        """
        # Arrange
        mock_processor = mocker.Mock()
        dataset_mapping: MAPPED_GROUPED_ITEMS = {
            "pipeline1": {
                "collection1": {GenericMetadata: {"file1": []}},
            },
            "pipeline2": {
                "collection2": {GenericMetadata: {"file2": []}},
                "collection3": {GenericMetadata: {"file3": []}},
            },
        }

        expected_collection1_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file1": []},
        }
        expected_collection2_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file2": []},
        }
        expected_collection3_mapping: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file3": []},
        }

        # Act
        _run_mapping_processor_per_pipeline_and_collection(mock_processor, dataset_mapping)

        # Assert
        assert (
            mock_processor.call_count == 3
        ), f"Expected processor to be called 3 times (once per collection), but got {mock_processor.call_count} calls"

        # Verify calls were made with expected arguments for all collections
        expected_calls = [
            mocker.call(expected_collection1_mapping, "collection1.pipeline1"),
            mocker.call(expected_collection2_mapping, "collection2.pipeline2"),
            mocker.call(expected_collection3_mapping, "collection3.pipeline2"),
        ]
        mock_processor.assert_has_calls(expected_calls, any_order=True)

        # Verify individual call arguments in detail for better error reporting
        call_args_list = mock_processor.call_args_list
        assert len(call_args_list) == 3, f"Expected 3 call records, but got {len(call_args_list)}"

        # Extract and verify all collection names passed to processor
        actual_collection_names = {call_args[0][1] for call_args in call_args_list}
        expected_collection_names = {"collection1.pipeline1", "collection2.pipeline2", "collection3.pipeline2"}
        assert (
            actual_collection_names == expected_collection_names
        ), f"Expected collection names {expected_collection_names}, but got {actual_collection_names}"

        # Verify that each mapping contains the correct metadata type and file structure
        for call_args in call_args_list:
            actual_mapping, collection_name = call_args[0]
            assert (
                GenericMetadata in actual_mapping
            ), f"Expected GenericMetadata type to be present in mapping for {collection_name}"
            assert len(actual_mapping[GenericMetadata]) == 1, (
                f"Expected exactly 1 file in mapping for {collection_name}, "
                f"but got {len(actual_mapping[GenericMetadata])}"
            )

            # Verify the file key matches expected pattern based on collection name
            file_keys = list(actual_mapping[GenericMetadata].keys())
            if collection_name == "collection1.pipeline1":
                assert "file1" in file_keys, f"Expected 'file1' in mapping for {collection_name}"
            elif collection_name == "collection2.pipeline2":
                assert "file2" in file_keys, f"Expected 'file2' in mapping for {collection_name}"
            elif collection_name == "collection3.pipeline2":
                assert "file3" in file_keys, f"Expected 'file3' in mapping for {collection_name}"


class TestFlattenMiddleListMapping:
    """Test cases for the flatten_middle_list_mapping function."""

    @pytest.mark.unit
    def test_flatten_middle_list_mapping_basic_case(self) -> None:
        """Test flatten_middle_list_mapping correctly flattens middle level of 4-level nested structure.

        This test verifies that flatten_middle_list_mapping takes a 4-level nested dictionary
        (pipeline -> collection -> metadata_type -> file -> metadata_list) and flattens the collection level
        to produce a 3-level structure (pipeline -> metadata_type -> file -> metadata_list).
        The function should merge files from multiple collections within each pipeline under the same metadata type.
        """
        # Arrange
        mapping: dict[str, dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]]] = {
            "pipeline1": {
                "collection1": {GenericMetadata: {"file1": []}},
                "collection2": {GenericMetadata: {"file2": []}},
            },
        }
        expected: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {
            "pipeline1": {GenericMetadata: {"file1": [], "file2": []}},
        }

        # Act
        result = flatten_middle_list_mapping(mapping)

        # Assert
        assert result == expected, f"Expected flattened mapping {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 1, f"Expected exactly 1 pipeline in result, but got {len(result)}"
        assert "pipeline1" in result, "Expected pipeline 'pipeline1' to be present in result"

        # Verify the flattening behavior - collections should be merged at metadata type level
        pipeline_result = result["pipeline1"]
        assert GenericMetadata in pipeline_result, "Expected GenericMetadata type to be present after flattening"
        assert (
            len(pipeline_result[GenericMetadata]) == 2
        ), f"Expected 2 files after flattening collections, but got {len(pipeline_result[GenericMetadata])}"

        # Verify both files from different collections are present in flattened result
        flattened_files = pipeline_result[GenericMetadata]
        assert "file1" in flattened_files, "Expected 'file1' from collection1 to be present in flattened result"
        assert "file2" in flattened_files, "Expected 'file2' from collection2 to be present in flattened result"
        assert (
            flattened_files["file1"] == []
        ), f"Expected 'file1' to have empty list, but got {flattened_files['file1']}"
        assert (
            flattened_files["file2"] == []
        ), f"Expected 'file2' to have empty list, but got {flattened_files['file2']}"

    @pytest.mark.unit
    def test_flatten_middle_list_mapping_empty_dict(self) -> None:
        """Test flatten_middle_list_mapping with empty dictionary returns empty dict and maintains type consistency.

        This test verifies that flatten_middle_list_mapping properly handles the edge case of an empty input
        dictionary by returning an empty dictionary of the correct type. This ensures the function
        gracefully handles cases where no pipelines are provided for flattening, which is important
        for robustness when processing variable dataset structures.
        """
        # Arrange
        mapping: dict[str, dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]]] = {}
        expected: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {}

        # Act
        result = flatten_middle_list_mapping(mapping)

        # Assert
        assert result == expected, f"Expected empty dict, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 0, f"Expected empty dict with length 0, but got length {len(result)}"

        # Verify that the function doesn't raise any exceptions with empty input
        # This is important for ensuring the function is robust in edge cases
        assert result is not None, "Expected function to return a valid dict object, not None"

    @pytest.mark.unit
    def test_flatten_middle_list_mapping_multiple_pipelines(self) -> None:
        """Test flatten_middle_list_mapping correctly processes multiple pipelines with different collection structures.

        This test verifies that flatten_middle_list_mapping:
        1. Processes multiple pipelines independently
        2. Flattens collection level within each pipeline while preserving pipeline separation
        3. Merges files from multiple collections within pipeline1 under the same metadata type
        4. Preserves single collection structure in pipeline2
        5. Maintains correct metadata type associations and file mappings
        """
        # Arrange - Create test data with realistic metadata structures
        metadata_list_1: list[BaseMetadata] = []
        metadata_list_2: list[BaseMetadata] = []
        metadata_list_3: list[BaseMetadata] = []

        mapping: dict[str, dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]]] = {
            "pipeline1": {
                "collection1": {GenericMetadata: {"file1": metadata_list_1}},
                "collection2": {GenericMetadata: {"file2": metadata_list_2}},
            },
            "pipeline2": {
                "collection3": {GenericMetadata: {"file3": metadata_list_3}},
            },
        }
        expected: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {
            "pipeline1": {GenericMetadata: {"file1": metadata_list_1, "file2": metadata_list_2}},
            "pipeline2": {GenericMetadata: {"file3": metadata_list_3}},
        }

        # Act
        result = flatten_middle_list_mapping(mapping)

        # Assert - Verify overall structure and pipeline count
        assert result == expected, f"Expected flattened mapping {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 2, f"Expected exactly 2 pipelines in result, but got {len(result)}"

        # Verify both pipelines are present
        expected_pipelines = {"pipeline1", "pipeline2"}
        actual_pipelines = set(result.keys())
        assert (
            actual_pipelines == expected_pipelines
        ), f"Expected pipelines {expected_pipelines}, but got {actual_pipelines}"

        # Assert - Verify pipeline1 flattening behavior (multiple collections merged)
        pipeline1_result = result["pipeline1"]
        assert GenericMetadata in pipeline1_result, "Expected GenericMetadata type to be present in pipeline1 result"
        assert len(pipeline1_result[GenericMetadata]) == 2, (
            f"Expected exactly 2 files after flattening collections in pipeline1, "
            f"but got {len(pipeline1_result[GenericMetadata])}"
        )

        # Verify specific files from different collections are merged correctly
        expected_files_p1 = {"file1", "file2"}
        actual_files_p1 = set(pipeline1_result[GenericMetadata].keys())
        assert (
            actual_files_p1 == expected_files_p1
        ), f"Expected files {expected_files_p1} in pipeline1, but got {actual_files_p1}"

        # Verify metadata lists are preserved correctly
        assert pipeline1_result[GenericMetadata]["file1"] is metadata_list_1, (
            f"Expected 'file1' to reference original metadata_list_1, "
            f"but got {pipeline1_result[GenericMetadata]['file1']}"
        )
        assert pipeline1_result[GenericMetadata]["file2"] is metadata_list_2, (
            f"Expected 'file2' to reference original metadata_list_2, "
            f"but got {pipeline1_result[GenericMetadata]['file2']}"
        )

        # Assert - Verify pipeline2 structure (single collection preserved)
        pipeline2_result = result["pipeline2"]
        assert GenericMetadata in pipeline2_result, "Expected GenericMetadata type to be present in pipeline2 result"
        assert len(pipeline2_result[GenericMetadata]) == 1, (
            f"Expected exactly 1 file from single collection in pipeline2, "
            f"but got {len(pipeline2_result[GenericMetadata])}"
        )

        # Verify single file is preserved correctly
        assert (
            "file3" in pipeline2_result[GenericMetadata]
        ), "Expected 'file3' from collection3 to be present in pipeline2"
        assert pipeline2_result[GenericMetadata]["file3"] is metadata_list_3, (
            f"Expected 'file3' to reference original metadata_list_3, "
            f"but got {pipeline2_result[GenericMetadata]['file3']}"
        )


class TestFlattenListMapping:
    """Test cases for the flatten_list_mapping function."""

    @pytest.mark.unit
    def test_flatten_list_mapping_basic_case(self) -> None:
        """Test flatten_list_mapping correctly flattens nested dictionaries by merging inner collections.

        This test verifies that flatten_list_mapping takes a 3-level nested dictionary
        (collection -> metadata_type -> file -> metadata_list) and flattens it to a 2-level structure
        (metadata_type -> file -> metadata_list) by merging all collections under the same metadata type.
        """
        # Arrange
        mapping: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {
            "collection1": {GenericMetadata: {"file1": []}},
            "collection2": {GenericMetadata: {"file2": []}},
        }
        expected: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {"file1": [], "file2": []},
        }

        # Act
        result = flatten_list_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 1, f"Expected exactly 1 metadata type in result, but got {len(result)}"

        # Verify metadata type structure
        assert GenericMetadata in result, "Expected GenericMetadata type to be present in result"
        assert (
            len(result[GenericMetadata]) == 2
        ), f"Expected 2 files after flattening, but got {len(result[GenericMetadata])}"

        # Verify specific file presence and values
        assert "file1" in result[GenericMetadata], "Expected 'file1' from collection1 to be present"
        assert "file2" in result[GenericMetadata], "Expected 'file2' from collection2 to be present"
        assert (
            result[GenericMetadata]["file1"] == []
        ), f"Expected 'file1' to have empty list, but got {result[GenericMetadata]['file1']}"
        assert (
            result[GenericMetadata]["file2"] == []
        ), f"Expected 'file2' to have empty list, but got {result[GenericMetadata]['file2']}"

    @pytest.mark.unit
    def test_flatten_list_mapping_empty_dict(self) -> None:
        """Test flatten_list_mapping with empty dictionary returns empty dict and maintains type consistency.

        This test verifies that flatten_list_mapping properly handles the edge case of an empty input
        dictionary by returning an empty dictionary of the correct type, ensuring the function
        gracefully handles cases where no collections are provided for flattening.
        """
        # Arrange
        mapping: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {}
        expected: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {}

        # Act
        result = flatten_list_mapping(mapping)

        # Assert
        assert result == expected, f"Expected empty dict, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 0, f"Expected empty dict with length 0, but got length {len(result)}"

    @pytest.mark.unit
    def test_flatten_list_mapping_overlapping_metadata_types(self) -> None:
        """Test flatten_list_mapping behavior when collections have same metadata types with overlapping file keys.

        This test verifies that when multiple collections contain the same metadata type with overlapping
        file keys, the function correctly merges them using dict.update() behavior where later collections
        override earlier ones for duplicate file keys. Since Python dict iteration order is guaranteed
        as insertion order, the last collection in the mapping will override earlier values.
        """
        # Arrange - Create collections with distinct values for overlapping keys to test override behavior
        collection1_metadata: list[BaseMetadata] = []
        collection2_metadata: list[BaseMetadata] = [
            GenericMetadata(),
        ]  # Different from collection1 to verify which is kept

        mapping: dict[str, dict[type[BaseMetadata], dict[str, list[BaseMetadata]]]] = {
            "collection1": {GenericMetadata: {"file1": collection1_metadata, "shared_file": collection1_metadata}},
            "collection2": {GenericMetadata: {"file2": collection2_metadata, "shared_file": collection2_metadata}},
        }

        # Expected: collection2's value for shared_file should override collection1's due to dict.update() behavior
        expected: dict[type[BaseMetadata], dict[str, list[BaseMetadata]]] = {
            GenericMetadata: {
                "file1": collection1_metadata,
                "file2": collection2_metadata,
                "shared_file": collection2_metadata,
            },
        }

        # Act
        result = flatten_list_mapping(mapping)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 1, f"Expected exactly 1 metadata type in result, but got {len(result)}"

        # Verify metadata type structure and file merging
        assert GenericMetadata in result, "Expected GenericMetadata type to be present in result"
        assert len(result[GenericMetadata]) == 3, (
            f"Expected 3 files after merging collections (including overlapping file), "
            f"but got {len(result[GenericMetadata])}"
        )

        # Verify specific files are present after flattening
        expected_files = {"file1", "file2", "shared_file"}
        actual_files = set(result[GenericMetadata].keys())
        assert actual_files == expected_files, f"Expected files {expected_files} to be present, but got {actual_files}"

        # Verify that the override behavior worked correctly - shared_file should have collection2's value
        assert result[GenericMetadata]["shared_file"] is collection2_metadata, (
            f"Expected 'shared_file' to contain collection2's metadata due to dict.update() override behavior, "
            f"but got {result[GenericMetadata]['shared_file']}"
        )

        # Verify non-overlapping files preserve their original values
        assert (
            result[GenericMetadata]["file1"] is collection1_metadata
        ), f"Expected 'file1' to contain collection1's metadata, but got {result[GenericMetadata]['file1']}"
        assert (
            result[GenericMetadata]["file2"] is collection2_metadata
        ), f"Expected 'file2' to contain collection2's metadata, but got {result[GenericMetadata]['file2']}"


class TestExecuteOnMapping:
    """Test cases for the execute_on_mapping function."""

    @pytest.mark.unit
    def test_execute_on_mapping_applies_function_to_all_values(self) -> None:
        """Test execute_on_mapping applies executor function to all collection values in mapping structure.

        This test verifies that execute_on_mapping correctly applies a transformation function
        to every collection value across all pipelines in the nested mapping structure,
        preserving the pipeline and collection hierarchy while transforming the values.
        """
        # Arrange
        mapping: dict[str, dict[str, int]] = {
            "pipeline1": {"collection1": 1, "collection2": 2},
            "pipeline2": {"collection3": 3},
        }

        def double_function(x: int) -> int:
            return x * 2

        expected: dict[str, dict[str, int]] = {
            "pipeline1": {"collection1": 2, "collection2": 4},
            "pipeline2": {"collection3": 6},
        }

        # Act
        result = execute_on_mapping(mapping, double_function)

        # Assert
        assert result == expected, f"Expected mapping {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 2, f"Expected 2 pipelines in result, but got {len(result)}"

        # Verify pipeline structure preservation
        assert "pipeline1" in result, "Expected 'pipeline1' to be present in result"
        assert "pipeline2" in result, "Expected 'pipeline2' to be present in result"

        # Verify specific value transformations
        assert len(result["pipeline1"]) == 2, f"Expected 2 collections in pipeline1, but got {len(result['pipeline1'])}"
        assert len(result["pipeline2"]) == 1, f"Expected 1 collection in pipeline2, but got {len(result['pipeline2'])}"

        # Verify function was applied correctly to each value
        assert (
            result["pipeline1"]["collection1"] == 2
        ), f"Expected function to transform 1 to 2, but got {result['pipeline1']['collection1']}"
        assert (
            result["pipeline1"]["collection2"] == 4
        ), f"Expected function to transform 2 to 4, but got {result['pipeline1']['collection2']}"
        assert (
            result["pipeline2"]["collection3"] == 6
        ), f"Expected function to transform 3 to 6, but got {result['pipeline2']['collection3']}"

    @pytest.mark.unit
    def test_execute_on_mapping_empty_dict(self) -> None:
        """Test execute_on_mapping with empty dictionary returns empty dict and maintains type consistency."""
        # Arrange
        mapping: dict[str, dict[str, int]] = {}

        def identity_function(x: int) -> int:
            return x

        expected: dict[str, dict[str, int]] = {}

        # Act
        result = execute_on_mapping(mapping, identity_function)

        # Assert
        assert result == expected, f"Expected empty dict, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 0, f"Expected empty dict with length 0, but got length {len(result)}"

    @pytest.mark.unit
    def test_execute_on_mapping_string_transformation(self) -> None:
        """Test execute_on_mapping applies string transformation function to all collection values.

        This test verifies that execute_on_mapping correctly applies a string transformation
        function (uppercase) to every collection value while preserving the pipeline and
        collection structure. It ensures the function works with non-numeric data types.
        """
        # Arrange
        mapping: dict[str, dict[str, str]] = {
            "pipeline1": {"collection1": "hello", "collection2": "world"},
        }

        def uppercase_function(x: str) -> str:
            return x.upper()

        expected: dict[str, dict[str, str]] = {
            "pipeline1": {"collection1": "HELLO", "collection2": "WORLD"},
        }

        # Act
        result = execute_on_mapping(mapping, uppercase_function)

        # Assert
        assert result == expected, f"Expected {expected}, but got {result}"
        assert isinstance(result, dict), f"Expected result to be a dict, but got {type(result)}"
        assert len(result) == 1, f"Expected 1 pipeline in result, but got {len(result)}"

        # Verify pipeline structure preservation
        assert "pipeline1" in result, "Expected 'pipeline1' to be present in result"
        assert len(result["pipeline1"]) == 2, f"Expected 2 collections in pipeline1, but got {len(result['pipeline1'])}"

        # Verify specific transformations
        assert result["pipeline1"]["collection1"] == "HELLO", "Expected 'hello' to be transformed to 'HELLO'"
        assert result["pipeline1"]["collection2"] == "WORLD", "Expected 'world' to be transformed to 'WORLD'"
