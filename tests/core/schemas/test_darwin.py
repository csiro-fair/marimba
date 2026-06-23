"""Tests for marimba.core.schemas.darwin module."""

import logging
from abc import ABC
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.darwin import DarwinCoreMetadata


@pytest.fixture
def darwin_complete_implementation() -> type[DarwinCoreMetadata]:
    """Fixture providing a complete DarwinCoreMetadata implementation for testing.

    Returns a concrete implementation class that provides all required abstract
    methods with realistic test data values for comprehensive testing.
    """

    class CompleteDarwinMetadata(DarwinCoreMetadata):
        """Complete implementation of DarwinCoreMetadata for testing purposes."""

        def __init__(self) -> None:
            self._hash: str | None = None

        @property
        def datetime(self) -> datetime | None:
            return datetime(2024, 6, 15, 14, 30, 0, tzinfo=UTC)

        @property
        def latitude(self) -> float | None:
            return -25.2744

        @property
        def longitude(self) -> float | None:
            return 133.7751

        @property
        def altitude(self) -> float | None:
            return 545.0

        @property
        def context(self) -> str | None:
            return "Marine biological survey"

        @property
        def license(self) -> str | None:
            return "CC-BY-4.0"

        @property
        def creators(self) -> list[str]:
            return ["Dr. Jane Smith", "Research Team Alpha"]

        @property
        def hash_sha256(self) -> str | None:
            return self._hash

        @hash_sha256.setter
        def hash_sha256(self, value: str) -> None:
            self._hash = value or None

        @classmethod
        def create_dataset_metadata(
            cls,
            dataset_name: str,
            root_dir: Path,
            items: dict[str, list["BaseMetadata"]],
            metadata_name: str | None = None,
            *,
            dry_run: bool = False,
            saver_overwrite: Callable[[Path, str, dict[str, Any]], None] | None = None,
        ) -> None:
            pass

        @classmethod
        def process_files(
            cls,
            dataset_mapping: dict[Path, tuple[list["BaseMetadata"], dict[str, Any] | None]],
            max_workers: int | None = None,
            logger: logging.Logger | None = None,
            *,
            dry_run: bool = False,
            chunk_size: int | None = None,
            image_set_uuid: str | None = None,
        ) -> None:
            pass

    return CompleteDarwinMetadata


class TestDarwinCoreMetadata:
    """Test DarwinCoreMetadata class."""

    @pytest.mark.unit
    def test_darwin_core_metadata_inheritance(self) -> None:
        """Test that DarwinCoreMetadata properly inherits from BaseMetadata.

        This test verifies the inheritance relationship is correctly established,
        ensuring DarwinCoreMetadata is a proper subclass of BaseMetadata and ABC,
        maintains abstract behavior, and preserves the base interface contract.
        """
        # Arrange
        expected_base_abstract_methods_count = len(BaseMetadata.__abstractmethods__)

        # Act
        is_base_subclass = issubclass(DarwinCoreMetadata, BaseMetadata)
        is_abc_subclass = issubclass(DarwinCoreMetadata, ABC)
        has_abstract_methods = hasattr(DarwinCoreMetadata, "__abstractmethods__")
        abstract_methods_count = len(DarwinCoreMetadata.__abstractmethods__)

        # Assert: Verify proper inheritance hierarchy
        assert is_base_subclass, "DarwinCoreMetadata must inherit from BaseMetadata"
        assert is_abc_subclass, "DarwinCoreMetadata must inherit from ABC through BaseMetadata"

        # Assert: Verify abstract behavior is preserved
        assert (
            has_abstract_methods
        ), "DarwinCoreMetadata must preserve __abstractmethods__ attribute for ABC enforcement"
        assert abstract_methods_count > 0, (
            f"DarwinCoreMetadata should inherit abstract methods from BaseMetadata, "
            f"but found {abstract_methods_count} abstract methods"
        )
        assert abstract_methods_count == expected_base_abstract_methods_count, (
            f"DarwinCoreMetadata should inherit all {expected_base_abstract_methods_count} abstract methods "
            f"from BaseMetadata, but found {abstract_methods_count} abstract methods. "
            f"Expected: {expected_base_abstract_methods_count}, Actual: {abstract_methods_count}"
        )

    @pytest.mark.unit
    def test_darwin_abstract_methods_inherited(self) -> None:
        """Test that DarwinCoreMetadata maintains abstract method requirements.

        This test verifies that DarwinCoreMetadata properly inherits abstract methods
        from BaseMetadata and remains abstract until implemented, ensuring all required
        abstract methods are present and match the BaseMetadata interface exactly.
        """
        # Arrange
        expected_abstract_methods = frozenset(
            {
                "datetime",
                "latitude",
                "longitude",
                "altitude",
                "context",
                "license",
                "creators",
                "hash_sha256",
                "create_dataset_metadata",
                "process_files",
            },
        )
        base_abstract_methods = BaseMetadata.__abstractmethods__

        # Act
        actual_abstract_methods = DarwinCoreMetadata.__abstractmethods__

        # Assert: Verify abstract methods count matches expectations
        assert len(actual_abstract_methods) == len(
            expected_abstract_methods,
        ), f"Expected {len(expected_abstract_methods)} abstract methods, found {len(actual_abstract_methods)}"

        # Assert: Verify abstract methods exactly match BaseMetadata
        assert actual_abstract_methods == base_abstract_methods, (
            f"DarwinCoreMetadata should inherit all abstract methods from BaseMetadata. "
            f"BaseMetadata methods: {sorted(base_abstract_methods)}, "
            f"DarwinCoreMetadata methods: {sorted(actual_abstract_methods)}"
        )

        # Assert: Verify abstract methods match expected set
        assert expected_abstract_methods == actual_abstract_methods, (
            f"Abstract methods mismatch with expected interface. "
            f"Missing: {sorted(expected_abstract_methods - actual_abstract_methods)}, "
            f"Unexpected: {sorted(actual_abstract_methods - expected_abstract_methods)}"
        )

    @pytest.mark.unit
    def test_darwin_complete_implementation_succeeds(
        self,
        darwin_complete_implementation: type[DarwinCoreMetadata],
    ) -> None:
        """Test that complete DarwinCoreMetadata implementation can be instantiated and is functional.

        This test verifies that when all abstract methods are implemented,
        DarwinCoreMetadata subclasses can be successfully instantiated and
        provide the expected interface functionality with actual working properties
        and methods that return expected values rather than just existing.
        """
        # Arrange
        # Complete implementation class from fixture

        # Act
        metadata = darwin_complete_implementation()

        # Assert: Verify proper inheritance hierarchy
        assert isinstance(metadata, DarwinCoreMetadata), "Instance should be DarwinCoreMetadata"
        assert isinstance(metadata, BaseMetadata), "Instance should be BaseMetadata through inheritance"
        assert isinstance(metadata, darwin_complete_implementation), "Instance should be concrete implementation"

        # Assert: Verify that the instance provides the complete Darwin Core interface
        required_properties = [
            "datetime",
            "latitude",
            "longitude",
            "altitude",
            "context",
            "license",
            "creators",
            "hash_sha256",
        ]
        for property_name in required_properties:
            assert hasattr(metadata, property_name), f"Instance should have {property_name} property"

        # Assert: Verify that properties are actually callable and return values (not just exist)
        assert metadata.datetime is not None, "datetime property should return a value, not None"
        assert metadata.latitude is not None, "latitude property should return a value, not None"
        assert metadata.longitude is not None, "longitude property should return a value, not None"
        assert metadata.altitude is not None, "altitude property should return a value, not None"
        assert metadata.context is not None, "context property should return a value, not None"
        assert metadata.license is not None, "license property should return a value, not None"
        assert len(metadata.creators) > 0, "creators property should return non-empty list"

        # Assert: Verify that class methods are callable (testing interface completeness)
        assert callable(metadata.create_dataset_metadata), "create_dataset_metadata should be callable"
        assert callable(metadata.process_files), "process_files should be callable"

    @pytest.mark.unit
    def test_darwin_implementation_property_values(
        self,
        darwin_complete_implementation: type[DarwinCoreMetadata],
    ) -> None:
        """Test that complete DarwinCoreMetadata implementation returns expected property values.

        This test verifies that all properties of a complete implementation
        return the expected values and types, ensuring proper interface compliance
        and that the concrete implementation correctly implements the Darwin Core interface.
        """
        # Arrange
        expected_datetime = datetime(2024, 6, 15, 14, 30, 0, tzinfo=UTC)
        expected_latitude = -25.2744
        expected_longitude = 133.7751
        expected_altitude = 545.0
        expected_context = "Marine biological survey"
        expected_license = "CC-BY-4.0"
        expected_creators = ["Dr. Jane Smith", "Research Team Alpha"]

        # Act
        metadata = darwin_complete_implementation()

        # Assert: Verify all property values match expected values exactly
        assert (
            metadata.datetime == expected_datetime
        ), f"datetime property should return {expected_datetime}, got {metadata.datetime}"
        assert (
            metadata.latitude == expected_latitude
        ), f"latitude property should return {expected_latitude}, got {metadata.latitude}"
        assert (
            metadata.longitude == expected_longitude
        ), f"longitude property should return {expected_longitude}, got {metadata.longitude}"
        assert (
            metadata.altitude == expected_altitude
        ), f"altitude property should return {expected_altitude}, got {metadata.altitude}"
        assert (
            metadata.context == expected_context
        ), f"context property should return '{expected_context}', got '{metadata.context}'"
        assert (
            metadata.license == expected_license
        ), f"license property should return '{expected_license}', got '{metadata.license}'"
        assert (
            metadata.creators == expected_creators
        ), f"creators property should return {expected_creators}, got {metadata.creators}"

        # Assert: Verify hash property initial state (should be None before setting)
        assert metadata.hash_sha256 is None, f"hash_sha256 should initially be None, got {metadata.hash_sha256}"

        # Assert: Verify property types are correct
        assert isinstance(metadata.datetime, datetime), f"datetime should be datetime, got {type(metadata.datetime)}"
        assert isinstance(metadata.latitude, float), f"latitude should be float, got {type(metadata.latitude)}"
        assert isinstance(metadata.longitude, float), f"longitude should be float, got {type(metadata.longitude)}"
        assert isinstance(metadata.altitude, float), f"altitude should be float, got {type(metadata.altitude)}"
        assert isinstance(metadata.context, str), f"context should be str, got {type(metadata.context)}"
        assert isinstance(metadata.license, str), f"license should be str, got {type(metadata.license)}"
        assert isinstance(metadata.creators, list), f"creators should be list, got {type(metadata.creators)}"
        assert all(isinstance(creator, str) for creator in metadata.creators), "All creators should be strings"

    @pytest.mark.unit
    def test_darwin_implementation_hash_property(
        self,
        darwin_complete_implementation: type[DarwinCoreMetadata],
    ) -> None:
        """Test that hash_sha256 property getter and setter work correctly in DarwinCoreMetadata implementation.

        This test verifies that the hash_sha256 property implements proper getter/setter behavior
        with initial None state, correct value storage and retrieval after assignment, and
        consistent behavior across multiple accesses. Tests edge cases including empty string assignment.
        """
        # Arrange
        expected_hash_value = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
        metadata = darwin_complete_implementation()

        # Act & Assert: Verify initial state is None
        initial_hash = metadata.hash_sha256
        assert (
            initial_hash is None
        ), f"hash_sha256 property should initially be None before any value is assigned, but found: {initial_hash}"

        # Act: Set hash value using property setter
        metadata.hash_sha256 = expected_hash_value
        retrieved_hash = metadata.hash_sha256

        # Assert: Verify setter stored the value correctly and getter retrieves it
        assert retrieved_hash == expected_hash_value, (
            f"hash_sha256 property should return the exact value that was set. "
            f"Expected: {expected_hash_value}, but got: {retrieved_hash}"
        )

        # Assert: Verify value persists across multiple accesses
        second_retrieval = metadata.hash_sha256
        assert second_retrieval == expected_hash_value, (
            f"hash_sha256 property should consistently return the same value across multiple accesses. "
            f"Expected: {expected_hash_value}, but got: {second_retrieval}"
        )

        # Act & Assert: Test edge case - setting empty string value
        metadata.hash_sha256 = ""
        empty_hash = metadata.hash_sha256
        assert empty_hash is None, (
            f"hash_sha256 property should convert empty string to None for cleaner data handling. "
            f"Expected: None, but got: {empty_hash}"
        )

        # Act & Assert: Test setting a different hash value
        different_hash = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        metadata.hash_sha256 = different_hash
        final_different_hash = metadata.hash_sha256
        assert final_different_hash == different_hash, (
            f"hash_sha256 property should accept updates with new values. "
            f"Expected: {different_hash}, but got: {final_different_hash}"
        )

    @pytest.mark.unit
    def test_darwin_abstract_method_accessibility(self) -> None:
        """Test that DarwinCoreMetadata properly exposes inherited abstract methods.

        This test verifies that all abstract methods from BaseMetadata are accessible
        through DarwinCoreMetadata class, ensuring proper inheritance without
        modifying method signatures or losing abstract behavior.
        """
        # Arrange
        expected_methods = {"create_dataset_metadata", "process_files"}
        expected_properties = {
            "datetime",
            "latitude",
            "longitude",
            "altitude",
            "context",
            "license",
            "creators",
            "hash_sha256",
        }

        # Act
        actual_abstract_methods = DarwinCoreMetadata.__abstractmethods__

        # Assert: Verify abstract class methods are accessible
        for method_name in expected_methods:
            assert hasattr(
                DarwinCoreMetadata,
                method_name,
            ), f"DarwinCoreMetadata should expose {method_name} method from BaseMetadata"
            method = getattr(DarwinCoreMetadata, method_name)
            assert callable(method), f"{method_name} should be callable on DarwinCoreMetadata"

        # Assert: Verify abstract properties are accessible
        for prop_name in expected_properties:
            assert hasattr(
                DarwinCoreMetadata,
                prop_name,
            ), f"DarwinCoreMetadata should expose {prop_name} property from BaseMetadata"

        # Assert: Verify all expected abstract items are in __abstractmethods__
        expected_all = expected_methods | expected_properties
        missing_methods = expected_all - actual_abstract_methods
        assert (
            not missing_methods
        ), f"DarwinCoreMetadata missing abstract methods from BaseMetadata: {sorted(missing_methods)}"

    @pytest.mark.unit
    def test_darwin_abstract_instantiation_fails(self) -> None:
        """Test that DarwinCoreMetadata cannot be instantiated directly due to abstract methods.

        This test verifies that attempting to instantiate DarwinCoreMetadata directly
        raises a TypeError indicating that it cannot be instantiated due to unimplemented
        abstract methods, confirming proper abstract class behavior.
        """
        # Arrange
        expected_error_pattern = r"Can't instantiate abstract class DarwinCoreMetadata.*abstract methods"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            DarwinCoreMetadata()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_darwin_class_definition_minimal(self) -> None:
        """Test that DarwinCoreMetadata class definition is minimal and appropriate.

        This test verifies that DarwinCoreMetadata doesn't add unnecessary complexity
        and serves as a proper abstract specialization of BaseMetadata for Darwin Core.
        The test checks that it's a minimal class with only essential attributes and
        proper inheritance structure.
        """
        # Arrange
        class_dict = DarwinCoreMetadata.__dict__
        # Exclude standard dunder methods, ABC attributes, and focus on actual new functionality
        expected_class_attributes = {
            "__module__",
            "__doc__",
            "__qualname__",
            "__abstractmethods__",  # ABC automatically adds this
            "_abc_impl",  # ABC automatically adds this
            "__firstlineno__",  # Python 3.13+ adds this automatically
            "__static_attributes__",  # Python 3.13+ adds this automatically
        }
        actual_class_attributes = set(class_dict.keys())
        new_attributes = actual_class_attributes - expected_class_attributes
        docstring = DarwinCoreMetadata.__doc__
        expected_base_class = BaseMetadata

        # Act
        has_docstring = docstring is not None and docstring.strip() != ""
        docstring_contains_darwin_core = "Darwin Core" in docstring if docstring else False
        direct_base_classes = DarwinCoreMetadata.__bases__

        # Assert: Verify class is minimal (no additional methods or attributes beyond standard ones)
        assert len(new_attributes) == 0, (
            f"DarwinCoreMetadata should be a minimal abstract class with no additional attributes or methods. "
            f"Found unexpected attributes: {sorted(new_attributes)}. "
            f"Expected only: {sorted(expected_class_attributes)}"
        )

        # Assert: Verify proper inheritance structure
        assert len(direct_base_classes) == 1, (
            f"DarwinCoreMetadata should inherit from exactly one base class. "
            f"Found {len(direct_base_classes)} base classes: {direct_base_classes}"
        )
        assert expected_base_class in direct_base_classes, (
            f"DarwinCoreMetadata should directly inherit from BaseMetadata. "
            f"Current base classes: {direct_base_classes}"
        )

        # Assert: Verify documentation requirements
        assert has_docstring, "DarwinCoreMetadata must have a descriptive docstring"
        assert (
            docstring_contains_darwin_core
        ), f"Docstring should reference 'Darwin Core' metadata standard. Current docstring: {docstring!r}"
