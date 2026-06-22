"""Tests for marimba.core.schemas.base module."""

import inspect
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from marimba.core.schemas.base import BaseMetadata


@pytest.fixture
def complete_metadata_class():
    """Fixture providing a complete BaseMetadata implementation for testing."""

    class CompleteMetadata(BaseMetadata):
        """Complete implementation of BaseMetadata for testing purposes."""

        def __init__(self) -> None:
            self._hash: str | None = None

        @property
        def datetime(self) -> datetime | None:
            return datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

        @property
        def latitude(self) -> float | None:
            return 37.7749

        @property
        def longitude(self) -> float | None:
            return -122.4194

        @property
        def altitude(self) -> float | None:
            return 100.0

        @property
        def context(self) -> str | None:
            return "Test context"

        @property
        def license(self) -> str | None:
            return "MIT"

        @property
        def creators(self) -> list[str]:
            return ["Test Creator"]

        @property
        def hash_sha256(self) -> str | None:
            return self._hash

        @hash_sha256.setter
        def hash_sha256(self, value: str) -> None:
            self._hash = value

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
        ) -> None:
            pass

    return CompleteMetadata


@pytest.fixture
def none_values_metadata_class():
    """Fixture providing a BaseMetadata implementation that returns None for optional properties."""

    class TypedMetadata(BaseMetadata):
        """Implementation with specific return types for testing None values."""

        def __init__(self) -> None:
            self._hash: str | None = None

        @property
        def datetime(self) -> datetime | None:
            return None

        @property
        def latitude(self) -> float | None:
            return None

        @property
        def longitude(self) -> float | None:
            return None

        @property
        def altitude(self) -> float | None:
            return None

        @property
        def context(self) -> str | None:
            return None

        @property
        def license(self) -> str | None:
            return None

        @property
        def creators(self) -> list[str]:
            return []

        @property
        def hash_sha256(self) -> str | None:
            return self._hash

        @hash_sha256.setter
        def hash_sha256(self, value: str) -> None:
            self._hash = value

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
        ) -> None:
            pass

    return TypedMetadata


class TestBaseMetadata:
    """Test BaseMetadata abstract base class."""

    @pytest.mark.unit
    def test_base_metadata_is_abstract(self) -> None:
        """Test that BaseMetadata cannot be instantiated directly due to abstract methods.

        This test verifies that the BaseMetadata class is properly defined as abstract
        and prevents direct instantiation, forcing users to create concrete implementations.
        """
        # Arrange: BaseMetadata is defined as an abstract base class

        # Act & Assert: Attempting to instantiate directly should raise TypeError
        expected_error_pattern = r"Can't instantiate abstract class BaseMetadata.*abstract methods"
        with pytest.raises(TypeError, match=expected_error_pattern):
            BaseMetadata()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_incomplete_implementation_prevents_instantiation(self) -> None:
        """Test that implementations missing abstract methods cannot be instantiated."""

        # Arrange: Create incomplete implementation with only one property
        class IncompleteMetadata(BaseMetadata):
            """Incomplete implementation missing some properties."""

            @property
            def datetime(self) -> datetime | None:
                return datetime.now(UTC)

            # Missing other required properties and methods

        # Act & Assert: Attempting to instantiate incomplete implementation should raise TypeError
        expected_error_pattern = (
            r"Can't instantiate abstract class IncompleteMetadata without an implementation for abstract methods"
        )
        with pytest.raises(TypeError, match=expected_error_pattern):
            IncompleteMetadata()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_complete_implementation_can_be_instantiated(self, complete_metadata_class: type[BaseMetadata]) -> None:
        """Test that complete BaseMetadata implementation can be instantiated without errors.

        This test verifies that a concrete implementation with all abstract methods defined
        can be successfully instantiated and is properly recognized as both a BaseMetadata
        instance and an instance of the concrete class type.
        """
        # Arrange: Complete implementation class from fixture

        # Act: Create instance of complete implementation
        metadata = complete_metadata_class()

        # Assert: Verify instantiation succeeds and inheritance is correct
        assert isinstance(
            metadata,
            BaseMetadata,
        ), f"Complete implementation should be instance of BaseMetadata, but got type: {type(metadata).__name__}"
        assert isinstance(metadata, complete_metadata_class), (
            f"Instance should be of the concrete implementation type {complete_metadata_class.__name__}, "
            f"but got type: {type(metadata).__name__}"
        )

        # Assert: Verify the instance is functional by accessing a basic property
        assert hasattr(
            metadata,
            "datetime",
        ), "Complete implementation should have all required abstract properties implemented"

        # Assert: Verify instance can be used (property access doesn't raise)
        try:
            _ = metadata.datetime
        except (AttributeError, NotImplementedError, ValueError, TypeError) as e:
            pytest.fail(
                f"Complete implementation should allow property access without errors, "
                f"but accessing datetime property raised: {type(e).__name__}: {e}",
            )

    @pytest.mark.unit
    def test_complete_implementation_property_access(self, complete_metadata_class: type[BaseMetadata]) -> None:
        """Test that all properties of complete BaseMetadata implementation return expected values.

        This test verifies that a concrete BaseMetadata implementation correctly implements
        all abstract properties by returning the exact expected values. It ensures that
        property getters work correctly and that the implementation follows the contract
        defined by the abstract base class.
        """
        # Arrange: Expected property values for verification
        expected_datetime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        expected_latitude = 37.7749
        expected_longitude = -122.4194
        expected_altitude = 100.0
        expected_context = "Test context"
        expected_license = "MIT"
        expected_creators = ["Test Creator"]

        # Act: Create instance and access all properties
        metadata = complete_metadata_class()

        # Assert: Verify all properties return expected values
        assert metadata.datetime == expected_datetime, "datetime property should return expected datetime value"
        assert metadata.latitude == expected_latitude, "latitude property should return expected coordinate"
        assert metadata.longitude == expected_longitude, "longitude property should return expected coordinate"
        assert metadata.altitude == expected_altitude, "altitude property should return expected elevation"
        assert metadata.context == expected_context, "context property should return expected string"
        assert metadata.license == expected_license, "license property should return expected license identifier"
        assert metadata.creators == expected_creators, "creators property should return expected list of creators"

        # Assert: Verify hash property initial state (should be None before setting)
        assert metadata.hash_sha256 is None, "hash_sha256 property should initially be None in complete implementation"

    @pytest.mark.unit
    def test_complete_implementation_hash_property_behavior(self, complete_metadata_class: type[BaseMetadata]) -> None:
        """Test that hash_sha256 property getter and setter work correctly in complete implementation.

        This test verifies that the hash_sha256 property implements proper getter/setter behavior
        with initial None state and correct value storage and retrieval after assignment.
        """
        # Arrange: Expected test hash value and metadata instance
        expected_hash_value = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
        metadata = complete_metadata_class()

        # Act: Get initial hash value
        initial_hash = metadata.hash_sha256

        # Assert: Verify initial state is None
        assert (
            initial_hash is None
        ), f"hash_sha256 property should initially be None before any value is assigned, but found: {initial_hash}"

        # Act: Set hash value using property setter
        metadata.hash_sha256 = expected_hash_value

        # Act: Retrieve hash value after setting
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

    @pytest.mark.unit
    def test_abstract_property_requirements(self) -> None:
        """Test that all expected abstract properties and methods are defined as abstract."""
        # Arrange: Define expected abstract properties and methods (including hash_sha256 setter)
        expected_abstract_members = {
            "datetime",
            "latitude",
            "longitude",
            "altitude",
            "context",
            "license",
            "creators",
            "hash_sha256",  # This covers both getter and setter
        }

        # Act: Get abstract members from the class
        abstract_members = BaseMetadata.__abstractmethods__

        # Assert: Check that all expected properties are abstract
        for member in expected_abstract_members:
            assert (
                member in abstract_members
            ), f"Abstract member {member} should be abstract but was not found in abstract members"

    @pytest.mark.unit
    def test_abstract_method_requirements(self) -> None:
        """Test that all expected abstract methods are defined as abstract.

        This test verifies that the BaseMetadata class properly defines the required
        class methods as abstract, ensuring concrete implementations must provide
        these methods for dataset metadata creation and file processing.
        """
        # Arrange: Define expected abstract methods
        expected_methods = {"create_dataset_metadata", "process_files"}

        # Act: Get abstract members from the class
        abstract_members = BaseMetadata.__abstractmethods__

        # Assert: Check that all expected methods are abstract
        for method in expected_methods:
            assert (
                method in abstract_members
            ), f"Method {method} should be abstract but was not found in abstract members"

    @pytest.mark.unit
    def test_create_dataset_metadata_signature(self) -> None:
        """Test the signature of create_dataset_metadata abstract method matches expected interface.

        Verifies that the method signature enforces the correct contract for dataset metadata creation,
        ensuring proper parameter types, defaults, and keyword-only arguments that prevent misuse
        and provide clear API boundaries for concrete implementations.
        """
        # Arrange: Define expected signature components for dataset metadata creation
        expected_required_params = ["dataset_name", "root_dir", "items"]
        expected_optional_params = ["metadata_name"]
        expected_keyword_only_params = ["dry_run", "saver_overwrite"]

        # Act: Extract method signature information
        method = BaseMetadata.create_dataset_metadata
        sig = inspect.signature(method)
        params = sig.parameters
        param_names = list(params.keys())

        # Assert: Verify all required parameters are present
        for param in expected_required_params:
            assert param in param_names, (
                f"Required parameter '{param}' missing from create_dataset_metadata signature. "
                f"Dataset metadata creation requires this parameter to identify and locate dataset components."
            )

        # Assert: Verify optional parameters are present
        for param in expected_optional_params:
            assert param in param_names, (
                f"Optional parameter '{param}' missing from create_dataset_metadata signature. "
                f"This parameter provides flexibility for metadata customization."
            )

        # Assert: Verify keyword-only parameters are present and correctly configured
        for param in expected_keyword_only_params:
            assert param in param_names, (
                f"Keyword-only parameter '{param}' missing from create_dataset_metadata signature. "
                f"This parameter must be keyword-only to prevent positional argument misuse."
            )
            assert params[param].kind == inspect.Parameter.KEYWORD_ONLY, (
                f"Parameter '{param}' should be keyword-only to enforce explicit usage and prevent "
                f"accidental positional argument passing that could break API contracts."
            )

        # Assert: Verify parameter defaults enforce safe behavior
        assert params["metadata_name"].default is None, (
            "Parameter 'metadata_name' should default to None to allow automatic name generation "
            "when no custom metadata name is specified."
        )
        assert params["dry_run"].default is False, (
            "Parameter 'dry_run' should default to False to ensure actual execution by default, "
            "with dry-run behavior explicitly opted into for safety."
        )
        assert params["saver_overwrite"].default is None, (
            "Parameter 'saver_overwrite' should default to None to use standard saving behavior "
            "unless custom saving logic is explicitly provided."
        )

        # Assert: Verify return type annotation indicates no return value
        assert sig.return_annotation is None, (
            "Method 'create_dataset_metadata' should have None return type annotation to indicate "
            "it performs side effects (file creation) rather than returning computed values."
        )

    @pytest.mark.unit
    def test_process_files_signature(self) -> None:
        """Test the signature of process_files abstract method matches expected interface.

        Verifies that the method signature includes all required parameters with correct types,
        default values, and parameter kinds (positional vs keyword-only).
        """
        # Arrange: Get method and its signature
        method = BaseMetadata.process_files
        sig = inspect.signature(method)
        params = sig.parameters

        # Act: Extract parameter information
        param_names = list(params.keys())
        expected_required_params = ["dataset_mapping"]
        expected_optional_params = ["max_workers", "logger"]
        expected_keyword_only_params = ["dry_run", "chunk_size"]

        # Assert: Check that all required parameters are present
        for param in expected_required_params:
            assert param in param_names, (
                f"Required parameter '{param}' missing from process_files signature. "
                f"File processing requires this parameter to identify dataset structure and metadata."
            )

        # Assert: Check that all optional parameters are present
        for param in expected_optional_params:
            assert param in param_names, (
                f"Optional parameter '{param}' missing from process_files signature. "
                f"This parameter provides flexibility for processing configuration."
            )

        # Assert: Check that all keyword-only parameters are present and correctly configured
        for param in expected_keyword_only_params:
            assert param in param_names, (
                f"Keyword-only parameter '{param}' missing from process_files signature. "
                f"This parameter must be keyword-only to prevent positional argument misuse."
            )
            assert params[param].kind == inspect.Parameter.KEYWORD_ONLY, (
                f"Parameter '{param}' should be keyword-only to enforce explicit usage and prevent "
                f"accidental positional argument passing that could break API contracts."
            )

        # Assert: Verify parameter defaults enforce safe behavior
        assert params["max_workers"].default is None, (
            "Parameter 'max_workers' should default to None to allow automatic worker count determination "
            "based on system resources when no specific limit is needed."
        )
        assert params["logger"].default is None, (
            "Parameter 'logger' should default to None to use standard logging behavior "
            "unless custom logging is explicitly provided."
        )
        assert params["dry_run"].default is False, (
            "Parameter 'dry_run' should default to False to ensure actual execution by default, "
            "with dry-run behavior explicitly opted into for safety."
        )
        assert params["chunk_size"].default is None, (
            "Parameter 'chunk_size' should default to None to allow automatic chunk size determination "
            "based on processing requirements when no specific size is needed."
        )

        # Assert: Verify return type annotation indicates no return value
        assert sig.return_annotation is None, (
            "Method 'process_files' should have None return type annotation to indicate "
            "it performs side effects (file processing) rather than returning computed values."
        )

    @pytest.mark.unit
    def test_base_metadata_inheritance_structure(self) -> None:
        """Test that BaseMetadata properly inherits from ABC and enforces abstract interface contracts.

        This test verifies that the BaseMetadata class correctly implements the Abstract Base Class
        pattern from Python's abc module, ensuring it cannot be instantiated directly and that
        subclasses must implement all abstract members to be instantiable.
        """
        # Arrange: Import ABC for inheritance verification
        from abc import ABC

        # Act: Check inheritance structure and abstract behavior
        inheritance_check = issubclass(BaseMetadata, ABC)
        has_abstract_methods = hasattr(BaseMetadata, "__abstractmethods__")
        abstract_methods_count = len(BaseMetadata.__abstractmethods__)

        # Assert: Verify proper ABC inheritance and structure
        assert inheritance_check, "BaseMetadata must inherit from ABC to enforce abstract interface"
        assert has_abstract_methods, "BaseMetadata must have __abstractmethods__ attribute for ABC enforcement"
        assert abstract_methods_count > 0, (
            f"BaseMetadata should have abstract methods to enforce implementation, "
            f"but found {abstract_methods_count} abstract methods"
        )

        # Assert: Verify direct instantiation is prevented
        with pytest.raises(TypeError, match=r"Can't instantiate abstract class BaseMetadata.*abstract methods"):
            BaseMetadata()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_base_metadata_abstract_member_count(self) -> None:
        """Test that BaseMetadata has the expected abstract members and count.

        This test verifies that the BaseMetadata abstract base class defines exactly
        the expected number of abstract members (properties and methods) to ensure
        the interface contract is maintained.
        """
        # Arrange: Define expected abstract members
        # 7 property getters + 1 property setter + 2 class methods = 10 total abstract members
        expected_abstract_members = {
            "datetime",  # property getter
            "latitude",  # property getter
            "longitude",  # property getter
            "altitude",  # property getter
            "context",  # property getter
            "license",  # property getter
            "creators",  # property getter
            "hash_sha256",  # property getter and setter (counted as one)
            "create_dataset_metadata",  # class method
            "process_files",  # class method
        }
        expected_abstract_count = 10

        # Act: Get actual abstract members from the class
        actual_abstract_members = BaseMetadata.__abstractmethods__
        actual_abstract_count = len(actual_abstract_members)

        # Assert: Verify exact count matches expectation
        assert actual_abstract_count == expected_abstract_count, (
            f"BaseMetadata should have exactly {expected_abstract_count} abstract members, "
            f"but found {actual_abstract_count}. Actual members: {sorted(actual_abstract_members)}"
        )

        # Assert: Verify all expected members are present
        missing_members = expected_abstract_members - actual_abstract_members
        assert not missing_members, (
            f"Missing expected abstract members: {sorted(missing_members)}. "
            f"Actual members: {sorted(actual_abstract_members)}"
        )

        # Assert: Verify no unexpected members are present
        unexpected_members = actual_abstract_members - expected_abstract_members
        assert not unexpected_members, (
            f"Found unexpected abstract members: {sorted(unexpected_members)}. "
            f"Expected members: {sorted(expected_abstract_members)}"
        )

    @pytest.mark.unit
    def test_property_none_values_are_accepted(self, none_values_metadata_class: type[BaseMetadata]) -> None:
        """Test that BaseMetadata implementations properly handle None values for optional properties.

        This test verifies that concrete BaseMetadata implementations can return None for all
        optional properties without breaking the interface contract. This is important for
        implementations that may not have all metadata fields available, ensuring they can
        still be instantiated and used without requiring placeholder values for missing data.
        """
        # Arrange: Create test metadata instance that returns None for optional properties
        metadata = none_values_metadata_class()

        # Act: Access all optional properties that should accept None values
        datetime_value = metadata.datetime
        latitude_value = metadata.latitude
        longitude_value = metadata.longitude
        altitude_value = metadata.altitude
        context_value = metadata.context
        license_value = metadata.license
        hash_value = metadata.hash_sha256
        creators_value = metadata.creators

        # Assert: Verify that None values are properly handled for optional properties
        assert datetime_value is None, (
            "datetime property should accept and return None when metadata implementation "
            "has no timestamp information available"
        )
        assert latitude_value is None, (
            "latitude property should accept and return None when metadata implementation "
            "has no coordinate information available"
        )
        assert longitude_value is None, (
            "longitude property should accept and return None when metadata implementation "
            "has no coordinate information available"
        )
        assert altitude_value is None, (
            "altitude property should accept and return None when metadata implementation "
            "has no elevation information available"
        )
        assert context_value is None, (
            "context property should accept and return None when metadata implementation "
            "has no contextual information available"
        )
        assert license_value is None, (
            "license property should accept and return None when metadata implementation "
            "has no license information specified"
        )
        assert hash_value is None, (
            "hash_sha256 property should accept and return None when metadata implementation "
            "has no hash value computed or assigned"
        )

        # Assert: Verify that creators property returns empty list instead of None (required behavior)
        assert isinstance(creators_value, list), (
            "creators property must always return a list type, never None, to maintain "
            "consistent iteration behavior even when no creators are specified"
        )
        assert creators_value == [], (
            "creators property should return an empty list when metadata implementation "
            "has no creator information rather than None to enable safe list operations"
        )

        # Assert: Verify instance can still be used functionally after None value access
        assert isinstance(
            metadata,
            BaseMetadata,
        ), "Instance should remain valid BaseMetadata after accessing None-valued properties"

    @pytest.mark.unit
    def test_property_return_types_validation(
        self,
        complete_metadata_class: type[BaseMetadata],
        none_values_metadata_class: type[BaseMetadata],
    ) -> None:
        """Test that property return types match their type annotations for concrete implementations.

        This test validates that implementations return values that match the expected type annotations,
        testing both implementations that return concrete values and those that return None values.
        It ensures type safety across different implementation scenarios.
        """
        # Arrange: Create instances and expected values for complete implementation
        complete_metadata = complete_metadata_class()
        none_metadata = none_values_metadata_class()
        expected_datetime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

        # Act: Get property values from complete implementation
        actual_datetime = complete_metadata.datetime
        actual_latitude = complete_metadata.latitude
        actual_longitude = complete_metadata.longitude
        actual_altitude = complete_metadata.altitude
        actual_context = complete_metadata.context
        actual_license = complete_metadata.license
        actual_creators = complete_metadata.creators
        actual_hash = complete_metadata.hash_sha256

        # Assert: Test complete implementation returns exact expected types and values
        assert isinstance(
            actual_datetime,
            datetime,
        ), "Complete implementation datetime property should return datetime object"
        assert actual_datetime == expected_datetime, "Complete implementation should return expected datetime value"
        assert isinstance(actual_latitude, float), "Complete implementation latitude property should return float"
        assert actual_latitude == 37.7749, "Complete implementation should return expected latitude value"
        assert isinstance(actual_longitude, float), "Complete implementation longitude property should return float"
        assert actual_longitude == -122.4194, "Complete implementation should return expected longitude value"
        assert isinstance(actual_altitude, float), "Complete implementation altitude property should return float"
        assert actual_altitude == 100.0, "Complete implementation should return expected altitude value"
        assert isinstance(actual_context, str), "Complete implementation context property should return string"
        assert actual_context == "Test context", "Complete implementation should return expected context value"
        assert isinstance(actual_license, str), "Complete implementation license property should return string"
        assert actual_license == "MIT", "Complete implementation should return expected license value"
        assert isinstance(actual_creators, list), "Complete implementation creators property should return list"
        assert actual_creators == ["Test Creator"], "Complete implementation should return expected creators value"
        assert actual_hash is None, "Complete implementation hash_sha256 should initially be None"

        # Act: Get property values from None implementation
        none_datetime = none_metadata.datetime
        none_latitude = none_metadata.latitude
        none_longitude = none_metadata.longitude
        none_altitude = none_metadata.altitude
        none_context = none_metadata.context
        none_license = none_metadata.license
        none_creators = none_metadata.creators
        none_hash = none_metadata.hash_sha256

        # Assert: Test None implementation returns None for optional properties
        assert none_datetime is None, "None implementation datetime property should return None"
        assert none_latitude is None, "None implementation latitude property should return None"
        assert none_longitude is None, "None implementation longitude property should return None"
        assert none_altitude is None, "None implementation altitude property should return None"
        assert none_context is None, "None implementation context property should return None"
        assert none_license is None, "None implementation license property should return None"
        assert none_hash is None, "None implementation hash_sha256 property should return None"

        # Assert: Test creators is always a list (never None) with correct element types
        assert isinstance(none_creators, list), "None implementation creators property should return list, never None"
        assert none_creators == [], "None implementation creators should return empty list"

        # Assert: Verify all creator elements are strings in both implementations
        for creator in actual_creators:
            assert isinstance(creator, str), f"Creator '{creator}' should be string in complete implementation"
        for creator in none_creators:
            assert isinstance(creator, str), f"Creator '{creator}' should be string in None implementation"
