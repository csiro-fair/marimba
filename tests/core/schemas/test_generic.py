"""Tests for marimba.core.schemas.generic module."""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.generic import GenericMetadata

_LOGGER = logging.getLogger(__name__)


class TestGenericMetadata:
    """Test GenericMetadata class."""

    @pytest.fixture
    def sample_datetime(self):
        """Sample datetime for testing."""
        return datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)

    @pytest.fixture
    def basic_metadata(self, sample_datetime):
        """Basic metadata instance for testing."""
        return GenericMetadata(
            datetime_=sample_datetime,
            latitude=37.7749,
            longitude=-122.4194,
            altitude=100.0,
            context="Test context",
            license_="MIT",
            creators=["Alice", "Bob"],
            hash_sha256_="abc123",
        )

    @pytest.mark.unit
    def test_initialization_all_parameters_stores_correctly(self, sample_datetime: datetime) -> None:
        """Test GenericMetadata initialization with all parameters stores values correctly.

        Verifies that when GenericMetadata is initialized with all supported parameters,
        each value is properly stored and accessible through the respective properties.
        """
        # Arrange
        expected_datetime = sample_datetime
        expected_latitude = 37.7749
        expected_longitude = -122.4194
        expected_altitude = 100.0
        expected_context = "Test context"
        expected_license = "MIT"
        expected_creators = ["Alice", "Bob"]
        expected_hash = "abc123"

        # Act
        metadata = GenericMetadata(
            datetime_=expected_datetime,
            latitude=expected_latitude,
            longitude=expected_longitude,
            altitude=expected_altitude,
            context=expected_context,
            license_=expected_license,
            creators=expected_creators,
            hash_sha256_=expected_hash,
        )

        # Assert
        assert metadata.datetime == expected_datetime, "Datetime should match the provided value"
        assert metadata.latitude == expected_latitude, "Latitude should match the provided value"
        assert metadata.longitude == expected_longitude, "Longitude should match the provided value"
        assert metadata.altitude == expected_altitude, "Altitude should match the provided value"
        assert metadata.context == expected_context, "Context should match the provided value"
        assert metadata.license == expected_license, "License should match the provided value"
        assert metadata.creators == expected_creators, "Creators list should match the provided value"
        assert metadata.hash_sha256 == expected_hash, "Hash should match the provided hex string value"

    @pytest.mark.unit
    def test_initialization_with_minimal_parameters(self) -> None:
        """Test GenericMetadata initialization with selective parameters works correctly.

        Verifies that when GenericMetadata is initialized with only some optional parameters,
        the instance is created successfully with provided values stored correctly and
        non-provided fields using their appropriate default values.
        """
        # Arrange
        test_datetime = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)
        test_context = "minimal test"

        # Act
        metadata = GenericMetadata(datetime_=test_datetime, context=test_context)

        # Assert - provided values are stored correctly
        assert metadata.datetime == test_datetime, "Provided datetime should be stored correctly"
        assert metadata.context == test_context, "Provided context should be stored correctly"

        # Assert - default values for non-provided parameters
        assert metadata.latitude is None, "Latitude should default to None when not provided"
        assert metadata.longitude is None, "Longitude should default to None when not provided"
        assert metadata.altitude is None, "Altitude should default to None when not provided"
        assert metadata.license is None, "License should default to None when not provided"
        assert metadata.creators == [], "Creators should default to empty list when not provided"
        assert metadata.hash_sha256 is None, "Hash should default to None when not provided"

        # Assert - instance validity
        assert isinstance(metadata, GenericMetadata), "Should create a valid GenericMetadata instance"

    @pytest.mark.unit
    def test_initialization_without_parameters_creates_empty_metadata_with_defaults(self) -> None:
        """Test GenericMetadata initialization without parameters creates instance with expected default values.

        Verifies that when GenericMetadata is initialized without any parameters,
        all metadata fields are properly initialized to their default None values,
        except creators which should default to an empty list. This tests the default
        constructor behavior to ensure proper initialization state.
        """
        # Arrange
        # No setup required - testing parameterless constructor

        # Act
        metadata = GenericMetadata()

        # Assert - All fields should have expected default values
        assert metadata.datetime is None, "Datetime field should default to None when no datetime_ parameter provided"
        assert metadata.latitude is None, "Latitude field should default to None when no latitude parameter provided"
        assert metadata.longitude is None, "Longitude field should default to None when no longitude parameter provided"
        assert metadata.altitude is None, "Altitude field should default to None when no altitude parameter provided"
        assert metadata.context is None, "Context field should default to None when no context parameter provided"
        assert metadata.license is None, "License field should default to None when no license_ parameter provided"
        assert (
            metadata.creators == []
        ), "Creators field should default to empty list when no creators parameter provided"
        assert metadata.hash_sha256 is None, "Hash field should default to None when no hash_sha256_ parameter provided"

        # Assert - Instance should be properly functional
        assert isinstance(metadata, GenericMetadata), "Should create a valid GenericMetadata instance"
        assert isinstance(metadata, BaseMetadata), "Should inherit from BaseMetadata interface"

    @pytest.mark.unit
    def test_hash_string_conversion_hex(self) -> None:
        """Test hash string conversion from valid hex string.

        GenericMetadata should accept valid hex strings, convert them to bytes internally,
        and return them as hex strings through the hash_sha256 property.
        """
        # Arrange
        hex_hash = "abc123"
        expected_bytes = bytes.fromhex(hex_hash)

        # Act
        metadata = GenericMetadata(hash_sha256_=hex_hash)

        # Assert
        assert (
            metadata.hash_sha256 == hex_hash
        ), "Valid hex string should be returned unchanged after round-trip conversion"
        assert isinstance(metadata.hash_sha256, str), "hash_sha256 property should return string type"
        # Verify internal bytes storage through round-trip conversion
        assert (
            bytes.fromhex(metadata.hash_sha256) == expected_bytes
        ), "Round-trip conversion should preserve bytes representation"

    @pytest.mark.unit
    def test_hash_string_conversion_invalid_hex(self) -> None:
        """Test hash string conversion from invalid hex falls back to utf-8 encoding.

        When an invalid hex string is provided, GenericMetadata should gracefully
        handle it by encoding as utf-8 bytes internally and return as hex string.
        """
        # Arrange
        invalid_hex = "invalid_hex_string"
        expected_bytes = invalid_hex.encode("utf-8")
        expected_hex = expected_bytes.hex()

        # Act
        metadata = GenericMetadata(hash_sha256_=invalid_hex)

        # Assert
        assert metadata.hash_sha256 == expected_hex, "Invalid hex should be UTF-8 encoded and returned as hex string"
        assert isinstance(metadata.hash_sha256, str), "hash_sha256 property should return string type"
        # Verify UTF-8 encoding through round-trip conversion
        assert (
            bytes.fromhex(metadata.hash_sha256) == expected_bytes
        ), "Round-trip conversion should preserve UTF-8 encoded bytes"

    @pytest.mark.unit
    def test_hash_bytes_input(self) -> None:
        """Test hash input accepts bytes directly and returns as hex string.

        GenericMetadata should accept bytes input for hash, store it internally as bytes,
        and return it as a hexadecimal string through the hash_sha256 property.
        """
        # Arrange
        hash_bytes = b"test_hash"
        expected_hex = hash_bytes.hex()

        # Act
        metadata = GenericMetadata(hash_sha256_=hash_bytes)

        # Assert
        assert metadata.hash_sha256 == expected_hex, "Bytes input should be returned as hex string"
        assert isinstance(metadata.hash_sha256, str), "Hash property should return string type"
        # Verify bytes preservation through round-trip conversion
        assert bytes.fromhex(metadata.hash_sha256) == hash_bytes, "Round-trip conversion should preserve original bytes"

    @pytest.mark.unit
    def test_strftime_with_datetime_formats_date_correctly(self, basic_metadata: GenericMetadata) -> None:
        """Test strftime method with valid datetime formats date string correctly.

        Verifies that when GenericMetadata has a datetime value, the strftime method
        correctly formats it using the provided format string, ensuring proper date
        formatting functionality for metadata display and export operations.
        """
        # Arrange - fixture provides metadata with datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        expected_format_string = "%Y-%m-%d"
        expected_formatted_date = "2024-01-15"

        # Act
        result = basic_metadata.strftime(expected_format_string)

        # Assert
        assert (
            result == expected_formatted_date
        ), "strftime should format datetime correctly using provided format string"
        assert isinstance(result, str), "strftime should return a string type"

    @pytest.mark.unit
    def test_strftime_without_datetime_raises_value_error(self) -> None:
        """Test strftime method raises ValueError when datetime is None.

        When GenericMetadata is initialized without a datetime value, calling strftime()
        should raise a ValueError with a specific error message. This ensures that the
        method fails fast and provides clear feedback when the required datetime data
        is missing, preventing silent failures in date formatting operations.
        """
        # Arrange
        metadata = GenericMetadata()
        format_string = "%Y-%m-%d"
        assert metadata.datetime is None, "Precondition: metadata should have no datetime value"

        # Act & Assert
        with pytest.raises(ValueError, match="Cannot format datetime: datetime is None"):
            metadata.strftime(format_string)

    @pytest.mark.unit
    def test_isoformat_with_datetime_returns_correct_iso_format(self, basic_metadata: GenericMetadata) -> None:
        """Test isoformat with datetime returns correct ISO 8601 formatted string.

        Verifies that when GenericMetadata has a datetime value, the isoformat method
        returns the correct ISO 8601 formatted string representation with timezone information.
        This tests the datetime formatting functionality for metadata display and export operations.
        """
        # Arrange - fixture provides metadata with datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        expected_iso_format = "2024-01-15T12:30:45+00:00"

        # Act
        result = basic_metadata.isoformat()

        # Assert
        assert result == expected_iso_format, f"isoformat should return '{expected_iso_format}' but got '{result}'"
        assert isinstance(result, str), "isoformat should return a string type"
        assert "T" in result, "ISO format should contain 'T' separator between date and time"
        assert result.endswith("+00:00"), "UTC timezone should be represented as '+00:00' in ISO format"

    @pytest.mark.unit
    def test_isoformat_without_datetime_raises_value_error(self) -> None:
        """Test isoformat method raises ValueError when datetime is None.

        When GenericMetadata is initialized without a datetime value, calling isoformat()
        should raise a ValueError with a specific error message. This ensures that the
        method fails fast and provides clear feedback when the required datetime data
        is missing, preventing silent failures in date formatting operations.
        """
        # Arrange
        metadata = GenericMetadata()
        assert metadata.datetime is None, "Precondition: metadata should have no datetime value"

        # Act & Assert
        with pytest.raises(ValueError, match="Cannot format datetime: datetime is None"):
            metadata.isoformat()

    @pytest.mark.unit
    def test_comparison_operators_with_datetime(self, sample_datetime: datetime) -> None:
        """Test comparison operators between GenericMetadata and datetime objects.

        GenericMetadata should support comparison with datetime objects directly,
        allowing mixed comparisons between metadata instances and datetime values
        based on the datetime property of the metadata object.
        """
        # Arrange
        earlier_dt = datetime(2024, 1, 14, 12, 30, 45, tzinfo=UTC)
        later_dt = datetime(2024, 1, 16, 12, 30, 45, tzinfo=UTC)
        metadata = GenericMetadata(datetime_=sample_datetime)

        # Act & Assert - Test with datetime objects
        assert metadata > earlier_dt, "Metadata should be newer than earlier datetime"
        assert metadata < later_dt, "Metadata should be older than later datetime"
        assert metadata == sample_datetime, "Metadata should equal the same datetime"
        assert metadata <= sample_datetime, "Metadata should be less than or equal to same datetime"
        assert metadata >= sample_datetime, "Metadata should be greater than or equal to same datetime"

    @pytest.mark.unit
    def test_comparison_operators_with_metadata(self, sample_datetime: datetime) -> None:
        """Test comparison operators between GenericMetadata objects based on datetime values.

        GenericMetadata comparison is based solely on datetime values, testing all comparison
        operators (__lt__, __gt__, __eq__, __le__, __ge__) with different metadata instances.
        """
        # Arrange
        earlier_dt = datetime(2024, 1, 14, 12, 30, 45, tzinfo=UTC)
        later_dt = datetime(2024, 1, 16, 12, 30, 45, tzinfo=UTC)
        metadata = GenericMetadata(datetime_=sample_datetime)
        earlier_metadata = GenericMetadata(datetime_=earlier_dt)
        later_metadata = GenericMetadata(datetime_=later_dt)
        same_metadata = GenericMetadata(datetime_=sample_datetime)

        # Act & Assert - Test comparison operators with other GenericMetadata objects
        assert metadata > earlier_metadata, "Metadata with later datetime should be greater than earlier metadata"
        assert metadata < later_metadata, "Metadata with earlier datetime should be less than later metadata"
        assert metadata == same_metadata, "Metadata objects with identical datetime should be equal"
        assert metadata <= same_metadata, "Metadata should be less than or equal to identical metadata"
        assert metadata >= same_metadata, "Metadata should be greater than or equal to identical metadata"

        # Assert inverse relationships for completeness
        assert earlier_metadata < metadata, "Earlier metadata should be less than later metadata"
        assert later_metadata > metadata, "Later metadata should be greater than earlier metadata"

    @pytest.mark.unit
    def test_comparison_operators_with_none_datetime(self) -> None:
        """Test comparison operators when one metadata has None datetime.

        This tests the asymmetric behavior where None datetime is treated as
        "earlier than" any actual datetime value for sorting purposes.
        """
        # Arrange
        metadata_none = GenericMetadata()
        metadata_with_dt = GenericMetadata(datetime_=datetime(2024, 1, 15, tzinfo=UTC))

        # Act & Assert - None datetime should be less than any datetime
        assert metadata_none < metadata_with_dt, "None datetime should be less than any datetime"
        assert not (metadata_none > metadata_with_dt), "None datetime should not be greater than any datetime"
        assert not (metadata_with_dt < metadata_none), "Datetime should not be less than None datetime"
        assert metadata_with_dt > metadata_none, "Datetime should be greater than None datetime"

        # Assert inequality - None datetime should not equal actual datetime
        assert metadata_none != metadata_with_dt, "None datetime should not equal actual datetime"

    @pytest.mark.unit
    def test_comparison_operators_both_none_datetime(self) -> None:
        """Test comparison operators when both metadata objects have None datetime.

        Note: Current implementation has asymmetric behavior where None < None returns True.
        This documents the existing behavior which may need correction in future versions.
        """
        # Arrange
        metadata1 = GenericMetadata()
        metadata2 = GenericMetadata()

        # Act & Assert - Test current implementation behavior for None datetimes
        assert metadata1 == metadata2, "Two metadata with None datetime should be equal"
        assert metadata1 <= metadata2, "None datetime should be less than or equal to None datetime"
        assert metadata1 >= metadata2, "None datetime should be greater than or equal to None datetime"

        # Current implementation behavior: __lt__ returns True when self.datetime is None (regardless of other)
        # This is a documented behavior that may need review in future versions
        assert metadata1 < metadata2, "Current implementation: None datetime < None datetime returns True"
        assert not (metadata1 > metadata2), "None datetime should not be greater than None datetime"

    @pytest.mark.unit
    def test_comparison_operators_with_invalid_type_return_notimplemented(
        self,
        basic_metadata: GenericMetadata,
    ) -> None:
        """Test comparison operators return NotImplemented for unsupported types.

        When GenericMetadata comparison operators receive types other than GenericMetadata
        or datetime objects, they should return NotImplemented following Python's comparison
        protocol. This allows Python to try the reflected operation on the other object,
        enabling proper fallback behavior in comparison chains.
        """
        # Arrange
        test_values = [
            ("string_value", "string"),
            (12345, "integer"),
            ([1, 2, 3], "list"),
        ]
        comparison_methods = [
            ("__lt__", "<"),
            ("__gt__", ">"),
            ("__eq__", "=="),
            ("__le__", "<="),
            ("__ge__", ">="),
        ]

        # Act & Assert - Test all comparison operators with each invalid type
        for test_value, type_name in test_values:
            for method_name, operator_symbol in comparison_methods:
                result = getattr(basic_metadata, method_name)(test_value)
                assert (
                    result == NotImplemented
                ), f"{type_name.capitalize()} type should return NotImplemented for {operator_symbol} operator"

    @pytest.mark.unit
    def test_hash_method(self, sample_datetime: datetime) -> None:
        """Test __hash__ method enables use in sets and as dict keys.

        GenericMetadata instances with the same datetime should have identical
        hash values, enabling proper deduplication in sets and use as dict keys.
        Tests both regular datetimes and None datetime edge case.
        """
        # Arrange
        metadata1 = GenericMetadata(datetime_=sample_datetime, latitude=37.7749)
        metadata2 = GenericMetadata(datetime_=sample_datetime, longitude=-122.4194)
        different_datetime = datetime(2024, 1, 16, 12, 30, 45, tzinfo=UTC)
        metadata3 = GenericMetadata(datetime_=different_datetime)
        metadata_none1 = GenericMetadata(latitude=40.0)
        metadata_none2 = GenericMetadata(longitude=-75.0)

        # Act
        hash1 = hash(metadata1)
        hash2 = hash(metadata2)
        hash3 = hash(metadata3)
        hash_none1 = hash(metadata_none1)
        hash_none2 = hash(metadata_none2)
        metadata_set = {metadata1, metadata2, metadata3, metadata_none1, metadata_none2}

        # Assert
        assert (
            hash1 == hash2
        ), "Metadata objects with same datetime should have identical hashes regardless of other attributes"
        assert hash1 != hash3, "Metadata objects with different datetimes should have different hashes"
        assert hash2 != hash3, "Metadata objects with different datetimes should have different hashes"
        assert (
            hash_none1 == hash_none2
        ), "Metadata objects with None datetime should have identical hashes regardless of other attributes"
        assert hash1 != hash_none1, "Metadata with datetime should have different hash than None datetime"
        assert hash3 != hash_none1, "Metadata with datetime should have different hash than None datetime"
        assert (
            len(metadata_set) == 3
        ), "Set should deduplicate: one for same datetime, one for different datetime, one for None datetime"

    @pytest.mark.unit
    def test_hash_sha256_setter_with_non_hex_string(self) -> None:
        """Test hash_sha256 setter with non-hex string value.

        The hash_sha256 property should encode non-hex strings as UTF-8
        and return them as hexadecimal representation.
        """
        # Arrange
        metadata = GenericMetadata()
        new_hash = "new_hash_value"
        expected_hex = new_hash.encode("utf-8").hex()
        expected_bytes = new_hash.encode("utf-8")

        # Act
        metadata.hash_sha256 = new_hash

        # Assert
        assert metadata.hash_sha256 == expected_hex, "Setter should encode non-hex string as UTF-8 and return as hex"
        # Verify UTF-8 encoding through round-trip conversion
        assert (
            bytes.fromhex(metadata.hash_sha256) == expected_bytes
        ), "Round-trip conversion should preserve UTF-8 encoded bytes"

    @pytest.mark.unit
    def test_hash_sha256_setter_with_hex_string(self) -> None:
        """Test hash_sha256 setter with valid hex string value.

        The hash_sha256 property should accept valid hex strings and
        return them as-is after round-trip conversion through bytes.
        """
        # Arrange
        metadata = GenericMetadata()
        hex_hash = "abc123"
        expected_bytes = bytes.fromhex(hex_hash)

        # Act
        metadata.hash_sha256 = hex_hash

        # Assert
        assert (
            metadata.hash_sha256 == hex_hash
        ), "Setter should handle valid hex string and return it unchanged after round-trip conversion"
        assert isinstance(
            metadata.hash_sha256,
            str,
        ), "hash_sha256 property should return string type after setter operation"
        # Verify bytes preservation through round-trip conversion
        assert (
            bytes.fromhex(metadata.hash_sha256) == expected_bytes
        ), "Round-trip conversion should preserve bytes representation for valid hex strings"

    @pytest.mark.unit
    def test_hash_sha256_setter_with_none_value(self) -> None:
        """Test hash_sha256 setter with None value clears existing hash correctly.

        Verifies that when the hash_sha256 property is set to None, it properly clears
        any existing hash value both in the property interface and internal storage.
        This ensures the setter can reset hash values to a clean state.
        """
        # Arrange
        initial_hash = "abc123"
        metadata = GenericMetadata(hash_sha256_=initial_hash)
        assert metadata.hash_sha256 == initial_hash, "Precondition: initial hash should be set correctly"
        assert metadata.hash_sha256 is not None, "Precondition: hash should be accessible through property"

        # Act
        metadata.hash_sha256 = None

        # Assert
        assert metadata.hash_sha256 is None, "Setter should accept None and clear hash value from property interface"

    @pytest.mark.unit
    def test_format_hash_with_value(self, basic_metadata: GenericMetadata) -> None:
        """Test format_hash returns the hash value when hash exists.

        The format_hash method should return the same value as the hash_sha256 property,
        both providing the hash as a hexadecimal string representation.
        """
        # Arrange - basic_metadata fixture has hash_sha256_="abc123"

        # Act
        result = basic_metadata.format_hash()

        # Assert
        assert result is not None, "format_hash should return a value when hash exists"
        assert isinstance(result, str), "format_hash should return a string"
        assert result == "abc123", "format_hash should return the exact hex string that was provided"
        assert result == basic_metadata.hash_sha256, "format_hash should return the same value as hash_sha256 property"

    @pytest.mark.unit
    def test_format_hash_without_value_returns_none(self) -> None:
        """Test format_hash returns None when no hash is set.

        GenericMetadata.format_hash() should return None when no hash
        value is present, providing a consistent interface for hash formatting.
        """
        # Arrange
        metadata = GenericMetadata()
        assert metadata.hash_sha256 is None, "Precondition: metadata should have no hash value"

        # Act
        result = metadata.format_hash()

        # Assert
        assert result is None, "format_hash should return None when no hash value is set"
        assert metadata.hash_sha256 is None, "Hash property should remain None after format_hash call"

    @pytest.mark.unit
    def test_create_dataset_metadata_dry_run(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
        sample_datetime: datetime,
    ) -> None:
        """Test create_dataset_metadata with dry_run=True does not save files but processes data correctly.

        Verifies that dry_run=True prevents file creation while ensuring the method
        still processes the input data correctly, including metadata serialization logic.
        """
        # Arrange
        mock_saver = mocker.patch("marimba.core.schemas.generic.json_saver")
        items: dict[str, list[BaseMetadata]] = {
            "file1.jpg": [GenericMetadata(datetime_=sample_datetime, latitude=37.7749, context="test")],
            "file2.jpg": [GenericMetadata(datetime_=sample_datetime, longitude=-122.4194)],
        }

        # Act
        GenericMetadata.create_dataset_metadata(
            dataset_name="test_dataset",
            root_dir=tmp_path,
            items=items,
            dry_run=True,
            logger=_LOGGER,
        )

        # Assert
        mock_saver.assert_not_called()  # Dry run should prevent saver from being called

        # Verify no metadata files were created in filesystem
        metadata_files = list(tmp_path.glob("metadata*"))
        assert len(metadata_files) == 0, "Dry run should not create any metadata files in filesystem"

    @pytest.mark.unit
    def test_create_dataset_metadata_with_custom_saver(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
        sample_datetime: datetime,
    ) -> None:
        """Test create_dataset_metadata with custom saver function override.

        Verifies that when a custom saver is provided via saver_overwrite parameter,
        it is used instead of the default json_saver and receives the correct parameters.
        """
        # Arrange
        mock_default_saver = mocker.patch("marimba.core.schemas.generic.json_saver")
        mock_custom_saver = mocker.Mock()
        items: dict[str, list[BaseMetadata]] = {
            "file1.jpg": [GenericMetadata(datetime_=sample_datetime, latitude=37.7749, context="test")],
        }

        # Act
        GenericMetadata.create_dataset_metadata(
            dataset_name="test_dataset",
            root_dir=tmp_path,
            items=items,
            saver_overwrite=mock_custom_saver,
            logger=_LOGGER,
        )

        # Assert
        mock_default_saver.assert_not_called()  # Default json_saver should not be called when custom saver is provided
        mock_custom_saver.assert_called_once()  # Custom saver should be called exactly once

        # Verify custom saver received correct parameters
        call_args = mock_custom_saver.call_args
        assert len(call_args[0]) == 3, "Custom saver should receive exactly 3 positional arguments"
        assert call_args[0][0] == tmp_path, "First argument should be the root directory path"
        assert call_args[0][1] == "metadata", "Second argument should be the default metadata name"
        assert isinstance(call_args[0][2], dict), "Third argument should be the metadata dictionary"

        # Verify metadata structure passed to custom saver
        metadata_dict = call_args[0][2]
        assert "header" in metadata_dict, "Metadata should contain header field"
        assert "items" in metadata_dict, "Metadata should contain items field"
        assert metadata_dict["header"]["name"] == "test_dataset", "Dataset name should match input value"
        assert "file1.jpg" in metadata_dict["items"], "Items should contain the input file key"

        # Verify file metadata structure (latitude is common, so deduped to header)
        file_metadata = metadata_dict["items"]["file1.jpg"][0]
        assert file_metadata["datetime"] == sample_datetime.isoformat(), "Datetime should be serialized as ISO format"
        assert metadata_dict["header"]["latitude"] == 37.7749, "Common latitude should be in header"
        assert metadata_dict["header"]["context"] == "test", "Common context should be in header"

    @pytest.mark.unit
    def test_create_dataset_metadata_with_custom_name(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
        sample_datetime: datetime,
    ) -> None:
        """Test create_dataset_metadata uses custom metadata filename when provided.

        When a custom metadata_name is provided, the method should pass it to the saver
        function instead of using the default metadata name, ensuring proper file naming.
        """
        # Arrange
        items: dict[str, list[BaseMetadata]] = {
            "file1.jpg": [GenericMetadata(datetime_=sample_datetime)],
        }
        mock_saver = mocker.patch("marimba.core.schemas.generic.json_saver")
        custom_name = "custom_metadata"

        # Act
        GenericMetadata.create_dataset_metadata(
            dataset_name="test_dataset",
            root_dir=tmp_path,
            items=items,
            metadata_name=custom_name,
            logger=_LOGGER,
        )

        # Assert
        mock_saver.assert_called_once(), "json_saver should be called exactly once"
        call_args = mock_saver.call_args
        assert call_args[0][0] == tmp_path, "First argument should be the root directory path"
        assert call_args[0][1] == custom_name, "Second argument should be the custom metadata name"
        assert isinstance(call_args[0][2], dict), "Third argument should be the metadata dictionary"

        # Verify the structure of the metadata dictionary
        metadata_dict = call_args[0][2]
        assert "header" in metadata_dict, "Metadata should contain header field"
        assert "items" in metadata_dict, "Metadata should contain items field"
        assert metadata_dict["header"]["name"] == "test_dataset", "Dataset name should match input"

    @pytest.mark.unit
    def test_create_dataset_metadata_with_items_missing_format_hash(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test create_dataset_metadata handles BaseMetadata items without format_hash method gracefully.

        When BaseMetadata items lack the format_hash method, the method should continue processing
        and set hash_sha256 to None in the output metadata, demonstrating robust error handling.
        """
        # Arrange
        mock_metadata = mocker.Mock(spec=BaseMetadata)
        mock_metadata.datetime = None
        mock_metadata.latitude = 42.0
        mock_metadata.longitude = -71.0
        mock_metadata.altitude = 10.0
        mock_metadata.context = "test context"
        mock_metadata.license = "MIT"
        mock_metadata.creators = ["Test Creator"]
        # Mock does not have format_hash method, simulating missing method scenario

        items: dict[str, list[BaseMetadata]] = {"file1.jpg": [mock_metadata]}
        mock_saver = mocker.patch("marimba.core.schemas.generic.json_saver")

        # Act
        GenericMetadata.create_dataset_metadata(
            dataset_name="test_dataset",
            root_dir=tmp_path,
            items=items,
            logger=_LOGGER,
        )

        # Assert
        mock_saver.assert_called_once(), "json_saver should be called despite missing format_hash method"

        # Verify saver was called with correct parameters
        call_args = mock_saver.call_args
        assert len(call_args[0]) == 3, "Saver should receive exactly 3 positional arguments"
        assert call_args[0][0] == tmp_path, "First argument should be the root directory path"
        assert call_args[0][1] == "metadata", "Second argument should be the default metadata name"
        assert isinstance(call_args[0][2], dict), "Third argument should be the metadata dictionary"

        # Verify metadata structure contains expected top-level fields
        metadata_dict = call_args[0][2]
        assert "header" in metadata_dict, "Metadata should contain header field"
        assert "items" in metadata_dict, "Metadata should contain items field"
        assert metadata_dict["header"]["name"] == "test_dataset", "Dataset name should match input value"
        assert "file1.jpg" in metadata_dict["items"], "Items should contain the input file key"

        # Common fields (all same across one item) are deduplicated to header
        header = metadata_dict["header"]
        assert header["latitude"] == 42.0, "Common latitude should be in header"
        assert header["longitude"] == -71.0, "Common longitude should be in header"
        assert header["altitude"] == 10.0, "Common altitude should be in header"
        assert header["context"] == "test context", "Common context should be in header"
        assert header["license"] == "MIT", "Common license should be in header"
        assert header["creators"] == ["Test Creator"], "Common creators should be in header"

        # Item should have datetime only (other fields deduplicated); hash_sha256 absent (no format_hash)
        file_metadata = metadata_dict["items"]["file1.jpg"][0]
        assert "datetime" not in file_metadata, "Datetime should be absent when mock has None datetime"
        assert "latitude" not in file_metadata, "Deduplicated latitude should not appear in item"

    @pytest.mark.unit
    def test_process_files_method_noop_with_empty_mapping(self) -> None:
        """Test process_files method no-op implementation with empty dataset mapping.

        GenericMetadata implements process_files as a no-op method since it doesn't
        require file processing. This test verifies the method interface contract
        by ensuring it accepts all required parameters and returns None without errors.
        """
        # Arrange
        empty_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]] = {}

        # Act - Method should execute without raising exceptions
        result = GenericMetadata.process_files(
            dataset_mapping=empty_mapping,
            max_workers=1,
            logger=None,
            dry_run=True,
            chunk_size=10,
        )

        # Assert
        assert result is None, "process_files should return None for GenericMetadata no-op implementation"
        assert len(empty_mapping) == 0, "Input mapping should remain unchanged for no-op implementation"

    @pytest.mark.unit
    def test_process_files_method_noop_with_populated_mapping(self, sample_datetime: datetime) -> None:
        """Test process_files method no-op implementation with populated dataset mapping.

        GenericMetadata implements process_files as a no-op method that doesn't
        modify the input data. This test verifies that the method preserves input
        data integrity while fulfilling the interface contract.
        """
        # Arrange
        metadata_item = GenericMetadata(datetime_=sample_datetime, latitude=37.7749)
        test_path = Path("/test/path/file.jpg")
        test_options = {"test_option": "value"}
        populated_mapping: dict[Path, tuple[list[BaseMetadata], dict[str, Any] | None]] = {
            test_path: ([metadata_item], test_options),
        }
        original_mapping_size = len(populated_mapping)
        original_datetime = metadata_item.datetime
        original_latitude = metadata_item.latitude

        # Act
        result = GenericMetadata.process_files(
            dataset_mapping=populated_mapping,
            max_workers=2,
            logger=None,
            dry_run=False,
            chunk_size=5,
        )

        # Assert
        assert result is None, "process_files should return None for GenericMetadata no-op implementation"
        assert len(populated_mapping) == original_mapping_size, "Input mapping size should remain unchanged for no-op"
        assert test_path in populated_mapping, "Original mapping keys should be preserved by no-op implementation"

        # Verify mapping structure integrity is maintained by no-op
        stored_metadata, stored_options = populated_mapping[test_path]
        assert len(stored_metadata) == 1, "Metadata list should remain unchanged"
        assert stored_metadata[0] is metadata_item, "Original metadata instance should be preserved"
        assert stored_options is test_options, "Original options should be preserved"

        # Verify metadata item properties remain unchanged after no-op processing
        assert metadata_item.datetime == original_datetime, "Metadata datetime should remain unchanged by no-op"
        assert metadata_item.latitude == original_latitude, "Metadata latitude should remain unchanged by no-op"


class TestGenericMetadataDeduplication:
    """Test auto-deduplication helpers in GenericMetadata."""

    @pytest.mark.unit
    def test_extract_common_fields_all_same(self) -> None:
        """Fields identical across all items are returned as common."""
        items: dict[str, list[BaseMetadata]] = {
            "img1.jpg": [GenericMetadata(latitude=45.0, longitude=-123.0, license_="CC-BY")],
            "img2.jpg": [GenericMetadata(latitude=45.0, longitude=-123.0, license_="CC-BY")],
        }
        result = GenericMetadata._extract_common_fields(items)
        assert result["latitude"] == 45.0
        assert result["longitude"] == -123.0
        assert result["license"] == "CC-BY"

    @pytest.mark.unit
    def test_extract_common_fields_varying_excluded(self) -> None:
        """Fields that differ are excluded from common fields."""
        items: dict[str, list[BaseMetadata]] = {
            "img1.jpg": [GenericMetadata(latitude=45.0, longitude=-123.0)],
            "img2.jpg": [GenericMetadata(latitude=46.0, longitude=-123.0)],
        }
        result = GenericMetadata._extract_common_fields(items)
        assert "latitude" not in result
        assert result["longitude"] == -123.0

    @pytest.mark.unit
    def test_extract_common_fields_empty(self) -> None:
        """Empty input returns empty dict."""
        assert GenericMetadata._extract_common_fields({}) == {}

    @pytest.mark.unit
    def test_deduplicate_items_removes_common_fields(self) -> None:
        """Common fields are absent from deduplicated items."""
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        items: dict[str, list[BaseMetadata]] = {
            "img1.jpg": [GenericMetadata(datetime_=dt, latitude=45.0, license_="CC-BY")],
            "img2.jpg": [GenericMetadata(datetime_=dt, latitude=45.0, license_="CC-BY")],
        }
        common = {"latitude": 45.0, "license": "CC-BY"}
        result = GenericMetadata._deduplicate_items(items, common)
        for filename in ("img1.jpg", "img2.jpg"):
            item = result[filename][0]
            assert "latitude" not in item, "Common latitude should be removed from item"
            assert "license" not in item, "Common license should be removed from item"
            assert "datetime" in item, "Datetime should remain in item"

    @pytest.mark.unit
    def test_create_dataset_metadata_promotes_common_fields_to_header(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Common fields appear in header and not in items."""
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        items: dict[str, list[BaseMetadata]] = {
            "img1.jpg": [GenericMetadata(datetime_=dt, latitude=45.0, license_="CC-BY")],
            "img2.jpg": [GenericMetadata(datetime_=dt, latitude=45.0, license_="CC-BY")],
        }
        mock_saver = mocker.patch("marimba.core.schemas.generic.json_saver")

        GenericMetadata.create_dataset_metadata(
            dataset_name="TestDataset",
            root_dir=tmp_path,
            items=items,
            logger=_LOGGER,
        )

        call_args = mock_saver.call_args
        data = call_args[0][2]
        header = data["header"]
        assert header["latitude"] == 45.0, "Common latitude should be in header"
        assert header["license"] == "CC-BY", "Common license should be in header"

        for filename in ("img1.jpg", "img2.jpg"):
            item = data["items"][filename][0]
            assert "latitude" not in item, "Latitude should not remain in item"
            assert "license" not in item, "License should not remain in item"

    @pytest.mark.unit
    def test_create_dataset_metadata_non_common_fields_stay_in_items(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Fields that vary across items remain in item data."""
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        items: dict[str, list[BaseMetadata]] = {
            "img1.jpg": [GenericMetadata(datetime_=dt, latitude=45.0)],
            "img2.jpg": [GenericMetadata(datetime_=dt, latitude=46.0)],
        }
        mock_saver = mocker.patch("marimba.core.schemas.generic.json_saver")

        GenericMetadata.create_dataset_metadata(
            dataset_name="TestDataset",
            root_dir=tmp_path,
            items=items,
            logger=_LOGGER,
        )

        data = mock_saver.call_args[0][2]
        assert "latitude" not in data["header"], "Varying latitude should not be in header"
        assert data["items"]["img1.jpg"][0]["latitude"] == 45.0
        assert data["items"]["img2.jpg"][0]["latitude"] == 46.0
