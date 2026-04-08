import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_mock
from ifdo.models import ImageData

if TYPE_CHECKING:
    from marimba.core.schemas.base import BaseMetadata

from marimba.core.schemas.ifdo import iFDOMetadata


class TestiFDOMetadataProperties:
    """Test all properties of iFDOMetadata class.

    This test class verifies that all property getters and setters function correctly,
    handle edge cases properly, and enforce type validation where appropriate.
    """

    @pytest.fixture
    def sample_image_data(self) -> ImageData:
        """Create a sample ImageData for testing."""
        return ImageData(
            image_datetime=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
            image_latitude=45.0,
            image_longitude=-123.0,
            image_altitude_meters=100.0,
            image_hash_sha256="abc123",
        )

    @pytest.fixture
    def ifdo_metadata(self, sample_image_data: ImageData) -> iFDOMetadata:
        """Create iFDOMetadata instance for testing."""
        return iFDOMetadata(sample_image_data)

    @pytest.mark.unit
    def test_datetime_property(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test datetime property returns correct value from ImageData.

        This test verifies that the datetime property correctly retrieves the datetime
        from the underlying ImageData object without transformation.
        """
        # Arrange
        expected_datetime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

        # Act
        result = ifdo_metadata.datetime

        # Assert
        assert result == expected_datetime, "Datetime property should return exact ImageData datetime value"

    @pytest.mark.unit
    def test_datetime_property_none(self) -> None:
        """Test datetime property returns None when ImageData.image_datetime is None.

        This test ensures that None values are handled correctly without raising exceptions.
        """
        # Arrange
        image_data = ImageData(image_datetime=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.datetime

        # Assert
        assert result is None, "Datetime property should return None when ImageData.image_datetime is None"

    @pytest.mark.unit
    def test_datetime_property_wrong_type(self) -> None:
        """Test datetime property raises TypeError when image_datetime contains invalid type.

        This test ensures that type validation is properly enforced in the datetime property.
        When the underlying ImageData.image_datetime is set to a non-datetime value
        (which could happen with malformed data or direct assignment), the property
        should fail fast with a clear TypeError rather than silently returning invalid data.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_datetime = "not a datetime"  # Force invalid type to trigger TypeError
        metadata = iFDOMetadata(image_data)

        # Act & Assert
        with pytest.raises(TypeError, match=r"Expected datetime or None, got <class 'str'>"):
            _ = metadata.datetime

    @pytest.mark.unit
    def test_latitude_property(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test latitude property returns correct value from ImageData.

        This test verifies that the latitude property correctly retrieves the latitude
        from the underlying ImageData object without transformation.
        """
        # Arrange
        expected_latitude = 45.0

        # Act
        result = ifdo_metadata.latitude

        # Assert
        assert result == expected_latitude, "Latitude property should return exact ImageData latitude value"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == ifdo_metadata.primary_image_data.image_latitude
        ), "Latitude property should retrieve value from primary_image_data.image_latitude"

    @pytest.mark.unit
    def test_latitude_property_none(self) -> None:
        """Test latitude property returns None when ImageData.image_latitude is None.

        This test ensures that None latitude values are handled correctly.
        """
        # Arrange
        image_data = ImageData(image_latitude=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.latitude

        # Assert
        assert result is None, "Latitude property should return None when ImageData.image_latitude is None"

    @pytest.mark.unit
    def test_latitude_property_int(self) -> None:
        """Test latitude property correctly handles integer input by converting to float.

        This test verifies that integer latitude values are properly handled and returned
        as float values for type consistency, following the property's type casting behavior.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_latitude = 45  # Force integer type to test property behavior
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.latitude

        # Assert
        assert result == 45.0, "Integer latitude values should be converted to float"
        assert isinstance(result, (int, float)), "Property should return numeric type"

        # Verify the underlying data is actually accessed correctly
        assert (
            result == metadata.primary_image_data.image_latitude
        ), "Property should return the exact value from underlying ImageData"

    @pytest.mark.unit
    def test_latitude_property_wrong_type(self) -> None:
        """Test latitude property raises TypeError when image_latitude contains invalid type.

        This test ensures that type validation is properly enforced in the latitude property.
        When the underlying ImageData.image_latitude is set to a non-numeric value
        (which could happen with malformed data or direct assignment), the property
        should fail fast with a clear TypeError rather than silently returning invalid data.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_latitude = "not a number"  # Force invalid type to trigger TypeError
        metadata = iFDOMetadata(image_data)

        # Verify the invalid data was actually set in the underlying ImageData
        assert (
            metadata.primary_image_data.image_latitude == "not a number"
        ), "Test setup should have invalid type in underlying ImageData"

        # Act & Assert
        with pytest.raises(TypeError, match=r"Expected float or None, got <class 'str'>"):
            _ = metadata.latitude

    @pytest.mark.unit
    def test_longitude_property(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test longitude property returns correct value from ImageData.

        This test verifies that the longitude property correctly retrieves the longitude
        from the underlying ImageData object without transformation.
        """
        # Arrange
        expected_longitude = -123.0

        # Act
        result = ifdo_metadata.longitude

        # Assert
        assert result == expected_longitude, "Longitude property should return exact ImageData longitude value"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == ifdo_metadata.primary_image_data.image_longitude
        ), "Longitude property should retrieve value from primary_image_data.image_longitude"

    @pytest.mark.unit
    def test_longitude_property_none(self) -> None:
        """Test longitude property returns None when ImageData.image_longitude is None.

        This test ensures that None longitude values are handled correctly without
        raising exceptions, maintaining consistent behavior for missing coordinate data.
        """
        # Arrange
        image_data = ImageData(image_longitude=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.longitude

        # Assert
        assert result is None, "Longitude property should return None when ImageData.image_longitude is None"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_longitude
        ), "Longitude property should retrieve None value from primary_image_data.image_longitude"

    @pytest.mark.unit
    def test_longitude_property_int(self) -> None:
        """Test longitude property correctly handles integer input.

        This test verifies that integer longitude values are properly handled and returned
        as numeric values, maintaining type compatibility with the property's type annotation.
        """
        # Arrange
        image_data = ImageData(image_longitude=-123)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.longitude

        # Assert
        assert result == -123, "Integer longitude values should be preserved as-is"
        assert isinstance(result, (int, float)), "Longitude should be returned as numeric type"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_longitude
        ), "Property should return the exact value from underlying ImageData"

    @pytest.mark.unit
    def test_longitude_property_wrong_type(self) -> None:
        """Test longitude property raises TypeError when image_longitude contains invalid type.

        This test ensures that type validation is properly enforced in the longitude property.
        When the underlying ImageData.image_longitude is set to a non-numeric value
        (which could happen with malformed data or direct assignment), the property
        should fail fast with a clear TypeError rather than silently returning invalid data.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_longitude = "not a number"  # Force invalid type to trigger TypeError
        metadata = iFDOMetadata(image_data)

        # Verify the invalid data was actually set in the underlying ImageData
        assert (
            metadata.primary_image_data.image_longitude == "not a number"
        ), "Test setup should have invalid type in underlying ImageData"

        # Act & Assert
        with pytest.raises(TypeError, match=r"Expected float or None, got <class 'str'>"):
            _ = metadata.longitude

    @pytest.mark.unit
    def test_altitude_property(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test altitude property returns correct value from ImageData.

        This test verifies that the altitude property correctly retrieves the altitude
        from the underlying ImageData object without transformation.
        """
        # Arrange
        expected_altitude = 100.0

        # Act
        result = ifdo_metadata.altitude

        # Assert
        assert result == expected_altitude, "Altitude property should return exact ImageData altitude value"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == ifdo_metadata.primary_image_data.image_altitude_meters
        ), "Altitude property should retrieve value from primary_image_data.image_altitude_meters"

    @pytest.mark.unit
    def test_altitude_property_none(self) -> None:
        """Test altitude property returns None when ImageData.image_altitude_meters is None.

        This test ensures that None altitude values are handled correctly without
        raising exceptions, maintaining consistent behavior for missing altitude data.
        """
        # Arrange
        image_data = ImageData(image_altitude_meters=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.altitude

        # Assert
        assert result is None, "Altitude property should return None when ImageData.image_altitude_meters is None"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_altitude_meters
        ), "Altitude property should retrieve None value from primary_image_data.image_altitude_meters"

    @pytest.mark.unit
    def test_altitude_property_int(self) -> None:
        """Test altitude property correctly handles integer input by converting to float.

        This test verifies that integer altitude values are properly handled and returned
        as float values for type consistency.
        """
        # Arrange
        image_data = ImageData(image_altitude_meters=100)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.altitude

        # Assert
        assert result == 100.0, "Integer altitude values should be converted to float"
        assert isinstance(result, float), "Altitude should be returned as float type"

    @pytest.mark.unit
    def test_altitude_property_wrong_type(self) -> None:
        """Test altitude property raises TypeError when image_altitude_meters contains invalid type.

        This test ensures that type validation is properly enforced in the altitude property.
        When the underlying ImageData.image_altitude_meters is set to a non-numeric value
        (which could happen with malformed data or direct assignment), the property
        should fail fast with a clear TypeError rather than silently returning invalid data.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_altitude_meters = "not a number"  # Force invalid type to trigger TypeError
        metadata = iFDOMetadata(image_data)

        # Act & Assert
        with pytest.raises(TypeError, match=r"Expected float or None, got <class 'str'>"):
            _ = metadata.altitude

    @pytest.mark.unit
    def test_context_property_with_name(self) -> None:
        """Test context property returns name attribute from ImageData.image_context.

        This test verifies that when image_context has a name attribute,
        the context property correctly extracts and returns it from the
        underlying ImageData object using real iFDO ImageContext objects.
        """
        # Arrange
        from ifdo.models.ifdo_core import ImageContext

        # Create a real ImageContext object with name and uri
        context_obj = ImageContext(name="test context", uri="http://example.com/context")
        image_data = ImageData(image_context=context_obj)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.context

        # Assert
        assert result == "test context", "Context property should return the name attribute of image_context"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_context.name
        ), "Context property should retrieve value from primary_image_data.image_context.name"

        # Verify the underlying context object is the real iFDO type
        assert isinstance(
            metadata.primary_image_data.image_context,
            ImageContext,
        ), "Underlying image_context should be a real iFDO ImageContext instance"

    @pytest.mark.unit
    def test_context_property_none(self) -> None:
        """Test context property returns None when ImageData.image_context is None.

        This test ensures that None context values are handled correctly without
        raising exceptions, maintaining consistent behavior for missing context data.
        """
        # Arrange
        image_data = ImageData(image_context=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.context

        # Assert
        assert result is None, "Context property should return None when ImageData.image_context is None"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_context
        ), "Context property should retrieve None value from primary_image_data.image_context"

    @pytest.mark.unit
    def test_license_property_with_name(self) -> None:
        """Test license property returns name attribute from ImageData.image_license.

        This test verifies that when image_license has a name attribute,
        the license property correctly extracts and returns it from the
        underlying ImageData object using real iFDO ImageLicense objects.
        """
        # Arrange
        from ifdo.models.ifdo_core import ImageLicense

        # Create a real ImageLicense object with name and uri
        license_obj = ImageLicense(name="MIT License", uri="https://opensource.org/licenses/MIT")
        image_data = ImageData(image_license=license_obj)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.license

        # Assert
        assert result == "MIT License", "License property should return the name attribute of image_license"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_license.name
        ), "License property should retrieve value from primary_image_data.image_license.name"

        # Verify the underlying license object is the real iFDO type
        assert isinstance(
            metadata.primary_image_data.image_license,
            ImageLicense,
        ), "Underlying image_license should be a real iFDO ImageLicense instance"

    @pytest.mark.unit
    def test_license_property_none(self) -> None:
        """Test license property returns None when ImageData.image_license is None.

        This test ensures that None license values are handled correctly without
        raising exceptions, maintaining consistent behavior for missing license data.
        """
        # Arrange
        image_data = ImageData(image_license=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.license

        # Assert
        assert result is None, "License property should return None when ImageData.image_license is None"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == metadata.primary_image_data.image_license
        ), "License property should retrieve None value from primary_image_data.image_license"

    @pytest.mark.unit
    def test_creators_property_with_names(self) -> None:
        """Test creators property returns list of names from ImageData.image_creators.

        This test verifies that when image_creators contains ImageCreator objects,
        the creators property correctly extracts and returns all creator names as a list
        using real iFDO ImageCreator objects.
        """
        # Arrange
        from ifdo.models.ifdo_core import ImageCreator

        # Create real ImageCreator objects with name and uri
        creator1 = ImageCreator(name="Creator One", uri="https://orcid.org/0000-0000-0000-0001")
        creator2 = ImageCreator(name="Creator Two", uri="https://orcid.org/0000-0000-0000-0002")

        image_data = ImageData(image_creators=[creator1, creator2])
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.creators

        # Assert
        expected = ["Creator One", "Creator Two"]
        assert (
            result == expected
        ), f"Creators property should return list of creator names: expected {expected}, got {result}"

        # Verify the values are extracted from the underlying ImageData
        assert len(result) == len(
            metadata.primary_image_data.image_creators,
        ), "Creators property should return same number of items as underlying image_creators"

        for i, (creator_name, source_creator) in enumerate(
            zip(result, metadata.primary_image_data.image_creators, strict=False),
        ):
            assert (
                creator_name == source_creator.name
            ), f"Creator {i} name should match underlying image_creators[{i}].name"
            assert isinstance(
                source_creator,
                ImageCreator,
            ), f"Creator {i} should be a real iFDO ImageCreator instance, got {type(source_creator)}"

    @pytest.mark.unit
    def test_creators_property_empty(self) -> None:
        """Test creators property returns empty list when ImageData.image_creators is empty.

        This test ensures that empty creator lists are handled correctly without exceptions.
        """
        # Arrange
        image_data = ImageData(image_creators=[])
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.creators

        # Assert
        assert result == [], "Creators property should return empty list when ImageData.image_creators is empty"

    @pytest.mark.unit
    def test_creators_property_none(self) -> None:
        """Test creators property returns empty list when ImageData.image_creators is None.

        This test ensures that None creator values are handled correctly by returning an empty list.
        """
        # Arrange
        image_data = ImageData(image_creators=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.creators

        # Assert
        assert result == [], "Creators property should return empty list when ImageData.image_creators is None"

    @pytest.mark.unit
    def test_hash_sha256_property(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test hash_sha256 property returns correct value from ImageData.

        This test verifies that the hash_sha256 property correctly retrieves the hash
        from the underlying ImageData object without transformation.
        """
        # Arrange
        expected_hash = "abc123"

        # Act
        result = ifdo_metadata.hash_sha256

        # Assert
        assert (
            result == expected_hash
        ), f"Hash SHA256 property should return exact ImageData hash value: expected '{expected_hash}', got '{result}'"

        # Verify the value actually comes from the underlying ImageData
        assert (
            result == ifdo_metadata.primary_image_data.image_hash_sha256
        ), "Hash SHA256 property should retrieve value from primary_image_data.image_hash_sha256"

    @pytest.mark.unit
    def test_hash_sha256_property_none(self) -> None:
        """Test hash_sha256 property returns None when ImageData.image_hash_sha256 is None.

        This test ensures that None hash values are handled correctly.
        """
        # Arrange
        image_data = ImageData(image_hash_sha256=None)
        metadata = iFDOMetadata(image_data)

        # Act
        result = metadata.hash_sha256

        # Assert
        assert result is None, "Hash SHA256 property should return None when ImageData.image_hash_sha256 is None"

    @pytest.mark.unit
    def test_hash_sha256_property_wrong_type(self) -> None:
        """Test hash_sha256 property raises TypeError when image_hash_sha256 contains invalid type.

        This test ensures that type validation is properly enforced in the hash_sha256 property.
        When the underlying ImageData.image_hash_sha256 is set to a non-string value
        (which could happen with malformed data or direct assignment), the property
        should fail fast with a clear TypeError rather than silently returning invalid data.
        """
        # Arrange
        image_data = ImageData()
        image_data.image_hash_sha256 = 123  # Force invalid type to trigger TypeError
        metadata = iFDOMetadata(image_data)

        # Act & Assert
        with pytest.raises(TypeError, match=r"Expected str or None, got <class 'int'>"):
            _ = metadata.hash_sha256

    @pytest.mark.unit
    def test_hash_sha256_setter(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test hash_sha256 setter correctly updates underlying ImageData.image_hash_sha256.

        This test verifies that the setter properly propagates the new hash value
        to the underlying ImageData object, can be retrieved via the getter,
        and directly updates the primary_image_data.image_hash_sha256 attribute.
        """
        # Arrange
        new_hash = "new_hash_value_123"
        original_hash = ifdo_metadata.hash_sha256

        # Act
        ifdo_metadata.hash_sha256 = new_hash

        # Assert
        result = ifdo_metadata.hash_sha256
        assert (
            result == new_hash
        ), f"Hash SHA256 getter should return updated value: expected '{new_hash}', got '{result}'"

        # Verify underlying ImageData was actually updated
        actual_hash = ifdo_metadata.primary_image_data.image_hash_sha256
        assert (
            actual_hash == new_hash
        ), f"Underlying ImageData.image_hash_sha256 should be updated: expected '{new_hash}', got '{actual_hash}'"

        # Verify the value actually changed from original
        assert (
            result != original_hash
        ), f"Hash value should have changed from original '{original_hash}' to '{new_hash}'"

    @pytest.mark.unit
    def test_hash_sha256_property_none_after_direct_assignment(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test hash_sha256 property returns None when underlying ImageData is directly modified.

        This test verifies that when the underlying ImageData.image_hash_sha256 is directly
        set to None (bypassing the property setter), the property getter correctly handles
        and returns None. This tests the property's robustness against direct data modifications.
        """
        # Arrange - Verify initial state
        original_hash = ifdo_metadata.hash_sha256
        assert original_hash is not None, "Test setup requires initial hash to be non-None"

        # Act - Directly modify underlying ImageData to test getter behavior
        ifdo_metadata.primary_image_data.image_hash_sha256 = None

        # Assert - Property getter should return None
        result = ifdo_metadata.hash_sha256
        assert result is None, "Hash SHA256 property should return None when underlying data is None"

        # Assert - Verify underlying data was actually modified
        assert (
            ifdo_metadata.primary_image_data.image_hash_sha256 is None
        ), "Underlying ImageData.image_hash_sha256 should be None after direct assignment"

    @pytest.mark.unit
    def test_hash_sha256_getter_type_validation_after_corruption(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test hash_sha256 getter validates corrupted underlying data and raises TypeError.

        This test verifies that when the underlying ImageData.image_hash_sha256 is corrupted
        with an invalid type (bypassing the setter), the getter properly validates the data
        and raises a TypeError with a clear message rather than returning invalid data.
        """
        # Arrange - Verify initial state is valid
        original_hash = ifdo_metadata.hash_sha256
        assert isinstance(original_hash, str), "Test setup requires initial hash to be valid string"

        # Arrange - Corrupt underlying data to simulate data integrity issue
        ifdo_metadata.primary_image_data.image_hash_sha256 = 123  # Invalid type (int instead of str)

        # Verify the corruption actually occurred in the underlying data
        assert (
            ifdo_metadata.primary_image_data.image_hash_sha256 == 123
        ), "Test setup should have corrupted the underlying data with invalid type"

        # Act & Assert - Getter should detect corruption and raise TypeError
        with pytest.raises(TypeError, match=r"Expected str or None, got <class 'int'>"):
            _ = ifdo_metadata.hash_sha256

    @pytest.mark.unit
    def test_is_video_property_false(self, ifdo_metadata: iFDOMetadata) -> None:
        """Test is_video property returns False for single ImageData object.

        This test verifies that when iFDOMetadata is initialized with a single ImageData
        object, the is_video property correctly returns False, indicating that the
        metadata represents a single image rather than a video/sequence.
        """
        # Arrange
        # (ifdo_metadata fixture provides single ImageData instance)

        # Act
        result = ifdo_metadata.is_video

        # Assert
        assert result is False, f"is_video property should return False for single ImageData object, got {result}"

        # Verify the underlying data structure supports this result
        assert not isinstance(
            ifdo_metadata.image_data,
            list,
        ), "Underlying image_data should not be a list for single ImageData initialization"

    @pytest.mark.unit
    def test_is_video_property_true(self) -> None:
        """Test is_video property returns True for list of ImageData objects.

        This test verifies that when iFDOMetadata is initialized with a list of ImageData
        objects, the is_video property correctly returns True, indicating video/sequence data.
        The test confirms both the boolean result and the underlying data structure that enables it.
        """
        # Arrange
        image_data_list = [ImageData(), ImageData()]
        metadata = iFDOMetadata(image_data_list)

        # Act
        result = metadata.is_video

        # Assert
        assert result is True, f"is_video property should return True for list of ImageData objects, got {result}"

        # Verify the underlying data structure supports this result
        assert isinstance(
            metadata.image_data,
            list,
        ), "Underlying image_data should be a list when is_video is True"
        assert len(metadata.image_data) == 2, "List should contain the expected number of ImageData objects"

    @pytest.mark.unit
    def test_primary_image_data_single(self, sample_image_data: ImageData) -> None:
        """Test primary_image_data returns single ImageData when initialized with single object.

        This test verifies that for single ImageData initialization, the primary_image_data
        property returns the exact same ImageData instance with all properties preserved.
        """
        # Arrange
        metadata = iFDOMetadata(sample_image_data)
        expected_datetime = sample_image_data.image_datetime
        expected_altitude = sample_image_data.image_altitude_meters
        expected_hash = sample_image_data.image_hash_sha256

        # Act
        result = metadata.primary_image_data

        # Assert - Verify identity (same object reference)
        assert result is sample_image_data, (
            "primary_image_data should return the exact same ImageData instance "
            f"for single ImageData initialization, got different object: {type(result)}"
        )

        # Assert - Verify the returned object maintains all expected properties
        assert result.image_datetime == expected_datetime, (
            f"Returned ImageData should preserve datetime: expected {expected_datetime}, "
            f"got {result.image_datetime}"
        )
        assert result.image_altitude_meters == expected_altitude, (
            f"Returned ImageData should preserve altitude: expected {expected_altitude}, "
            f"got {result.image_altitude_meters}"
        )
        assert (
            result.image_hash_sha256 == expected_hash
        ), f"Returned ImageData should preserve hash: expected '{expected_hash}', got '{result.image_hash_sha256}'"

    @pytest.mark.unit
    def test_primary_image_data_list(self) -> None:
        """Test primary_image_data returns first ImageData when initialized with list.

        This test verifies that for list ImageData initialization (video scenario),
        the primary_image_data property returns the first ImageData instance in the list
        and that the relationship is maintained correctly.
        """
        # Arrange
        image_data1 = ImageData(image_altitude_meters=100.0)
        image_data2 = ImageData(image_altitude_meters=200.0)
        metadata = iFDOMetadata([image_data1, image_data2])

        # Act
        result = metadata.primary_image_data

        # Assert
        assert (
            result is image_data1
        ), "primary_image_data should return the exact same first ImageData instance for list initialization"

        # Verify it's specifically the first element, not just any element
        assert result is not image_data2, "primary_image_data should not return the second ImageData instance"

        # Verify the altitude value to confirm correct instance
        assert (
            result.image_altitude_meters == 100.0
        ), "primary_image_data should have the altitude from the first ImageData instance"


class TestiFDOMetadataStaticMethods:
    """Test static methods of iFDOMetadata class.

    This test class verifies static utility methods including file type detection
    and case-insensitive handling of file extensions.
    """

    @pytest.mark.unit
    def test_is_video_file_video_extensions(self) -> None:
        """Test _is_video_file static method correctly identifies all supported video file extensions.

        This test verifies that the static method correctly identifies all video file
        extensions as defined in the implementation and returns True for every supported format.
        The test comprehensively covers all 30 video extensions supported by the implementation.
        """
        # Arrange - All video extensions supported by iFDOMetadata._is_video_file
        video_files = [
            # Common modern formats
            "movie.mp4",
            "video.avi",
            "clip.mov",
            "film.wmv",
            "animation.webm",
            "test.mkv",
            "sample.m4v",
            "mobile.3gp",
            "open.ogv",
            "stream.ts",
            # Additional supported formats
            "legacy.flv",
            "broadcast.mts",
            "bluray.m2ts",
            "dvd.vob",
            "old.rm",
            "old.rmvb",
            "windows.asf",
            "tape.dv",
            "flash.f4v",
            "mpeg1.m1v",
            "mpeg2.m2v",
            "mpeg.mpe",
            "standard.mpeg",
            "video.mpg",
            "stream.mpv",
            "quicktime.qt",
            "flash.swf",
            "vivo.viv",
            "streaming.vivo",
            "raw.yuv",
        ]

        # Act & Assert
        for filename in video_files:
            result = iFDOMetadata._is_video_file(filename)
            assert result is True, f"_is_video_file should return True for video file: {filename}"

    @pytest.mark.unit
    def test_is_video_file_image_extensions(self) -> None:
        """Test _is_video_file static method correctly identifies image file extensions.

        This test verifies that the static method correctly identifies common image file
        extensions and returns False for all of them, including comprehensive coverage
        of typical image formats and edge cases.
        """
        # Arrange
        image_files = [
            # Common formats
            "photo.jpg",
            "photo.jpeg",
            "image.png",
            "graphic.bmp",
            "picture.tiff",
            "picture.tif",
            "icon.gif",
            "image.webp",
            # RAW formats
            "photo.cr2",
            "photo.nef",
            "photo.arw",
            "photo.dng",
            # Other formats
            "image.psd",
            "vector.svg",
            "icon.ico",
            # Case variations (should be handled case-insensitively)
            "PHOTO.JPG",
            "Image.PNG",
            "GRAPHIC.gif",
        ]

        # Act & Assert
        for filename in image_files:
            result = iFDOMetadata._is_video_file(filename)
            assert result is False, f"_is_video_file should return False for image file: {filename}"

    @pytest.mark.unit
    def test_is_video_file_case_insensitive(self) -> None:
        """Test _is_video_file static method handles case insensitive file extensions.

        This test verifies that the static method correctly identifies video files
        regardless of the case of the file extension, and correctly rejects non-video
        files regardless of case.
        """
        # Arrange
        video_test_cases = [
            "VIDEO.MP4",  # All uppercase
            "video.MP4",  # Mixed case filename, uppercase extension
            "VIDEO.mp4",  # Uppercase filename, lowercase extension
            "Test.AVI",  # Mixed case with different video extension
            "movie.MOV",  # Another video extension in uppercase
            "clip.WebM",  # Mixed case modern video format
            "sample.MKV",  # Mixed case container format
        ]

        image_test_cases = [
            "PHOTO.JPG",  # All uppercase image extension
            "Image.PNG",  # Mixed case image extension
            "PICTURE.gif",  # Mixed case, non-video extension
            "graphic.TIFF",  # Uppercase non-video extension
        ]

        edge_cases = [
            "file",  # No extension
            "file.",  # Empty extension
            "file.TXT",  # Non-media file extension
        ]

        # Act & Assert - Video files should return True regardless of case
        for filename in video_test_cases:
            # Act
            result = iFDOMetadata._is_video_file(filename)

            # Assert
            assert result is True, (
                f"Case insensitive video detection failed: '{filename}' should be identified as video file, "
                f"but got {result}"
            )

        # Act & Assert - Non-video files should return False regardless of case
        for filename in image_test_cases + edge_cases:
            # Act
            result = iFDOMetadata._is_video_file(filename)

            # Assert
            assert result is False, (
                f"Case insensitive non-video detection failed: '{filename}' should NOT be identified as video file, "
                f"but got {result}"
            )


class TestiFDOMetadataProcessMethods:
    """Test processing methods of iFDOMetadata class.

    This test class verifies video and image metadata processing methods,
    including path handling and metadata transformation logic.
    """

    @pytest.mark.unit
    def test_process_video_metadata(self) -> None:
        """Test _process_video_metadata method correctly flattens mixed metadata types.

        This test verifies that the method can handle both single ImageData and lists
        of ImageData, flattening them into a single list and setting the correct
        image_set_local_path for subdirectory files.
        """
        # Arrange
        image_data1 = ImageData(image_altitude_meters=100.0)
        image_data2 = ImageData(image_altitude_meters=200.0)
        image_data3 = ImageData(image_altitude_meters=300.0)

        # Mix of single and list metadata
        single_metadata = iFDOMetadata(image_data1)
        list_metadata = iFDOMetadata([image_data2, image_data3])

        path = Path("subdir/video.mp4")

        # Act
        result = iFDOMetadata._process_video_metadata([single_metadata, list_metadata], path)

        # Assert - Verify flattening behavior
        assert len(result) == 3, "Should flatten all ImageData objects into single list"
        assert isinstance(result, list), "Result should be a list of ImageData objects"

        # Assert - Verify correct ordering and data preservation
        assert result[0].image_altitude_meters == 100.0, "First ImageData should be from single metadata"
        assert result[1].image_altitude_meters == 200.0, "Second ImageData should be first from list metadata"
        assert result[2].image_altitude_meters == 300.0, "Third ImageData should be second from list metadata"

        # Assert - Verify original metadata objects are unchanged
        assert (
            single_metadata.primary_image_data.image_altitude_meters == 100.0
        ), "Original single metadata should be unchanged"
        assert len(list_metadata.image_data) == 2, "Original list metadata should be unchanged"

        # Assert - Check that image_set_local_path is set for subdirectory
        for i, img_data in enumerate(result):
            assert (
                img_data.image_set_local_path == "subdir"
            ), f"ImageData {i} should have image_set_local_path set to subdirectory name"

    @pytest.mark.unit
    def test_process_video_metadata_root_path(self) -> None:
        """Test _process_video_metadata with root path doesn't set image_set_local_path.

        This test verifies that when the file path indicates a root-level video file,
        the image_set_local_path remains None for all ImageData objects (the method
        does not execute the path-setting logic for root-level files).
        """
        # Arrange
        image_data1 = ImageData(image_altitude_meters=100.0)
        image_data2 = ImageData(image_altitude_meters=200.0)
        single_metadata = iFDOMetadata(image_data1)
        list_metadata = iFDOMetadata([image_data2])
        path = Path("video.mp4")  # Root level

        # Act
        result = iFDOMetadata._process_video_metadata([single_metadata, list_metadata], path)

        # Assert - Verify correct flattening behavior
        assert len(result) == 2, "Should flatten both ImageData objects into single list"
        assert result[0].image_altitude_meters == 100.0, "Should preserve first ImageData altitude value"
        assert result[1].image_altitude_meters == 200.0, "Should preserve second ImageData altitude value"

        # Assert - Verify image_set_local_path remains None for root-level files
        for i, img_data in enumerate(result):
            assert img_data.image_set_local_path is None, (
                f"ImageData {i} should have image_set_local_path as None for root-level files, "
                f"got {img_data.image_set_local_path}"
            )

    @pytest.mark.unit
    def test_process_image_metadata_single_metadata_subdirectory(self) -> None:
        """Test _process_image_metadata correctly processes single image metadata for subdirectory files.

        This test verifies that the method:
        - Extracts the correct ImageData from the first metadata item
        - Sets image_set_local_path for subdirectory files specifically
        - Returns the exact same ImageData object (not a copy)
        - Modifies the original ImageData object by setting image_set_local_path
        """
        # Arrange
        image_data = ImageData(
            image_altitude_meters=100.0,
            image_latitude=45.0,
            image_longitude=-123.0,
            image_hash_sha256="test_hash_123",
        )
        metadata = iFDOMetadata(image_data)
        path = Path("subdir/image.jpg")

        # Verify initial state - image_set_local_path should not be set
        assert (
            not hasattr(image_data, "image_set_local_path") or image_data.image_set_local_path is None
        ), "Initial ImageData should not have image_set_local_path set"

        # Act
        result = iFDOMetadata._process_image_metadata([metadata], path)

        # Assert - Verify correct ImageData extraction and identity
        assert isinstance(result, ImageData), "Result should be an ImageData instance"
        assert result is image_data, "Result should be the exact same ImageData instance, not a copy"

        # Assert - Verify all original properties are preserved
        assert (
            result.image_altitude_meters == 100.0
        ), f"Should preserve ImageData altitude value: expected 100.0, got {result.image_altitude_meters}"
        assert result.image_latitude == 45.0, "Should preserve latitude from original ImageData"
        assert result.image_longitude == -123.0, "Should preserve longitude from original ImageData"
        assert result.image_hash_sha256 == "test_hash_123", "Should preserve hash from original ImageData"

        # Assert - Verify subdirectory path handling (method modifies original object)
        assert hasattr(result, "image_set_local_path"), "Result should have image_set_local_path attribute"
        assert result.image_set_local_path == "subdir", (
            f"Should set image_set_local_path to subdirectory name: "
            f"expected 'subdir', got '{result.image_set_local_path}'"
        )

        # Assert - Verify the original ImageData object was modified (this is the actual behavior)
        assert (
            image_data.image_set_local_path == "subdir"
        ), "Original ImageData should have image_set_local_path set by the method"
        assert (
            metadata.primary_image_data.image_set_local_path == "subdir"
        ), "Metadata's primary_image_data should reflect the modification"

    @pytest.mark.unit
    def test_process_image_metadata_video_metadata_input(self) -> None:
        """Test _process_image_metadata correctly handles video metadata (list of ImageData).

        This test verifies that when given iFDOMetadata containing a list of ImageData
        (video scenario), the method extracts the first ImageData correctly and modifies
        the original ImageData object by setting the image_set_local_path for subdirectory files.
        """
        # Arrange
        image_data_list = [
            ImageData(image_altitude_meters=150.0, image_hash_sha256="first_frame"),
            ImageData(image_altitude_meters=200.0, image_hash_sha256="second_frame"),
        ]
        video_metadata = iFDOMetadata(image_data_list)
        path = Path("videos/clip.mp4")

        # Verify initial state - image_set_local_path should not be set
        assert (
            not hasattr(image_data_list[0], "image_set_local_path") or image_data_list[0].image_set_local_path is None
        ), "Initial ImageData should not have image_set_local_path set"

        # Act
        result = iFDOMetadata._process_image_metadata([video_metadata], path)

        # Assert - Verify first ImageData is selected and returned as the same object
        assert isinstance(result, ImageData), "Result should be an ImageData instance"
        assert result is image_data_list[0], "Result should be the exact same first ImageData instance, not a copy"
        assert (
            result.image_altitude_meters == 150.0
        ), "Should select first ImageData from video metadata: expected 150.0"
        assert (
            result.image_hash_sha256 == "first_frame"
        ), "Should select first ImageData with correct hash from video metadata"

        # Assert - Verify subdirectory path is set on the returned object
        assert hasattr(result, "image_set_local_path"), "Result should have image_set_local_path attribute"
        assert (
            result.image_set_local_path == "videos"
        ), "Should set image_set_local_path for video subdirectory: expected 'videos'"

        # Assert - Verify the original ImageData object was modified (this is the actual behavior)
        assert (
            image_data_list[0].image_set_local_path == "videos"
        ), "Original first ImageData should have image_set_local_path set by the method"
        assert (
            video_metadata.primary_image_data.image_set_local_path == "videos"
        ), "Video metadata's primary_image_data should reflect the modification"

        # Assert - Verify second ImageData was not affected
        assert (
            not hasattr(image_data_list[1], "image_set_local_path") or image_data_list[1].image_set_local_path is None
        ), "Second ImageData should not be affected by processing the first one"

    @pytest.mark.unit
    def test_process_image_metadata_multiple_metadata_items(self) -> None:
        """Test _process_image_metadata processes only the first metadata item from multiple items.

        This test verifies that when given multiple iFDOMetadata items,
        the method correctly selects and processes only the first one,
        ignoring subsequent items and setting the subdirectory path correctly.
        """
        # Arrange
        first_image_data = ImageData(
            image_altitude_meters=300.0,
            image_hash_sha256="first_metadata",
            image_latitude=45.0,
            image_longitude=-123.0,
        )
        second_image_data = ImageData(
            image_altitude_meters=400.0,
            image_hash_sha256="second_metadata",
            image_latitude=50.0,
            image_longitude=-130.0,
        )
        first_metadata = iFDOMetadata(first_image_data)
        second_metadata = iFDOMetadata(second_image_data)
        path = Path("data/multiple.jpg")

        # Act
        result = iFDOMetadata._process_image_metadata([first_metadata, second_metadata], path)

        # Assert - Verify only first metadata is processed
        assert isinstance(result, ImageData), "Result should be an ImageData instance"
        assert (
            result.image_altitude_meters == 300.0
        ), f"Should process first metadata item: expected 300.0, got {result.image_altitude_meters}"
        assert result.image_hash_sha256 == "first_metadata", "Should use first metadata item's ImageData, not second"
        assert result.image_latitude == 45.0, "Should preserve latitude from first metadata item only"
        assert result.image_longitude == -123.0, "Should preserve longitude from first metadata item only"

        # Assert - Verify second metadata is completely ignored
        assert result.image_altitude_meters != 400.0, "Should not use second metadata item's altitude"
        assert result.image_hash_sha256 != "second_metadata", "Should not use second metadata item's hash"
        assert result.image_latitude != 50.0, "Should not use second metadata item's latitude"
        assert result.image_longitude != -130.0, "Should not use second metadata item's longitude"

        # Assert - Verify subdirectory path handling
        assert result.image_set_local_path == "data", (
            f"Should set image_set_local_path to subdirectory name: "
            f"expected 'data', got '{result.image_set_local_path}'"
        )

        # Assert - Verify original metadata objects are unchanged
        assert (
            first_metadata.primary_image_data.image_altitude_meters == 300.0
        ), "Original first metadata should remain unchanged after processing"
        assert (
            second_metadata.primary_image_data.image_altitude_meters == 400.0
        ), "Original second metadata should remain unchanged after processing"

    @pytest.mark.unit
    def test_process_image_metadata_root_path(self) -> None:
        """Test _process_image_metadata with root path doesn't set image_set_local_path.

        This test verifies that when the file path indicates a root-level file,
        the image_set_local_path is not set or remains None, and all other
        ImageData properties are preserved correctly.
        """
        # Arrange
        image_data = ImageData(
            image_altitude_meters=100.0,
            image_latitude=45.0,
            image_longitude=-123.0,
            image_hash_sha256="test_hash_root",
        )
        metadata = iFDOMetadata(image_data)
        path = Path("image.jpg")  # Root level
        original_altitude = image_data.image_altitude_meters

        # Act
        result = iFDOMetadata._process_image_metadata([metadata], path)

        # Assert - Verify correct ImageData extraction
        assert isinstance(result, ImageData), "Result should be an ImageData instance"
        assert (
            result.image_altitude_meters == 100.0
        ), f"Should preserve ImageData altitude value: expected 100.0, got {result.image_altitude_meters}"

        # Assert - Verify all properties are preserved
        assert result.image_latitude == 45.0, "Should preserve latitude from original ImageData"
        assert result.image_longitude == -123.0, "Should preserve longitude from original ImageData"
        assert result.image_hash_sha256 == "test_hash_root", "Should preserve hash from original ImageData"

        # Assert - Verify no image_set_local_path is set for root-level files
        assert (
            not hasattr(result, "image_set_local_path") or result.image_set_local_path is None
        ), "Should not set image_set_local_path for root-level files"

        # Assert - Verify original metadata is unchanged
        assert (
            metadata.primary_image_data.image_altitude_meters == original_altitude
        ), "Original metadata should remain unchanged after processing"


class TestiFDOMetadataDatasetCreation:
    """Test dataset metadata creation functionality of iFDOMetadata.

    This test class verifies the complete dataset metadata creation workflow,
    including iFDO structure generation, file type handling, and integration
    with external savers.
    """

    @pytest.fixture
    def sample_image_metadata(self) -> iFDOMetadata:
        """Create sample iFDOMetadata for testing.

        Returns iFDOMetadata initialized with a single ImageData object to simulate
        standard image metadata.
        """
        image_data = ImageData(image_altitude_meters=100.0)
        return iFDOMetadata(image_data)

    @pytest.fixture
    def sample_video_metadata(self) -> iFDOMetadata:
        """Create sample video iFDOMetadata for testing.

        Returns iFDOMetadata initialized with a list of ImageData objects to simulate
        video metadata with multiple frames/timestamps.
        """
        image_data_list = [
            ImageData(image_altitude_meters=100.0),
            ImageData(image_altitude_meters=200.0),
        ]
        return iFDOMetadata(image_data_list)

    @pytest.mark.unit
    def test_create_dataset_metadata_basic_functionality(
        self,
        sample_image_metadata: iFDOMetadata,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test create_dataset_metadata produces correct iFDO structure with standard inputs.

        This unit test verifies that the method correctly processes single image metadata
        through the complete workflow from BaseMetadata input to iFDO structure generation,
        including proper UUID generation, header creation, and saver interaction.
        """
        # Arrange
        dataset_name = "TestDataSet"
        root_dir = Path("/tmp")
        items = {"image.jpg": [cast("BaseMetadata", sample_image_metadata)]}

        mock_saver = mocker.Mock()

        # Act
        iFDOMetadata.create_dataset_metadata(
            dataset_name,
            root_dir,
            items,
            saver_overwrite=mock_saver,
        )

        # Assert - Verify saver function was called with correct parameters
        mock_saver.assert_called_once()
        call_args = mock_saver.call_args
        assert call_args is not None, "Saver function should have been called exactly once"

        # Verify call arguments: (path, output_name, data)
        assert (
            call_args[0][0] == root_dir
        ), f"Saver should receive correct root directory path: expected {root_dir}, got {call_args[0][0]}"
        assert call_args[0][1] == "ifdo", f"Default metadata name should be 'ifdo', got '{call_args[0][1]}'"

        # Assert - Verify complete iFDO structure
        actual_data = call_args[0][2]
        assert "image-set-header" in actual_data, "iFDO data must contain image-set-header section"
        assert "image-set-items" in actual_data, "iFDO data must contain image-set-items section"

        # Assert - Verify header structure and content
        header = actual_data["image-set-header"]
        required_header_keys = {"image-set-name", "image-set-uuid", "image-set-handle", "image-set-ifdo-version"}
        assert required_header_keys.issubset(
            set(header.keys()),
        ), f"Header should contain at least: {required_header_keys}, got {set(header.keys())}"
        assert (
            header["image-set-name"] == dataset_name
        ), f"Header name should match input: expected '{dataset_name}', got '{header['image-set-name']}'"

        # Verify UUID is valid format (36 characters with hyphens)
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        assert re.match(
            uuid_pattern,
            header["image-set-uuid"],
        ), f"Header should contain valid UUID format, got '{header['image-set-uuid']}'"

        assert header["image-set-handle"] == "", "Header handle should be empty string"
        assert (
            header["image-set-ifdo-version"] == "v2.1.0"
        ), f"Header version should be 'v2.1.0', got '{header['image-set-ifdo-version']}'"

        # Assert - Verify image-set-items structure; common fields are deduplicated to header
        items_data = actual_data["image-set-items"]
        assert "image.jpg" in items_data, "Items should contain the input image filename"
        image_data = items_data["image.jpg"]
        assert isinstance(image_data, dict), f"Image data should be a dict, got {type(image_data)}"
        # Altitude is a common field (only one image), so it should be promoted to the header
        assert "image-altitude-meters" in header, "Common altitude field should be deduplicated to header"
        assert header["image-altitude-meters"] == 100.0, "Deduplicated altitude should match sample metadata value"
        assert "image-altitude-meters" not in image_data, "Common altitude field should not remain in item data"

    @pytest.mark.unit
    def test_create_dataset_metadata_custom_name(
        self,
        sample_image_metadata: iFDOMetadata,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test create_dataset_metadata handles custom metadata names correctly.

        This test verifies that custom metadata names are processed properly,
        including automatic addition of .ifdo extension when missing. Tests the
        complete workflow from input validation to saver function interaction.
        """
        # Arrange
        custom_name = "custom"
        dataset_name = "TestDataSet"
        root_dir = Path("/tmp")
        items = {"image.jpg": [cast("BaseMetadata", sample_image_metadata)]}

        mock_saver = mocker.Mock()

        # Act
        iFDOMetadata.create_dataset_metadata(
            dataset_name,
            root_dir,
            items,
            metadata_name=custom_name,
            saver_overwrite=mock_saver,
        )

        # Assert
        mock_saver.assert_called_once()
        call_args = mock_saver.call_args
        assert call_args is not None, "Saver function should have been called exactly once"

        # Verify call arguments: (path, output_name, data)
        assert (
            call_args[0][0] == root_dir
        ), f"Saver should receive correct root directory: expected {root_dir}, got {call_args[0][0]}"
        assert (
            call_args[0][1] == "custom.ifdo"
        ), f"Custom metadata name should have .ifdo extension added: expected 'custom.ifdo', got '{call_args[0][1]}'"

        # Verify data structure is valid iFDO format
        data = call_args[0][2]
        assert isinstance(data, dict), "Saver should receive dictionary data"
        assert "image-set-header" in data, "iFDO data must contain image-set-header section"
        assert "image-set-items" in data, "iFDO data must contain image-set-items section"

        # Verify header contains correct dataset name
        header = data["image-set-header"]
        assert (
            header["image-set-name"] == dataset_name
        ), f"Header should contain correct dataset name: expected '{dataset_name}', got '{header['image-set-name']}'"

    @pytest.mark.unit
    def test_create_dataset_metadata_video_processing(
        self,
        sample_video_metadata: iFDOMetadata,
        tmp_path: Path,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test create_dataset_metadata correctly handles video files with multiple ImageData objects.

        This unit test verifies that video files (detected by file extension) are processed
        as lists of ImageData objects in the iFDO structure and that the complete workflow
        from input to saver interaction functions correctly with mocked external dependencies.
        """
        # Arrange
        dataset_name = "VideoTestDataSet"
        items = {"video.mp4": [cast("BaseMetadata", sample_video_metadata)]}

        mock_saver = mocker.Mock()

        # Act
        iFDOMetadata.create_dataset_metadata(
            dataset_name,
            tmp_path,
            items,
            saver_overwrite=mock_saver,
        )

        # Assert - Verify saver was called with correct parameters
        mock_saver.assert_called_once()
        call_args = mock_saver.call_args
        assert call_args is not None, "Saver function should have been called exactly once"

        # Verify call arguments: (path, output_name, data)
        assert (
            call_args[0][0] == tmp_path
        ), f"Saver should receive correct path: expected {tmp_path}, got {call_args[0][0]}"
        assert call_args[0][1] == "ifdo", f"Default output name should be 'ifdo', got {call_args[0][1]}"

        # Assert - Verify iFDO structure contains required sections
        captured_data = call_args[0][2]
        assert "image-set-header" in captured_data, "iFDO data must contain image-set-header section"
        assert "image-set-items" in captured_data, "iFDO data must contain image-set-items section"

        # Assert - Verify header content and structure
        header = captured_data["image-set-header"]
        assert (
            header["image-set-name"] == dataset_name
        ), f"Header should contain correct dataset name: expected '{dataset_name}', got '{header['image-set-name']}'"

        # Verify UUID is valid format (36 characters with hyphens)
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        assert re.match(
            uuid_pattern,
            header["image-set-uuid"],
        ), f"Header should contain valid UUID format, got '{header['image-set-uuid']}'"

        assert header["image-set-handle"] == "", "Header handle should be empty string"
        assert (
            header["image-set-ifdo-version"] == "v2.1.0"
        ), f"Header should contain correct iFDO version: expected 'v2.1.0', got '{header['image-set-ifdo-version']}'"

        # Assert - Verify video file processing creates list structure
        assert "video.mp4" in captured_data["image-set-items"], "Video file should be present in image-set-items"
        video_items = captured_data["image-set-items"]["video.mp4"]
        assert isinstance(
            video_items,
            list,
        ), f"Video files should generate list entries in image-set-items, got {type(video_items)}"
        assert (
            len(video_items) == 2
        ), f"Video should contain exactly 2 ImageData objects from sample fixture, got {len(video_items)}"

        # Assert - Verify individual frame data preservation matches sample fixture
        assert video_items[0]["image-altitude-meters"] == 100.0, (
            f"First video frame should preserve altitude from sample fixture: expected 100.0, "
            f"got {video_items[0].get('image-altitude-meters')}"
        )
        assert video_items[1]["image-altitude-meters"] == 200.0, (
            f"Second video frame should preserve altitude from sample fixture: expected 200.0, "
            f"got {video_items[1].get('image-altitude-meters')}"
        )

    @pytest.mark.unit
    def test_create_dataset_metadata_dry_run_mode(
        self,
        sample_image_metadata: iFDOMetadata,
        mocker: pytest_mock.MockerFixture,
    ) -> None:
        """Test create_dataset_metadata respects dry_run flag and doesn't save files.

        This unit test verifies that when dry_run=True, the saver function is not called,
        which is the primary behavior of dry-run mode. This prevents file operations
        while still allowing the user to validate their inputs.
        """
        # Arrange
        dataset_name = "DryRunTestDataSet"
        root_dir = Path("/tmp/dry-run")
        items = {"image.jpg": [cast("BaseMetadata", sample_image_metadata)]}

        # Mock only the external dependency (saver function)
        mock_saver = mocker.Mock()

        # Act
        iFDOMetadata.create_dataset_metadata(
            dataset_name,
            root_dir,
            items,
            dry_run=True,
            saver_overwrite=mock_saver,
        )

        # Assert - Primary dry-run behavior: no file operations performed
        mock_saver.assert_not_called(), "Saver function should not be called when dry_run=True"


class TestiFDOMetadataDeduplication:
    """Test auto-deduplication helpers in iFDOMetadata."""

    @pytest.mark.unit
    def test_extract_common_header_fields_all_same(self) -> None:
        """Fields identical across all images are extracted as common."""
        items: dict[str, ImageData | list[ImageData]] = {
            "img1.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
            "img2.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
        }
        result = iFDOMetadata._extract_common_header_fields(items)
        assert result["image_latitude"] == 45.0
        assert result["image_altitude_meters"] == 100.0

    @pytest.mark.unit
    def test_extract_common_header_fields_varying_field_excluded(self) -> None:
        """Fields that differ between images are not extracted."""
        items: dict[str, ImageData | list[ImageData]] = {
            "img1.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
            "img2.jpg": ImageData(image_latitude=45.0, image_altitude_meters=200.0),
        }
        result = iFDOMetadata._extract_common_header_fields(items)
        assert "image_latitude" in result
        assert "image_altitude_meters" not in result

    @pytest.mark.unit
    def test_extract_common_header_fields_empty(self) -> None:
        """Empty input returns empty dict."""
        assert iFDOMetadata._extract_common_header_fields({}) == {}

    @pytest.mark.unit
    def test_extract_common_header_fields_video_items(self) -> None:
        """Video lists are flattened before comparison."""
        items: dict[str, ImageData | list[ImageData]] = {
            "video.mp4": [
                ImageData(image_latitude=45.0),
                ImageData(image_latitude=45.0),
            ],
            "img.jpg": ImageData(image_latitude=45.0),
        }
        result = iFDOMetadata._extract_common_header_fields(items)
        assert result["image_latitude"] == 45.0

    @pytest.mark.unit
    def test_extract_common_header_fields_none_value_not_deduplicated(self) -> None:
        """A field that is None for any image is not treated as common, even if others share a value."""
        items: dict[str, ImageData | list[ImageData]] = {
            "img1.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
            "img2.jpg": ImageData(image_latitude=45.0, image_altitude_meters=None),
        }
        result = iFDOMetadata._extract_common_header_fields(items)
        assert result.get("image_latitude") == 45.0
        assert "image_altitude_meters" not in result

    @pytest.mark.unit
    def test_extract_common_header_fields_none_first_value_second_not_deduplicated(self) -> None:
        """None appearing before a non-None value is also not treated as common."""
        items: dict[str, ImageData | list[ImageData]] = {
            "img1.jpg": ImageData(image_latitude=45.0, image_altitude_meters=None),
            "img2.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
        }
        result = iFDOMetadata._extract_common_header_fields(items)
        assert result.get("image_latitude") == 45.0
        assert "image_altitude_meters" not in result

    @pytest.mark.unit
    def test_remove_common_fields_sets_to_none(self) -> None:
        """Common fields are set to None in individual items."""
        items: dict[str, ImageData | list[ImageData]] = {
            "img1.jpg": ImageData(image_latitude=45.0, image_altitude_meters=100.0),
            "img2.jpg": ImageData(image_latitude=45.0, image_altitude_meters=200.0),
        }
        result = iFDOMetadata._remove_common_fields(items, {"image_latitude"})
        img1 = result["img1.jpg"]
        img2 = result["img2.jpg"]
        assert isinstance(img1, ImageData)
        assert isinstance(img2, ImageData)
        assert img1.image_latitude is None
        assert img2.image_latitude is None
        assert img1.image_altitude_meters == 100.0
        assert img2.image_altitude_meters == 200.0

    @pytest.mark.unit
    def test_remove_common_fields_video_list(self) -> None:
        """Common fields are removed from each frame in video lists."""
        items: dict[str, ImageData | list[ImageData]] = {
            "video.mp4": [
                ImageData(image_latitude=45.0, image_altitude_meters=100.0),
                ImageData(image_latitude=45.0, image_altitude_meters=200.0),
            ],
        }
        result = iFDOMetadata._remove_common_fields(items, {"image_latitude"})
        frames = result["video.mp4"]
        assert isinstance(frames, list)
        assert all(f.image_latitude is None for f in frames)
        assert frames[0].image_altitude_meters == 100.0
        assert frames[1].image_altitude_meters == 200.0

    @pytest.mark.unit
    def test_create_dataset_metadata_deduplicates_to_header(self) -> None:
        """Common fields appear in iFDO header and not in individual items."""
        meta1 = iFDOMetadata(ImageData(image_latitude=45.0, image_altitude_meters=100.0))
        meta2 = iFDOMetadata(ImageData(image_latitude=45.0, image_altitude_meters=100.0))
        items = {
            "img1.jpg": [cast("BaseMetadata", meta1)],
            "img2.jpg": [cast("BaseMetadata", meta2)],
        }

        captured: list[dict[str, Any]] = []

        def mock_saver(_root: Path, _name: str, data: dict[str, Any]) -> None:
            captured.append(data)

        iFDOMetadata.create_dataset_metadata(
            "TestDataset",
            Path("/tmp"),
            items,
            saver_overwrite=mock_saver,
        )

        data = captured[0]
        header = data["image-set-header"]
        assert "image-latitude" in header, "Common latitude should be promoted to header"
        assert "image-altitude-meters" in header, "Common altitude should be promoted to header"

        for filename in ("img1.jpg", "img2.jpg"):
            item = data["image-set-items"][filename]
            assert "image-latitude" not in item, "Latitude should not remain in item after deduplication"
            assert "image-altitude-meters" not in item, "Altitude should not remain in item after deduplication"
