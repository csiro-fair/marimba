"""Tests for marimba.core.utils.ifdo module."""

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from ifdo import iFDO
from ifdo.models import ImageSetHeader
from pytest_mock import MockerFixture

from marimba.core.utils.ifdo import load_ifdo, save_ifdo

if TYPE_CHECKING:
    from tests.conftest import TestDataFactory


class TestIfdoUtilities:
    """Test iFDO utility functions."""

    @pytest.fixture
    def sample_ifdo(self, test_data_factory: "TestDataFactory") -> iFDO:
        """Create a sample iFDO object for testing using TestDataFactory."""
        metadata = test_data_factory.create_ifdo_metadata()
        header_data = metadata["image-set-header"]
        return iFDO(
            image_set_header=ImageSetHeader(
                image_set_name=header_data["image-set-name"],
                image_set_uuid=header_data["image-set-uuid"],
                image_set_handle="test_image_set_handle",
            ),
            image_set_items={},
        )

    @pytest.mark.unit
    def test_load_ifdo_with_path_object_delegates_to_ifdo_load(self, mocker: MockerFixture) -> None:
        """
        Test load_ifdo function delegates correctly to iFDO.load with Path object.

        This unit test verifies that the load_ifdo wrapper function correctly
        delegates to the underlying iFDO.load method when given a Path object,
        ensuring proper integration with the iFDO library without testing file I/O.
        """
        # Arrange
        test_path = Path("/test/path/to/ifdo.yaml")
        mock_ifdo = mocker.Mock(spec=iFDO)
        mock_load = mocker.patch("ifdo.iFDO.load", return_value=mock_ifdo)

        # Act
        result = load_ifdo(test_path)

        # Assert
        mock_load.assert_called_once_with(test_path)
        assert result is mock_ifdo, "Expected function to return the exact iFDO object from the underlying library"

    @pytest.mark.unit
    def test_load_ifdo_with_string_path_delegates_to_ifdo_load(self, mocker: MockerFixture) -> None:
        """
        Test load_ifdo function delegates correctly to iFDO.load with string path.

        This unit test verifies that the load_ifdo wrapper function correctly
        handles string path inputs and delegates to the underlying iFDO.load method,
        ensuring consistent behavior regardless of input path type.
        """
        # Arrange
        test_path = "/test/path/to/ifdo.yaml"
        mock_ifdo = mocker.Mock(spec=iFDO)
        mock_load = mocker.patch("ifdo.iFDO.load", return_value=mock_ifdo)

        # Act
        result = load_ifdo(test_path)

        # Assert
        mock_load.assert_called_once_with(test_path)
        assert result is mock_ifdo, "Expected function to return the exact iFDO object from the underlying library"

    @pytest.mark.unit
    def test_load_ifdo_propagates_exceptions_from_underlying_library(self, mocker: MockerFixture) -> None:
        """
        Test load_ifdo propagates exceptions from underlying iFDO.load without modification.

        This unit test verifies that the load_ifdo wrapper function correctly
        propagates any exceptions from the iFDO library without modifying them,
        ensuring proper error handling behavior. Uses a generic exception to avoid
        duplicating file-specific integration tests.
        """
        # Arrange
        test_path = Path("/test/path/ifdo.yaml")
        expected_error = ValueError("Invalid iFDO format")
        mock_load = mocker.patch("ifdo.iFDO.load", side_effect=expected_error)

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid iFDO format"):
            load_ifdo(test_path)

        # Verify the underlying library was called with correct parameters
        mock_load.assert_called_once_with(test_path)

    @pytest.mark.integration
    def test_load_ifdo_with_real_file_loads_correctly(self, tmp_path: Path, sample_ifdo: iFDO) -> None:
        """
        Test loading iFDO file with real file I/O operations.

        This integration test verifies that load_ifdo correctly loads an iFDO file
        using real file I/O operations and integrates properly with the iFDO library.
        """
        # Arrange
        ifdo_path = tmp_path / "test_ifdo.yaml"
        sample_ifdo.save(ifdo_path)

        # Verify preconditions: file should exist and have content
        assert ifdo_path.exists(), "Test file should exist after saving"
        assert ifdo_path.stat().st_size > 0, "Test file should have non-zero size"

        # Act
        loaded_ifdo = load_ifdo(ifdo_path)

        # Assert
        assert isinstance(loaded_ifdo, iFDO), "Expected loaded object to be an iFDO instance"
        assert (
            loaded_ifdo.image_set_header.image_set_name == sample_ifdo.image_set_header.image_set_name
        ), "Expected image set name to match original"
        assert (
            loaded_ifdo.image_set_header.image_set_handle == sample_ifdo.image_set_header.image_set_handle
        ), "Expected image set handle to match original"
        assert (
            loaded_ifdo.image_set_header.image_set_uuid == sample_ifdo.image_set_header.image_set_uuid
        ), "Expected image set UUID to match original"

    @pytest.mark.integration
    def test_load_ifdo_with_nonexistent_file_raises_filenotfound_error(self, tmp_path: Path) -> None:
        """
        Test loading nonexistent iFDO file raises FileNotFoundError.

        This integration test verifies that load_ifdo properly propagates FileNotFoundError
        when attempting to load a file that doesn't exist, testing the real file system
        interaction without mocking the underlying iFDO library behavior.
        """
        # Arrange
        nonexistent_path = tmp_path / "nonexistent_file.yaml"

        # Verify preconditions: file should not exist
        assert not nonexistent_path.exists(), "Test file should not exist for this test"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match=r".*nonexistent_file\.yaml"):
            load_ifdo(nonexistent_path)

    @pytest.mark.unit
    def test_save_ifdo_with_path_object_delegates_to_ifdo_save(self, mocker: MockerFixture) -> None:
        """
        Test save_ifdo function delegates correctly to iFDO.save with Path object.

        This unit test verifies that the save_ifdo wrapper function correctly
        delegates to the underlying iFDO.save method when given a Path object,
        ensuring proper integration with the iFDO library without testing file I/O.
        """
        # Arrange
        mock_ifdo = mocker.Mock(spec=iFDO)
        test_path = Path("/test/path/to/ifdo.yaml")

        # Act
        save_ifdo(mock_ifdo, test_path)

        # Assert
        mock_ifdo.save.assert_called_once_with(test_path)

    @pytest.mark.unit
    def test_save_ifdo_with_string_path_delegates_to_ifdo_save(self, mocker: MockerFixture) -> None:
        """
        Test save_ifdo function delegates correctly to iFDO.save with string path.

        This unit test verifies that the save_ifdo wrapper function correctly
        handles string path inputs and delegates to the underlying iFDO.save method,
        ensuring consistent behavior regardless of input path type.
        """
        # Arrange
        mock_ifdo = mocker.Mock(spec=iFDO)
        test_path = "/test/path/to/ifdo.yaml"

        # Act
        save_ifdo(mock_ifdo, test_path)

        # Assert
        mock_ifdo.save.assert_called_once_with(test_path)

    @pytest.mark.integration
    def test_save_ifdo_with_real_file_creates_valid_file(self, tmp_path: Path, sample_ifdo: iFDO) -> None:
        """
        Test saving iFDO object with real file I/O operations.

        This integration test verifies that save_ifdo correctly saves an iFDO object
        to a file and that the saved file can be loaded back with identical content.
        Tests the complete save/load cycle to ensure data integrity.
        """
        # Arrange
        ifdo_path = tmp_path / "test_ifdo.yaml"

        # Act
        save_ifdo(sample_ifdo, ifdo_path)

        # Assert
        assert ifdo_path.exists(), "Expected iFDO file to be created at the specified path"
        assert ifdo_path.is_file(), "Expected saved path to be a regular file"
        assert ifdo_path.stat().st_size > 0, "Expected saved file to have non-zero size"

        # Verify file can be loaded back with identical content
        loaded_ifdo = load_ifdo(ifdo_path)
        assert isinstance(loaded_ifdo, iFDO), "Expected loaded object to be an iFDO instance"
        assert (
            loaded_ifdo.image_set_header.image_set_name == sample_ifdo.image_set_header.image_set_name
        ), "Expected image set name to be preserved after save/load cycle"
        assert (
            loaded_ifdo.image_set_header.image_set_uuid == sample_ifdo.image_set_header.image_set_uuid
        ), "Expected image set UUID to be preserved after save/load cycle"
        assert (
            loaded_ifdo.image_set_header.image_set_handle == sample_ifdo.image_set_header.image_set_handle
        ), "Expected image set handle to be preserved after save/load cycle"

    @pytest.mark.integration
    def test_save_ifdo_with_nonexistent_parent_directories_raises_filenotfound_error(
        self,
        tmp_path: Path,
        sample_ifdo: iFDO,
    ) -> None:
        """
        Test saving iFDO to path with nonexistent parent directories raises FileNotFoundError.

        This integration test verifies that save_ifdo correctly propagates FileNotFoundError
        when attempting to save to a file path where parent directories don't exist.
        The underlying iFDO library does not automatically create parent directories,
        and this test ensures our wrapper function maintains that behavior.
        """
        # Arrange
        nested_path = tmp_path / "deep" / "nested" / "directories" / "test_ifdo.yaml"

        # Verify preconditions: parent directories should not exist
        assert not nested_path.parent.exists(), "Parent directories should not exist for this test"
        assert tmp_path.exists(), "Base tmp_path should exist for this test"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match=r".*(deep|nested|directories).*"):
            save_ifdo(sample_ifdo, nested_path)
