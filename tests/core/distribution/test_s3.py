"""Tests for marimba.core.distribution.s3 module."""

from pathlib import Path
from typing import Any

import pytest
import pytest_mock
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from marimba.core.distribution.base import DistributionTargetBase
from marimba.core.distribution.s3 import S3DistributionTarget


class TestS3DistributionTarget:
    """Test S3DistributionTarget functionality."""

    @pytest.fixture
    def s3_credentials(self) -> dict[str, str]:
        """Provide S3 credentials for testing."""
        return {
            "bucket_name": "test-bucket",
            "endpoint_url": "https://s3.example.com",
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
            "base_prefix": "datasets",
        }

    @pytest.fixture
    def mock_dataset_wrapper(self, tmp_path: Path, mocker: pytest_mock.MockerFixture) -> Any:
        """Create a mock dataset wrapper with test files."""
        dataset_dir = tmp_path / "test_dataset"
        dataset_dir.mkdir()

        # Create test files
        (dataset_dir / "metadata.yaml").write_text("test: data")
        (dataset_dir / "data.txt").write_text("sample data")

        subdir = dataset_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested content")

        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = dataset_dir
        mock_wrapper.name = "test_dataset"

        return mock_wrapper

    @pytest.mark.unit
    def test_s3_target_init(self, mocker: pytest_mock.MockerFixture, s3_credentials: dict[str, str]) -> None:
        """
        Test S3DistributionTarget initialization with AWS resource setup in isolation.

        Verifies that the S3DistributionTarget constructor correctly initializes all required
        components: creates boto3 S3 resource with provided credentials, sets up S3 bucket
        reference, configures transfer settings, and properly stores internal state. This unit
        test isolates the initialization logic by mocking external AWS dependencies only.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Act - Initialize S3DistributionTarget with credentials
        target = S3DistributionTarget(**s3_credentials)

        # Assert - Verify boto3 resource was created with correct parameters
        mock_resource.assert_called_once_with(
            "s3",
            endpoint_url=s3_credentials["endpoint_url"],
            aws_access_key_id=s3_credentials["access_key_id"],
            aws_secret_access_key=s3_credentials["secret_access_key"],
        ), "S3 resource should be created with provided credentials and endpoint"

        # Assert - Verify S3 bucket was set up correctly
        mock_s3.Bucket.assert_called_once_with(
            s3_credentials["bucket_name"],
        ), "S3 bucket should be initialized with correct bucket name"

        # Assert - Verify internal state was set correctly
        assert target._bucket_name == s3_credentials["bucket_name"], "Bucket name should be stored internally"
        assert target._base_prefix == s3_credentials["base_prefix"], "Base prefix should be stored internally"
        assert target._bucket == mock_bucket, "Bucket reference should be stored for upload operations"

    @pytest.mark.unit
    def test_s3_target_init_strip_prefix(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
    ) -> None:
        """
        Test S3DistributionTarget strips trailing slashes from base_prefix during initialization.

        Verifies that the initialization process correctly normalizes the base_prefix by removing
        trailing slashes. This ensures consistent S3 key generation regardless of how users
        specify the prefix parameter, preventing double slashes in S3 object keys.
        """
        # Arrange - Mock external AWS dependencies only
        mocker.patch("marimba.core.distribution.s3.resource")

        # Create modified credentials without affecting the original fixture
        modified_credentials = s3_credentials.copy()
        modified_credentials["base_prefix"] = "datasets///"

        # Act - Initialize S3 target with prefix containing trailing slashes
        target = S3DistributionTarget(**modified_credentials)

        # Assert - Verify trailing slashes were stripped from prefix
        assert (
            target._base_prefix == "datasets"
        ), "Base prefix should have trailing slashes stripped during initialization"

    @pytest.mark.unit
    def test_check_bucket_success(self, mocker: pytest_mock.MockerFixture, s3_credentials: dict[str, str]) -> None:
        """
        Test successful bucket accessibility check in isolation.

        Verifies that the _check_bucket method correctly calls boto3's head_bucket API
        and completes without raising exceptions when the bucket is accessible. This unit test
        isolates the _check_bucket method behavior by mocking the AWS client dependency.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_client = mocker.Mock()
        mock_s3.meta.client = mock_client
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the bucket check operation
        target._check_bucket()

        # Assert - Verify head_bucket was called with correct bucket name and method completed successfully
        mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

    @pytest.mark.unit
    def test_check_bucket_error(self, mocker: pytest_mock.MockerFixture, s3_credentials: dict[str, str]) -> None:
        """
        Test bucket check with ClientError.

        Verifies that when boto3's head_bucket call raises a ClientError (e.g., bucket not found,
        access denied), the _check_bucket method properly propagates the exception without modification.
        This tests the error handling path for bucket accessibility verification in isolation.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_client = mocker.Mock()

        expected_error = ClientError(
            {"Error": {"Code": "404", "Message": "The specified bucket does not exist"}},
            "HeadBucket",
        )
        mock_client.head_bucket.side_effect = expected_error
        mock_s3.meta.client = mock_client
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)

        # Act & Assert - Verify ClientError is propagated with specific error details
        with pytest.raises(ClientError, match="The specified bucket does not exist"):
            target._check_bucket()

        # Verify the head_bucket method was called with correct bucket name
        mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

    @pytest.mark.unit
    def test_iterate_dataset_wrapper(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test file discovery and S3 key generation in isolation.

        Verifies that the _iterate_dataset_wrapper method correctly discovers all files
        in a dataset directory structure and generates appropriate S3 keys with the
        configured base prefix. This unit test isolates the file discovery and key
        generation logic by mocking only external AWS dependencies.
        """
        # Arrange - Create real dataset structure for authentic file system testing
        dataset_dir = tmp_path / "test_dataset"
        dataset_dir.mkdir()

        # Create test files in dataset structure
        (dataset_dir / "metadata.yaml").write_text("test: metadata")
        (dataset_dir / "data.txt").write_text("sample data content")

        subdir = dataset_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested file content")

        # Create mock dataset wrapper with minimal mocking (only interface needed)
        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = dataset_dir
        mock_wrapper.name = "test_dataset"

        # Mock only external AWS dependencies to isolate unit under test
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the iteration method
        path_key_pairs = list(target._iterate_dataset_wrapper(mock_wrapper))

        # Assert - Verify file discovery count
        assert len(path_key_pairs) == 3, "Should discover exactly 3 files (excluding directories)"

        # Extract paths and keys for detailed verification
        discovered_files = {}
        for path, key, _size in path_key_pairs:
            filename = path.name
            discovered_files[filename] = {
                "path": path,
                "key": key,
                "exists": path.exists(),
                "is_file": path.is_file(),
            }

        # Verify all expected files were discovered with specific error messages
        expected_files = ["metadata.yaml", "data.txt", "nested.txt"]
        for filename in expected_files:
            assert filename in discovered_files, f"File {filename} should be discovered in file iteration"
            file_info = discovered_files[filename]
            assert file_info["exists"], f"Discovered file {filename} should exist on filesystem"
            assert file_info["is_file"], f"Discovered path for {filename} should be a file, not directory"

        # Verify S3 key generation with exact expected values
        assert (
            discovered_files["metadata.yaml"]["key"] == "datasets/metadata.yaml"
        ), "Root-level file should have correct S3 key with base prefix"
        assert (
            discovered_files["data.txt"]["key"] == "datasets/data.txt"
        ), "Root-level file should have correct S3 key with base prefix"
        assert (
            discovered_files["nested.txt"]["key"] == "datasets/subdir/nested.txt"
        ), "Nested file should include subdirectory in S3 key"

        # Verify paths are Path objects and within dataset directory
        for filename, file_info in discovered_files.items():
            path_obj = file_info["path"]
            assert isinstance(path_obj, Path), f"Path for {filename} should be a Path object"
            assert (
                dataset_dir in path_obj.parents or path_obj.parent == dataset_dir
            ), f"Path for {filename} should be within dataset directory structure"

    @pytest.mark.unit
    def test_iterate_dataset_wrapper_no_prefix(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test file discovery and S3 key generation without prefix in isolation.

        Verifies that when no base prefix is configured, the _iterate_dataset_wrapper method
        correctly discovers all files in a dataset directory structure and generates
        appropriate S3 keys without a prefix (resulting in leading slash). This unit test
        isolates the file discovery and key generation logic by mocking only external AWS
        dependencies, focusing on the edge case of empty prefix configuration.
        """
        # Arrange - Create real dataset structure
        dataset_dir = tmp_path / "test_dataset"
        dataset_dir.mkdir()

        # Create test files
        (dataset_dir / "metadata.yaml").write_text("test: metadata")
        (dataset_dir / "data.txt").write_text("sample data content")

        subdir = dataset_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested file content")

        # Create dataset wrapper-like object
        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = dataset_dir
        mock_wrapper.name = "test_dataset"

        # Configure S3 target with empty prefix
        s3_credentials["base_prefix"] = ""

        # Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the iteration method
        path_key_pairs = list(target._iterate_dataset_wrapper(mock_wrapper))

        # Assert - Verify file discovery and key generation without prefix
        assert len(path_key_pairs) == 3, "Should discover exactly 3 files"

        # Extract paths and keys for verification
        discovered_files = {}
        for path, key, _size in path_key_pairs:
            filename = path.name
            discovered_files[filename] = {"path": path, "key": key}

        # Verify all expected files were discovered
        expected_files = ["metadata.yaml", "data.txt", "nested.txt"]
        for filename in expected_files:
            assert filename in discovered_files, f"File {filename} should be discovered"

        # Verify S3 key generation without prefix (empty prefix creates leading slash)
        assert discovered_files["metadata.yaml"]["key"] == "/metadata.yaml"
        assert discovered_files["data.txt"]["key"] == "/data.txt"
        assert discovered_files["nested.txt"]["key"] == "/subdir/nested.txt"

    @pytest.mark.integration
    def test_upload_success(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test successful file upload to S3 with correct parameters and configuration.

        This integration test verifies that the _upload method correctly interacts with
        boto3's upload_file method using proper parameter passing and AWS SDK configuration.
        It tests the integration between the _upload method, boto3 SDK, and transfer
        configuration while mocking only the external AWS service calls.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Create S3 target instance
        target = S3DistributionTarget(**s3_credentials)

        # Create test file with real filesystem
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Act - Execute the upload operation
        target._upload(test_file, "test-key")

        # Assert - Verify upload was called with correct parameters
        mock_bucket.upload_file.assert_called_once_with(
            str(test_file.absolute()),
            "test-key",
            Config=target._config,
        )

        # Verify the method completed without raising exceptions
        # (implicitly tested by reaching this point without exception)

    @pytest.mark.integration
    def test_distribute_success(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        mock_dataset_wrapper: Any,
    ) -> None:
        """
        Test successful dataset distribution through complete S3 upload workflow.

        This integration test verifies that the distribute method correctly processes
        all files in a dataset, generates appropriate S3 keys with the configured prefix,
        and calls the AWS S3 upload_file method for each file with correct parameters.
        It tests the integration between file discovery, key generation, and upload orchestration
        without mocking internal business logic.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Mock successful upload responses
        mock_bucket.upload_file.return_value = None

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the distribution process
        target.distribute(mock_dataset_wrapper)

        # Assert - Verify upload was called correctly for each expected file
        expected_uploads = [
            ("metadata.yaml", "datasets/metadata.yaml"),
            ("data.txt", "datasets/data.txt"),
            ("nested.txt", "datasets/subdir/nested.txt"),
        ]

        assert (
            mock_bucket.upload_file.call_count == 3
        ), "Distribution should upload exactly 3 files from the test dataset structure"

        # Verify each expected file was uploaded with correct S3 key
        upload_calls = mock_bucket.upload_file.call_args_list
        uploaded_files = []

        for call in upload_calls:
            file_path = call[0][0]  # First positional arg is the file path
            s3_key = call[0][1]  # Second positional arg is the S3 key
            config = call[1]["Config"]  # Config is a keyword arg

            # Extract filename for comparison
            filename = file_path.split("/")[-1]
            uploaded_files.append((filename, s3_key))

            # Verify transfer config was passed for every upload
            assert config == target._config, f"Transfer configuration should be applied to upload of {filename}"

        # Verify all expected files were uploaded with correct S3 keys
        for expected_file, expected_key in expected_uploads:
            matching_upload = next((upload for upload in uploaded_files if upload[0] == expected_file), None)
            assert matching_upload is not None, f"File '{expected_file}' should be included in the upload process"
            actual_key = matching_upload[1] if matching_upload else "None"
            assert (
                matching_upload[1] == expected_key
            ), f"File '{expected_file}' should have S3 key '{expected_key}', got '{actual_key}'"

    @pytest.mark.unit
    def test_distribute_s3_upload_error(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        mock_dataset_wrapper: Any,
    ) -> None:
        """
        Test distribution with S3UploadFailedError during file upload in isolation.

        Verifies that when boto3 raises an S3UploadFailedError during file upload
        (e.g., network timeout, connection lost), the distribute method properly catches
        it and raises a DistributionTargetBase.DistributionError with the exact expected
        error message format. This unit test isolates error handling by mocking only
        external boto3 dependencies while testing real error handling behavior.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Configure the bucket mock to raise S3UploadFailedError on upload_file call
        expected_s3_error = S3UploadFailedError("Network timeout during upload")
        mock_bucket.upload_file.side_effect = expected_s3_error

        target = S3DistributionTarget(**s3_credentials)

        # Act & Assert - Verify S3UploadFailedError is caught and wrapped properly
        with pytest.raises(
            DistributionTargetBase.DistributionError,
            match=r"Distribution error:[\s\S]*S3 upload failed while uploading[\s\S]*Network timeout during upload",
        ) as exc_info:
            target.distribute(mock_dataset_wrapper)

        # Verify the original S3UploadFailedError is properly chained as the root cause
        # Note: Error is wrapped twice - once in _distribute, once in distribute
        assert exc_info.value.__cause__ is not None, "Exception should have a cause"
        assert exc_info.value.__cause__.__cause__ is not None, "Exception should have a root cause"
        root_cause = exc_info.value.__cause__.__cause__
        assert root_cause is expected_s3_error, "Original S3UploadFailedError should be chained as the root cause"

        # Uploads run in parallel; the first observed failure aborts the as_completed loop.
        # Already-submitted in-flight workers may complete before the abort propagates, so
        # call_count is bounded by the number of files in the test fixture.
        assert mock_bucket.upload_file.call_count >= 1, "Upload should be attempted at least once before the error"

    @pytest.mark.integration
    def test_distribute_client_error(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        mock_dataset_wrapper: Any,
    ) -> None:
        """
        Test distribution with AWS client error during upload.

        Verifies that when boto3 raises a ClientError during file upload (e.g., access denied,
        invalid credentials), the distribute method properly catches it and raises a
        DistributionTargetBase.DistributionError with appropriate error message. This tests
        the error handling path for AWS service-level errors during the upload process.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)
        mock_upload = mocker.patch.object(target, "_upload")

        expected_client_error = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "PutObject",
        )
        mock_upload.side_effect = expected_client_error

        # Act & Assert - Verify ClientError is caught and wrapped properly
        with pytest.raises(
            DistributionTargetBase.DistributionError,
            match=r"Distribution error:[\s\S]*AWS client error while uploading",
        ) as exc_info:
            target.distribute(mock_dataset_wrapper)

        # Verify the original ClientError is properly chained as the root cause
        # Note: Error is wrapped twice - once in _distribute, once in distribute
        assert exc_info.value.__cause__ is not None, "Exception should have a cause"
        assert exc_info.value.__cause__.__cause__ is not None, "Exception should have a root cause"
        root_cause = exc_info.value.__cause__.__cause__
        assert root_cause is expected_client_error, "Original ClientError should be chained as the root cause"

        # Verify upload was attempted at least once before the error occurred
        assert mock_upload.call_count >= 1, "Upload method should have been called before the error"

    @pytest.mark.unit
    def test_distribute_generic_error(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        mock_dataset_wrapper: Any,
    ) -> None:
        """
        Test distribution with unexpected exception during upload in isolation.

        Verifies that when an unexpected (non-AWS specific) exception occurs during file upload,
        the distribute method properly catches it and raises a DistributionTargetBase.DistributionError
        with appropriate error message. This unit test isolates error handling by mocking the
        _upload method and testing only the exception wrapping behavior.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)
        mock_upload = mocker.patch.object(target, "_upload")

        # Use a more specific unexpected error type
        expected_unexpected_error = OSError("Unexpected filesystem error during upload")
        mock_upload.side_effect = expected_unexpected_error

        # Act & Assert - Verify unexpected error is caught and wrapped properly
        with pytest.raises(
            DistributionTargetBase.DistributionError,
            match=r"Distribution error:[\s\S]*Failed to upload[\s\S]*Unexpected filesystem error during upload",
        ) as exc_info:
            target.distribute(mock_dataset_wrapper)

        # Verify the original unexpected error is properly chained as the cause
        assert exc_info.value.__cause__ is not None, "Exception should have a cause"
        assert exc_info.value.__cause__.__cause__ is not None, "Exception should have a root cause"
        root_cause = exc_info.value.__cause__.__cause__
        assert root_cause is expected_unexpected_error, "Original OSError should be chained as the root cause"

        # Uploads run in parallel; the first observed failure aborts the as_completed loop.
        # Already-submitted in-flight workers may complete before the abort propagates, so
        # call_count is bounded by the number of files in the test fixture.
        assert mock_upload.call_count >= 1, "Upload method should have been called at least once before the error"

    @pytest.mark.unit
    def test_distribute_outer_exception(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        mock_dataset_wrapper: Any,
    ) -> None:
        """
        Test distribute method exception wrapping in isolation.

        Verifies that when the internal _distribute method raises any exception, the public
        distribute method properly catches it and wraps it in a DistributionTargetBase.DistributionError
        with appropriate error message and preserves the original exception as the cause.
        This unit test isolates the outer exception handling wrapper behavior.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        target = S3DistributionTarget(**s3_credentials)
        mock_inner = mocker.patch.object(target, "_distribute")

        expected_inner_error = ValueError("Internal processing failure during distribution")
        mock_inner.side_effect = expected_inner_error

        # Act & Assert - Verify exception is wrapped with specific error pattern
        with pytest.raises(
            DistributionTargetBase.DistributionError,
            match=r"Distribution error:[\s\S]*Internal processing failure during distribution",
        ) as exc_info:
            target.distribute(mock_dataset_wrapper)

        # Verify the original exception is properly chained as the cause
        assert exc_info.value.__cause__ is expected_inner_error, "Original exception should be chained as the cause"

        # Verify the _distribute method was called exactly once with correct parameters
        mock_inner.assert_called_once_with(mock_dataset_wrapper)

    @pytest.mark.unit
    def test_transfer_config_setup(self, mocker: pytest_mock.MockerFixture, s3_credentials: dict[str, str]) -> None:
        """
        Test S3DistributionTarget initializes TransferConfig with correct parameters.

        Verifies that during S3DistributionTarget initialization, the TransferConfig object
        is properly configured with the expected multipart upload threshold (100MB).
        This ensures that large files will be uploaded using S3's multipart upload
        mechanism for improved performance and reliability.
        """
        # Arrange - Mock external AWS dependencies only
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Act - Initialize S3DistributionTarget
        target = S3DistributionTarget(**s3_credentials)

        # Assert - Verify TransferConfig is properly initialized as correct type
        from boto3.s3.transfer import TransferConfig

        assert isinstance(target._config, TransferConfig), "Should create a TransferConfig instance"

        # Assert - Verify TransferConfig is configured with correct multipart threshold
        assert (
            target._config.multipart_threshold == 100 * 1024 * 1024
        ), "TransferConfig should be configured with 100MB multipart upload threshold"

        # Assert - Verify TransferConfig was stored as instance attribute
        assert hasattr(target, "_config"), "Target should store TransferConfig as _config attribute"

    @pytest.mark.unit
    def test_distribute_empty_dataset_success(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test distribute method handles empty dataset edge case in isolation.

        Verifies that when a dataset contains no files, the distribute method completes
        successfully without attempting any S3 uploads. This unit test isolates the
        edge case behavior by mocking external AWS dependencies while testing that
        the file iteration and distribution logic handles empty datasets gracefully.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Create empty dataset directory (no files)
        empty_dataset = tmp_path / "empty_dataset"
        empty_dataset.mkdir()

        # Create mock wrapper for empty dataset
        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = empty_dataset
        mock_wrapper.name = "empty_dataset"

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute distribution on empty dataset
        target.distribute(mock_wrapper)

        # Assert - Verify no upload attempts were made
        # Since there are no files, upload_file should never be called
        mock_bucket.upload_file.assert_not_called()

    @pytest.mark.unit
    def test_large_file_handling(  # noqa: PLR0915
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test distribution of large files uses TransferConfig for multipart uploads in isolation.

        Verifies that when files exceed the multipart threshold (100MB), the S3DistributionTarget
        properly processes them using the configured TransferConfig. This unit test isolates
        the large file handling behavior by mocking external AWS dependencies and file sizes
        while testing that the transfer configuration is correctly applied during uploads.
        """
        # Arrange - Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Create real dataset structure
        dataset_dir = tmp_path / "large_dataset"
        dataset_dir.mkdir()

        # Create test files
        large_file = dataset_dir / "large_video.mp4"
        large_file.write_text("mock large video content")

        small_file = dataset_dir / "metadata.yaml"
        small_file.write_text("metadata: content")

        # Create dataset wrapper with minimal mocking
        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = dataset_dir
        mock_wrapper.name = "large_dataset"

        # Mock file sizes for specific test files while preserving real file system behavior
        original_stat = Path.stat

        def mock_stat_large_file(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get the real stat first for all files
            real_stat = original_stat(self, *args, **kwargs)

            # Check if this is a file by examining the st_mode from real_stat (avoid recursion)
            import stat as stat_module

            is_regular_file = stat_module.S_ISREG(real_stat.st_mode)

            # Only override size for our specific test files that exist as regular files
            if is_regular_file:
                if self.name == "large_video.mp4":
                    # Create mock object with needed attributes from real stat
                    mock_stat = mocker.Mock()
                    mock_stat.st_size = 200 * 1024 * 1024  # 200MB - exceeds threshold
                    mock_stat.st_mode = real_stat.st_mode
                    mock_stat.st_mtime = real_stat.st_mtime
                    mock_stat.st_ctime = real_stat.st_ctime
                    mock_stat.st_atime = real_stat.st_atime
                    return mock_stat
                if self.name == "metadata.yaml":
                    # Create mock object with needed attributes from real stat
                    mock_stat = mocker.Mock()
                    mock_stat.st_size = 1024  # 1KB - below threshold
                    mock_stat.st_mode = real_stat.st_mode
                    mock_stat.st_mtime = real_stat.st_mtime
                    mock_stat.st_ctime = real_stat.st_ctime
                    mock_stat.st_atime = real_stat.st_atime
                    return mock_stat

            # Return real stat for all other cases (directories, other files, etc.)
            return real_stat

        mocker.patch("pathlib.Path.stat", mock_stat_large_file)

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the distribution process
        target.distribute(mock_wrapper)

        # Assert - Verify both files were uploaded with correct configuration
        assert (
            mock_bucket.upload_file.call_count == 2
        ), "Should upload exactly 2 files (large and small) from dataset structure"

        # Verify transfer configuration was applied to all uploads
        for call in mock_bucket.upload_file.call_args_list:
            config_used = call[1]["Config"]  # Config is keyword argument
            assert (
                config_used == target._config
            ), "All uploads should use the target's configured TransferConfig instance"
            assert (
                config_used.multipart_threshold == 100 * 1024 * 1024
            ), "TransferConfig should have 100MB multipart upload threshold for large file handling"

        # Verify correct S3 keys were generated with proper prefix
        uploaded_keys = [call[0][1] for call in mock_bucket.upload_file.call_args_list]
        expected_keys = {"datasets/large_video.mp4", "datasets/metadata.yaml"}
        actual_keys = set(uploaded_keys)

        assert (
            actual_keys == expected_keys
        ), f"Should upload files with correct S3 keys. Expected: {expected_keys}, Got: {actual_keys}"

        # Verify large file was handled with same config as small file
        # (demonstrates that TransferConfig applies to all uploads regardless of size)
        large_file_calls = [call for call in mock_bucket.upload_file.call_args_list if "large_video.mp4" in call[0][1]]
        small_file_calls = [call for call in mock_bucket.upload_file.call_args_list if "metadata.yaml" in call[0][1]]

        assert len(large_file_calls) == 1, "Large file should be uploaded exactly once"
        assert len(small_file_calls) == 1, "Small file should be uploaded exactly once"

        # Both files should use identical TransferConfig instance
        large_config = large_file_calls[0][1]["Config"]
        small_config = small_file_calls[0][1]["Config"]
        assert large_config is small_config, "Both large and small files should use the same TransferConfig instance"

    @pytest.mark.unit
    def test_progress_tracking(
        self,
        mocker: pytest_mock.MockerFixture,
        s3_credentials: dict[str, str],
        tmp_path: Path,
    ) -> None:
        """
        Test S3DistributionTarget distribute method executes progress workflow phases.

        Verifies that the distribute method successfully executes all three progress phases:
        file collection, size calculation, and upload processing. This unit test focuses on
        workflow execution by mocking external dependencies while testing that the internal
        progress workflow completes without errors for a minimal dataset.
        """
        # Arrange - Create minimal real dataset structure
        dataset_dir = tmp_path / "progress_test_dataset"
        dataset_dir.mkdir()

        # Create single test file for progress workflow
        test_file = dataset_dir / "test.txt"
        test_file.write_text("test content")

        # Create dataset wrapper with minimal mocking
        mock_wrapper = mocker.Mock()
        mock_wrapper.root_dir = dataset_dir
        mock_wrapper.name = "progress_test_dataset"

        # Mock only external AWS dependencies
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")
        mock_s3 = mocker.Mock()
        mock_bucket = mocker.Mock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_resource.return_value = mock_s3

        # Mock upload method to prevent actual S3 calls
        mock_upload = mocker.patch.object(S3DistributionTarget, "_upload")

        target = S3DistributionTarget(**s3_credentials)

        # Act - Execute the distribution process
        target.distribute(mock_wrapper)

        # Assert - Verify the progress workflow executed successfully
        # Test that upload was called exactly once for our single test file
        assert mock_upload.call_count == 1, "Upload should be called once for single test file"

        # Verify upload was called with correct parameters
        upload_call = mock_upload.call_args_list[0]
        uploaded_path = upload_call[0][0]  # First positional arg (path)
        uploaded_key = upload_call[0][1]  # Second positional arg (key)

        assert uploaded_path == test_file, "Should upload the test file"
        assert uploaded_key == "datasets/test.txt", "Should generate correct S3 key with prefix"
