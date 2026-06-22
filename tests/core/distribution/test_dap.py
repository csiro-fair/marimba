"""Tests for marimba.core.distribution.dap module."""

import pytest
from pytest_mock import MockerFixture

from marimba.core.distribution.dap import CSIRODapDistributionTarget
from marimba.core.distribution.s3 import S3DistributionTarget


class TestCSIRODapDistributionTarget:
    """Test CSIRODapDistributionTarget functionality."""

    @pytest.fixture
    def dap_credentials(self) -> dict[str, str]:
        """Provide DAP credentials for testing."""
        return {
            "endpoint_url": "https://dap.example.com",
            "access_key": "test_key",
            "secret_access_key": "test_secret",
        }

    @pytest.mark.unit
    def test_initialization_and_inheritance(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """Test basic initialization and inheritance behavior.

        Verifies that the DAP target correctly inherits from S3DistributionTarget
        and initializes with proper type hierarchy and attribute parsing.
        """
        # Arrange
        remote_directory = "bucket_name/path/to/data"
        # Mock only external S3 resource creation to avoid network calls
        mocker.patch("marimba.core.distribution.s3.resource")

        # Act
        target = CSIRODapDistributionTarget(
            remote_directory=remote_directory,
            **dap_credentials,
        )

        # Assert
        assert isinstance(target, S3DistributionTarget), "Should inherit from S3DistributionTarget"
        assert isinstance(target, CSIRODapDistributionTarget), "Should be instance of CSIRODapDistributionTarget"
        assert target._bucket_name == "bucket_name", "Bucket name should be parsed correctly"
        assert target._base_prefix == "path/to/data", "Base prefix should be parsed correctly"

    @pytest.mark.unit
    def test_parameter_mapping_to_parent_constructor(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """Test that DAP-style parameters are correctly mapped to parent S3 constructor parameters.

        Verifies that the CSIRODapDistributionTarget correctly maps DAP-style authentication
        parameters to the expected S3DistributionTarget constructor parameters:
        - access_key -> aws_access_key_id (parameter name transformation)
        - secret_access_key -> aws_secret_access_key (unchanged)
        - endpoint_url -> endpoint_url (unchanged)
        """
        # Arrange
        remote_directory = "test-bucket/path/to/data"
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")

        # Act
        CSIRODapDistributionTarget(
            remote_directory=remote_directory,
            **dap_credentials,
        )

        # Assert
        mock_resource.assert_called_once_with(
            "s3",
            endpoint_url=dap_credentials["endpoint_url"],
            aws_access_key_id=dap_credentials["access_key"],  # DAP access_key mapped to aws_access_key_id
            aws_secret_access_key=dap_credentials["secret_access_key"],  # Direct mapping
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("remote_directory", "expected_bucket", "expected_prefix"),
        [
            ("bucket_name/", "bucket_name", ""),  # Root path with trailing slash
            ("bucket_name", "bucket_name", ""),  # Bucket name only
            ("my-bucket/data/2023/project-x/results", "my-bucket", "data/2023/project-x/results"),  # Nested path
            ("bucket/path/with/multiple/slashes", "bucket", "path/with/multiple/slashes"),  # Multiple slashes
            ("bucket//path", "bucket", "/path"),  # Double slash in middle - preserves leading slash in prefix
            ("simple-bucket/nested/deep/path", "simple-bucket", "nested/deep/path"),  # Deeply nested structure
        ],
        ids=[
            "trailing_slash",
            "bucket_only",
            "nested_path",
            "multiple_slashes",
            "double_slash",
            "deep_nesting",
        ],
    )
    def test_remote_directory_parsing_variations(
        self,
        dap_credentials: dict[str, str],
        remote_directory: str,
        expected_bucket: str,
        expected_prefix: str,
        mocker: MockerFixture,
    ) -> None:
        """Test remote directory parsing handles various path formats correctly.

        This parameterized test verifies the parsing logic for different remote directory
        formats, including edge cases like:
        - Bucket names with trailing slashes
        - Bucket names without any path components
        - Deeply nested path structures
        - Double slashes in paths (preserves leading slash in prefix)

        The parsing logic splits on the first '/' character to separate bucket name from prefix.
        """
        # Arrange
        # Mock only external S3 resource creation to avoid network calls
        mocker.patch("marimba.core.distribution.s3.resource")

        # Act
        target = CSIRODapDistributionTarget(
            remote_directory=remote_directory,
            **dap_credentials,
        )

        # Assert
        assert target._bucket_name == expected_bucket, (
            f"Bucket name parsing failed for remote_directory '{remote_directory}': "
            f"expected bucket '{expected_bucket}', but got '{target._bucket_name}'"
        )
        assert target._base_prefix == expected_prefix, (
            f"Base prefix parsing failed for remote_directory '{remote_directory}': "
            f"expected prefix '{expected_prefix}', but got '{target._base_prefix}'"
        )

    @pytest.mark.unit
    def test_base_prefix_stripping_logic(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """Test that base prefix trailing slashes are stripped correctly.

        Verifies that the DAP target properly handles trailing slashes in the base prefix
        by leveraging the parent S3DistributionTarget's base_prefix stripping logic.
        This tests the integration between DAP parsing and S3 prefix normalization.
        """
        # Arrange
        remote_directory = "test-bucket/data/path/"  # Note trailing slash
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")

        # Act
        target = CSIRODapDistributionTarget(
            remote_directory=remote_directory,
            **dap_credentials,
        )

        # Assert
        assert target._base_prefix == "data/path", (
            f"Base prefix should have trailing slash stripped: "
            f"expected 'data/path', but got '{target._base_prefix}'"
        )
        assert target._bucket_name == "test-bucket", "Bucket name should be parsed correctly"

        # Verify that S3DistributionTarget receives the correctly transformed parameters
        mock_resource.assert_called_once_with(
            "s3",
            endpoint_url=dap_credentials["endpoint_url"],
            aws_access_key_id=dap_credentials["access_key"],  # DAP access_key mapped to aws_access_key_id
            aws_secret_access_key=dap_credentials["secret_access_key"],  # Direct mapping
        )


class TestCSIRODapDistributionTargetEdgeCases:
    """Wrapper-specific edge cases.

    Retry / error behaviour is exercised via the inherited S3DistributionTarget
    tests at tests/core/distribution/test_s3.py (lines 459, 507, 553, 597) —
    DAP is a thin convenience wrapper that adds only the remote_directory
    parsing on top of S3, so the dap-specific tests stay focused on the
    wrapper-specific behaviour.
    """

    @pytest.fixture
    def dap_credentials(self) -> dict[str, str]:
        return {
            "endpoint_url": "https://dap.example.com",
            "access_key": "test_key",
            "secret_access_key": "test_secret",
        }

    @pytest.mark.unit
    def test_empty_remote_directory_yields_empty_bucket(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """An empty remote_directory parses to empty bucket + empty prefix without raising."""
        mocker.patch("marimba.core.distribution.s3.resource")

        target = CSIRODapDistributionTarget(remote_directory="", **dap_credentials)

        assert target._bucket_name == ""
        assert target._base_prefix == ""

    @pytest.mark.unit
    def test_only_slash_remote_directory(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """A bare "/" remote_directory parses to empty bucket + empty prefix."""
        mocker.patch("marimba.core.distribution.s3.resource")

        target = CSIRODapDistributionTarget(remote_directory="/", **dap_credentials)

        assert target._bucket_name == ""
        assert target._base_prefix == ""

    @pytest.mark.unit
    def test_inherits_s3_resource_construction(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """DAP delegates s3 resource construction to S3DistributionTarget unchanged."""
        mock_resource = mocker.patch("marimba.core.distribution.s3.resource")

        CSIRODapDistributionTarget(remote_directory="any-bucket/some/prefix", **dap_credentials)

        # Confirm boto3.resource was called exactly once with the s3 service tag.
        assert mock_resource.call_count == 1
        positional, _ = mock_resource.call_args
        assert positional == ("s3",)

    @pytest.mark.unit
    def test_remote_directory_with_dotted_bucket(
        self,
        dap_credentials: dict[str, str],
        mocker: MockerFixture,
    ) -> None:
        """Buckets with dots (legal in S3 naming) parse intact."""
        mocker.patch("marimba.core.distribution.s3.resource")

        target = CSIRODapDistributionTarget(
            remote_directory="my.org.bucket/path/file",
            **dap_credentials,
        )

        assert target._bucket_name == "my.org.bucket"
        assert target._base_prefix == "path/file"
