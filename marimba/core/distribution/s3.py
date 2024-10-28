"""
Marimba S3 Distribution Target.

This module provides an S3 distribution target class for distributing datasets to an S3 bucket. It allows uploading
dataset files to a specified S3 bucket using the provided credentials and configuration.

Imports:
    - pathlib.Path: Provides classes for working with file system paths.
    - typing.Iterable: Provides generic type hints for iterable objects.
    - typing.Tuple: Provides generic type hints for tuple objects.
    - boto3.resource: Provides a resource service client for interacting with AWS services.
    - boto3.exceptions.S3UploadFailedError: Represents an exception raised when an S3 upload fails.
    - boto3.s3.transfer.TransferConfig: Represents the configuration for an S3 transfer.
    - botocore.exceptions.ClientError: Represents an exception raised when an AWS client encounters an error.
    - rich.progress.DownloadColumn: Provides a progress bar column for tracking download progress.
    - rich.progress.Progress: Provides a progress bar for tracking the progress of an operation.
    - rich.progress.SpinnerColumn: Provides a spinning progress indicator column.
    - marimba.core.distribution.bases.DistributionTargetBase: Provides a base class for distribution targets.
    - marimba.core.utils.rich.get_default_columns: Provides default columns for the progress bar.
    - marimba.core.wrappers.dataset.DatasetWrapper: Provides a wrapper class for datasets.

Classes:
    - S3DistributionTarget: Represents an S3 bucket distribution target for datasets.
"""

from collections.abc import Iterable
from pathlib import Path

from boto3 import resource
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from rich.progress import DownloadColumn, Progress, SpinnerColumn

from marimba.core.distribution.bases import DistributionTargetBase
from marimba.core.utils.rich import get_default_columns
from marimba.core.wrappers.dataset import DatasetWrapper


class S3DistributionTarget(DistributionTargetBase):
    """
    S3 bucket distribution target.
    """

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        base_prefix: str = "",
    ) -> None:
        """
        Initialise the class instance.

        Args:
            bucket_name: A string representing the name of the S3 bucket.
            endpoint_url: A string representing the URL of the S3 endpoint.
            access_key_id: A string representing the access key ID for accessing the S3 bucket.
            secret_access_key: A string representing the secret access key for accessing the S3 bucket.
            base_prefix: An optional string representing the base prefix for the S3 bucket.
        """
        self._bucket_name = bucket_name
        self._base_prefix = base_prefix.rstrip("/")

        # Create S3 resource and Bucket
        self._s3 = resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
        self._bucket = self._s3.Bucket(self._bucket_name)

        # Define the transfer config
        self._config = TransferConfig(
            multipart_threshold=100 * 1024 * 1024,
        )

        self._check_bucket()

    def _check_bucket(self) -> None:
        """
        Check that the bucket exists and that we have access to it.

        Args:
            bucket_name: The name of the bucket to check.

        Raises:
            botocore.exceptions.ClientError: If the bucket does not exist or we do not have access to it.
        """
        self._s3.meta.client.head_bucket(Bucket=self._bucket_name)

    def _iterate_dataset_wrapper(self, dataset_wrapper: DatasetWrapper) -> Iterable[tuple[Path, str]]:
        """
        Iterate over a dataset structure and generate (path, key) tuples.

        Args:
            dataset_wrapper: The dataset wrapper to iterate over.

        Returns:
            An iterable of (path, key) tuples.
        """

        def path_to_key(path: Path) -> str:
            """
            Convert a path to an S3 key.

            Args:
                path: The path to convert.

            Returns:
                An S3 key.
            """
            rel_path = path.relative_to(dataset_wrapper.root_dir)
            parts = (self._base_prefix, *rel_path.parts)
            return "/".join(parts)

        # Iterate over all files in the dataset
        for path in dataset_wrapper.root_dir.glob("**/*"):
            if path.is_file():
                yield path, path_to_key(path)

    def _upload(self, path: Path, key: str) -> None:
        """
        Upload a file to S3.

        Args:
            path: The path to the file to upload.
            key: The S3 key to upload the file to.
        """
        self._bucket.upload_file(str(path.absolute()), key, Config=self._config)

    def _distribute(self, dataset_wrapper: DatasetWrapper) -> None:
        path_key_tups = list(self._iterate_dataset_wrapper(dataset_wrapper))

        total_bytes = sum(path.stat().st_size for path, _ in path_key_tups)

        with Progress(SpinnerColumn(), *get_default_columns(), DownloadColumn(binary_units=True)) as progress:
            task = progress.add_task("[green]Uploading", total=total_bytes)

            for path, key in path_key_tups:
                file_bytes = path.stat().st_size

                try:
                    self._upload(path, key)
                except S3UploadFailedError as e:
                    raise DistributionTargetBase.DistributionError(
                        f"S3 upload failed while uploading {path} to {key}:\n{e}",
                    ) from e
                except ClientError as e:
                    raise DistributionTargetBase.DistributionError(
                        f"AWS client error while uploading {path} to {key}:\n{e}",
                    ) from e
                except Exception as e:
                    raise DistributionTargetBase.DistributionError(f"Failed to upload {path} to {key}:\n{e}") from e

                progress.update(task, advance=file_bytes)

    def distribute(self, dataset_wrapper: DatasetWrapper) -> None:
        """
        Distributes the dataset_wrapper to the distribution target.

        Args:
            dataset_wrapper: The dataset wrapper object containing the dataset to be distributed.

        Raises:
            DistributionTargetBase.DistributionError: If there is an error during the distribution process.

        Returns:
            None
        """
        try:
            return self._distribute(dataset_wrapper)
        except Exception as e:
            raise DistributionTargetBase.DistributionError(f"Distribution error:\n{e}") from e
