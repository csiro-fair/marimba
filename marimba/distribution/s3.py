from pathlib import Path
from typing import Iterable, Tuple

from boto3 import resource
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from marimba.distribution.bases import DistributionTargetBase
from marimba.wrappers.dataset import DatasetWrapper


class S3DistributionTarget(DistributionTargetBase):
    """
    S3 bucket distribution target.
    """

    def __init__(self, bucket_name: str, endpoint_url: str, access_key: str, secret_access_key: str, base_prefix: str = ""):
        self._bucket_name = bucket_name
        self._base_prefix = base_prefix.rstrip("/")

        # Create S3 resource and Bucket
        self._s3 = resource("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        self._bucket = self._s3.Bucket(self._bucket_name)

        # Define the transfer config
        self._config = TransferConfig(
            multipart_threshold=100 * 1024 * 1024,
        )

        # self._check_bucket()

    def _check_bucket(self):
        """
        Check that the bucket exists and that we have access to it.

        Args:
            bucket_name: The name of the bucket to check.

        Raises:
            botocore.exceptions.ClientError: If the bucket does not exist or we do not have access to it.
        """
        self._s3.meta.client.head_bucket(Bucket=self._bucket_name)

    def _iterate_dataset_wrapper(self, dataset_wrapper: DatasetWrapper) -> Iterable[Tuple[Path, str]]:
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

            parts = (self._base_prefix,) + rel_path.parts
            key = "/".join(parts)

            return key

        # Iterate over all files in the dataset
        for path in dataset_wrapper.root_dir.glob("**/*"):
            if path.is_file():
                yield path, path_to_key(path)

    def _upload(self, path: Path, key: str):
        """
        Upload a file to S3.

        Args:
            path: The path to the file to upload.
            key: The S3 key to upload the file to.
        """
        self._bucket.upload_file(str(path.absolute()), key, Config=self._config)

    def _distribute(self, dataset_wrapper: DatasetWrapper):
        for path, key in self._iterate_dataset_wrapper(dataset_wrapper):
            try:
                self._upload(path, key)
            except S3UploadFailedError as e:
                raise DistributionTargetBase.DistributionError(f"S3 upload failed while uploading {path} to {key}:\n{e}") from e
            except ClientError as e:
                raise DistributionTargetBase.DistributionError(f"AWS client error while uploading {path} to {key}:\n{e}") from e
            except Exception as e:
                raise DistributionTargetBase.DistributionError(f"Failed to upload {path} to {key}:\n{e}") from e

    def distribute(self, dataset_wrapper: DatasetWrapper):
        try:
            return self._distribute(dataset_wrapper)
        except Exception as e:
            raise DistributionTargetBase.DistributionError(f"Distribution error:\n{e}") from e
