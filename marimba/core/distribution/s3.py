"""
Marimba S3 Distribution Target.

This module provides an S3 distribution target class for distributing datasets to an S3 bucket. It allows uploading
dataset files to a specified S3 bucket using the provided credentials and configuration.

"""

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from stat import S_ISREG

from boto3 import resource
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from rich.progress import DownloadColumn, Progress, SpinnerColumn

from marimba.core.distribution.base import DistributionTargetBase
from marimba.core.utils.constants import S3_MULTIPART_THRESHOLD_BYTES, S3_UPLOAD_MAX_WORKERS
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
            multipart_threshold=S3_MULTIPART_THRESHOLD_BYTES,
        )

        # TODO @<cjackett>: The _check_bucket() method currently fails on the CSIRO DAP S3
        # self._check_bucket()  # noqa: ERA001

    def _check_bucket(self) -> None:
        """
        Check that the bucket exists and that we have access to it.

        Args:
            bucket_name: The name of the bucket to check.

        Raises:
            botocore.exceptions.ClientError: If the bucket does not exist or we do not have access to it.
        """
        self._s3.meta.client.head_bucket(Bucket=self._bucket_name)

    def _iterate_dataset_wrapper(
        self,
        dataset_wrapper: DatasetWrapper,
    ) -> Iterable[tuple[Path, str, int]]:
        """
        Iterate over a dataset structure and generate (path, key, size) tuples.

        Walks the dataset tree once; ``stat()`` is invoked per file on the way through
        so callers don't need a second pass to compute total upload size.

        Args:
            dataset_wrapper: The dataset wrapper to iterate over.

        Returns:
            An iterable of ``(path, key, size_bytes)`` tuples, one per file.
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

        # Single tree walk: stat each file on the way through so the size totalling
        # doesn't need a second pass.
        for path in dataset_wrapper.root_dir.glob("**/*"):
            try:
                stat = path.stat()
            except OSError:
                continue
            if not S_ISREG(stat.st_mode):
                continue
            yield path, path_to_key(path), stat.st_size

    def _upload(self, path: Path, key: str) -> None:
        """
        Upload a file to S3.

        Args:
            path: The path to the file to upload.
            key: The S3 key to upload the file to.
        """
        self._bucket.upload_file(str(path.absolute()), key, Config=self._config)

    def _distribute(self, dataset_wrapper: DatasetWrapper) -> None:
        # Single tree walk: collect (path, key, size) triples in one pass.
        with Progress(SpinnerColumn(), *get_default_columns()) as collection_progress:
            collection_task = collection_progress.add_task(
                "[green]Collecting files to upload",
                total=None,
            )
            self.logger.info("Started collecting files for upload")

            path_key_size_tups: list[tuple[Path, str, int]] = []
            total_bytes = 0
            for path, key, size_bytes in self._iterate_dataset_wrapper(dataset_wrapper):
                path_key_size_tups.append((path, key, size_bytes))
                total_bytes += size_bytes
                collection_progress.update(collection_task, advance=1)

            collection_progress.update(collection_task)
            self.logger.info(f"Found {len(path_key_size_tups)} files to upload")
            self.logger.info(f"Total upload size: {total_bytes / (1024 * 1024):.2f} MB")

        with Progress(
            SpinnerColumn(),
            *get_default_columns(),
            DownloadColumn(binary_units=True),
        ) as progress:
            task = progress.add_task("[green]Uploading dataset", total=total_bytes)

            # Upload files in parallel. boto3's TransferConfig already parallelises *parts within
            # a single multipart file*; this loop adds parallelism *between files* so a dataset of
            # many small-to-medium files saturates available bandwidth instead of stalling per file.
            with ThreadPoolExecutor(max_workers=S3_UPLOAD_MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self._upload, path, key): (path, key, file_bytes)
                    for path, key, file_bytes in path_key_size_tups
                }
                for future in as_completed(futures):
                    path, key, file_bytes = futures[future]
                    try:
                        future.result()
                    except S3UploadFailedError as e:
                        msg = f"S3 upload failed while uploading {path} to {key}:\n{e}"
                        raise DistributionTargetBase.DistributionError(msg) from e
                    except ClientError as e:
                        msg = f"AWS client error while uploading {path} to {key}:\n{e}"
                        raise DistributionTargetBase.DistributionError(msg) from e
                    except Exception as e:
                        msg = f"Failed to upload {path} to {key}:\n{e}"
                        raise DistributionTargetBase.DistributionError(msg) from e

                    progress.update(task, advance=file_bytes)

    def distribute(self, dataset_wrapper: DatasetWrapper) -> None:
        """
        Distributes the dataset_wrapper to the distribution target.

        Args:
            dataset_wrapper: The dataset wrapper object containing the dataset to be distributed.

        Raises:
            DistributionTargetBase.DistributionError: If there is an error during the distribution process.
        """
        try:
            return self._distribute(dataset_wrapper)
        except Exception as e:
            msg = f"Distribution error:\n{e}"
            raise DistributionTargetBase.DistributionError(
                msg,
            ) from e
