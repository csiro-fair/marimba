"""
Marimba DAP Distribution Target.

This module contains classes and functions for interacting with CSIRO's Data Access Portal (DAP) distribution
targets. It provides a convenience class for specifying DAP-style parameters and methods for iterating over dataset
wrappers.

"""

from pathlib import Path

from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.wrappers.dataset import DatasetWrapper


class CSIRODapDistributionTarget(S3DistributionTarget):
    """
    CSIRO DAP (Data Access Portal) distribution target. Convenience class for specifying parameters DAP-style.

    ``remote_directory`` is the bucket and path the DAP shows for a collection's S3 upload, e.g.
    ``dapprd/000012345v001/data``. That path is the collection's single file root, which every upload to the
    collection shares, so each dataset is placed in its own folder beneath it, named after the dataset:
    ``dapprd/000012345v001/data/<dataset name>/...``. The plain S3 target does not do this; a generic bucket
    prefix is taken to be the dataset's own location.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_access_key: str,
        remote_directory: str,
    ) -> None:
        """
        Initialise the class instance.

        Args:
            endpoint_url (str): The URL of the remote endpoint.
            access_key (str): The access key for authentication.
            secret_access_key (str): The secret access key for authentication.
            remote_directory (str): The remote directory path where the files will be accessed.

        """
        first_slash = remote_directory.find("/")
        if first_slash == -1:
            # No slash found - entire string is bucket name, empty prefix
            bucket_name, base_prefix = remote_directory, ""
        else:
            # Slash found - split at first slash
            bucket_name, base_prefix = (
                remote_directory[:first_slash],
                remote_directory[first_slash + 1 :],
            )

        super().__init__(
            bucket_name,
            endpoint_url,
            access_key_id=access_key,
            secret_access_key=secret_access_key,
            base_prefix=base_prefix,
        )

    def _key_parts(self, dataset_wrapper: DatasetWrapper, rel_path: Path) -> tuple[str, ...]:
        """
        Key segments for a DAP upload: the collection path, the dataset's own folder, then the file path.

        An empty collection path (a bucket-only ``remote_directory``) is dropped rather than producing a key
        with a leading slash.

        Args:
            dataset_wrapper: The dataset being distributed.
            rel_path: The file's path relative to the dataset root.

        Returns:
            The key segments.
        """
        prefix = (self._base_prefix,) if self._base_prefix else ()
        return (*prefix, dataset_wrapper.name, *rel_path.parts)

    def _destination(self, dataset_wrapper: DatasetWrapper) -> str:
        """The dataset's own folder beneath the collection root, in ``s3://bucket/prefix/<dataset name>/`` form."""
        return f"{super()._destination(dataset_wrapper)}{dataset_wrapper.name}/"
