"""
Marimba DAP Distribution Target.

This module contains classes and functions for interacting with CSIRO's Data Access Portal (DAP) distribution
targets. It provides a convenience class for specifying DAP-style parameters and methods for iterating over dataset
wrappers.

Imports:
    - pathlib.Path: Represents filesystem paths.
    - typing.Iterable: Defines a generic version of collections.abc.Iterable.
    - typing.Tuple: Defines a generic version of tuple.
    - marimba.core.distribution.s3.S3DistributionTarget: Represents an S3 distribution target.
    - marimba.core.wrappers.dataset.DatasetWrapper: Represents a dataset wrapper.

Classes:
    - CSIRODapDistributionTarget: CSIRO DAP (Data Access Portal) distribution target. Convenience class for
    specifying parameters DAP-style.

Functions:
    - path_to_key: Convert a path to an S3 key.
"""

from collections.abc import Iterable
from pathlib import Path

from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.wrappers.dataset import DatasetWrapper


class CSIRODapDistributionTarget(S3DistributionTarget):
    """
    CSIRO DAP (Data Access Portal) distribution target. Convenience class for specifying parameters DAP-style.
    """

    def __init__(self, endpoint_url: str, access_key: str, secret_access_key: str, remote_directory: str) -> None:
        """
        Initialise the class instance.

        Args:
            endpoint_url (str): The URL of the remote endpoint.
            access_key (str): The access key for authentication.
            secret_access_key (str): The secret access key for authentication.
            remote_directory (str): The remote directory path where the files will be accessed.

        """
        first_slash = remote_directory.find("/")
        bucket_name, base_prefix = remote_directory[:first_slash], remote_directory[first_slash + 1 :]

        super().__init__(bucket_name, endpoint_url, access_key, secret_access_key, base_prefix=base_prefix)

    def _iterate_dataset_wrapper(self, dataset_wrapper: DatasetWrapper) -> Iterable[tuple[Path, str]]:
        def path_to_key(path: Path) -> str:
            """
            Convert a path to an S3 key.

            Args:
                path: The path to convert.

            Returns:
                An S3 key.

            """
            rel_path = path.relative_to(dataset_wrapper.root_dir.parent)
            parts = (self._base_prefix, *rel_path.parts)
            return "/".join(parts)

        # Iterate over all files in the dataset
        for path in dataset_wrapper.root_dir.glob("**/*"):
            if path.is_file():
                yield path, path_to_key(path)
