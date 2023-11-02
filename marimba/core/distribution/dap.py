from pathlib import Path
from typing import Iterable, Tuple

from marimba.core.distribution.s3 import S3DistributionTarget
from marimba.core.wrappers.dataset import DatasetWrapper


class CSIRODapDistributionTarget(S3DistributionTarget):
    """
    CSIRO DAP (Data Access Portal) distribution target. Convenience class for specifying parameters DAP-style.
    """

    def __init__(self, endpoint_url: str, access_key: str, secret_access_key: str, remote_directory: str):
        first_slash = remote_directory.find("/")
        bucket_name, base_prefix = remote_directory[:first_slash], remote_directory[first_slash + 1 :]

        super().__init__(bucket_name, endpoint_url, access_key, secret_access_key, base_prefix=base_prefix)

    def _iterate_dataset_wrapper(self, dataset_wrapper: DatasetWrapper) -> Iterable[Tuple[Path, str]]:
        def path_to_key(path: Path) -> str:
            """
            Convert a path to an S3 key.

            Args:
                path: The path to convert.

            Returns:
                An S3 key.
            """
            rel_path = path.relative_to(dataset_wrapper.root_dir.parent)

            parts = (self._base_prefix,) + rel_path.parts
            key = "/".join(parts)

            return key

        # Iterate over all files in the dataset
        for path in dataset_wrapper.root_dir.glob("**/*"):
            if path.is_file():
                yield path, path_to_key(path)
