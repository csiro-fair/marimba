from marimba.core.distribution.s3 import S3DistributionTarget


class CSIRODapDistributionTarget(S3DistributionTarget):
    """
    CSIRO DAP (Data Access Portal) distribution target. Convenience class for specifying parameters DAP-style.
    """

    def __init__(self, endpoint_url: str, access_key: str, secret_access_key: str, remote_directory: str):
        first_slash = remote_directory.find("/")
        bucket_name, base_prefix = remote_directory[:first_slash], remote_directory[first_slash + 1 :]

        super().__init__(bucket_name, endpoint_url, access_key, secret_access_key, base_prefix=base_prefix)
