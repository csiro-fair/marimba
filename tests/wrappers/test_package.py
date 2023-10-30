from pathlib import Path
from shutil import rmtree
from unittest import TestCase, mock

from ifdo.models import ImageData, ImageSetHeader, iFDO

from marimba.wrappers.package import PackageWrapper


class TestPackageWrapper(TestCase):
    def setUp(self):
        self.package = PackageWrapper.create(Path(__file__).parent / "test_package")

    def tearDown(self):
        root_dir = self.package.root_dir
        del self.package
        rmtree(root_dir)

    def test_check_dataset_mapping(self):
        # Test that an invalid dataset mapping raises an error
        dataset_mapping = {"test": {Path("nonexistent_file.txt"): (Path("destination.txt"), [])}}
        with self.assertRaises(PackageWrapper.InvalidDatasetMappingError):
            self.package.check_dataset_mapping(dataset_mapping)

        # Test that a valid path mapping does not raise an error
        dataset_mapping = {"test": {Path(__file__): (Path("destination.txt"), [])}}
        with mock.patch.object(Path, "exists", return_value=True):
            self.package.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with duplicate source paths raises an error
            dataset_mapping = {
                "test": {
                    Path("file1.txt"): (Path("destination1.txt"), []),
                    Path("file2.txt"): (Path("destination2.txt"), []),
                    Path("some_dir/../file1.txt"): (Path("destination3.txt"), []),
                }
            }
            with self.assertRaises(PackageWrapper.InvalidDatasetMappingError):
                self.package.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with absolute destination paths raises an error
            dataset_mapping = {"test": {Path("file.txt"): (Path("path/to/destination.txt").absolute(), [])}}
            with self.assertRaises(PackageWrapper.InvalidDatasetMappingError):
                self.package.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with colliding destination paths raises an error
            dataset_mapping = {
                "test": {
                    Path("file1.txt"): (Path("destination.txt"), []),
                    Path("file2.txt"): (Path("destination.txt"), []),
                }
            }
            with self.assertRaises(PackageWrapper.InvalidDatasetMappingError):
                self.package.check_dataset_mapping(dataset_mapping)
