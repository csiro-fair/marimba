from pathlib import Path
from shutil import rmtree
from unittest import TestCase, mock

from ifdo.models import ImageSetHeader, iFDO

from marimba.wrappers.package import PackageWrapper


class TestPackageWrapper(TestCase):
    def setUp(self):
        self.ifdo = iFDO(
            image_set_header=ImageSetHeader(
                image_set_name="Test Package", image_set_uuid="00000000-0000-0000-0000-000000000000", image_set_handle=""
            ),
            image_set_items=[],
        )
        self.package = PackageWrapper.create(Path(__file__).parent / "test_package", self.ifdo)

    def tearDown(self):
        root_dir = self.package.root_dir
        del self.package
        rmtree(root_dir)

    def test_check_path_mapping(self):
        # Test that an invalid path mapping raises an error
        path_mapping = {Path("nonexistent_file.txt"): Path("destination.txt")}
        with self.assertRaises(PackageWrapper.InvalidPathMappingError):
            self.package.check_path_mapping(path_mapping)

        # Test that a valid path mapping does not raise an error
        path_mapping = {Path(__file__): Path("destination.txt")}
        with mock.patch.object(Path, "exists", return_value=True):
            self.package.check_path_mapping(path_mapping)

            # Test that a path mapping with duplicate source paths raises an error
            path_mapping = {
                Path("file1.txt"): Path("destination1.txt"),
                Path("file2.txt"): Path("destination2.txt"),
                Path("some_dir/../file1.txt"): Path("destination3.txt"),
            }
            with self.assertRaises(PackageWrapper.InvalidPathMappingError):
                self.package.check_path_mapping(path_mapping)

            # Test that a path mapping with absolute destination paths raises an error
            path_mapping = {Path("file.txt"): Path("path/to/destination.txt").absolute()}
            with self.assertRaises(PackageWrapper.InvalidPathMappingError):
                self.package.check_path_mapping(path_mapping)

            # Test that a path mapping with colliding destination paths raises an error
            path_mapping = {
                Path("file1.txt"): Path("destination.txt"),
                Path("file2.txt"): Path("destination.txt"),
            }
            with self.assertRaises(PackageWrapper.InvalidPathMappingError):
                self.package.check_path_mapping(path_mapping)
