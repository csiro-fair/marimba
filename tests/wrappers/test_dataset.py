from pathlib import Path
from shutil import rmtree
from typing import Any, Dict
from unittest import TestCase, mock

from marimba.core.wrappers.dataset import DatasetWrapper


class TestDatasetWrapper(TestCase):
    def setUp(self) -> None:
        self.dataset_wrapper = DatasetWrapper.create(Path(__file__).parent / "test_dataset")

    def tearDown(self) -> None:
        root_dir = self.dataset_wrapper.root_dir
        del self.dataset_wrapper
        rmtree(root_dir)

    def test_check_dataset_mapping(self) -> None:
        # Test that an invalid dataset mapping raises an error
        dataset_mapping: Dict[Any, Any] = {
            "test": {Path("nonexistent_file.txt"): (Path("destination.txt"), None, None)}
        }
        with self.assertRaises(DatasetWrapper.InvalidDatasetMappingError):
            self.dataset_wrapper.check_dataset_mapping(dataset_mapping)

        # Test that a valid path mapping does not raise an error
        dataset_mapping = {"test": {Path(__file__): (Path("destination.txt"), None, None)}}
        with mock.patch.object(Path, "exists", return_value=True):
            self.dataset_wrapper.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with duplicate source paths raises an error
            dataset_mapping = {
                "test": {
                    Path("file1.txt"): (Path("destination1.txt"), None, None),
                    Path("file2.txt"): (Path("destination2.txt"), None, None),
                    Path("some_dir/../file1.txt"): (Path("destination3.txt"), None, None),
                }
            }
            with self.assertRaises(DatasetWrapper.InvalidDatasetMappingError):
                self.dataset_wrapper.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with absolute destination paths raises an error
            dataset_mapping = {"test": {Path("file.txt"): (Path("path/to/destination.txt").absolute(), None, None)}}
            with self.assertRaises(DatasetWrapper.InvalidDatasetMappingError):
                self.dataset_wrapper.check_dataset_mapping(dataset_mapping)

            # Test that a path mapping with colliding destination paths raises an error
            dataset_mapping = {
                "test": {
                    Path("file1.txt"): (Path("destination.txt"), None, None),
                    Path("file2.txt"): (Path("destination.txt"), None, None),
                }
            }
            with self.assertRaises(DatasetWrapper.InvalidDatasetMappingError):
                self.dataset_wrapper.check_dataset_mapping(dataset_mapping)
