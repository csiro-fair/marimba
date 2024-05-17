from pathlib import Path
from unittest import TestCase

import yaml

from marimba.core.utils.config import load_config


class TestLoadConfig(TestCase):
    """

    TestLoadConfig(TestCase)

    A class to test the functionality of the load_config function with different scenarios.

    Attributes:
        config_path (Path): The path to the test config file.
        config_data (dict): The expected configuration data.

    Methods:
        setUp() -> None:
            Set up the necessary attributes for testing.

        tearDown() -> None:
            Clean up the test environment after testing.

        test_load_config_with_valid_yaml() -> None:
            Test the load_config function with a valid YAML file.
            Assert that the loaded configuration data is equal to the expected data.

        test_load_config_with_invalid_yaml() -> None:
            Test the load_config function with an invalid YAML file.
            Assert that a yaml.scanner.ScannerError is raised.

        test_load_config_with_nonexistent_file() -> None:
            Test the load_config function with a nonexistent file.
            Assert that a FileNotFoundError is raised.

    """

    def setUp(self) -> None:
        self.config_path = Path("test_config.yaml")
        self.config_data = {"key": "value"}

    def tearDown(self) -> None:
        if self.config_path.exists():
            self.config_path.unlink()

    def test_load_config_with_valid_yaml(self) -> None:
        with self.config_path.open("w", encoding="utf-8") as f:
            f.write("key: value")

        config_data = load_config(self.config_path)
        self.assertEqual(config_data, self.config_data)

    def test_load_config_with_invalid_yaml(self) -> None:
        with self.config_path.open("w", encoding="utf-8") as f:
            f.write("key: value\ninvalid")

        with self.assertRaises(yaml.scanner.ScannerError):
            load_config(self.config_path)

    def test_load_config_with_nonexistent_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_config(self.config_path)
