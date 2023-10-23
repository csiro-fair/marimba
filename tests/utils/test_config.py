from pathlib import Path
from unittest import TestCase

import yaml

from marimba.utils.config import load_config


class TestLoadConfig(TestCase):
    def setUp(self):
        self.config_path = Path("test_config.yaml")
        self.config_data = {"key": "value"}

    def tearDown(self):
        if self.config_path.exists():
            self.config_path.unlink()

    def test_load_config_with_valid_yaml(self):
        with self.config_path.open("w") as f:
            f.write("key: value")

        config_data = load_config(self.config_path)
        self.assertEqual(config_data, self.config_data)

    def test_load_config_with_invalid_yaml(self):
        with self.config_path.open("w") as f:
            f.write("key: value\ninvalid")

        with self.assertRaises(yaml.scanner.ScannerError):
            load_config(self.config_path)

    def test_load_config_with_nonexistent_file(self):
        with self.assertRaises(FileNotFoundError):
            load_config(self.config_path)
