from pathlib import Path
from unittest import TestCase, mock

from marimba.core.utils.file_system import create_directory_if_necessary


class TestCreateDirectoryIfNecessary(TestCase):
    def setUp(self):
        self.path = Path("test_directory")

    def tearDown(self):
        if self.path.exists():
            self.path.rmdir()

    def test_create_directory_if_necessary_with_existing_directory(self):
        with mock.patch("marimba.core.utils.file_system.logger") as mock_logger:
            self.path.mkdir()
            create_directory_if_necessary(self.path)
            mock_logger.info.assert_not_called()

    def test_create_directory_if_necessary_with_nonexistent_directory(self):
        with mock.patch("marimba.core.utils.file_system.logger") as mock_logger:
            create_directory_if_necessary(self.path)
            mock_logger.info.assert_called_once_with(f"Creating new directory path: {self.path}")
            self.assertTrue(self.path.exists())
