from pathlib import Path
from unittest import TestCase, mock

from marimba.utils.context import get_instrument_path


class TestGetInstrumentPath(TestCase):
    @mock.patch("marimba.utils.context.get_collection_path")
    def test_get_instrument_path_with_collection_path_none(self, mock_get_collection_path):
        mock_get_collection_path.return_value = None
        instrument_path = get_instrument_path("marimba")
        self.assertIsNone(instrument_path)

    @mock.patch("marimba.utils.context.get_collection_path")
    def test_get_instrument_path_with_collection_path_not_none(self, mock_get_collection_path):
        mock_collection_path = Path("/path/to/collection")
        mock_get_collection_path.return_value = mock_collection_path
        instrument_name = "marimba"
        expected_instrument_path = mock_collection_path / "instruments" / instrument_name
        instrument_path = get_instrument_path(instrument_name)
        self.assertEqual(instrument_path, expected_instrument_path)
