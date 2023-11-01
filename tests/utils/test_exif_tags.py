from unittest import TestCase

from marimba.core.utils.exif_tags import TAGS, get_key


class TestGetKey(TestCase):
    def test_get_key_with_existing_name(self):
        # Test with a name that exists in the TAGS dictionary
        name = "Make"
        expected_key = 271
        self.assertEqual(get_key(name), expected_key)

    def test_get_key_with_nonexistent_name(self):
        # Test with a name that does not exist in the TAGS dictionary
        name = "NonexistentTag"
        self.assertIsNone(get_key(name))

    def test_get_key_with_empty_name(self):
        # Test with an empty string as the name
        name = ""
        self.assertIsNone(get_key(name))

    def test_get_key_with_none_name(self):
        # Test with None as the name
        name = None
        self.assertIsNone(get_key(name))

    def test_get_key_with_all_names(self):
        # Test with all names in the TAGS dictionary
        for name in TAGS.values():
            key = get_key(name)
            self.assertIsNotNone(key)
            self.assertIsInstance(key, int)
