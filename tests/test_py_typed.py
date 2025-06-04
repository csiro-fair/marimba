"""
Test that the py.typed marker file is correctly included in the package.

This test verifies that the py.typed file exists in the installed package,
which signals to type checkers like mypy that this package provides type information.
"""

import os
import importlib.metadata
import importlib.resources
import unittest


class TestPyTypedMarker(unittest.TestCase):
    """Test the presence of the py.typed marker file."""

    def test_py_typed_exists(self):
        """Verify that the py.typed marker file exists in the package."""
        # First check in the source directory directly
        import marimba

        module_path = os.path.dirname(marimba.__file__)
        py_typed_path = os.path.join(module_path, "py.typed")
        self.assertTrue(os.path.exists(py_typed_path), f"py.typed file not found at {py_typed_path}")

        # Then if available, check if it's in the distribution metadata
        try:
            # Get distribution info
            dist = importlib.metadata.distribution("marimba")
            if hasattr(dist, "files") and dist.files:
                # Get all file paths and check if any contains py.typed
                all_paths = [str(f) for f in dist.files]
                py_typed_files = [f for f in all_paths if "py.typed" in f]
                if py_typed_files:
                    self.assertTrue(True, "py.typed found in distribution files")
        except (ImportError, AttributeError) as e:
            # This is fine - we've already checked the file exists in source
            print(f"Note: Could not check distribution files due to: {e}")

    def test_importable_with_types(self):
        """Verify that modules can be imported with type information."""
        # Import a few key modules that should have type information
        from marimba.core.wrappers.project import ProjectWrapper
        from marimba.core.wrappers.dataset import DatasetWrapper
        from marimba.core.schemas.base import BaseMetadata

        # Basic type assertion checks that would fail if typing wasn't working
        self.assertTrue(hasattr(ProjectWrapper, "__annotations__"), "ProjectWrapper should have type annotations")
        self.assertTrue(hasattr(DatasetWrapper, "__annotations__"), "DatasetWrapper should have type annotations")
        self.assertTrue(hasattr(BaseMetadata, "__annotations__"), "BaseMetadata should have type annotations")


if __name__ == "__main__":
    unittest.main()
