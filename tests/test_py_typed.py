"""Tests for py.typed marker file presence and package type information.

This test module verifies that the marimba package correctly includes the py.typed
marker file, which signals to type checkers like mypy that the package provides
type information. It also validates that key modules can be imported and have
proper type annotations.
"""

from pathlib import Path

import pytest


class TestPyTypedMarker:
    """Test the presence and validity of the py.typed marker file."""

    @pytest.mark.unit
    def test_py_typed_marker_file_exists_in_package_root(self) -> None:
        """Verify that py.typed marker file exists in marimba package directory.

        The py.typed file must exist in the package root to signal PEP 561
        compliance to type checkers. This enables tools like mypy to use
        the package's inline type hints for static type checking.
        """
        # Arrange
        import marimba

        module_path = Path(marimba.__file__).parent
        py_typed_path = module_path / "py.typed"

        # Act
        file_exists = py_typed_path.exists()
        is_file = py_typed_path.is_file()

        # Assert
        assert file_exists, f"py.typed marker file not found at expected location: {py_typed_path}"
        assert is_file, f"py.typed must be a regular file, not a directory: {py_typed_path}"

    @pytest.mark.unit
    def test_core_wrapper_classes_have_type_annotated_init_parameters(self) -> None:
        """Verify that core wrapper classes have type-annotated __init__ parameters.

        Tests that ProjectWrapper and DatasetWrapper have complete type annotations
        on their constructor parameters. This validates that type information is
        available for static analysis tools and IDE autocomplete functionality.
        """
        # Arrange
        import inspect

        from marimba.core.wrappers.dataset import DatasetWrapper
        from marimba.core.wrappers.project import ProjectWrapper

        # Act - Get method signatures and extract parameters (excluding 'self')
        project_sig = inspect.signature(ProjectWrapper.__init__)
        dataset_sig = inspect.signature(DatasetWrapper.__init__)
        project_params = [p for name, p in project_sig.parameters.items() if name != "self"]
        dataset_params = [p for name, p in dataset_sig.parameters.items() if name != "self"]

        # Assert - Verify all parameters have type annotations
        assert len(project_params) > 0, "ProjectWrapper.__init__ should have at least one parameter"
        unannotated_project = [p.name for p in project_params if p.annotation == inspect.Parameter.empty]
        assert (
            len(unannotated_project) == 0
        ), f"ProjectWrapper.__init__ has unannotated parameters: {unannotated_project}"

        assert len(dataset_params) > 0, "DatasetWrapper.__init__ should have at least one parameter"
        unannotated_dataset = [p.name for p in dataset_params if p.annotation == inspect.Parameter.empty]
        assert (
            len(unannotated_dataset) == 0
        ), f"DatasetWrapper.__init__ has unannotated parameters: {unannotated_dataset}"

    @pytest.mark.unit
    def test_base_metadata_class_defines_required_abstract_properties(self) -> None:
        """Verify that BaseMetadata defines required abstract properties for all schemas.

        Tests that the BaseMetadata abstract base class defines the core properties
        (datetime, latitude, longitude) that all metadata schema implementations
        must provide. This ensures consistent interface across all metadata types.
        """
        # Arrange
        from marimba.core.schemas.base import BaseMetadata

        # Act - Get all public properties from BaseMetadata
        base_properties = [
            attr
            for attr in dir(BaseMetadata)
            if isinstance(getattr(BaseMetadata, attr, None), property) and not attr.startswith("_")
        ]

        # Assert - Verify required properties are defined
        assert len(base_properties) > 0, "BaseMetadata should define at least one abstract property"
        assert "datetime" in base_properties, "BaseMetadata must define 'datetime' property for temporal data"
        assert "latitude" in base_properties, "BaseMetadata must define 'latitude' property for spatial data"
        assert "longitude" in base_properties, "BaseMetadata must define 'longitude' property for spatial data"

    @pytest.mark.unit
    def test_py_typed_marker_file_is_empty_per_pep561(self) -> None:
        """Verify that py.typed marker file is empty as per PEP 561 specification.

        Per PEP 561, a py.typed file should typically be empty. An empty file
        indicates that all modules in the package support type checking.
        A non-empty file would contain additional configuration, which we don't use.
        """
        # Arrange
        import marimba

        module_path = Path(marimba.__file__).parent
        py_typed_path = module_path / "py.typed"

        # Act
        file_size = py_typed_path.stat().st_size

        # Assert
        assert file_size == 0, f"py.typed marker file should be empty per PEP 561, but has {file_size} bytes"

    @pytest.mark.unit
    def test_marimba_package_is_properly_installed_with_file_location(self) -> None:
        """Verify that marimba package is properly installed and importable.

        This test ensures that the marimba package can be successfully imported
        and has a valid __file__ attribute pointing to its installation location.
        This is a prerequisite for the py.typed marker file to work correctly.
        """
        # Arrange & Act - Import marimba package
        import marimba

        # Assert - Verify package is properly installed
        assert hasattr(marimba, "__file__"), "marimba package should be properly installed with __file__ attribute"
        assert marimba.__file__ is not None, "marimba package __file__ should point to an actual file location"
        assert Path(marimba.__file__).exists(), f"marimba package file should exist at {marimba.__file__}"

    @pytest.mark.unit
    def test_wrapper_classes_have_init_type_annotations_for_type_checkers(self) -> None:
        """Verify that core wrapper classes expose type annotations on their __init__ methods.

        This test validates that ProjectWrapper and DatasetWrapper have type annotations
        accessible via __annotations__ on their __init__ methods. This ensures type
        information is available for static analysis tools and IDE autocomplete.
        """
        # Arrange
        from marimba.core.wrappers.dataset import DatasetWrapper
        from marimba.core.wrappers.project import ProjectWrapper

        # Act - Check for __annotations__ on __init__ methods
        project_has_init_annotations = hasattr(ProjectWrapper.__init__, "__annotations__")
        dataset_has_init_annotations = hasattr(DatasetWrapper.__init__, "__annotations__")

        # Assert - Verify __init__ methods have type annotations
        assert (
            project_has_init_annotations
        ), "ProjectWrapper.__init__ should have __annotations__ attribute for type checkers"
        assert (
            dataset_has_init_annotations
        ), "DatasetWrapper.__init__ should have __annotations__ attribute for type checkers"
