"""
Shared fixtures for End-to-End tests.

This module provides common fixtures used across all e2e test modules,
including temporary directories, CLI runner, and cleanup functionality.
"""

import weakref
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from marimba.core.wrappers.dataset import DatasetWrapper


@pytest.fixture(autouse=True)
def cleanup_dataset_wrappers():
    """Automatically clean up any DatasetWrapper instances created during tests."""
    # Track all DatasetWrapper instances using weak references
    original_init = DatasetWrapper.__init__
    dataset_instances = []

    def tracked_init(self: DatasetWrapper, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        dataset_instances.append(weakref.ref(self))

    # Use setattr to avoid mypy method assignment error
    DatasetWrapper.__init__ = tracked_init  # type: ignore[method-assign]

    try:
        yield
    finally:
        # Clean up all tracked instances
        for dataset_ref in dataset_instances:
            dataset_instance = dataset_ref()
            if dataset_instance is not None:
                with suppress(Exception):
                    # Ignore cleanup errors
                    dataset_instance.close()

        # Restore original __init__ method
        DatasetWrapper.__init__ = original_init  # type: ignore[method-assign]


@pytest.fixture
def temp_project_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for E2E test projects."""
    return tmp_path / "test_project"


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary directory with sample data for testing."""
    data_dir = tmp_path / "sample_data"
    data_dir.mkdir()

    # Create some sample files to import
    (data_dir / "image1.jpg").write_text("fake image data")
    (data_dir / "image2.jpg").write_text("fake image data 2")
    (data_dir / "metadata.txt").write_text("sample metadata")

    return data_dir


@pytest.fixture
def runner() -> CliRunner:
    """CLI runner for testing typer commands."""
    return CliRunner()
