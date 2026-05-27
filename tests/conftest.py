"""
Global test fixtures for the marimba test suite.

This module provides shared fixtures used across all test modules,
including common test data, temporary directories, and testing utilities.
"""

import re
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import pytest_mock
from click.testing import Result
from typer.testing import CliRunner

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    # Rich emits ANSI bold / color codes between span boundaries even when NO_COLOR + TERM=dumb are set
    # under some CI runner configurations, breaking substring assertions like `"collection-name" in stdout`
    # because the literal becomes `\x1b[1;36m-collection\x1b[0m\x1b[1;36m-name\x1b[0m`. Strip ANSI before
    # substring checks against rendered Rich/Typer output.
    return _ANSI_ESCAPE_RE.sub("", text)


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_project_config() -> dict[str, Any]:
    """Sample project configuration for testing."""
    return {
        "name": "test_project",
        "version": "1.0.0",
        "description": "A test project for unit testing",
    }


@pytest.fixture
def sample_ifdo_metadata() -> dict[str, Any]:
    """Sample iFDO metadata for testing."""
    return {
        "image-set-header": {
            "image-set-name": "test_dataset",
            "image-set-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "image-acquisition": {
                "image-acquisition-uuid": "550e8400-e29b-41d4-a716-446655440001",
                "image-coordinate-system": {"coordinate-system-name": "WGS84"},
                "image-capturing": {
                    "image-capturing-uuid": "550e8400-e29b-41d4-a716-446655440002",
                    "image-camera": {
                        "camera-uuid": "550e8400-e29b-41d4-a716-446655440003",
                        "camera-name": "Test Camera",
                        "camera-model": "TestCam 1000",
                    },
                },
            },
        },
        "image-set-items": [],
    }


@pytest.fixture
def sample_test_data_files(temp_dir: Path) -> Path:
    """Create sample test data files in a temporary directory."""
    data_dir = temp_dir / "test_data"
    data_dir.mkdir(parents=True)

    # Create sample images
    (data_dir / "image001.jpg").write_bytes(b"fake_jpeg_data")
    (data_dir / "image002.jpg").write_bytes(b"fake_jpeg_data_2")
    (data_dir / "image003.png").write_bytes(b"fake_png_data")

    # Create metadata files
    (data_dir / "metadata.csv").write_text("filename,timestamp,depth\nimage001.jpg,2024-01-01T10:00:00Z,10.5\n")
    (data_dir / "config.yml").write_text("site_id: TEST01\nfield_of_view: 1000\n")

    return data_dir


@pytest.fixture
def mock_git_operations(mocker: pytest_mock.MockerFixture) -> dict[str, Any]:
    """Mock Git operations to avoid network dependencies in tests."""

    def mock_clone_from(_url: str, to_path: str, **_kwargs: Any) -> Any:
        """Mock git clone that creates expected directory structure."""
        repo_path = Path(to_path)
        repo_path.mkdir(parents=True, exist_ok=True)

        # Create basic pipeline structure
        (repo_path / "pipeline.yml").write_text(
            """
name: test_pipeline
version: 1.0.0
description: Test pipeline for unit testing
requirements:
  - python>=3.8
""",
        )
        (repo_path / "main.py").write_text("# Test pipeline main script")

        return mocker.Mock()

    mock_clone = mocker.patch("git.Repo.clone_from", side_effect=mock_clone_from)
    mock_repo = mocker.patch("git.Repo")

    return {
        "clone": mock_clone,
        "repo": mock_repo,
    }


@pytest.fixture(autouse=True, scope="session")
def _stabilise_rich_rendering() -> Generator[None, None, None]:
    # GitHub Actions sets GITHUB_ACTIONS=true, which Rich treats as a hint to
    # force terminal-mode rendering — that produces ANSI bold codes between
    # span boundaries (e.g. "-\x1b[0m\x1b[1m-collection\x1b[0m\x1b[1m-name\x1b[0m"),
    # breaking substring assertions against rendered CLI output even when
    # NO_COLOR is set. Pin TERM=dumb + NO_COLOR=1 session-wide so Rich emits
    # plain text. Deliberately leave COLUMNS untouched so default-width
    # tracebacks (e.g. in test_delete_project_invalid_structure) stay at the
    # narrow width tests were written for; tests that need wide output use
    # the cli_runner fixture below to set COLUMNS=200 explicitly.
    import os

    overrides = {"NO_COLOR": "1", "TERM": "dumb"}
    old: dict[str, str | None] = {k: os.environ.get(k) for k in overrides}
    for k, v in overrides.items():
        os.environ[k] = v
    try:
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


@pytest.fixture
def cli_runner() -> CliRunner:
    # COLUMNS=200, NO_COLOR=1, TERM=dumb pin help-text width and disable
    # Rich's terminal-mode rendering. CI runners set GITHUB_ACTIONS=true,
    # which Rich treats as a hint to force terminal mode; without TERM=dumb,
    # Rich emits ANSI codes between span boundaries (e.g. between the dashes
    # of --collection-name) and breaks substring assertions.
    return CliRunner(env={"COLUMNS": "200", "NO_COLOR": "1", "TERM": "dumb"})


# Test data constants
TEST_COORDINATES = [
    (151.2093, -33.8688),  # Sydney
    (-74.0060, 40.7128),  # New York
    (2.3522, 48.8566),  # Paris
]

TEST_TIMESTAMPS = [
    "2024-01-01T00:00:00Z",
    "2024-06-15T12:30:45Z",
    "2024-12-31T23:59:59Z",
]

# Test file extensions for various operations
SUPPORTED_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".tiff", ".bmp"]
SUPPORTED_VIDEO_EXTENSIONS = [".mp4", ".avi", ".mov", ".mkv"]


# Test Data Factories
class TestDataFactory:
    """Factory for creating consistent test data across the test suite."""

    @staticmethod
    def create_project_config(**overrides: Any) -> dict[str, Any]:
        """Create a project configuration with optional overrides."""
        config = {
            "name": "test_project",
            "version": "1.0.0",
            "description": "Test project for unit testing",
            "author": "test_user",
            "created": "2024-01-01T00:00:00Z",
        }
        config.update(overrides)
        return config

    @staticmethod
    def create_pipeline_config(**overrides: Any) -> dict[str, Any]:
        """Create a pipeline configuration with optional overrides."""
        config = {
            "name": "test_pipeline",
            "version": "1.0.0",
            "description": "Test pipeline for unit testing",
            "requirements": ["python>=3.8"],
            "parameters": {"threshold": 0.5, "max_depth": 100, "site_id": "TEST_SITE_01"},
        }
        config.update(overrides)
        return config

    @staticmethod
    def create_collection_config(**overrides: Any) -> dict[str, Any]:
        """Create a collection configuration with optional overrides."""
        config = {
            "name": "test_collection",
            "site_id": "TEST_SITE_01",
            "field_of_view": "1000",
            "instrument_type": "camera",
            "operation": "copy",
            "created": "2024-01-01T00:00:00Z",
        }
        config.update(overrides)
        return config

    @staticmethod
    def create_ifdo_metadata(**overrides: Any) -> dict[str, Any]:
        """Create iFDO metadata structure with optional overrides."""
        metadata = {
            "image-set-header": {
                "image-set-name": "test_dataset",
                "image-set-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "image-acquisition": {
                    "image-acquisition-uuid": "550e8400-e29b-41d4-a716-446655440001",
                    "image-coordinate-system": {"coordinate-system-name": "WGS84"},
                    "image-capturing": {
                        "image-capturing-uuid": "550e8400-e29b-41d4-a716-446655440002",
                        "image-camera": {
                            "camera-uuid": "550e8400-e29b-41d4-a716-446655440003",
                            "camera-name": "Test Camera",
                            "camera-model": "TestCam 1000",
                        },
                    },
                },
            },
            "image-set-items": [],
        }
        # Deep merge overrides
        if overrides:
            TestDataFactory._deep_update(metadata, overrides)
        return metadata

    @staticmethod
    def create_dataset_metadata(**overrides: Any) -> dict[str, Any]:
        """Create dataset metadata with optional overrides."""
        metadata = {
            "name": "test_dataset",
            "version": "1.0.0",
            "description": "Test dataset for unit testing",
            "contact": {"name": "Test User", "email": "test@example.com"},
            "created": "2024-01-01T00:00:00Z",
            "format": "ifdo",
            "license": "CC-BY-4.0",
        }
        metadata.update(overrides)
        return metadata

    @staticmethod
    def create_test_files(base_dir: Path, file_count: int = 3, file_size: str = "small") -> list[Path]:
        """Create test files in a directory with configurable size for performance."""
        base_dir.mkdir(parents=True, exist_ok=True)
        files = []

        # Optimize content size based on test needs
        if file_size == "small":
            content_template = "Test file content {}"
        elif file_size == "medium":
            content_template = "Test file content {} " + "x" * 100
        else:  # large
            content_template = "Test file content {} " + "x" * 1000

        for i in range(file_count):
            file_path = base_dir / f"test_file_{i:03d}.txt"
            file_path.write_text(content_template.format(i))
            files.append(file_path)
        return files

    @staticmethod
    def create_test_images(base_dir: Path, image_count: int = 3, image_size: str = "minimal") -> list[Path]:
        """Create fake test image files with configurable size for performance."""
        base_dir.mkdir(parents=True, exist_ok=True)
        images = []

        # Optimize image data size based on test needs
        if image_size == "minimal":
            # Minimal valid JPEG header (19 bytes)
            jpeg_data = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xd9"
        else:  # realistic
            # Larger but still fake JPEG data (for tests that need more realistic file sizes)
            jpeg_data = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00" + b"x" * 1000 + b"\xff\xd9"

        for i in range(image_count):
            image_path = base_dir / f"image_{i:03d}.jpg"
            image_path.write_bytes(jpeg_data)
            images.append(image_path)
        return images

    @staticmethod
    def _deep_update(base_dict: dict[str, Any], update_dict: dict[str, Any]) -> None:
        """Deep update a dictionary with another dictionary."""
        for key, value in update_dict.items():
            if isinstance(value, dict) and key in base_dict and isinstance(base_dict[key], dict):
                TestDataFactory._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value


@pytest.fixture
def test_data_factory() -> TestDataFactory:
    """Provide the TestDataFactory for tests."""
    return TestDataFactory()


# Common test helper functions
def assert_project_structure_exists(project_dir: Path, message_prefix: str = "") -> None:
    """Assert that a project has the expected directory structure."""
    prefix = f"{message_prefix}: " if message_prefix else ""
    assert project_dir.exists(), f"{prefix}Project directory should exist"
    assert (project_dir / ".marimba").exists(), f"{prefix}Marimba config directory should exist"
    assert (project_dir / "pipelines").exists(), f"{prefix}Pipelines directory should exist"
    assert (project_dir / "collections").exists(), f"{prefix}Collections directory should exist"
    assert (project_dir / "datasets").exists(), f"{prefix}Datasets directory should exist"
    assert (project_dir / "targets").exists(), f"{prefix}Targets directory should exist"


def assert_collection_exists(project_dir: Path, collection_name: str) -> Path:
    """Assert that a collection exists and return its path."""
    collection_dir = project_dir / "collections" / collection_name
    assert collection_dir.exists(), f"Collection {collection_name} directory should exist"

    collection_config = collection_dir / "collection.yml"
    assert collection_config.exists(), f"Collection {collection_name} config should exist"

    return collection_dir


def create_test_project_with_cli(runner: CliRunner, project_dir: Path) -> None:
    """Create a test project using the CLI and verify it was created correctly."""
    from marimba.main import marimba_cli as app

    result = runner.invoke(app, ["new", "project", str(project_dir)])
    assert result.exit_code == 0, f"Project creation should succeed: {result.stdout}"
    assert_project_structure_exists(project_dir, "Created project")


# CLI Testing Helpers - Phase 2 Deduplication
def assert_cli_success(result: Result, expected_message: str | None = None, context: str = "") -> None:
    """Helper for CLI success assertions with detailed error reporting."""
    error_context = f" ({context})" if context else ""

    # Enhanced error reporting for failed commands
    if result.exit_code != 0:
        error_output = (
            result.output if result.output else result.stderr if hasattr(result, "stderr") else "No output available"
        )
        msg = (
            f"CLI command failed{error_context}:\n"
            f"Exit code: {result.exit_code}\n"
            f"Output: {error_output}\n"
            f"Expected: Success (exit code 0)"
        )
        raise AssertionError(
            msg,
        )

    if expected_message:
        assert (
            expected_message in result.output
        ), f"Expected message '{expected_message}' not found in CLI output{error_context}:\n{result.output}"


def assert_cli_failure(
    result: Result,
    expected_error: str | None = None,
    expected_exit_code: int | None = None,
    context: str = "",
) -> None:
    """Helper for CLI failure assertions with detailed validation."""
    error_context = f" ({context})" if context else ""

    if expected_exit_code is not None:
        assert (
            result.exit_code == expected_exit_code
        ), f"Expected exit code {expected_exit_code}, got {result.exit_code}{error_context}:\n{result.output}"
    else:
        assert (
            result.exit_code != 0
        ), f"CLI command should have failed{error_context}, but got exit code 0:\n{result.output}"

    if expected_error:
        output_to_check = result.output or (result.stderr if hasattr(result, "stderr") else "")
        assert (
            expected_error in output_to_check
        ), f"Expected error '{expected_error}' not found in CLI output{error_context}:\n{output_to_check}"


def run_cli_command(
    runner: CliRunner,
    command_args: list[str],
    *,
    expected_success: bool = True,
    expected_message: str | None = None,
    context: str = "",
) -> Result:
    """Run a CLI command with automatic success/failure validation."""
    from marimba.main import marimba_cli as app

    result = runner.invoke(app, command_args)

    if expected_success:
        assert_cli_success(result, expected_message, context or f"Command: {' '.join(command_args)}")
    else:
        assert_cli_failure(result, expected_message, context=context or f"Command: {' '.join(command_args)}")

    return result


def assert_project_structure_complete(project_dir: Path, message_prefix: str = "") -> None:
    """Validate complete Marimba project structure with enhanced checking."""
    prefix = f"{message_prefix}: " if message_prefix else ""

    # Basic structure
    assert project_dir.exists(), f"{prefix}Project directory should exist"
    assert project_dir.is_dir(), f"{prefix}Project path should be a directory"

    # Core directories
    marimba_dir = project_dir / ".marimba"
    assert marimba_dir.exists(), f"{prefix}Marimba config directory should exist"
    assert marimba_dir.is_dir(), f"{prefix}.marimba should be a directory"

    pipelines_dir = project_dir / "pipelines"
    assert pipelines_dir.exists(), f"{prefix}Pipelines directory should exist"
    assert pipelines_dir.is_dir(), f"{prefix}Pipelines should be a directory"

    collections_dir = project_dir / "collections"
    assert collections_dir.exists(), f"{prefix}Collections directory should exist"
    assert collections_dir.is_dir(), f"{prefix}Collections should be a directory"

    datasets_dir = project_dir / "datasets"
    assert datasets_dir.exists(), f"{prefix}Datasets directory should exist"
    assert datasets_dir.is_dir(), f"{prefix}Datasets should be a directory"

    targets_dir = project_dir / "targets"
    assert targets_dir.exists(), f"{prefix}Targets directory should exist"
    assert targets_dir.is_dir(), f"{prefix}Targets should be a directory"


# Mock Structure Helpers - Phase 2 Enhancements
def create_mock_pipeline_structure(
    base_path: Path,
    pipeline_name: str = "test_pipeline",
    config_overrides: dict[str, Any] | None = None,
) -> Path:
    """Create standardized mock pipeline structure for testing."""
    pipeline_dir = base_path / "pipelines" / pipeline_name
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    # Create pipeline repository structure
    repo_dir = pipeline_dir / "repo"
    repo_dir.mkdir(exist_ok=True)

    # Create pipeline configuration
    config = TestDataFactory.create_pipeline_config(name=pipeline_name)
    if config_overrides:
        config.update(config_overrides)

    config_file = pipeline_dir / "pipeline.yml"
    config_content = f"""name: {config['name']}
version: {config['version']}
description: {config['description']}
requirements:
  - python>=3.8
parameters:
  threshold: {config['parameters']['threshold']}
  max_depth: {config['parameters']['max_depth']}
  site_id: {config['parameters']['site_id']}
"""
    config_file.write_text(config_content)

    # Create main script
    main_script = repo_dir / "main.py"
    main_script.write_text(f"# Main script for {pipeline_name}\nprint('Processing data...')")

    # Create README
    readme_file = repo_dir / "README.md"
    readme_file.write_text(f"# {config['name']}\n\n{config['description']}")

    return pipeline_dir


def create_mock_collection_structure(
    base_path: Path,
    *,
    collection_name: str = "test_collection",
    config_overrides: dict[str, Any] | None = None,
    add_sample_data: bool = True,
) -> Path:
    """Create standardized mock collection structure for testing."""
    collection_dir = base_path / "collections" / collection_name
    collection_dir.mkdir(parents=True, exist_ok=True)

    # Create collection configuration
    config = TestDataFactory.create_collection_config(name=collection_name)
    if config_overrides:
        config.update(config_overrides)

    config_file = collection_dir / "collection.yml"
    config_content = f"""name: {config['name']}
site_id: {config['site_id']}
field_of_view: {config['field_of_view']}
instrument_type: {config['instrument_type']}
operation: {config['operation']}
created: {config['created']}
"""
    config_file.write_text(config_content)

    if add_sample_data:
        # Create sample data directory
        data_dir = collection_dir / "data"
        data_dir.mkdir(exist_ok=True)

        # Add some sample files
        TestDataFactory.create_test_images(data_dir, image_count=3, image_size="minimal")
        TestDataFactory.create_test_files(data_dir, file_count=2, file_size="small")

    return collection_dir


def create_mock_dataset_structure(
    base_path: Path,
    dataset_name: str = "test_dataset",
    config_overrides: dict[str, Any] | None = None,
) -> Path:
    """Create standardized mock dataset structure for testing."""
    dataset_dir = base_path / "datasets" / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Create dataset configuration
    config = TestDataFactory.create_dataset_metadata(name=dataset_name)
    if config_overrides:
        config.update(config_overrides)

    config_file = dataset_dir / "dataset.yml"
    config_content = f"""name: {config['name']}
version: {config['version']}
description: {config['description']}
contact:
  name: {config['contact']['name']}
  email: {config['contact']['email']}
created: {config['created']}
format: {config['format']}
license: {config['license']}
"""
    config_file.write_text(config_content)

    # Create data directory
    data_dir = dataset_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # Create metadata file
    metadata_file = dataset_dir / "metadata.json"
    import json

    metadata_file.write_text(json.dumps(TestDataFactory.create_ifdo_metadata(), indent=2))

    return dataset_dir


def create_complete_mock_project(base_path: Path, project_name: str = "test_project") -> Path:
    """Create a complete mock project with pipelines, collections, and datasets."""
    project_dir = base_path / project_name
    project_dir.mkdir(exist_ok=True)

    # Create basic project structure
    (project_dir / ".marimba").mkdir(exist_ok=True)
    (project_dir / "pipelines").mkdir(exist_ok=True)
    (project_dir / "collections").mkdir(exist_ok=True)
    (project_dir / "datasets").mkdir(exist_ok=True)
    (project_dir / "targets").mkdir(exist_ok=True)

    # Add sample pipeline
    create_mock_pipeline_structure(project_dir, "sample_pipeline")

    # Add sample collection
    create_mock_collection_structure(project_dir, collection_name="sample_collection")

    # Add sample dataset
    create_mock_dataset_structure(project_dir, "sample_dataset")

    return project_dir
