"""Tests for marimba.core.wrappers.collection.CollectionWrapper."""

from pathlib import Path
from typing import Any

import pytest

from marimba.core.wrappers.collection import CollectionWrapper


@pytest.fixture
def collection_config() -> dict[str, Any]:
    return {"name": "test_collection", "version": "1.0.0", "extra": {"key": "value"}}


class TestCollectionWrapperCreate:
    """Cover CollectionWrapper.create and the surrounding lifecycle."""

    @pytest.mark.unit
    def test_create_builds_root_dir_and_config(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        root = tmp_path / "new_coll"

        wrapper = CollectionWrapper.create(root, collection_config)

        assert root.is_dir()
        assert wrapper.config_path.is_file()
        assert wrapper.root_dir == root

    @pytest.mark.unit
    def test_create_round_trips_config(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)

        loaded = wrapper.load_config()

        assert loaded == collection_config

    @pytest.mark.unit
    def test_create_raises_when_root_already_exists(
        self,
        tmp_path: Path,
        collection_config: dict[str, Any],
    ) -> None:
        existing = tmp_path / "existing"
        existing.mkdir()

        with pytest.raises(FileExistsError, match="already exists"):
            CollectionWrapper.create(existing, collection_config)

    @pytest.mark.unit
    def test_create_accepts_str_path(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        wrapper = CollectionWrapper.create(str(tmp_path / "as_str"), collection_config)

        assert isinstance(wrapper.root_dir, Path)
        assert wrapper.root_dir.is_dir()


class TestCollectionWrapperLoad:
    """Cover the __init__ structure validation path."""

    @pytest.mark.unit
    def test_load_existing_collection(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        created = CollectionWrapper.create(tmp_path / "c", collection_config)

        # Re-load by constructing a fresh wrapper pointing at the same dir.
        reloaded = CollectionWrapper(created.root_dir)

        assert reloaded.load_config() == collection_config

    @pytest.mark.unit
    def test_load_missing_root_raises_invalid_structure(self, tmp_path: Path) -> None:
        with pytest.raises(CollectionWrapper.InvalidStructureError, match="does not exist"):
            CollectionWrapper(tmp_path / "missing")

    @pytest.mark.unit
    def test_load_root_without_config_raises_invalid_structure(self, tmp_path: Path) -> None:
        root = tmp_path / "no_config"
        root.mkdir()

        with pytest.raises(CollectionWrapper.InvalidStructureError, match="collection.yml"):
            CollectionWrapper(root)


class TestCollectionWrapperConfig:
    """Cover save_config and the round-trip semantics."""

    @pytest.mark.unit
    def test_save_config_overwrites_existing(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)
        new_config = {"name": "renamed", "version": "2.0.0"}

        wrapper.save_config(new_config)

        assert wrapper.load_config() == new_config


class TestCollectionWrapperPipelineDataDirs:
    """Cover the pipeline-data-dir API."""

    @pytest.mark.unit
    def test_create_pipeline_data_dir(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)

        data_dir = wrapper.create_pipeline_data_dir("pipeline_one")

        assert data_dir.is_dir()
        assert data_dir == wrapper.root_dir / "pipeline_one"

    @pytest.mark.unit
    def test_create_pipeline_data_dir_duplicate_raises(
        self,
        tmp_path: Path,
        collection_config: dict[str, Any],
    ) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)
        wrapper.create_pipeline_data_dir("p")

        with pytest.raises(FileExistsError, match="already exists"):
            wrapper.create_pipeline_data_dir("p")

    @pytest.mark.unit
    def test_get_pipeline_data_dir(self, tmp_path: Path, collection_config: dict[str, Any]) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)
        wrapper.create_pipeline_data_dir("p")

        got = wrapper.get_pipeline_data_dir("p")

        assert got.is_dir()
        assert got == wrapper.root_dir / "p"

    @pytest.mark.unit
    def test_get_pipeline_data_dir_missing_raises(
        self,
        tmp_path: Path,
        collection_config: dict[str, Any],
    ) -> None:
        wrapper = CollectionWrapper.create(tmp_path / "c", collection_config)

        with pytest.raises(CollectionWrapper.NoSuchPipelineError, match="does not exist"):
            wrapper.get_pipeline_data_dir("never_created")
