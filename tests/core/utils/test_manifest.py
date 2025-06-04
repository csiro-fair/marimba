from pathlib import Path

from marimba.core.utils.manifest import Manifest


def test_get_subdirectories() -> None:
    base_dir = Path("tmp")
    files = {base_dir / "data" / "event" / "image.jpg", base_dir / "data" / "event" / "another.jpg"}
    sub_directories = Manifest._get_sub_directories(files, base_dir)

    assert sub_directories == {base_dir / "data", base_dir / "data" / "event"}
