from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

from ifdo.models import ImageData

from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.ifdo import iFDOMetadata


def test_create_dataset_metadata():
    mock_uuid = "a43a84f2-b657-44e0-bafe-72e2624115fa"

    def mock_saver(path: Path, output_name: str, data: dict[str, Any]) -> None:
        assert path.name == "tmp"
        assert output_name == "ifdo"
        assert data == {
            "image-set-header": {
                "image-set-name": "TestDataSet",
                "image-set-uuid": mock_uuid,
                "image-set-handle": "",
                "image-set-ifdo-version": "v2.1.0",
            },
            "image-set-items": {"image.jpg": {"image-altitude-meters": 0.0}},
        }

    data_setname = "TestDataSet"
    root_dir = Path("/tmp")
    items = {"image.jpg": [cast(BaseMetadata, iFDOMetadata(image_data=ImageData(image_altitude_meters=0.0)))]}
    with patch("uuid.uuid4", MagicMock(return_value=mock_uuid)):
        iFDOMetadata.create_dataset_metadata(data_setname, root_dir, items, saver_overwrite=mock_saver)
