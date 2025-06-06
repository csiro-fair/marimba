from datetime import datetime
from ifdo import iFDO
from ifdo.models import (
    ImageLicense,
    ImageContext,
    ImagePI,
    ImageCreator,
    ImageSetHeader,
)
from marimba.core.validators import iFDOValidator


def test_invalid_ifdo() -> None:
    validator = iFDOValidator.create()
    ifdo = iFDO(
        image_set_header=ImageSetHeader(
            image_set_name="SO268 SO268-1_21-1_OFOS SO_CAM-1_Photo_OFOS",
            image_set_uuid="f840644a-fe4a-46a7-9791-e32c211bcbf5",
            image_set_handle="https://hdl.handle.net/20.500.12085/f840644a-fe4a-46a7-9791-e32c211bcbf5",
        ),
        image_set_items={},
    )

    assert not validator(ifdo)


def test_valid_ifdo() -> None:
    validator = iFDOValidator.create()
    ifdo = iFDO(
        image_set_header=ImageSetHeader(
            image_set_name="SO268 SO268-1_21-1_OFOS SO_CAM-1_Photo_OFOS",
            image_set_uuid="f840644a-fe4a-46a7-9791-e32c211bcbf5",
            image_set_handle="https://hdl.handle.net/20.500.12085/f840644a-fe4a-46a7-9791-e32c211bcbf5",
        ),
        image_set_items={},
    )

    ifdo.image_set_header.image_abstract = "Abstract"
    ifdo.image_set_header.image_copyright = "Copyright (C)"
    ifdo.image_set_header.image_license = ImageLicense("CC-BY")
    ifdo.image_set_header.image_context = ImageContext("Image context")
    ifdo.image_set_header.image_project = ImageContext("Image project")
    ifdo.image_set_header.image_event = ImageContext("Image event")
    ifdo.image_set_header.image_platform = ImageContext("Image Platform")
    ifdo.image_set_header.image_sensor = ImageContext("Image sensor")
    ifdo.image_set_header.image_pi = ImagePI("Image PI")
    ifdo.image_set_header.image_creators = [ImageCreator("Image creator")]
    ifdo.image_set_header.image_latitude = 10.0
    ifdo.image_set_header.image_longitude = 10.0
    ifdo.image_set_header.image_altitude_meters = 1.0
    ifdo.image_set_header.image_coordinate_reference_system = "WSG84"
    ifdo.image_set_header.image_coordinate_uncertainty_meters = 0.1
    ifdo.image_set_header.image_datetime = datetime(2020, 1, 1)

    assert validator(ifdo)
