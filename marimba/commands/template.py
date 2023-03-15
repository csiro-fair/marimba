import os
from datetime import datetime
from pathlib import Path

from cookiecutter.main import cookiecutter

from marimba.utils.log import get_collection_logger

logger = get_collection_logger()


def create_tamplate(output_path, template_name):
    template_path = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, "templates", template_name)
    logger.debug(f"Template name: {template_name}")
    cookiecutter(template=template_path, output_dir=output_path, extra_context={"datestamp": datetime.today().strftime("%Y-%m-%d")})
