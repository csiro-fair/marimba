import logging
import os
from enum import Enum

import typer
from rich import print
from rich.panel import Panel

from marimba.utils.config import load_config, save_config
from marimba.utils.registry import Registry
from cookiecutter.main import cookiecutter
from pathlib import Path
from datetime import datetime


def create_tamplate(output_path, template_name):
    template_path = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, 'templates', template_name)
    print(template_name)
    cookiecutter(template=template_path, output_dir=output_path, extra_context={'datestamp': datetime.today().strftime("%Y-%m-%d")})
