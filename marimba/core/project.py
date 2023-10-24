from datetime import datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Dict, Optional, Union

from cookiecutter.exceptions import OutputDirExistsException
from cookiecutter.main import cookiecutter

from marimba.core.deployment import Deployment
from marimba.core.instrument import Instrument
from marimba.utils.log import LogMixin, get_file_handler, get_logger

logger = get_logger(__name__)


def get_base_templates_path() -> Path:
    base_templates_path = Path(__file__).parent.parent.parent / "templates"
    logger.info(f'Setting [bold][aquamarine3]MarImBA[/aquamarine3][/bold] base templates path to: "{base_templates_path}"')
    return base_templates_path


def check_template_exists(base_templates_path: Union[str, Path], template_name: str, template_type: str) -> Path:
    base_templates_path = Path(base_templates_path)
    logger.info(
        f"Checking that the provided [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]{template_type}[/light_pink3] template exists..."
    )
    template_path = base_templates_path / template_name / template_type

    if template_path.is_dir():
        logger.info(
            f"[bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]{template_type}[/light_pink3] template [orchid1]{Path(template_name) / template_type}[/orchid1] exists!"
        )
    else:
        error_message = f"The provided [light_pink3]{template_type}[/light_pink3] template name [orchid1]{Path(template_name) / template_type}[/orchid1] does not exists at {template_path}"
        logger.error(error_message)
        # print(
        #     Panel(
        #         error_message,
        #         title="Error",
        #         title_align="left",
        #         border_style="red",
        #     )
        # )
        # raise typer.Exit()

    return template_path


def check_output_path_exists(output_path: Union[str, Path], command: str):
    output_path = Path(output_path)
    logger.info(
        f"Checking that the provided [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]{command}[/light_pink3] output path exists..."
    )

    if output_path.is_dir():
        logger.info(f'[bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]{command}[/light_pink3] output path "{output_path}" exists!')
    else:
        error_message = f'The provided [bold][aquamarine3]MarImBA[/aquamarine3][/bold] [light_pink3]{command}[/light_pink3] output path "{output_path}" does not exists.'
        logger.error(error_message)
        # print(
        #     Panel(
        #         error_message,
        #         title="Error",
        #         title_align="left",
        #         border_style="red",
        #     )
        # )
        # raise typer.Exit()


class Project(LogMixin):
    class InvalidStructureError(Exception):
        """
        Raised when the project file structure is invalid.
        """

        pass

    class CreateInstrumentError(Exception):
        """
        Raised when an instrument cannot be created.
        """

        pass

    class CreateDeploymentError(Exception):
        """
        Raised when a deployment cannot be created.
        """

        pass

    def __init__(self, root_dir: Path):
        super().__init__()

        self._root_dir = root_dir

        self._instruments_dir = self._root_dir / "instruments"
        self._deployments_dir = self._root_dir / "deployments"

        self._instruments = {}  # instrument name -> instance
        self._deployments = {}  # deployment name -> instance

        self._check_file_structure()
        self._setup_logging()
        try:
            self._load_instruments()
        except ImportError as e:
            self.logger.error(f"Failed to load instrument: {e}")

    @classmethod
    def create(cls, root_dir: Union[str, Path]) -> "Project":
        """
        Create a project from a root directory.

        Args:
            root_dir: The root directory of the project.

        Returns:
            A project.

        Raises:
            FileExistsError: If the root directory already exists.
        """
        # Define the folder structure
        root_dir = Path(root_dir)
        instruments_dir = root_dir / "instruments"
        deployments_dir = root_dir / "deployments"

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'"{root_dir}" already exists.')

        # Create the folder structure
        root_dir.mkdir(parents=True)
        instruments_dir.mkdir()
        deployments_dir.mkdir()

        return cls(root_dir)

    def _check_file_structure(self):
        """
        Check that the project file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            Project.InvalidStructureError: If the project file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise Project.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self._root_dir)
        check_dir_exists(self._instruments_dir)
        check_dir_exists(self._deployments_dir)

    def _setup_logging(self):
        """
        Set up logging. Create file handler for this instance that writes to `project.log`.
        """
        # Create a file handler for this instance
        file_handler = get_file_handler(self.root_dir, self.root_dir.name, False)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def _load_instruments(self):
        """
        Load instrument instances from the `instruments` directory.

        Dynamically loads all modules named `instrument` in the `instruments` subdirectories and finds any Instrument subclasses.
        Populates the `_instruments` map with the instrument name (subdirectory name) as the key and the instrument instance as the value.

        Raises:
            ImportError: If an instrument module cannot be imported.
        """
        instrument_dirs = filter(lambda p: p.is_dir(), self._instruments_dir.iterdir())
        instrument_module_paths = map(lambda p: p / "instrument.py", instrument_dirs)
        instrument_module_paths = filter(lambda p: p.is_file(), instrument_module_paths)

        for instrument_module_path in instrument_module_paths:
            instrument_name = instrument_module_path.parent.name

            # Get the instrument module spec
            instrument_module_spec = spec_from_file_location("instrument", str(instrument_module_path.absolute()))

            # Load the instrument module
            instrument_module = module_from_spec(instrument_module_spec)
            instrument_module_spec.loader.exec_module(instrument_module)

            # Find any Instrument subclasses
            for _, obj in instrument_module.__dict__.items():
                if isinstance(obj, type) and issubclass(obj, Instrument) and obj is not Instrument:
                    # Create an instance of the instrument
                    instrument_instance = obj()

                    # Add the instrument
                    self._instruments[instrument_name] = instrument_instance
                    break

    def _load_deployments(self):
        """
        Load deployment instances from the `deployments` directory.
        """
        pass

    def create_instrument(self, name: str, template_name: str):
        """
        Create a new instrument.

        Args:
            name: The name of the instrument.
            template_name: The name of the template to use.

        Raises:
            Project.CreateInstrumentError: If the instrument cannot be created.
        """
        # Check that an instrument with the same name doesn't already exist
        instrument_dir = self._instruments_dir / name
        if instrument_dir.exists():
            raise Project.CreateInstrumentError(f'An instrument with the name "{name}" already exists.')

        # Get base template path and check that it exists
        base_templates_path = get_base_templates_path()
        template_path = check_template_exists(base_templates_path, template_name, "instrument")

        # Create the instrument directory
        instrument_dir.mkdir()

        # Run cookiecutter
        try:
            cookiecutter(
                template=str(template_path.absolute()),
                output_dir=str(instrument_dir.absolute()),
                extra_context={"datestamp": datetime.today().strftime("%Y-%m-%d")},
            )
        except OutputDirExistsException as e:
            raise Project.CreateInstrumentError(f'An instrument with the name "{name}" already exists.') from e
        except Exception as e:
            raise Project.CreateInstrumentError(f'Failed to create instrument "{name}": {e}') from e

        # Reload the instruments
        self._load_instruments()

    def create_deployment(self, name: str, parent: Optional[str] = None):
        """
        Create a new deployment.

        Args:
            name: The name of the deployment.
            parent: The name of the parent deployment.

        Raises:
            Project.CreateDeploymentError: If the deployment cannot be created.
        """
        # Check that a deployment with the same name doesn't already exist
        deployment_dir = self.deployments_dir / name
        if deployment_dir.exists():
            raise Project.CreateDeploymentError(f'A deployment with the name "{name}" already exists.')
        if parent is not None and parent not in self._deployments:
            raise Project.CreateDeploymentError(f'The parent deployment "{parent}" does not exist.')

        if parent is None:
            # TODO: Assign parent to the last deployment, if there is one
            pass

        # TODO: Use the parent deployment to populate the default deployment config

        # Create the deployment directory
        deployment_dir.mkdir()

        # Create the per-instrument directories
        for instrument_name, instrument_instance in self._instruments.items():
            # TODO: Direct this from the instrument implementation
            instrument_data_dir = deployment_dir / instrument_name
            instrument_data_dir.mkdir()

        # Reload the deployments
        self._load_deployments()

    @property
    def root_dir(self) -> Path:
        """
        The root directory of the project.
        """
        return self._root_dir

    @property
    def instruments_dir(self) -> Path:
        """
        The instruments directory of the project.
        """
        return self._instruments_dir

    @property
    def deployments_dir(self) -> Path:
        """
        The deployments directory of the project.
        """
        return self._deployments_dir

    @property
    def instruments(self) -> Dict[str, Instrument]:
        """
        The loaded instruments in the project.
        """
        return self._instruments

    @property
    def deployments(self) -> Dict[str, Deployment]:
        """
        The loaded deployments in the project.
        """
        return self._deployments
