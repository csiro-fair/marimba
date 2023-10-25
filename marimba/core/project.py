import logging
from datetime import datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Dict, List, Optional, Union

from cookiecutter.exceptions import OutputDirExistsException
from cookiecutter.main import cookiecutter

from marimba.core.base_instrument import BaseInstrument
from marimba.core.deployment import Deployment
from marimba.utils.config import load_config
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


def get_merged_keyword_args(kwargs: dict, extra_args: list, logger: logging.Logger) -> dict:
    """
    Merge any extra key-value arguments with other keyword arguments.

    Args:
        kwargs: The keyword arguments to merge with.
        extra_args: A list of extra key-value arguments to merge.
        logger: A logger object to log any warnings.

    Returns:
        A dictionary containing the merged keyword arguments.
    """
    extra_dict = {}
    if extra_args:
        for arg in extra_args:
            # Attempt to split the argument into a key and a value
            parts = arg.split("=")
            if len(parts) == 2:
                key, value = parts
                extra_dict[key] = value
            else:
                logger.warning(f'Invalid extra argument provided: "{arg}"')

    return {**kwargs, **extra_dict}


class Project(LogMixin):
    """
    MarImBA project class. Wraps a project directory and provides methods for interacting with the project.

    To create a new project, use the `create` method:
    ```python
    project = Project.create("my_project")
    ```

    To wrap an existing project, use the constructor:
    ```python
    project = Project("my_project")
    ```
    """

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

    class RunCommandError(Exception):
        """
        Raised when a command cannot be run.
        """

        pass

    def __init__(self, root_dir: Union[str, Path]):
        super().__init__()

        self._root_dir = Path(root_dir)

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
        self._load_deployments()

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
        file_handler = get_file_handler(self.root_dir, self.name, False, level=logging.DEBUG)

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
        self.logger.debug(f"Loading instruments from {self._instruments_dir}...")

        instrument_dirs = filter(lambda p: p.is_dir(), self._instruments_dir.iterdir())

        for instrument_dir in instrument_dirs:
            if not instrument_dir.is_dir():  # TODO: Log/raise
                continue

            instrument_module_path = instrument_dir / "instrument.py"
            if not instrument_module_path.is_file():  # TODO: Log/raise
                continue

            instrument_name = instrument_module_path.parent.name

            # Get the instrument module spec
            instrument_module_spec = spec_from_file_location("instrument", str(instrument_module_path.absolute()))

            # Load the instrument module
            instrument_module = module_from_spec(instrument_module_spec)
            instrument_module_spec.loader.exec_module(instrument_module)

            # Find any Instrument subclasses
            for _, obj in instrument_module.__dict__.items():
                if isinstance(obj, type) and issubclass(obj, BaseInstrument) and obj is not BaseInstrument:
                    # Read the instrument config
                    instrument_config = load_config(instrument_dir / "instrument.yml")

                    # Create an instance of the instrument
                    instrument_instance = obj(instrument_config, dry_run=False)

                    # Set up instrument file logging
                    file_handler = get_file_handler(instrument_dir, instrument_name, False, level=logging.DEBUG)
                    instrument_instance.logger.addHandler(file_handler)

                    # Add the instrument
                    self._instruments[instrument_name] = instrument_instance
                    break

    def _load_deployments(self):
        """
        Load deployment instances from the `deployments` directory.

        Raises:
            Deployment.InvalidStructureError: If the deployment directory structure is invalid.
        """
        self.logger.debug(f"Loading deployments from {self._deployments_dir}...")

        deployment_dirs = filter(lambda p: p.is_dir(), self._deployments_dir.iterdir())

        for deployment_dir in deployment_dirs:
            # Wrap the deployment directory
            deployment = Deployment(deployment_dir)

            # Add the deployment
            self._deployments[deployment_dir.name] = deployment

    def create_instrument(self, name: str, template_name: str):
        """
        Create a new instrument.

        Args:
            name: The name of the instrument.
            template_name: The name of the template to use.

        Raises:
            Project.CreateInstrumentError: If the instrument cannot be created.
        """
        self.logger.debug(f'Creating instrument "{name}" from template "{template_name}"...')

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
        self.logger.debug(f'Creating deployment "{name}"...')

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

        # Create the deployment
        deployment = Deployment.create(deployment_dir, {})

        # Create the per-instrument directories
        for instrument_name, instrument_instance in self._instruments.items():
            # TODO: Direct this from the instrument implementation?
            deployment.get_instrument_data_dir(instrument_name).mkdir()

        # Reload the deployments
        self._load_deployments()

    def run_command(
        self,
        command_name: str,
        instrument_name: Optional[str] = None,
        deployment_name: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        **kwargs: dict,
    ):
        """
        Run a command within the project.

        By default, this will run the command for all instruments and deployments in the project.
        If an instrument name is provided, it will run the command for all deployments of that instrument.
        If a deployment name is provided, it will run the command for that deployment only.
        These can be combined to run the command for a specific deployment of a specific instrument.

        Args:
            command_name: The name of the command to run.
            instrument_name: The name of the instrument to run the command for.
            deployment_name: The name of the deployment to run the command for.
            extra_args: Any extra arguments to pass to the command.
            kwargs: Any keyword arguments to pass to the command.

        Raises:
            Project.RunCommandError: If the command cannot be run.
        """
        merged_kwargs = get_merged_keyword_args(kwargs, extra_args, self.logger)

        if instrument_name is not None:
            instrument = self._instruments.get(instrument_name, None)
            if instrument is None:
                raise Project.RunCommandError(f'Instrument "{instrument_name}" does not exist within the project.')

        if deployment_name is not None:
            deployment = self._deployments.get(deployment_name, None)
            if deployment is None:
                raise Project.RunCommandError(f'Deployment "{deployment_name}" does not exist within the project.')

        # Select the instruments and deployments to run the command for
        instruments_to_run = {instrument_name: instrument} if instrument_name is not None else self._instruments
        deployments_to_run = {deployment_name: deployment} if deployment_name is not None else self._deployments

        # Check that the command exists for all instruments
        for run_instrument_name, run_instrument in instruments_to_run.items():
            if not hasattr(run_instrument, command_name):
                raise Project.RunCommandError(f'Command "{command_name}" does not exist for instrument "{run_instrument_name}".')

        # Invoke the command for each instrument and deployment
        for _, run_deployment in deployments_to_run.items():
            for run_instrument_name, run_instrument in instruments_to_run.items():
                # Get the instrument-specific data directory and config
                instrument_deployment_data_dir = run_deployment.get_instrument_data_dir(run_instrument_name)
                instrument_deployment_config = run_deployment.get_instrument_config(run_instrument_name)

                method = getattr(run_instrument, command_name)
                method(instrument_deployment_data_dir, instrument_deployment_config, **merged_kwargs)

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
    def name(self) -> str:
        """
        The name of the project.
        """
        return self._root_dir.name

    @property
    def instruments(self) -> Dict[str, BaseInstrument]:
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
