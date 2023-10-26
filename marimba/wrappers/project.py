import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from marimba.core.base_instrument import BaseInstrument
from marimba.utils.log import LogMixin, get_file_handler, get_logger
from marimba.wrappers.deployment import DeploymentWrapper
from marimba.wrappers.instrument import InstrumentWrapper

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


class ProjectWrapper(LogMixin):
    """
    MarImBA project directory wrapper. Provides methods for interacting with the project.

    To create a new project, use the `create` method:
    ```python
    project_wrapper = ProjectWrapper.create("my_project")
    ```

    To wrap an existing project, use the constructor:
    ```python
    project_wrapper = ProjectWrapper("my_project")
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
        self._marimba_dir = self._root_dir / ".marimba"

        self._instrument_wrappers = {}  # instrument name -> InstrumentWrapper instance
        self._deployment_wrappers = {}  # deployment name -> DeploymentWrapper instance

        self._check_file_structure()
        self._setup_logging()

        self._load_instruments()
        self._load_deployments()

    @classmethod
    def create(cls, root_dir: Union[str, Path]) -> "ProjectWrapper":
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
        marimba_dir = root_dir / ".marimba"

        # Check that the root directory doesn't already exist
        if root_dir.exists():
            raise FileExistsError(f'"{root_dir}" already exists.')

        # Create the folder structure
        root_dir.mkdir(parents=True)
        instruments_dir.mkdir()
        deployments_dir.mkdir()
        marimba_dir.mkdir()

        return cls(root_dir)

    def _check_file_structure(self):
        """
        Check that the project file structure is valid. If not, raise an InvalidStructureError with details.

        Raises:
            Project.InvalidStructureError: If the project file structure is invalid.
        """

        def check_dir_exists(path: Path):
            if not path.is_dir():
                raise ProjectWrapper.InvalidStructureError(f'"{path}" does not exist or is not a directory.')

        check_dir_exists(self._root_dir)
        check_dir_exists(self._instruments_dir)
        check_dir_exists(self._deployments_dir)
        check_dir_exists(self._marimba_dir)

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
        Load instrument wrappers from the `instruments` directory.

        Populates the `_instrument_wrappers` dictionary with `InstrumentWrapper` instances.

        Raises:
            InstrumentWrapper.InvalidStructureError: If the instrument directory structure is invalid.
        """
        instrument_dirs = filter(lambda p: p.is_dir(), self._instruments_dir.iterdir())

        self._instrument_wrappers.clear()
        for instrument_dir in instrument_dirs:
            self._instrument_wrappers[instrument_dir.name] = InstrumentWrapper(instrument_dir)

    def _load_deployments(self):
        """
        Load deployment instances from the `deployments` directory.

        Populates the `_deployment_wrappers` dictionary with `DeploymentWrapper` instances.

        Raises:
            Deployment.InvalidStructureError: If the deployment directory structure is invalid.
        """

        deployment_dirs = filter(lambda p: p.is_dir(), self._deployments_dir.iterdir())

        self._deployment_wrappers.clear()
        for deployment_dir in deployment_dirs:
            self._deployment_wrappers[deployment_dir.name] = DeploymentWrapper(deployment_dir)

    def create_instrument(self, name: str, url: str) -> InstrumentWrapper:
        """
        Create a new instrument.

        Args:
            name: The name of the instrument.
            url: URL of the instrument git repository.

        Returns:
            The instrument directory wrapper.

        Raises:
            Project.CreateInstrumentError: If the instrument cannot be created.
        """
        self.logger.debug(f'Creating instrument "{name}" from {url}')

        # Check that an instrument with the same name doesn't already exist
        instrument_dir = self._instruments_dir / name
        if instrument_dir.exists():
            raise ProjectWrapper.CreateInstrumentError(f'An instrument with the name "{name}" already exists.')

        # Create the instrument directory
        instrument_wrapper = InstrumentWrapper.create(instrument_dir, url)

        # Reload the instruments
        # TODO: Do we need to do this every time?
        self._load_instruments()

        return instrument_wrapper

    def create_deployment(self, name: str, parent: Optional[str] = None) -> DeploymentWrapper:
        """
        Create a new deployment.

        Args:
            name: The name of the deployment.
            parent: The name of the parent deployment.

        Returns:
            The deployment directory wrapper.

        Raises:
            Project.CreateDeploymentError: If the deployment cannot be created.
        """
        self.logger.debug(f'Creating deployment "{name}"')

        # Check that a deployment with the same name doesn't already exist
        deployment_dir = self.deployments_dir / name
        if deployment_dir.exists():
            raise ProjectWrapper.CreateDeploymentError(f'A deployment with the name "{name}" already exists.')
        if parent is not None and parent not in self._deployment_wrappers:
            raise ProjectWrapper.CreateDeploymentError(f'The parent deployment "{parent}" does not exist.')

        if parent is None:
            # TODO: Assign parent to the last deployment, if there is one
            pass

        # TODO: Use the parent deployment to populate the default deployment config

        # Create the deployment directory
        deployment_wrapper = DeploymentWrapper.create(deployment_dir, {})

        # Create the per-instrument directories
        for instrument_name in self._instrument_wrappers:
            # TODO: Direct this from the instrument implementation?
            deployment_wrapper.get_instrument_data_dir(instrument_name).mkdir()

        # Reload the deployments
        # TODO: Do we need to do this every time?
        self._load_deployments()

        return deployment_wrapper

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
            instrument_wrapper = self._instrument_wrappers.get(instrument_name, None)
            if instrument_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Instrument "{instrument_name}" does not exist within the project.')

        if deployment_name is not None:
            deployment_wrapper = self._deployment_wrappers.get(deployment_name, None)
            if deployment_wrapper is None:
                raise ProjectWrapper.RunCommandError(f'Deployment "{deployment_name}" does not exist within the project.')

        # Select the instruments and deployments to run the command for
        instrument_wrappers_to_run = {instrument_name: instrument_wrapper} if instrument_name is not None else self._instrument_wrappers
        deployment_wrappers_to_run = {deployment_name: deployment_wrapper} if deployment_name is not None else self._deployment_wrappers

        # Load instrument instances
        instruments_to_run = {
            instrument_name: instrument_wrapper.load_instrument() for instrument_name, instrument_wrapper in instrument_wrappers_to_run.items()
        }

        # Check that the command exists for all instruments
        for run_instrument_name, run_instrument in instruments_to_run.items():
            if not hasattr(run_instrument, command_name):
                raise ProjectWrapper.RunCommandError(f'Command "{command_name}" does not exist for instrument "{run_instrument_name}".')

        # Invoke the command for each instrument and deployment
        for _, run_deployment_wrapper in deployment_wrappers_to_run.items():
            for run_instrument_name, run_instrument in instruments_to_run.items():
                # Get the instrument-specific data directory and config
                instrument_deployment_data_dir = run_deployment_wrapper.get_instrument_data_dir(run_instrument_name)
                instrument_deployment_config = run_deployment_wrapper.load_config()

                method = getattr(run_instrument, command_name)
                method(instrument_deployment_data_dir, instrument_deployment_config, **merged_kwargs)

    @property
    def instrument_wrappers(self) -> Dict[str, InstrumentWrapper]:
        """
        The loaded instrument wrappers in the project.
        """
        return self._instrument_wrappers

    @property
    def deployment_wrappers(self) -> Dict[str, DeploymentWrapper]:
        """
        The loaded deployment wrappers in the project.
        """
        return self._deployment_wrappers

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
    def marimba_dir(self) -> Path:
        """
        The MarImBA directory of the project.
        """
        return self._marimba_dir

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
        return self._instrument_wrappers

    @property
    def deployments(self) -> Dict[str, DeploymentWrapper]:
        """
        The loaded deployments in the project.
        """
        return self._deployment_wrappers
