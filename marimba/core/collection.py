import importlib.util
import os
from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

from marimba.core.instrument import get_instrument_config, Instrument
from marimba.utils.config import load_config
from marimba.utils.log import setup_logging, get_collection_logger


def get_collection_config(collection_path) -> dict:
    """
    Return the collection config as a dictionary.

    Args:
        collection_path: The path to the MarImBA collection.

    Returns:
        The collection config data as a dictionary.
    """

    # Check that this is a valid MarImBA collection and
    if not os.path.isdir(collection_path):
        print(Panel(f"MarImBA collection path does not exist.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    collection_config_path = Path(collection_path) / "collection.yml"

    if not os.path.isfile(collection_config_path):
        print(Panel(f"Cannot find collection.yml in MarImBa collection - this is not a MarImBA collection.", title="Error", title_align="left", border_style="red"))
        raise typer.Exit()

    return load_config(collection_config_path)


def get_instrument_instance(collection_config, instrument_path) -> Instrument:
    # Get instrument config data
    instrument_config = get_instrument_config(instrument_path)
    instrument_class_name = instrument_config.get("class_name")
    instrument_class_path = Path(instrument_path) / "lib" / "instrument.py"

    # Import and load instrument class
    instrument_spec = importlib.util.spec_from_file_location("instrument", instrument_class_path)
    instrument_module = importlib.util.module_from_spec(instrument_spec)
    instrument_spec.loader.exec_module(instrument_module)
    instrument_class = getattr(instrument_module, instrument_class_name)
    instrument_instance = instrument_class(instrument_path, collection_config, instrument_config)

    return instrument_instance


def get_merged_keyword_args(kwargs, extra_args, logger) -> dict:
    # Process any extra key-value arguments that were provided and merge with other keyword arguments
    extra_dict = {}
    if extra_args:
        for arg in extra_args:
            # Attempt to split the argument into a key and a value
            parts = arg.split('=')
            if len(parts) == 2:
                key, value = parts
                extra_dict[key] = value
            else:
                logger.warning(f'Invalid extra argument provided: "{arg}"')

    return {**kwargs, **extra_dict}


def run_command(command_name: str, collection_path: str, instrument_id: str, deployment_name: str, extra_args: list[str], **kwargs):
    """
    Traverse the instrument directory and execute deployment-level processing for each instrument

    Args:
        command_name: Name of the MarImBA command to be executed.
        collection_path: The path to the MarImBA collection containing deployments that will be processed.
        instrument_id: MarImBA instrument containing files that will be processed.
        deployment_name: Name of the MarImBA deployment that will be processed.
        extra_args: Additional non-MarImBA keyword arguments to be passed through to command implementations.
        **kwargs: Additional MarImBA keyword arguments.
    """

    # Set up logging
    setup_logging(collection_path, kwargs.get("dry_run"))
    logger = get_collection_logger()

    # Get collection config data
    collection_config = get_collection_config(collection_path)

    # Define instruments path and get merged keyword arguments
    instruments_path = Path(collection_path) / "instruments"
    merged_kwargs = get_merged_keyword_args(kwargs, extra_args, logger)

    # Single deployment processing
    if instrument_id or deployment_name:
        instrument_path = Path(instruments_path) / instrument_id
        instrument_instance = get_instrument_instance(collection_config, instrument_path)

        if deployment_name:
            deployment_path = str(instrument_path / "work" / deployment_name)
            instrument_instance.logger.info(f"Executing the MarImBA [bold]{command_name}[/bold] command for deployment {deployment_name}...")
            instrument_instance.process_single_deployment(deployment_path, command_name, merged_kwargs)

        else:
            instrument_instance.logger.info(f"Executing the MarImBA [bold]{command_name}[/bold] command for instrument {instrument_id}...")
            instrument_instance.process_all_deployments(command_name, merged_kwargs)

    # Collection-level multi-instrument and multi-deployment processing
    else:
        # Traverse instruments in MarImBA collection
        for instrument in os.scandir(instruments_path):
            instrument_instance = get_instrument_instance(collection_config, instrument.path)
            instrument_instance.logger.info(f"Executing the MarImBA [bold]{command_name}[/bold] command for the collection")
            instrument_instance.process_all_deployments(command_name, merged_kwargs)
