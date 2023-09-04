import importlib.util
import os

from marimba.core.collection import get_collection_config
from marimba.core.instrument import get_instrument_config
from marimba.utils.log import setup_logging, get_collection_logger


def run_command(command_name: str, collection_path: str, instrument_id: str, extra_args: list[str], **kwargs):
    """
    Traverse the instrument directory and execute the command for each instrument

    Args:
        command_name: The method to be executed on the instrument instance.
        collection_path: The path to the MarImBA collection containing files that will be processed.
        instrument_id: MarImBA instrument containing files that will be processed.
        **kwargs: Additional keyword arguments for the method.
    """

    # Set up logging
    setup_logging(collection_path)
    logger = get_collection_logger()

    # Get collection config data
    collection_config = get_collection_config(collection_path)

    # Define instruments path
    instruments_path = os.path.join(collection_path, "instruments")

    # TODO: Implement selective instrument targeting if instrument_id is provided

    # Traverse instruments in MarImBA collection
    for instrument in os.scandir(instruments_path):
        # Get instrument config data
        instrument_config = get_instrument_config(instrument.path)
        instrument_class_name = instrument_config.get("class_name")
        instrument_class_path = os.path.join(instrument.path, "lib", "instrument.py")

        # Import and load instrument class
        instrument_spec = importlib.util.spec_from_file_location("instrument", instrument_class_path)
        instrument_module = importlib.util.module_from_spec(instrument_spec)
        instrument_spec.loader.exec_module(instrument_module)
        instrument_class = getattr(instrument_module, instrument_class_name)
        instrument_instance = instrument_class(instrument.path, collection_config, instrument_config)

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
        merged_kwargs = {**kwargs, **extra_dict}

        # Execute MarImBA instrument command
        instrument_instance.logger.info(f"Executing the MarImBA [bold]{command_name}[/bold] command")
        command = getattr(instrument_instance, command_name)
        command(**merged_kwargs)
