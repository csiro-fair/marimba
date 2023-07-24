import importlib.util
import os

from marimba.utils.collection import get_collection_config
from marimba.utils.instrument import get_instrument_config
from marimba.utils.log import setup_logging


def rename_command(
        collection_path: str,
        instrument_id: str,
        dry_run: bool,
):
    """
    Rename files in a directory.

    Args:
        collection_path: The path to the MarImBA collection containing files that will be renamed.
        instrument_id: MarImBA instrument containing files that will be renamed.
        dry_run: Whether to perform a dry run.
    """

    # Set up logging
    setup_logging(collection_path)

    # Get collection config data
    collection_config = get_collection_config(collection_path)

    # Define instruments path
    instruments_path = os.path.join(os.path.join(collection_path, "instruments"))

    # Traverse instruments in MarImBA collection
    for instrument in os.scandir(instruments_path):
        # Get instrument config data
        instrument_config = get_instrument_config(instrument.path)
        instrument_class_name = instrument_config.get("class_name")
        instrument_class_path = os.path.join(os.path.join(instrument.path, "lib", "instrument.py"))

        # Import and load instrument class
        instrument_spec = importlib.util.spec_from_file_location("instrument", instrument_class_path)
        instrument_module = importlib.util.module_from_spec(instrument_spec)
        instrument_spec.loader.exec_module(instrument_module)
        instrument_class = getattr(instrument_module, instrument_class_name)
        instrument_instance = instrument_class(instrument.path, collection_config, instrument_config)

        # Execute MarImBA instrument command
        instrument_instance.logger.info(f"Executing the MarImBA [bold]rename[/bold] command")
        instrument_instance.rename(dry_run)

