import yaml


def load_config(config_path) -> dict:
    """
    Load a YAML config file.

    Args:
        config_path: The path to the config file.

    Returns:
        The config data as a dictionary.
    """
    with open(config_path) as f:
        config_data = yaml.safe_load(f)

    return config_data


def save_config(config_path: str, config_data: dict):
    """
    Save a YAML config file.

    Args:
        config_path: The path to the config file.
        config_data: The config data as a dictionary.
    """
    with open(config_path, "w") as f:
        yaml.safe_dump(config_data, f)
