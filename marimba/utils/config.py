import yaml

def load_config(config_path) -> dict:

    with open(config_path) as f:
        config_data = yaml.safe_load(f)

    return config_data


def save_config(config_path: str, config_data: dict):
    
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_data, f)
