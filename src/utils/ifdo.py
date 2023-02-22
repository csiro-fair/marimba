import yaml

def load_ifdo(ifdo_path) -> dict:

    with open(ifdo_path) as f:
        ifdo_data = yaml.safe_load(f)

    return ifdo_data
