import os
import fnmatch
from pathlib import Path


def remove_gitkeep_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in fnmatch.filter(files, '.gitkeep'):
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                # print(f"Removed: {file_path}")
            except OSError as e:
                print(f"Error removing {file_path}: {e}")

selected_instrument_id = "{{cookiecutter.instrument_id}}"

remove_list = ["C600D", "C700D"]
remove_list.remove(selected_instrument_id)

instrument_path = Path(os.getcwd())
lib_dir = instrument_path / "lib"

# Remove all instrument files not selected
for path in remove_list:
    instrument_yml = instrument_path / f"{path}.yml"
    instrument_yml.unlink(missing_ok=False)

# Rename selected yaml file to "instrument.yml"
instrument_yml = instrument_path / f"{selected_instrument_id}.yml"
instrument_yml.rename("instrument.yml")

# Remove all .gitkeep files
remove_gitkeep_files(instrument_path)
