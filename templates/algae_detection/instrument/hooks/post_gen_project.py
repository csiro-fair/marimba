import os
from pathlib import Path

selected_instrument_id = "{{cookiecutter.instrument_id}}"

remove_list = ["ZAO", "ZAP"]
remove_list.remove(selected_instrument_id)

instrument_path = Path(os.getcwd())
lib_dir = instrument_path / "lib"

# Remove all instrument files not selected
for path in remove_list:
    instrument_file = lib_dir / f"{path}.py"
    instrument_yml = instrument_path / f"{path}.yml"
    instrument_file.unlink(missing_ok=False)
    instrument_yml.unlink(missing_ok=False)

# Rename selected instrument file to "instrument.py"
instrument_file = lib_dir / f"{selected_instrument_id}.py"
instrument_file.rename(lib_dir / "instrument.py")

instrument_yml = instrument_path / f"{selected_instrument_id}.yml"
instrument_yml.rename("instrument.yml")
