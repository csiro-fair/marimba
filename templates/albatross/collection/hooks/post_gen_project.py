import os
import fnmatch

def remove_gitkeep_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in fnmatch.filter(files, '.gitkeep'):
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                # print(f"Removed: {file_path}")
            except OSError as e:
                print(f"Error removing {file_path}: {e}")

# Remove all .gitkeep files
remove_gitkeep_files(os.getcwd())
