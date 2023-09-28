import os
import shutil


def copy_files_with_same_name(
    file: str, source_dir: str, dest_dir: str, available_files: set[str]
):
    base_name_without_ext = os.path.splitext(file)[0]
    for available_file in available_files:
        if available_file.startswith(base_name_without_ext):
            file_path = os.path.join(source_dir, available_file)
            shutil.copy2(file_path, os.path.join(dest_dir, available_file))
            print(f"Copied {available_file}")
