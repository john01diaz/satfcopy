import os.path


def create_new_folder(
        folder_path: str) \
        -> None:
    if not os.path.exists(folder_path):
        os.makedirs(
            folder_path,
            exist_ok=True)
