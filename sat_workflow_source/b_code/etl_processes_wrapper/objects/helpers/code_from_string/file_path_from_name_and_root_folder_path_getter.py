import os


def get_file_path_from_file_name_and_root_folder_path(
        filename_to_find: str,
        root_folder_path: str):
    for path, subdirs, filenames in os.walk(root_folder_path):
        for filename in filenames:
            if filename_to_find == filename:
                file_path = \
                    os.path.join(
                        path,
                        filename)

                return \
                    file_path
