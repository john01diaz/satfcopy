import os


def get_parquet_table_path(
        file_configuration: list) \
        -> str:
    user_specific_absolute_path_to_relative_path = \
        file_configuration[0]

    relative_path = \
        file_configuration[2]

    file_name_folder = \
        file_configuration[3]

    print(
        'Running on: ' + str(relative_path) + ' - ' + str(file_name_folder))

    parquet_folder_path = \
        os.path.join(
            user_specific_absolute_path_to_relative_path,
            relative_path,
            file_name_folder)

    return \
        parquet_folder_path
