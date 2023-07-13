import os
from sat_parquet_source.parquet_common.table_getters.parquet_table_as_delta_table_using_delta_lake_getter import \
    get_parquet_table_as_delta_table_using_delta_lake


def get_parquet_table_as_delta_table(
        file_configuration: list) \
        -> tuple:
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

    parquet_delta_table = \
        get_parquet_table_as_delta_table_using_delta_lake(
            parquet_folder_path)

    return \
        parquet_delta_table, \
        parquet_folder_path
