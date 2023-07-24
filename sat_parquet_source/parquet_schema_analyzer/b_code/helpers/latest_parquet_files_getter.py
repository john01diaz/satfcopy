import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_common.table_getters.parquet_table_as_delta_table_using_delta_lake_getter import \
    get_parquet_table_as_delta_table_using_delta_lake


def get_latest_parquet_files(
        child_parquet_folder: Folders) \
        -> list:
    parquet_table_latest_file_paths = \
        list()

    delta_table = \
        get_parquet_table_as_delta_table_using_delta_lake(
            absolute_table_name_folder_path=child_parquet_folder.absolute_path_string)

    latest_filenames = \
        delta_table.files()

    for latest_filename \
            in latest_filenames:
        latest_file_path = \
            os.path.join(
                child_parquet_folder.absolute_path_string,
                latest_filename)

        parquet_table_latest_file_paths.append(
            latest_file_path)

    return \
        parquet_table_latest_file_paths
