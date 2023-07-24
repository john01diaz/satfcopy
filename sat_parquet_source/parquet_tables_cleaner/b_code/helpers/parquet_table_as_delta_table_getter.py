from sat_parquet_source.parquet_common.table_getters.parquet_table_as_delta_table_using_delta_lake_getter import \
    get_parquet_table_as_delta_table_using_delta_lake
from sat_parquet_source.parquet_tables_cleaner.b_code.helpers.folder_path_getter import get_parquet_table_path


def get_parquet_table_as_delta_table(
        file_configuration: list) \
        -> tuple:
    print(
        'Running on: ' + str(file_configuration[2]) + ' - ' + str(file_configuration[3]))

    parquet_folder_path = \
        get_parquet_table_path(
            file_configuration=file_configuration)

    parquet_delta_table = \
        get_parquet_table_as_delta_table_using_delta_lake(
            parquet_folder_path)

    return \
        parquet_delta_table, \
        parquet_folder_path
