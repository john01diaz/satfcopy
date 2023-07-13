from sat_parquet_source.parquet_tables_cleaner.b_code.helpers.file_configuration_list_populator import \
    populate_file_configuration_list
from sat_parquet_source.parquet_tables_cleaner.b_code.helpers.folder_children_paths_getter import \
    get_folder_children_paths


def get_file_configuration_list(
        stage_name: str,
        input_parquet_table_path: str,
        file_configuration_list: list) \
        -> list:
    input_root_folder_children = \
        get_folder_children_paths(
            stage_name=stage_name,
            parquet_silver_root_folder_path=input_parquet_table_path)

    if not file_configuration_list:
        file_configuration_list = \
            populate_file_configuration_list(
                stage_name=stage_name,
                input_root_folder_children=input_root_folder_children,
                input_parquet_table_path=input_parquet_table_path,
                file_configuration_list=file_configuration_list)

    return \
        file_configuration_list
