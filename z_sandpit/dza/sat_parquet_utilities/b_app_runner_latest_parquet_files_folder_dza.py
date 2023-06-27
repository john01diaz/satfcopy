import os
import glob
from z_sandpit.dza.helpers.table_load_and_registerer import load_and_register_table


def __populate_file_configuration_list() \
        -> list:
    file_configurations = \
        list()

    for stage_folder_child \
            in input_root_folder_children:
        # Note: stage_folder_child = r'.../blob-temp-anusha_folder-sigraph_all_db_2023_05_21_1500'
        stage_folder_child_stage_folder_path = \
            glob.glob(
                stage_folder_child + "/*",
                recursive=False)[0]

        # Note: stage_folder_child_stage_folder_path = r'.../blob-temp-anusha_folder-sigraph_all_db_2023_05_21_1500/sigraph_all_db'
        stage_folder_child_stage_folder_name = \
            stage_folder_child_stage_folder_path.split(os.sep)[-1]

        # Note: stage_folder_child_stage_folder_name = r'sigraph_all_db'
        stage_folder_child_stage_folder_path_children = \
            glob.glob(
                stage_folder_child_stage_folder_path + "/*",
                recursive=False)

        for stage_folder_child_stage_folder_path_child \
                in stage_folder_child_stage_folder_path_children:
            if os.path.isdir(stage_folder_child_stage_folder_path_child):
                file_configurations.append(
                    [
                        stage_folder_child,
                        'parquet',
                        stage_folder_child_stage_folder_name,
                        stage_folder_child_stage_folder_path_child.split(os.sep)[-1],
                        'input',
                        None,
                        ''
                    ]
                )

    return \
        file_configurations


if __name__ == '__main__':
    input_root_folder_parquet = \
        r'C:\bWa\DZa\pyspark_etl_sigraph_exporter_ws\inputs\blob_parquet' \
        + os.sep + '2023_05_21_latest_parquet_files_collect_2023_05_30_18_33_07'

    file_configuration_list = \
        __populate_file_configuration_list()

    input_root_folder_children = \
        glob.glob(
            input_root_folder_parquet + "/*",
            recursive=False)

    for file_configuration \
            in file_configuration_list:
        load_and_register_table(
            file_configuration=file_configuration)
