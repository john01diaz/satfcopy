import os
import glob
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_parquet_source.b_code.table_load_and_registerer import load_and_register_table


def orchestrate_re_export_latest_parquet_files(
        file_configuration_list: list,
        parquet_silver_root_folder_path: str) \
        -> None:
    timestamped_output_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    stage_folder_child_stage_folder_path_children = \
        __get_stage_folder_child_stage_folder_path_children(
            parquet_silver_root_folder_path=parquet_silver_root_folder_path)

    if not file_configuration_list:
        file_configuration_list = \
            __populate_file_configuration_list(
                stage_folder_child_stage_folder_path_children=stage_folder_child_stage_folder_path_children,
                parquet_silver_root_folder_path=parquet_silver_root_folder_path,
                file_configuration_list=file_configuration_list)

    for file_configuration \
            in file_configuration_list:
        load_and_register_table(
            output_root_folder=timestamped_output_folder,
            file_configuration=file_configuration)


def __get_stage_folder_child_stage_folder_path_children(
        parquet_silver_root_folder_path: str) \
        -> list:
    stage_folder_child_stage_folder_path_children = \
        glob.glob(
            parquet_silver_root_folder_path + "/sigraph_silver/*",
            recursive=False)

    return \
        stage_folder_child_stage_folder_path_children


def __populate_file_configuration_list(
        stage_folder_child_stage_folder_path_children: list,
        parquet_silver_root_folder_path: str,
        file_configuration_list: list) \
        -> list:
    for stage_folder_child_stage_folder_path_child \
            in stage_folder_child_stage_folder_path_children:
        if os.path.isdir(stage_folder_child_stage_folder_path_child):
            file_configuration =  \
                [
                    parquet_silver_root_folder_path,
                    'parquet',
                    'sigraph_silver',
                    stage_folder_child_stage_folder_path_child.split(os.sep)[-1],
                    'input',
                    None,
                    ''
                ]

            file_configuration_list.append(
                file_configuration)

    return \
        file_configuration_list
