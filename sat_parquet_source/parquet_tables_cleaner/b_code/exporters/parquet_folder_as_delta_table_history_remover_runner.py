import glob
import os
import shutil
from deltalake import DeltaTable
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_parquet_source.parquet_tables_cleaner.b_code.runners.copy_latest_parquet_table_files_to_output_folder_runner import \
    run_latest_parquet_table_files_to_output_folder


def run_parquet_folder_as_delta_table_history_remover(
        output_root_folder: Folders,
        parquet_delta_table: DeltaTable,
        parquet_folder_path: str) \
        -> None:
    parquet_folder_children = \
        __get_parquet_folder_children(
            parquet_folder_path=parquet_folder_path)

    __duplicate_delta_log_table(
        parquet_folder_path=parquet_folder_path,
        output_root_folder=output_root_folder)

    for parquet_folder_child_path \
            in parquet_folder_children:
        run_latest_parquet_table_files_to_output_folder(
            parquet_delta_table=parquet_delta_table,
            output_root_folder=output_root_folder,
            parquet_folder_child_path=parquet_folder_child_path)

    log_message(
        '*'*25 + 'DONE: ' + str(parquet_folder_path))


def __get_parquet_folder_children(
        parquet_folder_path: str):
    parquet_folder_children = \
        glob.glob(
            parquet_folder_path + "**/*.snappy.parquet",
            recursive=True)

    return \
        parquet_folder_children


def __duplicate_delta_log_table(
        parquet_folder_path: str,
        output_root_folder: Folders):
    delta_log_table = \
        os.path.join(
            parquet_folder_path,
            '_delta_log')

    shutil.copytree(
        delta_log_table,
        output_root_folder.absolute_path_string + os.sep + '_delta_log')
