import os
import shutil
from deltalake import DeltaTable
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def run_latest_parquet_table_files_to_output_folder(
        parquet_folder_path: str,
        parquet_delta_table: DeltaTable,
        output_root_folder: Folders,
        parquet_folder_child_path: str) \
        -> None:
    parquet_folder_child_name = \
        parquet_folder_child_path.replace(
            parquet_folder_path + os.sep,
            str())

    __create_partitions_folder(
        output_root_folder=output_root_folder,
        parquet_folder_child_name=parquet_folder_child_name)

    if parquet_folder_child_name.replace(os.sep, '/') in parquet_delta_table.files():
        input_parquet_file = \
            Files(
                absolute_path_string=parquet_folder_child_path)

        shutil.copy2(
            input_parquet_file.absolute_path_string,
            output_root_folder.absolute_path_string + os.sep + parquet_folder_child_name)


def __create_partitions_folder(
        output_root_folder: Folders,
        parquet_folder_child_name: str) \
        -> None:
    relative_to_parquet_table_path = \
        str()

    if os.sep in parquet_folder_child_name:
        relative_to_parquet_table_path = \
            os.sep.join(parquet_folder_child_name.split(os.sep)[:-1])

    output_folder_path = \
        os.path.join(
            output_root_folder.absolute_path_string, relative_to_parquet_table_path)

    if not os.path.exists(output_folder_path):
        os.makedirs(
            output_folder_path)
