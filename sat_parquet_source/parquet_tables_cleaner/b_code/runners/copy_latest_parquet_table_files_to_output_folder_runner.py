import os
import shutil
from deltalake import DeltaTable
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def run_latest_parquet_table_files_to_output_folder(
        parquet_delta_table: DeltaTable,
        output_root_folder: Folders,
        parquet_folder_child_path: str) \
        -> None:
    parquet_folder_child_name = \
        parquet_folder_child_path.split(os.sep)[-1]

    if parquet_folder_child_name in parquet_delta_table.files():
        parquet_file = \
            Files(
                absolute_path_string=parquet_folder_child_path)

        shutil.copy2(
            parquet_file.absolute_path_string,
            output_root_folder.absolute_path_string + os.sep + parquet_folder_child_name)
