import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def get_output_parquet_table_folder(
        timestamped_output_folder: Folders,
        file_configuration: list) \
        -> Folders:
    output_parquet_table_path = \
        os.path.join(
            timestamped_output_folder.absolute_path_string,
            file_configuration[0].split(os.sep)[-1],
            file_configuration[2],
            file_configuration[3])

    output_parquet_table_folder = \
        Folders(
            absolute_path_string=output_parquet_table_path)

    return \
        output_parquet_table_folder
