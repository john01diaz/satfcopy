import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_tables_reducer.b_code.runners.parquet_table_reducer_runner import \
    run_parquet_table_reducer


def run_reduce_parquet_table(
        number_of_rows_to_keep: int,
        timestamped_output_folder: Folders,
        file_configuration: list) \
        -> None:
    output_parquet_table_path = \
        __get_output_parquet_table_path(
            timestamped_output_folder=timestamped_output_folder,
            file_configuration=file_configuration)

    run_parquet_table_reducer(
        number_of_rows_to_keep=number_of_rows_to_keep,
        output_root_folder=Folders(absolute_path_string=output_parquet_table_path),
        file_configuration=file_configuration)


def __get_output_parquet_table_path(
        timestamped_output_folder: Folders,
        file_configuration: list) \
        -> str:
    output_parquet_table_path = \
        os.path.join(
            timestamped_output_folder.absolute_path_string,
            file_configuration[0].split(os.sep)[-1],
            file_configuration[2],
            file_configuration[3])

    return \
        output_parquet_table_path
