from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_parquet_source.parquet_common.interfaces.parquet_table_reducer import reduce_parquet_table
from sat_parquet_source.parquet_common.interfaces.reduce_parquet_table_configuration import \
    ReduceParquetTableConfiguration
from sat_parquet_source.parquet_tables_reducer.b_code.helpers.file_configuration_list_getter import \
    get_file_configuration_list
from sat_parquet_source.parquet_tables_reducer.b_code.runners.reduce_parquet_table_runner import \
    run_reduce_parquet_table


def orchestrate_reduce_parquet_tables(
        file_configuration_list: list,
        stage_name: str,
        input_parquet_table_path: str,
        number_of_rows_to_keep: int,
        reduce_parquet_table_configuration: ReduceParquetTableConfiguration = None) \
        -> None:
    timestamped_output_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    file_configuration_list = \
        get_file_configuration_list(
            stage_name=stage_name,
            input_parquet_table_path=input_parquet_table_path,
            file_configuration_list=file_configuration_list)

    for file_configuration \
            in file_configuration_list:
        if reduce_parquet_table_configuration:
            reduce_parquet_table(
                reduce_parquet_table_configuration=reduce_parquet_table_configuration,
                number_of_rows_to_keep=number_of_rows_to_keep,
                timestamped_output_folder=timestamped_output_folder,
                file_configuration=file_configuration)

        else:
            run_reduce_parquet_table(
                number_of_rows_to_keep=number_of_rows_to_keep,
                timestamped_output_folder=timestamped_output_folder,
                file_configuration=file_configuration)
