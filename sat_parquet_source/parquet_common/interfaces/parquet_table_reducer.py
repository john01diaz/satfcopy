from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_parquet_source.parquet_common.interfaces.parquet_table_by_option_reducer import reduce_parquet_table_by_option
from sat_parquet_source.parquet_common.interfaces.reduce_parquet_table_configuration import \
    ReduceParquetTableConfiguration
from sat_parquet_source.parquet_common.helpers.output_parquet_table_folder_getter import \
    get_output_parquet_table_folder
from sat_parquet_source.parquet_common.helpers.parquet_folder_path_getter import get_parquet_folder_path


def reduce_parquet_table(
        reduce_parquet_table_configuration: ReduceParquetTableConfiguration,
        number_of_rows_to_keep: int,
        timestamped_output_folder: Folders,
        file_configuration: list) \
        -> None:
    if file_configuration[1] != 'snappy.parquet':
        return

    parquet_folder_path = \
        get_parquet_folder_path(
            file_configuration=file_configuration)

    output_parquet_table_path = \
        get_output_parquet_table_folder(
            timestamped_output_folder=timestamped_output_folder,
            file_configuration=file_configuration)

    try:
        reduce_parquet_table_by_option(
            reduce_parquet_table_configuration=reduce_parquet_table_configuration,
            number_of_rows_to_keep=number_of_rows_to_keep,
            parquet_folder_path=parquet_folder_path,
            output_parquet_table_path=output_parquet_table_path)

    except Exception as error:
        log_message(
            message='*' * 30 + ' ERROR: ' + file_configuration[3] + ' - ' + str(error))
