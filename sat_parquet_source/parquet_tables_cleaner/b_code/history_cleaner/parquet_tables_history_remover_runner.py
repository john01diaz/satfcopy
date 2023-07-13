from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_tables_cleaner.b_code.exporters.parquet_folder_as_delta_table_history_remover_runner import \
    run_parquet_folder_as_delta_table_history_remover
from sat_parquet_source.parquet_tables_cleaner.b_code.helpers.parquet_table_as_delta_table_getter import \
    get_parquet_table_as_delta_table


def run_parquet_tables_history_remover(
        file_configuration: list,
        output_root_folder: Folders) \
        -> None:
    if file_configuration[1] != 'snappy.parquet':
        return

    try:
        parquet_delta_table, parquet_folder_path = \
            get_parquet_table_as_delta_table(
                file_configuration=file_configuration)

        run_parquet_folder_as_delta_table_history_remover(
            output_root_folder=output_root_folder,
            parquet_delta_table=parquet_delta_table,
            parquet_folder_path=parquet_folder_path)

    except Exception as error:
        print('*'*30 + ' ERROR: ' + file_configuration[3] + ' - ' + str(error))
