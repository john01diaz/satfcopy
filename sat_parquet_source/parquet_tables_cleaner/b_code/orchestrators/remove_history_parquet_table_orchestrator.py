from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_parquet_source.parquet_tables_cleaner.b_code.helpers.file_configuration_list_getter import \
    get_file_configuration_list
from sat_parquet_source.parquet_tables_cleaner.b_code.runners.remove_history_parquet_table_runner import \
    run_remove_history_parquet_table


def orchestrate_remove_history_parquet_table(
        file_configuration_list: list,
        stage_name: str,
        parquet_table_root_folder_path: str) \
        -> None:
    timestamped_output_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    file_configuration_list = \
        get_file_configuration_list(
            stage_name=stage_name,
            parquet_table_root_folder_path=parquet_table_root_folder_path,
            file_configuration_list=file_configuration_list)

    for file_configuration \
            in file_configuration_list:
        run_remove_history_parquet_table(
            timestamped_output_folder=timestamped_output_folder,
            file_configuration=file_configuration)
