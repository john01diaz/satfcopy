import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_tables_cleaner.b_code.history_cleaner.parquet_tables_history_remover_runner import \
    run_parquet_tables_history_remover


def run_remove_history_parquet_table(
        timestamped_output_folder: Folders,
        file_configuration: list) \
        -> None:
    output_cleaned_parquet_folder_path = \
        os.path.join(
            timestamped_output_folder.absolute_path_string,
            file_configuration[0].split(os.sep)[-1],
            file_configuration[3])

    run_parquet_tables_history_remover(
        output_root_folder=Folders(absolute_path_string=output_cleaned_parquet_folder_path),
        file_configuration=file_configuration)
