import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.b_code.orchestrators.parquet_folder_consumer_orchestrator import \
    orchestrate_parquet_folder_consumer


def b_app_runner_for_parquet_files_reader(
        input_root_folder: Folders,
        output_root_folder: Folders,
        export_to_access: bool,
        export_to_sqlite: bool,
        export_csvs_to_sqlite: bool,
        export_parquet_file_to_csv: bool,
        data_chunk_size: int = None):
    session_name_string = \
        'PySparkParquetReader'

    output_folder_prefix = \
        'etl_parquet_folders_schema'

    output_folder_suffix = \
        input_root_folder.absolute_path_string.split(os.sep)[-2].split('-')[-1] + '_' \
        + input_root_folder.absolute_path_string.split(os.sep)[-1]

    output_folder_prefix = \
        output_folder_prefix + '_ ' + output_folder_suffix

    run_b_app(
        app_startup_method=orchestrate_parquet_folder_consumer,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=output_folder_prefix,
        output_root_folder=output_root_folder,
        input_root_folder=input_root_folder,
        output_file_suffix=output_folder_suffix,
        data_chunk_size=data_chunk_size,
        session_name_string=session_name_string,
        export_to_access=export_to_access,
        export_to_sqlite=export_to_sqlite,
        export_csvs_to_sqlite=export_csvs_to_sqlite,
        export_parquet_file_to_csv=export_parquet_file_to_csv)
