import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.parquet_tables_reducer.b_code.orchestrator.reduce_parquet_tables_orchestrator import \
    orchestrate_reduce_parquet_tables


if __name__ == '__main__':
    user_initials = \
        'AMi'

    blob_latest_output_folder_name = \
        'blob_latest'

    b_root_folder_path = \
        os.path.join(
            'c',
            os.sep,
            'bWa')

    etl_root_folder_path = \
        os.path.join(
            b_root_folder_path,
            user_initials,
            'etl')

    collect_blob_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            'blob_latest')

    output_root_folder = \
        Folders(
            absolute_path_string=os.path.join(
                etl_root_folder_path,
                'collect',
                blob_latest_output_folder_name))

    parquet_folder_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'clean_parquet_2023_07_11_09_26_09',
            'blob-temp-anusha_folder-sigraph_bronze_2023_06_27_1817')

    # TODO: comment lines below to run all snapshots inside the folder
    file_configuration_list = \
        [
            [
                parquet_folder_root_folder_path, 'snappy.parquet', 'sigraph_bronze', 'Loop', 'input', None, ''
            ]
        ]

    run_b_app(
        app_startup_method=orchestrate_reduce_parquet_tables,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='reduced_parquet',
        output_folder_suffix=str(),
        output_root_folder=output_root_folder,
        file_configuration_list=file_configuration_list,
        stage_name='sigraph_bronze',
        number_of_rows_to_keep=1000,
        input_parquet_table_path=parquet_folder_root_folder_path)
