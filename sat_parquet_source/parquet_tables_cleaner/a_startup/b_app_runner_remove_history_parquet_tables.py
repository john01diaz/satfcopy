import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.parquet_tables_cleaner.b_code.orchestrators.remove_history_parquet_table_orchestrator import \
    orchestrate_remove_history_parquet_table


if __name__ == '__main__':
    user_initials = \
        ''

    blob_latest_output_folder_name = \
        'blob_latest'

    b_root_folder_path = \
        os.path.join(
            'c:',
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
            'blob')

    output_root_folder = \
        Folders(
            absolute_path_string=os.path.join(
                etl_root_folder_path,
                'collect',
                blob_latest_output_folder_name))

    parquet_folder_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_silver_2023_06_27_1815')

    # TODO: comment lines below to run all snapshots inside the folder
    # file_configuration_list = \
    #     [
    #         [
    #             parquet_folder_root_folder_path, 'snappy.parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''
    #         ]
    #     ]

    run_b_app(
        app_startup_method=orchestrate_remove_history_parquet_table,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='clean_parquet',
        output_folder_suffix=str(),
        output_root_folder=output_root_folder,
        file_configuration_list=list(),
        stage_name='sigraph_silver',
        parquet_folder_root_folder_path=parquet_folder_root_folder_path)
