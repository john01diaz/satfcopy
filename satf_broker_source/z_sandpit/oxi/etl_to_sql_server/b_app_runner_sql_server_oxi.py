import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes

from satf_broker_source.z_sandpit.oxi.etl_to_sql_server.parquet_folder_to_sql_loader_oxi import \
    load_parquet_folder_to_sql_oxi

if __name__ == '__main__':
    user_initials = \
        'OXi'

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

    input_root_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            'blob',
            'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500',
            'sigraph_silver')

    input_root_folder = \
        Folders(
            absolute_path_string=input_root_folder_path)

    output_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'main_wrapper_outputs')

    run_b_app(
        app_startup_method=load_parquet_folder_to_sql_oxi,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='parquet_folder_to_sql_table_schema',
        output_root_folder=Folders(absolute_path_string=output_folder_path),
        input_root_folder=input_root_folder,
        table_name='S_ItemFunction')
