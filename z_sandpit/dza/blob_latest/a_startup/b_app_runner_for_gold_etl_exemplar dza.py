import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.b_code.orchestrators.re_export_latest_parquet_files_orchestrator import \
    orchestrate_re_export_latest_parquet_files

if __name__ == '__main__':
    user_initials = \
        'DZa'

    blob_latest_output_folder_name = \
        'blob_latest'

    b_root_folder_path = \
        os.path.join(
            '/Users/terraire',
            # os.sep,
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

    parquet_bronze_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500')

    parquet_silver_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500')

    # file_configuration_list = \
    #     [
    #         ##### SILVER #####
    #         # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
    #         [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
    #         # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None, ''],
    #         # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_ItemFunction', 'input', None, ''],
    #         # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
    #         # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
    #         ##### BRONZE #####
    #         # [parquet_bronze_root_folder_path, 'parquet', 'sigraph_bronze', 'Busbar', 'input', None, ''],
    #     ]

    # TODO: comment lines below to run all snapshots inside the folder
    file_configuration_list = \
        [
            ##### SILVER #####
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            [parquet_bronze_root_folder_path, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_ItemFunction', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
            ##### BRONZE #####
            # [parquet_bronze_root_folder_path, 'parquet', 'sigraph_bronze', 'Busbar', 'input', None, ''],
        ]

    run_b_app(
        app_startup_method=orchestrate_re_export_latest_parquet_files,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='blob_latest_parquet_files_wrapper',
        output_folder_suffix='reduced_to_1000_rows',
        output_root_folder=output_root_folder,
        file_configuration_list=list(),
        parquet_silver_root_folder_path=parquet_silver_root_folder_path)
