import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.parquet_common.interfaces.reduce_parquet_table_configuration import \
    ReduceParquetTableConfiguration
from sat_parquet_source.parquet_tables_reducer.b_code.orchestrator.reduce_parquet_tables_orchestrator import \
    orchestrate_reduce_parquet_tables


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

    parquet_folder_root_folder_path = \
        r'/Users/terraire/bWa/DZa/etl/collect/blob_latest/clean_parquet_2023_07_17_13_22_31/blob-temp-anusha_folder-sigraph_silver_2023_06_27_1815/'

    # TODO: comment lines below to run all snapshots inside the folder
    file_configuration_list = \
        [
            [parquet_folder_root_folder_path, 'snappy.parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, '']
        ]

    run_b_app(
        app_startup_method=orchestrate_reduce_parquet_tables,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='reduced_parquet_sigraph_silver',
        output_folder_suffix=str(),
        output_root_folder=output_root_folder,
        file_configuration_list=file_configuration_list,
        stage_name='sigraph_silver',
        number_of_rows_to_keep=1000,
        input_parquet_table_path=parquet_folder_root_folder_path,
        reduce_parquet_table_configuration=ReduceParquetTableConfiguration.OPTION_PANDAS_PYARROW)
