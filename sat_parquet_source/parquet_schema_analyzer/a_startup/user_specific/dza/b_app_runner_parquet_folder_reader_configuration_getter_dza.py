import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_schema_analyzer.b_code.configuration.parquet_folder_reader_configurations import \
    ParquetFolderReaderConfigurations


def get_b_app_runner_parquet_folder_reader_configuration_dza() \
        -> ParquetFolderReaderConfigurations:
    # NOTE: This folder should be the parent folder of the folder containing the parquet files
    input_root_folder = \
        Folders(
            absolute_path_string=r'/Users/terraire/bWa/DZa/etl/collect/blob_latest/clean_parquet_sigraph_bronze_2023_07_17_12_27_41/blob-temp-anusha_folder-sigraph_bronze_2023_06_27_1817/sigraph_bronze')

    output_root_folder = \
        Folders(
            absolute_path_string=r'/Users/terraire/bWa/DZa/satf_ws/sat_parquet_outputs')

    output_folder_prefix = \
        'etl_parquet_table_schema-sigraph_bronze'

    input_root_folder_name = \
        input_root_folder.absolute_path_string.split(os.sep)[-1]

    parquet_folder_reader_configuration = \
        ParquetFolderReaderConfigurations(
            input_root_folder=input_root_folder,
            output_root_folder=output_root_folder,
            output_folder_prefix=output_folder_prefix,
            input_root_folder_name=input_root_folder_name)

    return \
        parquet_folder_reader_configuration
