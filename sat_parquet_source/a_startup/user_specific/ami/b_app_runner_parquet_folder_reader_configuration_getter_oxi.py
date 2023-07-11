import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders

from sat_parquet_source.b_code.configuration.parquet_folder_reader_configurations import \
    ParquetFolderReaderConfigurations


def get_b_app_runner_parquet_folder_reader_configuration_oxi() \
        -> ParquetFolderReaderConfigurations:
    # NOTE: This folder should be the parent folder of the folder containing the parquet files
    input_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\OXi\etl\collect\tests\silver_test')

    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\OXi\etl\sat_parquet_outputs')

    output_folder_prefix = \
        'sat_parquet_folder_reader'

    input_root_folder_name = \
        input_root_folder.absolute_path_string.split(os.sep)[-1]

    output_folder_prefix = \
        output_folder_prefix + '_ ' + input_root_folder_name

    parquet_folder_reader_configuration = \
        ParquetFolderReaderConfigurations(
            input_root_folder=input_root_folder,
            output_root_folder=output_root_folder,
            output_folder_prefix=output_folder_prefix,
            input_root_folder_name=input_root_folder_name)

    return \
        parquet_folder_reader_configuration
