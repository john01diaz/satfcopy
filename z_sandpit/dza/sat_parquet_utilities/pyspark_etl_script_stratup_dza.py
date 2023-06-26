import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.a_startup.b_app_runner_for_parquet_files_reader import b_app_runner_for_parquet_files_reader


if __name__ == '__main__':
    input_root_folders_prefix = \
        r'/Users/terraire/bWa/DZa/etl/collect/blob'

    input_root_folders = \
        [
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500' + os.sep + 'sigraph_bronze'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_crosswalks_db_2023_05_21_1500' + os.sep + 'sigraph_crosswalks'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_gold_2023_05_21_1500' + os.sep + 'sigraph_gold'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_reference_files_2023_05_21_1500' + os.sep + 'crosswalks'),
            Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500' + os.sep + 'sigraph_silver'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_reference_files_2023_05_21_1500' + os.sep + 'sigraph_reference_files'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_all_db_2023_05_21_1500' + os.sep + 'sigraph_all_db'),
            # Folders(absolute_path_string=input_root_folders_prefix + os.sep + r'blob-temp-anusha_folder-sigraph_static_classes_2023_05_21_1500' + os.sep + 'sigraph_static_classes')
        ]

    output_root_folder = \
        Folders(absolute_path_string=r'/Users/terraire/bWa/DZa/pyspark_etl_sigraph_exporter_ws/outputs')

    for input_root_folder \
            in input_root_folders:
        b_app_runner_for_parquet_files_reader(
            input_root_folder=input_root_folder,
            output_root_folder=output_root_folder,
            export_to_access=False,
            export_to_sqlite=True,
            export_csvs_to_sqlite=True,
            export_parquet_file_to_csv=False,
            data_chunk_size=10)
