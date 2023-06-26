import glob
import os.path
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_parquet_source.b_code.helpers.latest_parquet_files_getter import get_latest_parquet_files
from sat_parquet_source.b_code.processors.parquet_files_container_processor import process_parquet_files_container


def orchestrate_parquet_file_reader(
        session_name_string: str,
        data_chunk_size: int,
        working_output_folder: Folders,
        parquet_files_container_dictionary: dict,
        input_root_folder_child_path: str) \
        -> None:
    try:
        __process_container_try_catch_wrapper(
            data_chunk_size=data_chunk_size,
            session_name_string=session_name_string,
            working_output_folder=working_output_folder,
            parquet_files_container_dictionary=parquet_files_container_dictionary,
            input_root_folder_child_path=input_root_folder_child_path)

    except Exception as error:
        log_message(
            message='Error in folder: ' + input_root_folder_child_path)


def __process_container_try_catch_wrapper(
        data_chunk_size: int,
        session_name_string: str,
        working_output_folder: Folders,
        parquet_files_container_dictionary: dict,
        input_root_folder_child_path: str) \
        -> None:
    try:
        if os.path.isdir(input_root_folder_child_path) \
                and '_delta_log' not in input_root_folder_child_path:
            pyspark_dataframes_as_table_dictionary = \
                dict()

            child_parquet_folder = \
                Folders(
                    absolute_path_string=input_root_folder_child_path)

            input_root_folder_child_path_children = \
                glob.glob(
                    input_root_folder_child_path + "/**/*.snappy.parquet",
                    recursive=True)

            # TODO: Add a filter to get just latest .parquet files
            input_root_folder_child_path_children = \
                get_latest_parquet_files(
                    child_parquet_folder=child_parquet_folder,
                    input_root_folder_child_path_children=input_root_folder_child_path_children)

            log_message(
                message='*' * 50 + 'Processing folder: ' + child_parquet_folder.base_name)

            process_parquet_files_container(
                working_output_folder=working_output_folder,
                pyspark_dataframes_as_table_dictionary=pyspark_dataframes_as_table_dictionary,
                input_root_folder_child_path_children=input_root_folder_child_path_children,
                child_parquet_folder=child_parquet_folder,
                session_name_string=session_name_string,
                data_chunk_size=data_chunk_size)

            parquet_files_container_dictionary[child_parquet_folder] = \
                pyspark_dataframes_as_table_dictionary

    except Exception as error:
        log_message(
            message=str(error))
