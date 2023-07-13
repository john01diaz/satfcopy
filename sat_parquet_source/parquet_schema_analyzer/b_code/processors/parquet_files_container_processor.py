import os.path
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.objects.file_system_objects import FileSystemObjects
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_parquet_source.parquet_schema_analyzer.b_code.helpers.parquet_file_reader import read_parquet_file
from pyspark.sql import SparkSession
from sat_parquet_source.parquet_schema_analyzer.b_code.helpers.spark_session_creator import create_pyspark_session


def process_parquet_files_container(
        pyspark_dataframes_as_table_dictionary: dict,
        input_root_folder_child_path_children: list,
        child_parquet_folder: Folders,
        working_output_folder: Folders,
        session_name_string: str,
        data_chunk_size: int) \
        -> None:
    for input_root_folder_child_path_children_path \
            in input_root_folder_child_path_children:
        child_file_system_object = \
            FileSystemObjects(
                absolute_path_string=input_root_folder_child_path_children_path)

        log_message(
            message='*' * 25 + 'Processing file {0} of {1}: {2}. File size: {3} MB'.format(
                input_root_folder_child_path_children.index(input_root_folder_child_path_children_path) + 1,
                len(input_root_folder_child_path_children),
                child_file_system_object.base_name,
                round((child_file_system_object.file_system_object_properties.length/1024/1024), 4)))

        spark_session = \
            create_pyspark_session(
                session_name_string=session_name_string + now_time_as_string_for_files())

        __read_child_hard_crash_wrapper(
            child_folder=child_parquet_folder,
            data_chunk_size=data_chunk_size,
            spark_session=spark_session,
            working_output_folder=working_output_folder,
            session_name_string=session_name_string,
            pyspark_dataframes_as_table_dictionary=pyspark_dataframes_as_table_dictionary,
            input_root_folder_child_path=input_root_folder_child_path_children_path)

        spark_session.stop()


def __read_child_hard_crash_wrapper(
        child_folder: Folders,
        pyspark_dataframes_as_table_dictionary: dict,
        data_chunk_size: int,
        working_output_folder: Folders,
        spark_session: SparkSession,
        session_name_string: str,
        input_root_folder_child_path: str) \
        -> None:
    try:
        input_file_system_object = \
            FileSystemObjects(
                absolute_path_string=input_root_folder_child_path)

        newest_pandas_dataframe, row_count, original_dtypes_tuples = \
            read_parquet_file(
                spark_session=spark_session,
                working_output_folder=working_output_folder,
                input_file_system_object_path=input_root_folder_child_path,
                data_chunk_size=data_chunk_size)

        parquet_file_relative_path = \
            os.path.relpath(
                input_root_folder_child_path,
                child_folder.absolute_path_string)

        newest_pandas_dataframe['parquet_file_relative_path'] = \
            parquet_file_relative_path if parquet_file_relative_path else str()

        log_message(
            message='*' * 25 + 'Added key: ' + parquet_file_relative_path)

        pyspark_dataframes_as_table_dictionary[input_file_system_object] = \
            {
                'dtypes': original_dtypes_tuples,
                'row_count': row_count,
                'data': newest_pandas_dataframe
            }

    except Exception as error:
        spark_session.sparkContext.cancelJobGroup(
            groupId=session_name_string)

        log_message(
            message=str(error[:100]))
