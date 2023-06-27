import csv
import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.identification_services.b_identity_service.b_identity_creators.b_identity_sum_from_strings_creator import \
    create_b_identity_sum_from_strings
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message


def export_parquet_files_to_csv(
        working_output_folder: Folders,
        parquet_files_dictionary: dict,
        pyspark_dataframe_to_pandas_dataframe_dictionary: dict) \
        -> Folders:
    for input_parquet_file, parquet_files_as_tables_dictionary \
            in parquet_files_dictionary.items():
        __export_pandas_dataframe_hard_crash_wrapper(
            pyspark_dataframe_to_pandas_dataframe_dictionary=pyspark_dataframe_to_pandas_dataframe_dictionary,
            working_output_folder=working_output_folder,
            input_parquet_file=input_parquet_file,
            parquet_files_as_tables_dictionary=parquet_files_as_tables_dictionary)

    return \
        working_output_folder


def __export_pandas_dataframe_hard_crash_wrapper(
        pyspark_dataframe_to_pandas_dataframe_dictionary: dict,
        working_output_folder: Folders,
        input_parquet_file: Files,
        parquet_files_as_tables_dictionary):
    try:
        __export_pandas_dataframe(
            pyspark_dataframe_to_pandas_dataframe_dictionary=pyspark_dataframe_to_pandas_dataframe_dictionary,
            working_output_folder=working_output_folder,
            input_parquet_file=input_parquet_file,
            parquet_files_as_tables_dictionary=parquet_files_as_tables_dictionary)

    except Exception as error:
        log_message(
            message='An error ocurred trying to export file content:' + input_parquet_file.absolute_path_string)


def __export_pandas_dataframe(
        pyspark_dataframe_to_pandas_dataframe_dictionary: dict,
        working_output_folder: Folders,
        input_parquet_file: Files,
        parquet_files_as_tables_dictionary) \
        -> None:
    container_folder_id_b_identity = \
        create_b_identity_sum_from_strings(
            strings=['sigraph_silver_parquet_files_container', input_parquet_file.base_name])

    output_file_path = \
        os.path.join(
            working_output_folder.absolute_path_string,
            input_parquet_file.base_name.lower() + '_container_' + str(container_folder_id_b_identity) + '.csv')

    if os.path.exists(output_file_path):
        raise \
            FileExistsError

    input_parquet_file = \
        list(parquet_files_as_tables_dictionary.keys())[0]

    dataframe = \
        parquet_files_as_tables_dictionary[input_parquet_file]['data']

    pyspark_dataframe_to_pandas_dataframe_dictionary[str(container_folder_id_b_identity)] = \
        dataframe.applymap(str)

    dataframe.to_csv(
        output_file_path,
        sep=',',
        quotechar='"',
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar='\\',
        encoding='utf-8')
