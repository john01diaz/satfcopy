from nf_common_source.code.services.access_service.access_database_creator import create_access_database_in_folder
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.access.all_csv_files_from_folder_to_access_exporter import \
    export_all_csv_files_from_folder_to_access
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_parquet_source.parquet_schema_analyzer.b_code.common_knowledge.pyspark_output_parser_constants import \
    PYSPARK_OUTPUTS_PARSER_DATABASE_FILE_NAME


def export_csv_files_to_ms_access(
        output_file_suffix: str,
        working_output_folder: Folders,
        datetime_stamp: str) \
        -> None:
    parent_folder_path = \
        working_output_folder.parent_absolute_path_string \
        if working_output_folder.base_name == 'working' \
        else working_output_folder.absolute_path_string

    access_database_path = \
        create_access_database_in_folder(
            parent_folder_path=parent_folder_path,
            database_name=PYSPARK_OUTPUTS_PARSER_DATABASE_FILE_NAME + '_' + output_file_suffix + '_' + datetime_stamp)

    __export_all_csv_files_from_folder_to_access_hard_crash_wrapper(
        working_output_folder=working_output_folder,
        access_database_path=access_database_path)


def __export_all_csv_files_from_folder_to_access_hard_crash_wrapper(
        working_output_folder: Folders,
        access_database_path: str) \
        -> None:
    try:
        export_all_csv_files_from_folder_to_access(
            csv_folder=working_output_folder,
            database_already_exists=True,
            database_full_path_if_already_exists=access_database_path)

    except Exception as error:
        log_message(
            message='An error ocurred loading files in: ' + working_output_folder.absolute_path_string)
