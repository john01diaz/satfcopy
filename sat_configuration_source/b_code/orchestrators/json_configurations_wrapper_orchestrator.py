import os.path
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_configuration_source.b_code.getters.databases.database_records_dictionary_from_database_getter import \
    get_database_records_dictionary_from_database
from sat_configuration_source.b_code.exporters.dictionary_to_json_file_exporter import \
    export_dictionary_to_json_file


@run_and_log_function
def orchestrate_json_configurations_wrapper(
        etl_database_configuration_file: Files,
        etl_json_configuration_file_name: str,
        etl_json_configuration_filtered_file_name: str) \
        -> None:
    output_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    etl_json_configuration_file_path = \
        os.path.join(
            output_folder.absolute_path_string,
            etl_json_configuration_file_name)

    etl_json_configuration_filtered_file_path = \
        os.path.join(
            output_folder.absolute_path_string,
            etl_json_configuration_filtered_file_name)

    database_records_dictionary, filtered_database_records_dictionary = \
        get_database_records_dictionary_from_database(
            database_full_file=etl_database_configuration_file)

    export_dictionary_to_json_file(
        etl_json_configuration_file_path=etl_json_configuration_file_path,
        database_records_dictionary=database_records_dictionary)

    export_dictionary_to_json_file(
        etl_json_configuration_file_path=etl_json_configuration_filtered_file_path,
        database_records_dictionary=filtered_database_records_dictionary)
