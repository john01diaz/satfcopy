import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from sat_configuration_source.b_code.exporters.dictionary_to_json_file_exporter import \
    export_dictionary_to_json_file
from sat_configuration_source.b_code.getters.databases.database_records_dictionary_from_database_getter import \
    get_database_records_dictionary_from_database


if __name__ == '__main__':
    user_initials = \
        'AGu'

    configuration_database_filename = \
        'process_table_configuration 2023-06-21 v0.02 AGu.accdb'

    b_root_folder_path = \
        os.path.join(
            'C:',
            os.sep,
            'bWa')

    etl_root_folder_path = \
        os.path.join(
            b_root_folder_path,
            user_initials,
            'etl')

    etl_database_configuration_file_path = \
        os.path.join(
            etl_root_folder_path,
            'configurations',
            configuration_database_filename)

    etl_json_configuration_file_path = \
        os.path.join(
            etl_root_folder_path,
            'configurations',
            'etl_run_configuration_full_RUN_NAME.json')

    etl_json_configuration_filtered_file_path = \
        os.path.join(
            etl_root_folder_path,
            'configurations',
            'etl_run_configuration_filtered_RUN_NAME.json')

    database_full_file = \
        Files(
            absolute_path_string=etl_database_configuration_file_path)

    database_records_dictionary, filtered_database_records_dictionary = \
        get_database_records_dictionary_from_database(
            database_full_file=database_full_file)

    export_dictionary_to_json_file(
        etl_json_configuration_file_path=etl_json_configuration_file_path,
        database_records_dictionary=database_records_dictionary)

    export_dictionary_to_json_file(
        etl_json_configuration_file_path=etl_json_configuration_filtered_file_path,
        database_records_dictionary=filtered_database_records_dictionary)
