import sqlite3
import pyodbc as pyodbc
import copy
from nf_common_source.code.services.access_service.access_database_connection_getter import \
    get_access_database_connection
from nf_common_source.code.services.file_system_service.objects.files import Files
from sat_configuration_source.b_code.getters.json_configuration_as_dictionary_getter import \
    get_json_configuration_as_dictionary
from sat_configuration_source.b_code.getters.databases.process_records_from_database_connection_getter import \
    get_process_records_from_database_connection
from sat_configuration_source.b_code.getters.databases.process_table_configuration_records_from_database_connection_getter import \
    get_process_table_configuration_records_from_database_connection
from sat_configuration_source.b_code.getters.databases.table_configuration_records_from_database_connection_getter import \
    get_table_configuration_records_from_database_connection


def get_database_records_dictionary_from_database(
        database_full_file: Files) \
        -> tuple:
    database_records_dictionary = \
        get_json_configuration_as_dictionary()

    filtered_database_records_dictionary = \
        copy.deepcopy(database_records_dictionary)

    if not hasattr(database_full_file, 'file_system_object_properties'):
        raise \
            FileExistsError(
                'File does not exist: ' + database_full_file.absolute_path_string)

    if database_full_file.file_system_object_properties.extension == '.accdb':
        database_connection = \
            get_access_database_connection(
                database_full_file_path=database_full_file.absolute_path_string)

    elif database_full_file.file_system_object_properties.extension == '.db':
        database_connection = \
            sqlite3.connect(
                database_full_file.absolute_path_string)

    else:
        raise \
            NotImplementedError(
                'Extension type not implemented: ' + database_full_file.file_system_object_properties.extension)

    __get_records_from_database_file(
        filtered_database_records=filtered_database_records_dictionary,
        database_records=database_records_dictionary,
        database_connection=database_connection)

    return \
        database_records_dictionary, \
        filtered_database_records_dictionary


def __get_records_from_database_file(
        filtered_database_records: dict,
        database_records: dict,
        database_connection: pyodbc.Connection) \
        -> None:
    get_process_records_from_database_connection(
        filtered_database_records_dictionary=filtered_database_records,
        database_records_dictionary=database_records,
        database_connection=database_connection,
        table_name='run_configuration_processes')

    get_process_table_configuration_records_from_database_connection(
        filtered_database_records_dictionary=filtered_database_records,
        database_records_dictionary=database_records,
        database_connection=database_connection,
        left_table_name='process_table_roles',
        left_table_join_column_name='bie_ids',
        right_table_name='run_configuration_process_table_input_roles',
        right_table_join_column_name='bie_process_table_role_ids')

    get_table_configuration_records_from_database_connection(
        filtered_database_records_dictionary=filtered_database_records,
        database_records_dictionary=database_records,
        database_connection=database_connection)
