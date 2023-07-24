import os
import pyodbc
import pyodbc as odbc_library
from pathlib import Path


def get_table_configuration_database_based_query(
        database_connection: odbc_library.Connection) \
        -> list:
    query_file_path = \
        __get_query_file(
            database_connection=database_connection)

    file_reader = \
        open(query_file_path, "r")

    sql_query_as_list_of_string = \
        file_reader.readlines()

    file_reader.close()

    return \
        sql_query_as_list_of_string


def __get_query_file(
        database_connection: odbc_library.Connection) \
        -> str:
    root_path = \
        Path(__file__).parent.parent.parent

    query_filename = \
        str()

    if 'pyodbc' in str(type(database_connection)):
        query_filename = \
            'table_configuration_records_ms_access_query.txt'

    elif 'sqlite' in str(type(database_connection)):
        query_filename = \
            'table_configuration_records_sqlite_query.txt'

    else:
        raise \
            NotImplementedError

    query_file_path = \
        os.path.join(
            root_path,
            'c_resources',
            query_filename)

    return \
        query_file_path
