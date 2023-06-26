import pandas
import pyodbc as odbc_library
from sat_configuration_source.b_code.getters.dataframe_from_query_getter import \
    get_dataframe_from_query
from sat_configuration_source.b_code.getters.filtered_bie_ids_getter import get_filtered_bie_ids


def get_process_table_configuration_records_from_database_connection(
        filtered_database_records_dictionary: dict,
        database_records_dictionary: dict,
        database_connection: odbc_library.Connection,
        table_name: str) \
        -> None:
    sql_query = \
        'SELECT * FROM ' \
        + table_name + ';'

    dataframe_first_table = \
        get_dataframe_from_query(
            database_connection=database_connection,
            sql_query=sql_query)

    columns_to_keep = \
        [
            'bie_process_ids',
            'bie_table_ids',
            'table_role_types',
            'input_origin_types'
        ]

    final_dataframe = \
        dataframe_first_table[
            columns_to_keep]

    final_dataframe = \
        final_dataframe.fillna(
            str())

    __populate_database_records(
        database_records_dictionary=database_records_dictionary,
        filtered_database_records_dictionary=filtered_database_records_dictionary,
        final_dataframe=final_dataframe,
        table_name=table_name)


def __populate_database_records(
        database_records_dictionary: dict,
        filtered_database_records_dictionary: dict,
        table_name: str,
        final_dataframe: pandas.DataFrame):
    for index, row \
            in final_dataframe.iterrows():
        __add_process_record(
            filtered_database_records_dictionary=filtered_database_records_dictionary,
            database_records_dictionary=database_records_dictionary,
            table_name=table_name,
            row=row.to_dict())


def __add_process_record(
        database_records_dictionary: dict,
        filtered_database_records_dictionary: dict,
        table_name: str,
        row: dict):
    bie_process_ids_to_run = \
        get_filtered_bie_ids(
            database_records_dictionary=filtered_database_records_dictionary,
            desired_key='bie_process_ids',
            table_name='processes')

    bie_process_ids = \
        None if row['bie_process_ids'] == str() else row['bie_process_ids']

    bie_table_ids = \
        row['bie_table_ids']

    table_role_types = \
        row['table_role_types']

    input_origin_types = \
        row['input_origin_types']

    process_table_configuration_record = \
        {
            'bie_process_ids': bie_process_ids,
            'bie_table_ids': bie_table_ids,
            'table_role_types': table_role_types,
            'input_origin_types': input_origin_types
        }

    database_records_dictionary['etl_run'][table_name].append(
        process_table_configuration_record)

    if bie_process_ids in bie_process_ids_to_run:
        filtered_database_records_dictionary['etl_run'][table_name].append(
            process_table_configuration_record)
