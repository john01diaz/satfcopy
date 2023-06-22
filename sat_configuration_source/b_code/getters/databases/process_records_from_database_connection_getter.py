import pandas
import pyodbc as odbc_library
from sat_configuration_source.b_code.getters.dataframe_from_query_getter import \
    get_dataframe_from_query


def get_process_records_from_database_connection(
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
            'bie_ids',
            'bie_process_ids',
            'process_types',
            'process_names',
            'code_process_names',
            'run_order',
            'run'
        ]

    final_dataframe = \
        dataframe_first_table[
            columns_to_keep]

    final_dataframe = \
        final_dataframe.rename(
            columns={
                'bie_ids': 'bie_run_process_ids'
            })

    final_dataframe = \
        final_dataframe.fillna(
            str())

    __populate_database_records(
        database_records_dictionary=database_records_dictionary,
        filtered_database_records_dictionary=filtered_database_records_dictionary,
        final_dataframe=final_dataframe)


def __populate_database_records(
        database_records_dictionary: dict,
        filtered_database_records_dictionary: dict,
        final_dataframe: pandas.DataFrame):
    for index, row \
            in final_dataframe.iterrows():
        __add_process_record(
            filtered_database_records_dictionary=filtered_database_records_dictionary,
            database_records_dictionary=database_records_dictionary,
            row=row.to_dict())


def __add_process_record(
        database_records_dictionary: dict,
        filtered_database_records_dictionary: dict,
        row: dict):
    run_flag = \
        row['run']

    bie_run_process_ids = \
        None if row['bie_run_process_ids'] == str() else str(int(float(row['bie_run_process_ids'])))

    bie_process_ids = \
        None if row['bie_process_ids'] == str() else str(int(float(row['bie_process_ids'])))

    process_names = \
        row['process_names'].replace('.sql', str())

    process_types = \
        row['process_types']

    code_process_names = \
        row['code_process_names']

    run_order = \
        None if row['run_order'] == str() else int(float(row['run_order']))

    process_record = \
        {
            'bie_run_process_ids': bie_run_process_ids,
            'bie_process_ids': bie_process_ids,
            'process_names': process_names,
            'process_types': process_types,
            'code_process_names': code_process_names,
            'run_order': run_order
        }

    database_records_dictionary['etl_run']['processes'].append(
        process_record)

    if run_flag == 'YES':
        filtered_database_records_dictionary['etl_run']['processes'].append(
            process_record)
