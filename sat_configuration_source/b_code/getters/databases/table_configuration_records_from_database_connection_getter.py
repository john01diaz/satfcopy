import pandas
import pyodbc as odbc_library
from sat_configuration_source.b_code.getters.dataframe_from_query_getter import \
    get_dataframe_from_query
from sat_configuration_source.b_code.getters.filtered_bie_ids_getter import get_filtered_bie_ids
from sat_configuration_source.b_code.getters.table_configuration_database_based_query_getter import \
    get_table_configuration_database_based_query


def get_table_configuration_records_from_database_connection(
        filtered_database_records_dictionary: dict,
        database_records_dictionary: dict,
        database_connection: odbc_library.Connection) \
        -> None:
    first_sql_query = \
        get_table_configuration_database_based_query(
            database_connection=database_connection)

    final_dataframe = \
        get_dataframe_from_query(
            database_connection=database_connection,
            sql_query=' '.join(first_sql_query))

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
    bie_process_ids_to_run = \
        get_filtered_bie_ids(
            database_records_dictionary=filtered_database_records_dictionary,
            desired_key='bie_process_ids',
            table_name='processes')

    bie_table_ids_to_run = \
        get_filtered_bie_ids(
            database_records_dictionary=filtered_database_records_dictionary,
            desired_key='bie_table_ids',
            table_name='process_table_configuration')

    bie_table_ids = \
        row['bie_table_ids']

    table_names = \
        row['table_names']

    alternative_table_names = \
        row['alternative_table_names']

    source_extension_types = \
        row['source_extension_types']

    table_source_folders = \
        row['table_source_minor_types_table_source_minor_types']

    table_source_relative_paths = \
        row['table_source_relative_paths']

    loader_excel_sheet_names = \
        row['loader_excel_sheet_names']

    bie_identifying_columns = \
        eval(row['bie_identifying_columns']) if row['bie_identifying_columns'] else row['bie_identifying_columns']

    expected_column_count = \
        None if row['expected_column_count'] == str() else row['expected_column_count']

    expected_row_count = \
        None if row['expected_row_count'] == str() else row['expected_row_count']

    process_table_columns_filters = \
        row['process_table_columns_filters']

    defaulted_process_table_columns = \
        eval(row['defaulted_process_table_columns']) if row['defaulted_process_table_columns'] else row['defaulted_process_table_columns']

    process_table_configuration_record = \
        {
            'bie_table_ids': bie_table_ids,
            'table_names': table_names,
            'alternative_table_names': alternative_table_names,
            'source_extension_types': source_extension_types,
            'table_source_folders': table_source_folders,
            'table_source_relative_paths': table_source_relative_paths,
            'loader_excel_sheet_names': loader_excel_sheet_names,
            'bie_identifying_columns': bie_identifying_columns,
            'expected_column_count': expected_column_count,
            'expected_row_count': expected_row_count,
            'process_table_columns_filters': process_table_columns_filters,
            'defaulted_process_table_columns': defaulted_process_table_columns
        }

    database_records_dictionary['etl_run']['table_configurations'].append(
        process_table_configuration_record)

    bie_process_ids = \
        None if row['bie_process_ids'] == str() else row['bie_process_ids']

    if bie_process_ids not in bie_process_ids_to_run:
        return
    
    if bie_table_ids not in bie_table_ids_to_run:
        return
        
    filtered_database_records_dictionary['etl_run']['table_configurations'].append(
        process_table_configuration_record)
