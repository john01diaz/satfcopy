import pandas
import pyodbc as odbc_library
from sat_configuration_source.b_code.getters.dataframe_from_query_getter import \
    get_dataframe_from_query


def get_process_table_roles_data(
        database_connection: odbc_library.Connection,
        left_table_name: str,
        left_table_join_column_name: str,
        right_table_name: str,
        right_table_join_column_name: str) \
        -> pandas.DataFrame:
    sql_query = \
        'SELECT * FROM {0} LEFT JOIN {1} ON {2}.{3} = {4}.{5} ;'.format(
            left_table_name,
            right_table_name,
            left_table_name,
            left_table_join_column_name,
            right_table_name,
            right_table_join_column_name)

    first_table_as_dataframe = \
        get_dataframe_from_query(
            database_connection=database_connection,
            sql_query=sql_query)

    return \
        first_table_as_dataframe
