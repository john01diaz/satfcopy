import glob
import os

import pandas
import pyodbc
import pyarrow.parquet as pq
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def load_parquet_folder_to_sql_oxi(
        input_root_folder: Folders,
        table_name: str):
    input_parquet_folder = \
        Folders(
            absolute_path_string=os.path.join(
                input_root_folder.absolute_path_string,
                table_name))

    # Read the parquet file
    input_root_folder_children = \
        glob.glob(
            input_parquet_folder.absolute_path_string + "**/*.snappy.parquet",
            recursive=True)

    parquet_folder_path = \
        input_root_folder_children[0]

    parquet_folder_as_dataframe = \
        pandas.read_parquet(
            parquet_folder_path)

    sql_server_connection, sql_server_cursor = \
        __get_sql_server_connection()

    # # Here we're assuming that the table doesn't exist yet, so we'll create it
    # table_name = \
    #     input_root_folder.absolute_path_string.split(
    #         os.sep)[-1]

    # Generate CREATE TABLE statement
    columns = \
        ', '.join(f'[{col}] {dtype}' for col, dtype in zip(parquet_folder_as_dataframe.columns, parquet_folder_as_dataframe.dtypes.replace({
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'NVARCHAR(MAX)',
            'bool': 'BIT',
        })))

    sql_query_statement = \
        f"CREATE TABLE {table_name} ({columns})"

    # Execute the statement
    sql_server_cursor.execute(
        sql_query_statement)

    sql_server_cursor.commit()

    # Then, use to_sql to push the data to the new SQL table
    parquet_folder_as_dataframe.to_sql(
        table_name,
        sql_server_connection,
        if_exists='append',
        index=False)

    __export_sql_schema_to_txt(
        sql_server_cursor=sql_server_cursor,
        table_name=table_name)

    sql_server_connection.close()


def __get_sql_server_connection():
    driver = \
        '{SQL Server}'

    # Server=localhost\SQLEXPRESS01;Database=master;Trusted_Connection=True;
    sql_server_connection_string = \
        f'DRIVER={driver};Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;'

    # Establish the connection
    sql_server_connection = \
        pyodbc.connect(
            sql_server_connection_string)

    # Create a cursor from the connection
    sql_server_cursor = \
        sql_server_connection.cursor()

    return \
        sql_server_connection, \
        sql_server_cursor


def __export_sql_schema_to_txt(
        sql_server_cursor,
        table_name: str):
    type_mapping = \
        {
            'int64': 'BIGINT',
            'float64': 'FLOAT',
            'object': 'NVARCHAR(MAX)',
            'bool': 'BIT',
            'datetime64[ns]': 'DATETIME2',
            'int32': 'INT',
            'float32': 'REAL',
            'string': 'NVARCHAR(MAX)',
            # add more if necessary
        }

    query_statement = \
        f"EXEC sp_columns '{table_name}';"

    table_schema = \
        sql_server_cursor.execute(
            query_statement).description

    # Write the statement to a text file
    schema_file_name = \
        table_name + '_schema.txt'

    with open(schema_file_name, 'w') as file_writer:
        file_writer.write(
            str(table_schema))
