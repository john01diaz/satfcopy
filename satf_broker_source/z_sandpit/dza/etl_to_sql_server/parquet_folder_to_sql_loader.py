import csv
import glob
import os
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sqlalchemy import create_engine
import pandas
import pyodbc
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def load_parquet_folder_to_sql(
        input_root_folder: Folders,
        stage_name: str,
        username: str,
        password: str):
    # Read the parquet file
    input_root_folder_children = \
        glob.glob(
            input_root_folder.absolute_path_string + "**/*.snappy.parquet",
            recursive=True)

    parquet_file_path = \
        input_root_folder_children[0]

    parquet_folder_as_dataframe = \
        pandas.read_parquet(
            parquet_file_path)

    sql_server_connection, sql_server_cursor, engine = \
        __get_sql_server_connection(
            stage_name=stage_name,
            username=username,
            password=password)

    # Here we're assuming that the table doesn't exist yet, so we'll create it
    table_name = \
        input_root_folder.absolute_path_string.split(
            os.sep)[-1]

    # Generate CREATE TABLE statement
    columns = \
        ', '.join(f'[{col}] {dtype}' for col, dtype in zip(parquet_folder_as_dataframe.columns, parquet_folder_as_dataframe.dtypes.replace({
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'NVARCHAR(255)',
            'bool': 'BIT',
        })))

    delete_existing_table_query = \
        f"DROP TABLE IF EXISTS {table_name};"

    sql_query_statement = \
        f"CREATE TABLE {table_name} ({columns})"

    sql_server_cursor.execute(
        delete_existing_table_query)

    sql_server_cursor.execute(
        sql_query_statement)

    # Then, use to_sql to push the data to the new SQL table
    # parquet_folder_as_dataframe.to_sql(
    #     table_name,
    #     con=engine,
    #     if_exists='append',
    #     schema='dbo',
    #     index=False)

    __export_sql_schema_to_csv(
        sql_server_cursor=sql_server_cursor,
        table_name=table_name)

    sql_server_connection.close()


def __get_sql_server_connection(
        stage_name: str,
        username: str,
        password: str):
    driver = \
        r'{SQL Server}'

    server_name = \
        r'localhost\SQLEXPRESS01'

    database_name = \
        stage_name

    # sql_server_connection_string = \
    #     f'DRIVER={driver};SERVER={server_name};Database={database_name};UID={username};PWD={password}'

    __create_database(
        database_name=database_name,
        driver=driver,
        server_name=server_name)

    sql_server_connection_string = \
        f'DRIVER={driver};Server={server_name};Database={database_name};Trusted_Connection=True;'

    sql_server_connection = \
        pyodbc.connect(
            sql_server_connection_string)

    sql_server_connection.autocommit = \
        True

    sql_server_cursor = \
        sql_server_connection.cursor()

    engine = \
        create_engine(
            f'mssql+pyodbc:///?odbc_connect={sql_server_connection_string}',
            fast_executemany=True)

    sql_server_cursor.setinputsizes([(pyodbc.SQL_WVARCHAR, 0, 0)])

    return \
        sql_server_connection, \
        sql_server_cursor, \
        engine


def __create_database(
        database_name: str,
        driver: str,
        server_name: str) \
        -> None:
    sql_server_connection_string = \
        f'DRIVER={driver};Server={server_name};Trusted_Connection=True;'

    sql_connection = \
        pyodbc.connect(
            sql_server_connection_string)

    sql_connection.autocommit = \
        True

    cursor = \
        sql_connection.cursor()

    query = \
        "SELECT COUNT(*) FROM sys.databases WHERE name = ?"

    cursor.execute(
        query,
        database_name)

    exists = \
        cursor.fetchone()[0]

    if exists != 1:
        create_db_query = \
            f"CREATE DATABASE {database_name};"

        sql_connection.execute(
            create_db_query)

        print(
            f"The database '{database_name}' has been created.")

    cursor.close()

    sql_connection.close()


def __export_sql_schema_to_csv(
        sql_server_cursor,
        table_name: str):
    output_folder = \
        Folders(
            absolute_path_string=os.path.join(LogFiles.folder_path))

    query_statement = \
        f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"

    sql_server_cursor.execute(
        query_statement)

    table_column_values = \
        sql_server_cursor.fetchall()

    column_names = \
        [column[0] for column in sql_server_cursor.description]

    list_of_rows = \
        [list(row) for row in table_column_values]

    dataframe = \
        pandas.DataFrame(
            data=list_of_rows,
            columns=column_names)

    schema_file_path = \
        output_folder.absolute_path_string + os.sep + table_name + '_schema.csv'

    dataframe.to_csv(
        schema_file_path,
        sep=',',
        quotechar='"',
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar='\\')
