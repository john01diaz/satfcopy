import os
from subprocess import check_output, CalledProcessError

import pandas as pd
import pyarrow.parquet as pq
from pandas import DataFrame
from pandasql import sqldf


# TODO: Beware this uses pandasql which is standard SQL, the ETL scripts use Spark SQL which can contain instructions
#  out of the SQL standard - Do the same thing but using a Spark SQL library similar to pandasql
def run_sql_query(
        input_tables: dict,
        sql_script_file_path: str) \
        -> DataFrame:
    individual_parquet_file_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_IO_Allocations\part-00000-745b42a2-df72-4c67-9bcc-49a7a542627b-c000.snappy.parquet'

    s_io_allocations = \
        pq.read_table(
            individual_parquet_file_path).to_pandas()

    s_io_allocations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=s_io_allocations)

    # TODO: Note - This is a translation to pandas of the creation of the view VW_Database_names found in:
    #  -- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration
    #  WARNING: This database_names dataframe should already been created and loaded in the main code - have a look
    vw_database_names = \
        pd.DataFrame({'Database_name': 'R_2016R3'.split(',')})

    vw_database_names_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=vw_database_names)

    base_sql_script_as_string = \
        read_file_content_as_string(
            file_path=sql_script_file_path)

    # TODO: In the query below the original FROM table has been changed from: Sigraph_Silver.S_IO_Allocations
    #  to: s_io_allocations (name of the imported dataframe from the parquet)
    #  Another change: the table names need to be converted to lowercase to match with the dataframe names
    renamed_sql_script_as_string = \
        refactor_sql_script(
            base_sql_script_as_string=base_sql_script_as_string,
            sql_table_names_mapping_dictionary={
                'Sigraph_Silver.S_IO_Allocations': 's_io_allocations',
                'VW_Database_names': 'vw_database_names'})

    result = \
        sqldf(
            renamed_sql_script_as_string,
            locals())

    # TODO: Need to translate this into SQL CREATE table
    result_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=result)

    return \
        result


def refactor_sql_script(
        base_sql_script_as_string: str,
        sql_table_names_mapping_dictionary: dict) \
        -> str:
    renamed_sql_script_as_string = \
        base_sql_script_as_string

    for original_table_name, refactored_table_name \
            in sql_table_names_mapping_dictionary.items():
        renamed_sql_script_as_string = \
            renamed_sql_script_as_string.replace(
                original_table_name,
                refactored_table_name)

    return \
        renamed_sql_script_as_string


def read_file_content_as_string(
        file_path: str) \
        -> str:
    with open(file_path, 'r') as current_file:
        file_content_as_string = \
            current_file.read()

    return \
        file_content_as_string


# TODO: Create a class BSqlSchemas with a dictionary and a method to return/export the SQL text file
def get_dataframe_schema_as_dictionary(
        dataframe: DataFrame) \
        -> dict:
    dataframe_schema = \
        dataframe.dtypes.apply(
            lambda x: x.name).to_dict()

    return \
        dataframe_schema


def get_git_root_path() \
        -> str:
    try:
        base = \
            check_output(
                'git rev-parse --show-toplevel',
                shell=True)

    except CalledProcessError:
        raise \
            IOError(
                'Current working directory is not a git repository')

    git_root_path = \
        base.decode('utf-8').strip()

    return \
        git_root_path


# TODO: Below is just for testing  #########################################################
sql_script_file_path = \
    os.path.join(
        get_git_root_path(),
        r'sat_workflow_source/gpt4_code/input_sql/gold_stage/15_IO_Allocations_sql_01_00.sql')

    # r'C:\OXi\PythonDev\code_boro_nf\etl_exemplar\sat_workflow_source\z_sandpit\oxi\sql_running\test_data\15_IO_Allocations_de_magic.sql'


result_dataframe = \
    run_sql_query(
        input_tables=dict(),
        sql_script_file_path=sql_script_file_path)

pass