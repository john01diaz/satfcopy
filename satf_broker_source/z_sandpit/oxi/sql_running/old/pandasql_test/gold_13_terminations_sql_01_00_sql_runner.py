import pyarrow.parquet as pq
from pandas import DataFrame
from pandasql import sqldf
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.helpers import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.dataframe_schema_as_dictionary_getter import \
    get_dataframe_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.file_content_as_string_reader import \
    read_file_content_as_string
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.parquet_file_schema_as_dictionary_getter import \
    get_parquet_file_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.tables_in_sql_script_renamer import \
    rename_tables_in_sql_script


# TODO: Beware this uses pandasql which is standard SQL, the ETL scripts use Spark SQL which can contain instructions
#  out of the SQL standard - Do the same thing but using a Spark SQL library similar to pandasql
def run_gold_13_terminations_sql_01_00_sql(
        input_tables: dict,
        vw_database_names: DataFrame,
        sql_script_file_path: str) \
        -> DataFrame:
    s_terminations = \
        __get_parquet_file_dataframe()

    base_sql_script_as_string = \
        read_file_content_as_string(
            file_path=sql_script_file_path)

    renamed_sql_script_as_string = \
        rename_tables_in_sql_script(
            base_sql_script_as_string=base_sql_script_as_string,
            sql_table_names_mapping_dictionary={
                'Sigraph_Silver.S_Terminations': 's_terminations',
                'VW_Database_names': 'vw_database_names'})

    g_terminations = \
        sqldf(
            renamed_sql_script_as_string,
            locals())

    rename_dataframe_columns_to_lowercase(
        dataframe=g_terminations)

    # TODO: Need to translate this into SQL CREATE table
    g_terminations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=g_terminations)

    return \
        g_terminations


def __get_parquet_file_dataframe():
    # TODO: Read all parquet files in parquet folder as dataframe - use the code already developed
    individual_parquet_file_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_Terminations\part-00000-cc8ceacf-5163-47e2-abfa-a7e3d9ff1a7e-c000.snappy.parquet'

    s_terminations_parquet_schema = \
        get_parquet_file_schema_as_dictionary(
            parquet_file_path=individual_parquet_file_path)

    s_terminations = \
        pq.read_table(
            individual_parquet_file_path).to_pandas()

    rename_dataframe_columns_to_lowercase(
        dataframe=s_terminations)

    s_terminations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=s_terminations)

    return \
        s_terminations
