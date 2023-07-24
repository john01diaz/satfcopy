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
def run_gold_15_io_allocations_sql_01_00_sql(
        input_tables: dict,
        vw_database_names: DataFrame,
        sql_script_file_path: str) \
        -> DataFrame:
    s_io_allocations = \
        __get_parquet_file_dataframe()

    base_sql_script_as_string = \
        read_file_content_as_string(
            file_path=sql_script_file_path)

    # TODO: In the query below the original FROM table has been changed from: Sigraph_Silver.S_IO_Allocations
    #  to: s_io_allocations (name of the imported dataframe from the parquet)
    #  Another change: the table names need to be converted to lowercase to match with the dataframe names
    renamed_sql_script_as_string = \
        rename_tables_in_sql_script(
            base_sql_script_as_string=base_sql_script_as_string,
            sql_table_names_mapping_dictionary={
                'Sigraph_Silver.S_IO_Allocations': 's_io_allocations',
                'VW_Database_names': 'vw_database_names'})

    g_io_allocations = \
        sqldf(
            renamed_sql_script_as_string,
            locals())

    rename_dataframe_columns_to_lowercase(
        dataframe=g_io_allocations)

    # TODO: Need to translate this into SQL CREATE table
    g_io_allocations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=g_io_allocations)

    return \
        g_io_allocations


def __get_parquet_file_dataframe():
    # TODO: Read all parquet files in parquet folder as dataframe
    individual_parquet_file_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_IO_Allocations\part-00000-745b42a2-df72-4c67-9bcc-49a7a542627b-c000.snappy.parquet'

    io_allocations_parquet_schema = \
        get_parquet_file_schema_as_dictionary(
            parquet_file_path=individual_parquet_file_path)

    io_allocations = \
        pq.read_table(
            individual_parquet_file_path).to_pandas()

    rename_dataframe_columns_to_lowercase(
        dataframe=io_allocations)

    io_allocations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=io_allocations)

    return \
        io_allocations
