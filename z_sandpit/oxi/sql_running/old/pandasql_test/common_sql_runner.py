from pandas import DataFrame
from pandasql import sqldf

from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.helpers import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.dataframe_schema_as_dictionary_getter import \
    get_dataframe_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.file_content_as_string_reader import \
    read_file_content_as_string
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.tables_in_sql_script_renamer import \
    rename_tables_in_sql_script


# TODO: Beware this uses pandasql which is standard SQL, the ETL scripts use Spark SQL which can contain instructions
#  out of the SQL standard - Do the same thing but using a Spark SQL library similar to pandasql
def run_common_sql(
        sql_script_file_path: str,
        sql_table_names_mapping_dictionary,
        env) \
        -> DataFrame:
    base_sql_script_as_string = \
        read_file_content_as_string(
            file_path=sql_script_file_path)

    renamed_sql_script_as_string = \
        rename_tables_in_sql_script(
            base_sql_script_as_string=base_sql_script_as_string,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary)

    output_dataframe = \
        sqldf(
            renamed_sql_script_as_string,
            env)

    rename_dataframe_columns_to_lowercase(
        dataframe=output_dataframe)

    # TODO: Need to translate this into SQL CREATE table
    output_dataframe_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=output_dataframe)

    return \
        output_dataframe
