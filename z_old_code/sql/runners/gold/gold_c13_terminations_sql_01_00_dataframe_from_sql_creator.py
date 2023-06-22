import os
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.common_spark_sql_runner import \
    run_common_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.sql_base_scripts_folder_path_getter import \
    get_sql_base_scripts_folder_path


# TODO: Beware this uses pandasql which is standard SQL, the ETL scripts use Spark SQL which can contain instructions
#  out of the SQL standard - Do the same thing but using a Spark SQL library similar to pandasql
def create_dataframe_gold_c13_terminations_sql_01_00_from_sql(
        input_tables: dict) \
        -> DataFrame:
    # move into JSON as the next step
    script_file_path = \
        os.path.join(
            get_sql_base_scripts_folder_path(),
            'gold',
            '13_Terminations_sql_01_00.sql')

    sql_table_names_mapping_dictionary = \
        {
            'Sigraph_Silver.S_Terminations': 's_terminations',
            'VW_Database_names': 'database_names'
        }

    output_dataframe = \
        run_common_spark_sql(
            sql_script_file_path=script_file_path,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary,
            input_tables=input_tables)

    return \
        output_dataframe
