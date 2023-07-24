from pandas import DataFrame

from satf_broker_source.z_sandpit.oxi.sql_running.old.pandasql_test.common_sql_runner import run_common_sql


# TODO: Beware this uses pandasql which is standard SQL, the ETL scripts use Spark SQL which can contain instructions
#  out of the SQL standard - Do the same thing but using a Spark SQL library similar to pandasql
def run_gold_13_terminations_sql_01_00_sql(
        input_tables: dict,
        vw_database_names: DataFrame,
        sql_script_file_path: str) \
        -> DataFrame:
    s_terminations = \
        input_tables['s_terminations']

    sql_table_names_mapping_dictionary = \
        {
            'Sigraph_Silver.S_Terminations': 's_terminations',
            'VW_Database_names': 'vw_database_names'
        }

    g_terminations = \
        run_common_sql(
            sql_script_file_path=sql_script_file_path,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary,
            env=locals())
    
    return \
        g_terminations
