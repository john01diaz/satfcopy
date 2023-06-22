# TODO: Note - This is a translation to pandas of the creation of the view VW_Database_names found in:
#  -- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration
#  WARNING: This database_names dataframe should already been created and loaded in the main code - have a look
from pandas import DataFrame


def run_execution_databases_configuration_sql_00_00_sql():
    vw_database_names = \
        DataFrame({'Database_name': 'R_2016R3'.split(',')})

    return \
        vw_database_names
