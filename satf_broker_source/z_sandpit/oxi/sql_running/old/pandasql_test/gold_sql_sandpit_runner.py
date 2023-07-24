import os

from z_old_code.sql import \
    run_execution_databases_configuration_sql_00_00_sql
from satf_broker_source.z_sandpit.oxi.sql_running.old.pandasql_test.gold_13_terminations_sql_01_00_sql_runner import \
    run_gold_13_terminations_sql_01_00_sql
from satf_broker_source.z_sandpit.oxi.sql_running.old.pandasql_test.gold_15_io_allocations_sql_01_00_sql_runner import \
    run_gold_15_io_allocations_sql_01_00_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path

if __name__ == '__main__':
    vw_database_names = \
        run_execution_databases_configuration_sql_00_00_sql()

    sql_13_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/gold/13_Terminations_sql_01_00.sql')

    g_terminations = \
        run_gold_13_terminations_sql_01_00_sql(
            input_tables=dict(),
            vw_database_names=vw_database_names,
            sql_script_file_path=sql_13_script_file_path)

    sql_15_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/gold/15_IO_Allocations_sql_01_00.sql')

    g_io_allocations = \
        run_gold_15_io_allocations_sql_01_00_sql(
            input_tables=dict(),
            vw_database_names=vw_database_names,
            sql_script_file_path=sql_15_script_file_path)

    pass
