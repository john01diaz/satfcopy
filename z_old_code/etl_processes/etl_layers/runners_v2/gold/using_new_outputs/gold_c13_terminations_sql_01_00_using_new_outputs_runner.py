from z_old_code.etl_processes.etl_layers.runners_v2.gold.gold_c13_terminations_sql_01_00_runner import \
    run_gold_c13_terminations_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_gold_c13_terminations_sql_01_00_using_new_outputs(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    run_gold_c13_terminations_sql_01_00(
        etl_processes_wrapper_universe=etl_processes_wrapper_universe,
        s_terminations_dataframe_source='new_output')
    