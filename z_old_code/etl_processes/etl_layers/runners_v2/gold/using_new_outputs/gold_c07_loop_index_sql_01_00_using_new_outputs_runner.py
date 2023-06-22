from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from z_old_code.etl_processes.etl_layers.runners_v2.gold.gold_c07_loop_index_sql_01_00_v0_02_runner \
    import run_gold_c07_loop_index_sql_01_00_v0_02
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


@run_and_log_function
def run_gold_c07_loop_index_sql_01_00_using_new_outputs(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    run_gold_c07_loop_index_sql_01_00_v0_02(
        etl_processes_wrapper_universe=etl_processes_wrapper_universe,
        s_loop_index_source='new_output')
