from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


@run_and_log_function
def process_files(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses,
        processes: list) \
        -> None:
    for process in processes:
        process(
            etl_processes_wrapper_universe=etl_processes_wrapper_universe)
