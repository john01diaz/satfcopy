from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


@run_and_log_function
def orchestrate_etl_processes_wrapper(
        etl_processes_wrapper_configuration: EtlProcessesWrapperConfigurations) \
        -> None:
    etl_processes_wrapper_universe = \
        EtlProcessesWrapperUniverses(
            etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)

    if GlobalFlags.RUN_PROCESS_FILES:
        etl_processes_wrapper_universe.process_files()

    if GlobalFlags.RUN_BIEIZE:
        etl_processes_wrapper_universe.bieize()

    etl_processes_wrapper_universe.export_report_files()
