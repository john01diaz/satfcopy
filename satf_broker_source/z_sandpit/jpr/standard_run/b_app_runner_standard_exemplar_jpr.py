from satf_broker_source.z_sandpit.jpr.standard_run.b_app_runner_standard_exemplar_configuration_getter_jpr_1_3 import \
    get_b_app_runner_standard_exemplar_configuration_jpr
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import orchestrate_etl_processes_wrapper


if __name__ == '__main__':
    etl_processes_wrapper_configuration = \
        get_b_app_runner_standard_exemplar_configuration_jpr()
    
    run_b_app(
            app_startup_method=orchestrate_etl_processes_wrapper,
            environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
            output_folder_prefix=etl_processes_wrapper_configuration.output_folder_prefix,
            output_folder_suffix=etl_processes_wrapper_configuration.output_folder_suffix,
            output_root_folder=etl_processes_wrapper_configuration.output_root_folder,
            etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
