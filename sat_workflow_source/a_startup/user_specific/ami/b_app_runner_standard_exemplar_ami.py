from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import orchestrate_etl_processes_wrapper
from sat_workflow_source.a_startup.user_specific.ami.b_app_runner_standard_exemplar_configuration_getter_ami import get_sat_workflow_b_app_runner_configuration_ami


if __name__ == '__main__':
    etl_processes_wrapper_configuration = \
        get_sat_workflow_b_app_runner_configuration_ami()
    
    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=etl_processes_wrapper_configuration.output_folder_prefix,
        output_folder_suffix=etl_processes_wrapper_configuration.output_folder_suffix,
        output_root_folder=etl_processes_wrapper_configuration.output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
