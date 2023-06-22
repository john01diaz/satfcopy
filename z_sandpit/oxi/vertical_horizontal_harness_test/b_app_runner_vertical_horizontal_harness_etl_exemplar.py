from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper
from z_sandpit.oxi.vertical_horizontal_harness_test.user_configuration_constants_oxi import USER_OUTPUT_ROOT_FOLDER, \
    FULL_USER_FILE_CONFIGURATION_LIST, FULL_USER_PROCESS_CONFIGURATION_LIST, USER_OUTPUT_FOLDER_PREFIX

if __name__ == '__main__':
    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=FULL_USER_FILE_CONFIGURATION_LIST,
            process_configuration_list=FULL_USER_PROCESS_CONFIGURATION_LIST,
            run_new_vs_original_comparison=True)

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=USER_OUTPUT_FOLDER_PREFIX,
        output_root_folder=USER_OUTPUT_ROOT_FOLDER,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
