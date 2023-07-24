from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.a_startup.common.common_local_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_local_b_app_runner_standard_exemplar_configuration


def get_b_app_runner_standard_exemplar_configuration_agu() \
        -> EtlProcessesWrapperConfigurations:
    GlobalFlags.RUN_PROCESS_FILES = \
        True
    
    GlobalFlags.RUN_BIEIZE = \
        True
    
    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        True
    
    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True
    
    etl_processes_wrapper_configuration = \
        get_common_local_b_app_runner_standard_exemplar_configuration(
            drive_name='C',
            user_initials='AGu',
            configuration_json_relative_file_path=r'database_configuration v20230717 v0.05 AGu with _sql_sql_only.json',
            output_folder_prefix='database_configuration v20230717 v0.05 AGu with _sql_sql_only',
            output_folder_suffix=str(),
            main_wrapper_outputs_folder_name='main_wrapper_outputs')
    
    return \
        etl_processes_wrapper_configuration
